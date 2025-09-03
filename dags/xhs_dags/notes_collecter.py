#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime, timedelta
import re 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
from appium.webdriver.common.appiumby import AppiumBy
import base64
import requests
from utils.xhs_appium import XHSOperator


def save_notes_to_db(notes: list) -> None:
    """
    保存笔记到数据库(如果表不存在，则初始新建该表)
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    try:
        # 检查表是否存在，如果不存在则创建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS xhs_notes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            keyword TEXT,
            title TEXT NOT NULL,
            author TEXT,
            userInfo TEXT,
            content TEXT,
            likes INT DEFAULT 0,
            collects INT DEFAULT 0,
            comments INT DEFAULT 0,
            note_url VARCHAR(512) DEFAULT NULL,
            collect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note_type TEXT,
            note_location TEXT 
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句 - 使用INSERT IGNORE避免重复插入
        insert_sql = """
        INSERT IGNORE INTO xhs_notes
        (keyword, title, author, userInfo, content, likes, collects, comments, note_url, collect_time, note_time, note_type, note_location)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 批量插入笔记数据
        insert_data = []
        for note in notes:
            # 获取URL - 优先使用note_url，如果不存在则尝试使用video_url
            note_url = note.get('note_url', note.get('video_url', ''))
            
            insert_data.append((
                note.get('keyword', ''),
                note.get('title', ''),
                note.get('author', ''),
                note.get('userInfo', ''),
                note.get('content', ''),
                note.get('likes', 0),
                note.get('collects', 0),
                note.get('comments', 0),
                note_url.replace(' 复制本条信息','').strip(),  # 使用统一处理后的URL
                note.get('collect_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                note.get('note_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                note.get('note_type', ''),
                note.get('note_location', ''),
            ))

        # 执行插入操作
        cursor.executemany(insert_sql, insert_data)

        # 获取实际插入的记录数
        cursor.execute("SELECT ROW_COUNT()")
        affected_rows = cursor.fetchone()[0]
        
        db_conn.commit()
        
        print(f"成功保存 {affected_rows} 条新笔记到数据库，跳过 {len(notes) - affected_rows} 条重复笔记")
        
    except Exception as e:
        db_conn.rollback()
        print(f"保存笔记到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()

def get_assigned_keyword(device_index, keywords_list, total_devices):
    """
    根据设备索引和关键词列表，为设备分配关键词
    Args:
        device_index: 设备索引
        keywords_list: 关键词列表
        total_devices: 总设备数量
    Returns:
        分配给该设备的关键词，如果没有分配则返回None
    """
    if not keywords_list:
        return None
    
    num_keywords = len(keywords_list)
    
    if num_keywords >= total_devices:
        # 关键词数量 >= 设备数量：每个设备分配一个关键词
        if device_index < num_keywords:
            return keywords_list[device_index]
        else:
            return None  # 超出关键词数量的设备不执行任务
    else:
        # 关键词数量 < 设备数量：平均分配设备到关键词
        devices_per_keyword = total_devices // num_keywords
        remaining_devices = total_devices % num_keywords
        
        current_device = 0
        for i, keyword in enumerate(keywords_list):
            # 前面的关键词可能会分配到额外的设备
            devices_for_this_keyword = devices_per_keyword + (1 if i < remaining_devices else 0)
            
            if current_device <= device_index < current_device + devices_for_this_keyword:
                return keyword
            
            current_device += devices_for_this_keyword
        
        return None


def collect_xhs_notes(device_index=0, **context) -> None:
    """
    收集小红书笔记
    Args:
        device_index: 设备索引
        **context: Airflow上下文参数字典
            - keyword: 搜索关键词（单个关键词，兼容旧版本）
            - keywords: 搜索关键词列表（多个关键词，新功能）
            - max_notes: 最大收集笔记数量
            - email: 用户邮箱（可选，如果指定了specific_device_id则忽略此参数）
            - specific_device_id: 指定的设备ID（可选，优先级高于email）
            - note_type: 笔记类型，可选值为 '图文' 或 '视频'，默认为 '图文'
    
    Returns:
        None
    """
    # 获取输入参数
    single_keyword = context['dag_run'].conf.get('keyword')  # 单个关键词（兼容旧版本）
    keywords_list = context['dag_run'].conf.get('keywords', [])  # 关键词列表（新功能）
    max_notes = int(context['dag_run'].conf.get('max_notes'))
    email = context['dag_run'].conf.get('email')
    specific_device_id = context['dag_run'].conf.get('specific_device_id')  # 指定设备ID
    note_type = context['dag_run'].conf.get('note_type')  # 默认为图文类型
    time_range = context['dag_run'].conf.get('time_range')
    search_scope=context['dag_run'].conf.get('search_scope')
    sort_by=context['dag_run'].conf.get('sort_by')
    
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 处理关键词：优先使用keywords列表，如果为空则使用单个keyword
    if keywords_list:
        # 多关键词模式：根据设备索引分配关键词
        total_devices = len(device_info_list)  # 动态获取实际设备数量
        assigned_keyword = get_assigned_keyword(device_index, keywords_list, total_devices)
        
        if not assigned_keyword:
            print(f"设备索引 {device_index} 未分配到关键词，跳过任务")
            raise AirflowSkipException(f"设备索引 {device_index} 未分配到关键词")
        
        keyword = assigned_keyword
        print(f"设备索引 {device_index} 分配到关键词: {keyword}")
    elif single_keyword:
        # 单关键词模式（兼容旧版本）
        keyword = single_keyword
        print(f"使用单关键词模式: {keyword}")
    else:
        raise ValueError("必须提供keyword或keywords参数")

    # 设备选择逻辑：优先使用specific_device_id，其次使用email
    if specific_device_id:
        # 使用指定的设备ID查找设备信息
        device_info = None
        selected_device_index = None
        
        for device in device_info_list:
            phone_device_list = device.get('phone_device_list', [])
            if specific_device_id in phone_device_list:
                device_info = device
                selected_device_index = phone_device_list.index(specific_device_id)
                break
        
        if not device_info:
            raise ValueError(f"未找到指定的设备ID: {specific_device_id}")
        
        print(f"使用指定设备ID: {specific_device_id}")
    else:
        # 使用email查找设备信息
        if not email:
            raise ValueError("必须提供email或specific_device_id参数")
        
        device_info = next((device for device in device_info_list if device.get('email') == email), None)
        if not device_info:
            raise ValueError(f"未找到email对应的设备信息: {email}")
        
        selected_device_index = device_index
        print(f"使用email查找设备: {email}")
    
    print(f"device_info: {device_info}")
    
    # 创建数据库连接信息，用于process_note中检查笔记是否存在
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    
    # 获取设备信息
    try:
        device_ip = device_info.get('device_ip')
        appium_port = device_info.get('available_appium_ports')[selected_device_index]
        actual_device_id = device_info.get('phone_device_list')[selected_device_index]
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")

    # 获取appium_server_url
    appium_server_url = f"http://{device_ip}:{appium_port}"
    
    print(f"选择设备 {actual_device_id}, appium_server_url: {appium_server_url}")
    print(f"开始收集关键词 '{keyword}' 的小红书笔记... ，数量为'{max_notes}'")
    
    xhs = None
    try:
        # 初始化小红书操作器（带重试机制）
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=actual_device_id)
        # 用于每收集三条笔记保存一次的工具函数
        batch_size = 3  # 每批次保存的笔记数量
        collected_notes = []  # 所有收集到的笔记
        current_batch = []  # 当前批次的笔记
        
        # 定义处理笔记的回调函数
        def process_note(note):
            nonlocal collected_notes, current_batch
            
            # 添加email信息到userInfo字段
            note['userInfo'] = email
            note['note_type'] = note_type
            
            # 检查笔记URL是否已存在（处理不同类型笔记的URL字段）
            # 图文笔记使用note_url，视频笔记使用video_url
            note_url = note.get('note_url', note.get('video_url', ''))
            
            # 如果是视频笔记，将video_url复制到note_url字段以便统一处理
            if 'video_url' in note and not 'note_url' in note:
                note['note_url'] = note['video_url']
            
            if note_url:
                # 从URL中提取笔记ID
                note_id = None
                # 匹配URL中的笔记ID部分（24位十六进制字符）
                match = re.search(r'/item/([a-f0-9]{24})', note_url)
                if match:
                    note_id = match.group(1)
                
                if note_id:
                    # 直接查询数据库，使用笔记ID去重
                    db_conn = db_hook.get_conn()
                    cursor = db_conn.cursor()
                    try:
                        #去重逻辑：根据笔记ID判断是否已存在
                        cursor.execute("SELECT 1 FROM xhs_notes WHERE note_url LIKE %s  AND keyword = %s LIMIT 1", (f'%{note_id}%',keyword))
                        if cursor.fetchone():
                            print(f"笔记ID {note_id} 已存在，跳过: {note.get('title', '')}")
                            return
                        else:
                            print(f"笔记ID {note_id} 不存在，添加: {note.get('title', '')},{note.get('keyword', '')},{note_url}, email: {email}")
                    finally:
                        cursor.close()
                        db_conn.close()
                else:
                    print(f"无法从URL中提取笔记ID: {note_url}")
            
            collected_notes.append(note)
            current_batch.append(note)
            
            # 当收集到3条笔记时保存到数据库
            if len(current_batch) >= batch_size:
                print(f"保存批次数据到数据库，当前批次包含 {len(current_batch)} 条笔记")
                save_notes_to_db(current_batch)
                current_batch = []  # 清空当前批次
        
        # 搜索关键词，并且开始收集
        print(f"搜索关键词: {keyword}, 笔记类型: {note_type}")
        collected_titles = []
        
        if note_type == '视频':
            # 使用视频搜索方法
            print(f"使用视频搜索方法搜索关键词: {keyword}")
            # search_keyword_of_video 方法内部已经处理了视频的收集和处理
            # 该方法会返回收集到的视频列表
            print(f"开始收集视频笔记,计划收集{max_notes}条...")
            collected_videos = xhs.search_keyword_of_video(keyword, max_videos=max_notes)
            
            # 处理收集到的视频数据
            if collected_videos:
                for video in collected_videos:
                    # 添加关键词信息
                    video['keyword'] = keyword
                    # 使用相同的处理函数处理视频数据
                    process_note(video)
            else:
                print(f"未找到关于 '{keyword}' 的视频笔记")
                
        else:
            # 使用默认搜索方法（图文）
            print(f"使用图文搜索方法搜索关键词: {keyword}")
            xhs.search_keyword(keyword, filters={
                "note_type": note_type,
                "time_range":time_range,
                "search_scope":search_scope,
                "sort_by":sort_by
            })
            initial_scroll_count = device_index * 3
            print(f"设备索引 {device_index}，执行初始滑动 {initial_scroll_count} 次")
            for i in range(initial_scroll_count):
                xhs.scroll_down()
                time.sleep(0.5)  # 短暂等待
            print(f"初始滑动完成，共滑动 {initial_scroll_count} 次")
            print(f"开始收集图文笔记,计划收集{max_notes}条...")
            print("---------------card----------------")
            xhs.print_all_elements()
            
            # 对于图文笔记，使用 get_note_card 函数收集
            get_note_card(xhs, collected_notes, collected_titles, max_notes, process_note, keyword)

        
        # 如果还有未保存的笔记，保存剩余的笔记
        if current_batch:
            print(f"保存剩余 {len(current_batch)} 条笔记到数据库")
            save_notes_to_db(current_batch)
        
        if not collected_notes:
            print(f"未找到关于 '{keyword}' 的笔记")
            return
            
        # 打印收集结果
        print("\n收集完成!")
        print(f"共收集到 {len(collected_notes)} 条笔记")
        
        # 提取笔记URL列表并存入XCom
        note_urls = [note.get('note_url', '') for note in collected_notes]
        context['ti'].xcom_push(key='note_urls', value=note_urls)
        context['ti'].xcom_push(key='keyword', value=keyword)
        
        return note_urls
            
    except Exception as e:
        error_msg = f"收集小红书笔记失败: {str(e)}"
        print(error_msg)
        raise
    finally:
        # 确保关闭小红书
        if xhs:
            xhs.close()

def get_note_card(xhs, collected_notes, collected_titles, max_notes, process_note, keyword):
    """
    收集小红书笔记卡片
    """
    import time
    from appium.webdriver.common.appiumby import AppiumBy
    while len(collected_notes) < max_notes:
        try:
            print("获取所有笔记卡片元素")
            note_cards = []
            try:
                # 只保留原始方法
                note_cards = xhs.driver.find_elements(
                    by=AppiumBy.XPATH,
                    value="//android.widget.FrameLayout[@resource-id='com.xingin.xhs:id/-' and @clickable='true']"
                )
                print(f"获取笔记卡片成功，共{len(note_cards)}个")
            except Exception as e:
                print(f"获取笔记卡片失败: {e}")
            for note_card in note_cards:
                if len(collected_notes) >= max_notes:
                    break
                try:
                    title_element = note_card.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView[contains(@text, '')]"
                    )
                    note_title_and_text = title_element.text
                    author_element = note_card.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.LinearLayout/android.widget.TextView[1]"
                    )
                    author = author_element.text
                    if note_title_and_text not in collected_titles:
                        print(f"收集笔记: {note_title_and_text}, 作者: {author}, 当前收集数量: {len(collected_notes)}")
                        
                        # 获取屏幕尺寸和元素位置
                        try:
                            screen_size = xhs.driver.get_window_size()
                            element_location = title_element.location
                            screen_height = screen_size['height']
                            element_y = element_location['y']
                            
                            # 检查元素是否位于屏幕高度的3/4以上
                            if element_y > screen_height * 0.25:
                                # 点击标题元素而不是整个卡片
                                print(f"元素位置正常，位于屏幕{element_y/screen_height:.2%}处，执行点击")
                                title_element.click()
                                
                                # 检测是否出现升级提示，如果有则点击"知道了"
                                try:
                                    upgrade_prompt = xhs.driver.find_elements(
                                        by=AppiumBy.XPATH,
                                        value="//android.widget.TextView[@resource-id='com.xingin.xhs:id/-' and @text='需要升级应用才能查看此内容，请更新到最新版本']"
                                    )
                                    if upgrade_prompt:
                                        print("检测到升级提示，点击知道了按钮")
                                        know_button = xhs.driver.find_element(
                                            by=AppiumBy.XPATH,
                                            value="//android.widget.TextView[@resource-id='com.xingin.xhs:id/-' and @text='知道了']"
                                        )
                                        know_button.click()
                                        time.sleep(0.5)
                                except Exception as e:
                                    print(f"未检测到版本升级提示: {e}")
                                
                                time.sleep(0.5)
                            else:
                                print(f"元素位置过高，位于屏幕{element_y/screen_height:.2%}处，跳过点击")
                                continue
                        except Exception as e:
                            error_msg = str(e)
                            print(f"检测元素位置失败: {error_msg}")

                            # 默认点击标题元素
                            # title_element.click()
                            time.sleep(0.5)
                        note_data = xhs.get_note_data(note_title_and_text)
                        # time.sleep(0.5)
                        # xhs.bypass_share()
                        if note_data:
                            note_data['keyword'] = keyword
                            collected_titles.append(note_title_and_text)
                            process_note(note_data)
                        back_btn = xhs.driver.find_element( by=AppiumBy.XPATH,
                        value="//android.widget.Button[@content-desc='返回']")
                        back_btn.click()
                        time.sleep(0.5)
                except Exception as e:
                    print(f"处理笔记卡片失败: {str(e)}")
                    continue
            if len(collected_notes) < max_notes:
                xhs.scroll_down()
                time.sleep(0.5)
        except Exception as e:
            print(f"收集笔记失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            break

with DAG(
    dag_id='notes_collector',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='定时收集小红书笔记 (支持图文和视频)',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=15,
    max_active_tasks=15,
) as dag:

    for index in range(15):
        PythonOperator(
            task_id=f'collect_xhs_notes_{index}',
            python_callable=collect_xhs_notes,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True,
            retries=3,
            retry_delay=timedelta(seconds=10)
        
        )
