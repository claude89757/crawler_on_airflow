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
                note_url,  # 使用统一处理后的URL
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

def collect_xhs_notes(device_index=0, **context) -> None:
    """
    收集小红书笔记
    Args:
        device_index: 设备索引
        **context: Airflow上下文参数字典
            - keyword: 搜索关键词
            - max_notes: 最大收集笔记数量
            - email: 用户邮箱
            - note_type: 笔记类型，可选值为 '图文' 或 '视频'，默认为 '图文'
    
    Returns:
        None
    """
    # 获取输入参数
    keyword = context['dag_run'].conf.get('keyword') 
    max_notes = int(context['dag_run'].conf.get('max_notes'))
    email = context['dag_run'].conf.get('email')
    note_type = context['dag_run'].conf.get('note_type')  # 默认为图文类型
    time_range = context['dag_run'].conf.get('time_range')
    search_scope=context['dag_run'].conf.get('search_scope')
    sort_by=context['dag_run'].conf.get('sort_by')

    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 根据email查找设备信息
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if device_info:
        print(f"device_info: {device_info}")
    else:
        raise ValueError("email参数不能为空")
    
    # 创建数据库连接信息，用于process_note中检查笔记是否存在
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    
    # 获取设备信息
    try:
        device_ip = device_info.get('device_ip')
        appium_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")

    # 获取appium_server_url
    appium_server_url = f"http://{device_ip}:{appium_port}"
    
    print(f"选择设备 {device_id}, appium_server_url: {appium_server_url}")
    print(f"开始收集关键词 '{keyword}' 的小红书笔记... ，数量为'{max_notes}'")
    
    xhs = None
    try:
        # 初始化小红书操作器（带重试机制）
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        
    
        
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
                # 直接查询数据库
                db_conn = db_hook.get_conn()
                cursor = db_conn.cursor()
                try:
                    cursor.execute("SELECT 1 FROM xhs_notes WHERE note_url = %s AND keyword = %s LIMIT 1", (note_url, keyword))
                    if cursor.fetchone():
                        print(f"笔记已存在，跳过: {note.get('title', '')}")
                        return
                    else:
                        print(f"笔记不存在，添加: {note.get('title', '')},{note.get('keyword', '')},{note_url}, email: {email}")
                finally:
                    cursor.close()
                    db_conn.close()
            
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
            
            print(f"开始收集图文笔记,计划收集{max_notes}条...")
            print("---------------card----------------")
            xhs.print_all_elements()
            
            # 对于图文笔记，使用 get_note_card_init 函数收集
            get_note_card_init(xhs, collected_notes, collected_titles, max_notes, process_note, keyword)

        
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
        # 确保关闭小红书操作器
        if xhs:
            xhs.close()


def get_note_card_init(xhs, collected_notes, collected_titles, max_notes, process_note, keyword):
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
    max_active_runs=5,
) as dag:

    for index in range(10):
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
