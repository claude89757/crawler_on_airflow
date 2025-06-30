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

def browse_xhs_notes(device_index=0, **context) -> None:
    """
    浏览小红书笔记
    Args:
        device_index: 设备索引
        **context: Airflow上下文参数字典
            - keyword: 搜索关键词
            - max_notes: 最大浏览笔记数量
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
    print(f"开始浏览关键词 '{keyword}' 的小红书笔记... ，数量为'{max_notes}'")
    
    xhs = None
    try:
        # 初始化小红书操作器（带重试机制）
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
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
            
            print(f"开始浏览图文笔记,计划浏览{max_notes}条...")
            # 对于图文笔记，使用 get_note_card_init 函数收集
            get_note_card(xhs, collected_notes, collected_titles, max_notes, process_note, keyword)

        if not collected_notes:
            print(f"未找到关于 '{keyword}' 的笔记")
            return
            
        print(f"共浏览到 {len(collected_notes)} 条笔记")
            
    except Exception as e:
        error_msg = f"收集小红书笔记失败: {str(e)}"
        print(error_msg)
        raise
    finally:
        # 确保关闭小红书操作器
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
    description='定浏览小红书笔记 (支持图文和视频)',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(10):
        PythonOperator(
            task_id=f'browse_xhs_notes_{index}',
            python_callable=browse_xhs_notes,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True,
            retries=3,
            retry_delay=timedelta(seconds=10)
        
        )
