#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime, timedelta
import re 
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
from appium.webdriver.common.appiumby import AppiumBy
from utils.xhs_utils import cos_to_device_via_host
from utils.xhs_appium import XHSOperator


def update_note_status(note_title, status, note_type=None):
    """根据笔记标题更新xhs_note_templates表中的发布状态和类型
    Args:
        note_title: 笔记标题
        status: 新的状态 (1代表发布成功)
        note_type: 笔记类型（视频/图片）
    """
    if not note_title:
        print("没有需要更新的笔记标题")
        return
        
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    try:
        # 根据笔记标题更新status字段和type字段
        if note_type is not None:
            update_sql = "UPDATE xhs_note_templates SET status = %s, type = %s WHERE title = %s"
            cursor.execute(update_sql, (status, note_type, note_title))
        else:
            update_sql = "UPDATE xhs_note_templates SET status = %s WHERE title = %s"
            cursor.execute(update_sql, (status, note_title))
        
        if cursor.rowcount > 0:
            db_conn.commit()
            print(f"成功更新笔记 '{note_title}' 的发布状态为: {status}")
        else:
            print(f"未找到标题为 '{note_title}' 的笔记记录")
        
    except Exception as e:
        db_conn.rollback()
        print(f"更新笔记状态失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()


def get_note_template_from_db(email=None, note_title=None, device_id=None):
    """
    从数据库获取笔记模板

    Args:
        email: 用户邮箱，如果指定则只获取该用户的模板，否则获取所有模板
        note_title: 笔记标题，用于精确匹配
        device_id: 设备号，用于精确匹配
        
    Returns:
        包含模板内容和图片列表的字典列表，每个字典包含id、title、content、author、device_id、img_list字段
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    # 构建查询条件
    where_conditions = []
    params = []
    
    if email:
        where_conditions.append("userInfo = %s")
        params.append(email)
        
    if note_title:
        where_conditions.append("title = %s")
        params.append(note_title)
        
    if device_id:
        where_conditions.append("device_id = %s")
        params.append(device_id)
    
    # 构建完整的SQL查询
    base_query = "SELECT id, title, content, author, device_id, img_list, userInfo FROM xhs_note_templates"
    if where_conditions:
        query = f"{base_query} WHERE {' AND '.join(where_conditions)}"
        print(f"根据条件查询笔记模板: email={email}, title={note_title}, device_id={device_id}")
        cursor.execute(query, tuple(params))
    else:
        print("查询所有笔记模板")
        cursor.execute(base_query)
    
    templates_data = cursor.fetchall()
    # 构建返回结果，只返回img_list
    result = []
    for row in templates_data:
        result.append(row[5])  # 只返回img_list字段

    cursor.close()
    db_conn.close()

    if not result:
        print("警告：未找到笔记模板，请确保xhs_note_templates表中有数据")
        # 返回一个默认模板，避免程序崩溃
        return ["默认图片列表"]
    
    print(f"成功获取 {len(result)} 条笔记模板")
    return result


def notes_publish(**context):
    """
    发布笔记
    """
    try:
        email = context['dag_run'].conf.get('email')
        device_id=context['dag_run'].conf.get('device_id')
        note_title=context['dag_run'].conf.get('note_title')
        note_tags_list=context['dag_run'].conf.get('note_tags_list', [])
        note_at_user=context['dag_run'].conf.get('note_at_user')
        note_location=context['dag_run'].conf.get('note_location', None)
        note_visit_scale=context['dag_run'].conf.get('note_visit_scale', None) #公开可见，仅互关好友可见，仅自己可见
        note_content=context['dag_run'].conf.get('note_content')
        # template_ids = context['dag_run'].conf.get('template_ids', [])
        # 获取设备列表
        device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
        device_info = next((device for device in device_info_list if device.get('email') == email), None)
        if device_info:
            print(f"device_info: {device_info}")
        else:
            raise ValueError("email参数不能为空")
        # 获取设备信息
    
        device_ip = device_info.get('device_ip')
        host_port = device_info.get('port')
        appium_port = device_info.get('available_appium_ports')[0]
        username = device_info.get('username')
        password = device_info.get('password')

        appium_server_url = f"http://{device_ip}:{appium_port}"
        
        print(f"选择设备 {device_id}, appium_server_url: {appium_server_url}")
        print(f"开始发布笔记'")
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        note_template = get_note_template_from_db(email=email, note_title=note_title, device_id=device_id)
        print(f"获取到的笔记模板: {note_template}")
        cos_base_url = Variable.get("XHS_NOTE_RESOURCE_COS_URL")

        # note_template是一个列表，取第一个元素作为img_list字符串
        if note_template and len(note_template) > 0:
            img_list_str = note_template[0]  # 获取第一个img_list字符串
            print(f"获取到的图片列表字符串: {img_list_str}")
            
            # 将字符串按逗号切割成多个URL路径
            if img_list_str:
                image_urls = [url.strip() for url in img_list_str.split(',') if url.strip()]
            else:
                image_urls = []
        else:
            image_urls = []
            
        print(f"切割后的图片URL列表: {image_urls}")
        
        # 初始化成功下载计数器
        successful_download_count = 0
        
        for image_url in image_urls:
            print('图片url:',image_url)
            # 添加重试机制，默认重试3次
            max_retries = 3
            retry_count = 0
            download_success = False
            
            while retry_count < max_retries and not download_success:
                try:
                    cos_url=f'{cos_base_url}{image_url}'
                    print(f"开始下载图片 (第{retry_count + 1}次尝试): {cos_url}")
                    cos_to_device_via_host(cos_url=cos_url,host_address=device_ip,host_username=username,device_id=device_id,host_password=password,host_port=host_port)
                    # cos下载成功，计数器加1
                    successful_download_count += 1
                    download_success = True
                    print(f"图片下载成功，当前成功下载数量: {successful_download_count}")
                except Exception as e:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"图片下载失败 (第{retry_count}次尝试): {image_url}, 错误: {str(e)}, 准备重试...")
                    else:
                        print(f"图片下载最终失败 (已重试{max_retries}次): {image_url}, 错误: {str(e)}")
        
        print(f"总共成功下载 {successful_download_count} 张图片")

        # 执行发布笔记，传入话题标签列表和成功下载的图片数量
        success = xhs.publish_note(note_title, note_content, note_tags_list, note_at_user, note_location, note_visit_scale, successful_download_count)
        
        # 如果成功发布笔记，更新数据库状态
        if success:
            print(f"成功发布笔记: {note_title}")
            # 更新笔记状态到数据库
            update_note_status(note_title, 1)
        else:
            update_note_status(note_title, -1)
            print("发布笔记失败")
    except Exception as e:
        update_note_status(note_title, -1)
        print(f"发布笔记过程中发生错误: {str(e)}")
    finally:
        # 确保关闭XHS操作器

        if 'xhs' in locals():
            xhs.close()
with DAG(
    dag_id='notes_publish',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='小红书笔记发布任务',
    schedule_interval=None,
    tags=['小红书','笔记发布'],
    catchup=False,
    max_active_runs=5,
) as dag:

    PythonOperator(
        task_id=f'notes_publish',
        python_callable=notes_publish,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(seconds=15)
        
        
    )
