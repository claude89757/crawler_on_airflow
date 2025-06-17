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

from utils.xhs_appium import XHSOperator

def save_msg_to_db(msg_list:list):
    """保存私信列表到数据库
    Args:
        msg_list: 私信列表
    """
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    try:
        # 检查表是否存在，如果不存在则创建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS xhs_msg_list (
            id INT AUTO_INCREMENT PRIMARY KEY,
            userInfo TEXT,
            user_name TEXT,
            message_type TEXT,    
            check_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            reply_status int DEFAULT NULL   
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句
        insert_sql = """
        INSERT INTO xhs_msg_list 
        (userInfo, user_name, message_type, check_time, reply_status) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # 批量插入私信数据
        insert_data = []
        for msg in msg_list['unreplied_users']:
            insert_data.append((
                msg_list.get('userInfo', ''),
                msg.get('username', ''),
                msg.get('message_type', ''),
                msg_list.get('check_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                msg.get('reply_status', 0)
            ))

        cursor.executemany(insert_sql, insert_data)

        db_conn.commit()
        print(f"成功保存 {len(msg_list['unreplied_users'])} 条私信到数据库，并更新笔记私信收集时间")
    except Exception as e:
        db_conn.rollback()
        print(f"保存私信到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()

def xhs_msg_check(device_index,**context):
    email = context['dag_run'].conf.get('email')
    
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if device_info:
        print(f"device_info: {device_info}")
    else:
        raise ValueError("email参数不能为空")
     # 获取设备信息
    try:
        device_ip = device_info.get('device_ip')
        appium_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")
    appium_server_url = f"http://{device_ip}:{appium_port}"
    
    print(f"选择设备 {device_id}, appium_server_url: {appium_server_url}")
    print(f"开始回复私信'")
    xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
    msg_list=xhs.check_unreplied_messages(device_id,email)
    if msg_list:
        print(f"未回复私信列表: {msg_list}")
        save_msg_to_db(msg_list)
    else:
        print("没有未回复的私信")
with DAG(
    dag_id='xhs_msg_check',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='小红书私信检查任务',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(10):
        PythonOperator(
            task_id=f'xhs_msg_check{index}',
            python_callable=xhs_msg_check,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True
        )
