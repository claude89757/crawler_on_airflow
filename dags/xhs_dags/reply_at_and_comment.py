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
from utils.xhs_appium import XHSOperator

def save_replied_data_to_db(replied_list):
    """
    将回复数据保存到xhs_recomment_list表中
    Args:
        replied_list: 已回复的评论列表，包含username、userInfo、reply_content等字段
    """
    if not replied_list:
        print("没有需要保存的回复数据")
        return
        
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    try:
        # 插入SQL语句
        insert_sql = """
        INSERT INTO xhs_recomment_list 
        (user_name, userInfo, reply_content, device_id, comment_content, reply_time) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        inserted_count = 0
        for reply_data in replied_list:
            username = reply_data.get('username', '')
            userInfo = reply_data.get('userInfo', '')
            reply_content = reply_data.get('reply_content', '')
            device_id = reply_data.get('device_id', '')
            comment_content = reply_data.get('comment_content', '')
            reply_time = reply_data.get('reply_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
            if not username:
                print("跳过无效的回复记录（缺少username）")
                continue
                
            try:
                cursor.execute(insert_sql, (
                    username,
                    userInfo,
                    reply_content,
                    device_id,
                    comment_content,
                    reply_time
                ))
                inserted_count += 1
                print(f"成功保存用户 {username} 的回复记录: {reply_content}")
            except Exception as e:
                print(f"保存用户 {username} 的回复记录失败: {str(e)}")
                continue
        
        db_conn.commit()
        print(f"总共保存了 {inserted_count} 条回复记录到xhs_recomment_list表")
        
    except Exception as e:
        db_conn.rollback()
        print(f"保存回复数据失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()

def reply_at_and_comment(device_index,**context):
    email = context['dag_run'].conf.get('email')
    reply_content = context['dag_run'].conf.get('reply_content')
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
        host_port=device_info.get('port')
        appium_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
        username = device_info.get('username')
        password = device_info.get('password')
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")
    appium_server_url = f"http://{device_ip}:{appium_port}"
    
    print(f"选择设备 {device_id}, appium_server_url: {appium_server_url}")
    print(f"开始回复评论")
    xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
    try:
        # 执行回复消息
        replied_list=xhs.reply_at_and_comment(reply_content,device_id,email)
        
        # 保存回复数据到数据库
        if replied_list:
            save_replied_data_to_db(replied_list)
            
    except Exception as e:
        print(f"回复评论过程中出错: {str(e)}")
        raise
    finally:
        # 确保关闭XHS操作器
        if 'xhs' in locals():
            xhs.close()

with DAG(
    dag_id='reply_at_and_comment',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='小红书@和评论回复任务',
    schedule_interval=None,
    tags=['小红书','评论回复'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(15):
        PythonOperator(
            task_id=f'msg_reply{index}',
            python_callable=reply_at_and_comment,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True
                   
        )
