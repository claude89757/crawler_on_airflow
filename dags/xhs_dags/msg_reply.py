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


def update_reply_status(replyed_msg_list):
    """更新回复状态到数据库
    Args:
        replyed_msg_list: 已回复的消息列表，包含msg_author和msg_content字段
    """
    if not replyed_msg_list:
        print("没有需要更新的回复状态")
        return
        
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    try:
        # 准备更新SQL语句，同时更新reply_status和msg_content
        update_sql = "UPDATE xhs_msg_list SET reply_status = 1, msg_content = %s, reply_time = %s WHERE user_name = %s"

        updated_count = 0
        for reply_msg in replyed_msg_list:
            msg_author = reply_msg.get('msg_author')
            msg_content = reply_msg.get('msg_content')
            reply_time = reply_msg.get('reply_time')

            if not msg_author:
                print("跳过无效的消息记录（缺少msg_author）")
                continue
                
            try:
                cursor.execute(update_sql, (msg_content, reply_time, msg_author))
                if cursor.rowcount > 0:
                    updated_count += 1
                    print(f"成功更新用户 {msg_author} 的回复状态和消息内容: {msg_content}")
                else:
                    print(f"未找到用户 {msg_author} 的记录")
            except Exception as e:
                print(f"更新用户 {msg_author} 的回复状态失败: {str(e)}")
                continue
        
        db_conn.commit()
        print(f"总共更新了 {updated_count} 条记录的回复状态")
        
    except Exception as e:
        db_conn.rollback()
        print(f"更新回复状态失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()


def msg_reply(device_index,**context):
    email = context['dag_run'].conf.get('email')
    msg= context['dag_run'].conf.get('msg')
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
    
    try:
        # 执行回复消息
        replyed_msg_list = xhs.reply_to_msg(msg)
        
        # 如果有成功回复的消息，更新数据库状态
        if replyed_msg_list:
            print(f"成功回复了 {len(replyed_msg_list)} 条私信")
            # 直接传递回复消息列表，包含msg_author和msg_content
            msg_author_list = [reply_msg.get('msg_author') for reply_msg in replyed_msg_list if reply_msg.get('msg_author')]
            
            if msg_author_list:
                print(f"准备更新以下用户的回复状态: {msg_author_list}")
                update_reply_status(replyed_msg_list)
            else:
                print("没有找到有效的回复用户信息")
        else:
            print("没有成功回复任何私信")
            
    except Exception as e:
        print(f"回复私信过程中出错: {str(e)}")
        raise
    finally:
        # 确保关闭XHS操作器
        if 'xhs' in locals():
            xhs.close()

with DAG(
    dag_id='msg_reply',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='小红书私信回复任务',
    schedule_interval=None,
    tags=['小红书','私信回复'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(15):
        PythonOperator(
            task_id=f'msg_reply{index}',
            python_callable=msg_reply,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True
                   
        )
