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



def get_account_name(device_index,**context):
    time.sleep(5*device_index)  # 随机等待1-3秒，避免频繁请求导致被封禁
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
    print(f"开始回复私信'")
    xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
    # 获取原本账号信息

    XHS_ACCOUNT_INFO = Variable.get("XHS_ACCOUNT_INFO", default_var={}, deserialize_json=True)
    print(f"当前账号信息: {XHS_ACCOUNT_INFO}")
    try:

        account_name = xhs.get_account_name()
        print(f"获取到小红书账号名: {account_name}")
        
        # 如果email不存在，创建新的列表
        if email not in XHS_ACCOUNT_INFO:
            XHS_ACCOUNT_INFO[email] = []
        
        # 检查是否已存在相同的device_id，如果存在则更新，否则添加新的
        device_found = False
        for device_mapping in XHS_ACCOUNT_INFO[email]:
            if device_id in device_mapping:
                device_mapping[device_id] = account_name
                device_found = True
                break
        
        # 如果没有找到相同的device_id，添加新的映射
        if not device_found:
            XHS_ACCOUNT_INFO[email].append({device_id: account_name})
        
        # 保存更新后的映射到Variable
        Variable.set("XHS_ACCOUNT_INFO", XHS_ACCOUNT_INFO, serialize_json=True)
        print(f"已更新账号名映射: {XHS_ACCOUNT_INFO}")
    
        Variable.set("XHS_ACCOUNT_INFO", XHS_ACCOUNT_INFO, serialize_json=True)
    finally:
        # 确保关闭XHS操作器
        if 'xhs' in locals():
            xhs.close()

with DAG(
    dag_id='xhs_account_name_colletor',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='获取小红书账号名任务',
    schedule_interval=None,
    tags=['小红书','账号名称获取'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(15):
        PythonOperator(
            task_id=f'xhs_account_name_colletor{index}',
            python_callable=get_account_name,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True
            retries=3,
            retry_delay=timedelta(seconds=10),
        )
