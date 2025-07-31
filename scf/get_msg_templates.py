#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取回复模板数据

Author: by cursor
Date: 2025-05-22
"""

import json
import os
import pymysql
import logging
from datetime import datetime

# 配置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_db_connection():
    """
    获取数据库连接
    """
    try:
        # 从环境变量获取数据库连接信息
        db_name = os.environ.get('DB_NAME')
        db_ip = os.environ.get('DB_IP')
        db_port = int(os.environ.get('DB_PORT', 3306))
        db_user = os.environ.get('DB_USER')
        db_password = os.environ.get('DB_PASSWORD')
        
        # 创建数据库连接
        connection = pymysql.connect(
            host=db_ip,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        return connection
    except Exception as e:
        logger.error(f"数据库连接失败: {str(e)}")
        raise e


def get_msg_templates(email=None):
    """
    获取用户的回复模板
    
    Args:
        email: 用户邮箱，如果为空，返回所有模板
        
    Returns:
        list: 回复模板列表
    """
    # 打印email参数
    print(f"get_message_template called with email: {email}")
    logger.info(f"get_message_template called with email: {email}")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询用户的回复模板
        if email:
            # 如果指定了email，只查询特定用户的模板
            query = "SELECT id, userInfo, content, image_urls, created_at FROM message_template WHERE userInfo = %s ORDER BY created_at DESC"
            cursor.execute(query, (email,))
        else:
            # 如果没有指定email，返回所有模板
            query = "SELECT id, userInfo, content, image_urls, created_at FROM message_template ORDER BY created_at DESC"
            cursor.execute(query)
        templates = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理日期时间格式，使其可JSON序列化
        for template in templates:
            for key, value in template.items():
                if isinstance(value, datetime):
                    template[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return templates
    except Exception as e:
        logger.error(f"获取回复模板失败: {str(e)}")
        return []


def get_template_by_id(template_id):
    """
    获取指定ID的回复模板
    
    Args:
        template_id: 模板ID
        
    Returns:
        dict: 回复模板信息
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询指定ID的回复模板
        query = "SELECT id, userInfo, content, image_urls, created_at FROM message_template WHERE id = %s"
        cursor.execute(query, (template_id,))
        template = cursor.fetchone()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理日期时间格式，使其可JSON序列化
        if template:
            for key, value in template.items():
                if isinstance(value, datetime):
                    template[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return template
    except Exception as e:
        logger.error(f"获取回复模板失败: {str(e)}")
        return None


def main_handler(event, context):
    """
    云函数入口函数，获取回复模板数据
    
    Args:
        event: 触发事件，包含查询参数
        context: 函数上下文
        
    Returns:
        JSON格式的回复模板列表
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    # 解析查询参数
    query_params = {}
    if 'queryString' in event:
        query_params = event['queryString']
    elif 'body' in event:
        try:
            # 尝试解析body为JSON
            if isinstance(event['body'], str):
                query_params = json.loads(event['body'])
            else:
                query_params = event['body']
        except:
            pass
    
    # 打印参数信息
    print(f"Query parameters: {json.dumps(query_params, ensure_ascii=False)}")
    logger.info(f"Query parameters: {json.dumps(query_params, ensure_ascii=False)}")
    
    try:
        # 根据参数决定使用哪种查询方式
        if 'template_id' in query_params:
            # 按模板ID查询
            template_id = query_params.get('template_id')
            template = get_template_by_id(template_id)
            
            if template:
                result = {
                    "code": 0,
                    "message": "success",
                    "data": template
                }
            else:
                result = {
                    "code": 1,
                    "message": "模板不存在",
                    "data": None
                }
        else:
            # 使用默认查询，带有可选的email参数
            email = query_params.get('email')
            templates = get_msg_templates(email)
            total_count = len(templates)
            
            result = {
                "code": 0,
                "message": "success",
                "data": {
                    "total": total_count,
                    "records": templates
                }
            }
        
        return result
    
    except Exception as e:
        logger.error(f"查询失败: {str(e)}")
        return {
            "code": 1,
            "message": f"查询失败: {str(e)}",
            "data": None
        }


if __name__ == "__main__":
    # 本地测试用
    test_event = {
        'queryString': {
            'email': 'zacks@example.com'
        }
    }
    
    # 测试获取所有模板
    # test_event = {
    #     'queryString': {}
    # }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))