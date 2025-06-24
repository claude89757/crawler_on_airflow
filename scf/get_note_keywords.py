#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取小红书笔记中的所有关键字

Author: by cursor
Date: 2025-05-12
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


def get_all_keywords(email=None):
    """
    获取所有关键字，可以按email过滤
    
    Args:
        email: 可选，用户邮箱，用于过滤特定用户的关键字
        
    Returns:
        list: 所有不重复的关键字列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询所有不重复的关键字，可选按email过滤
        if email:
            query = "SELECT DISTINCT keyword FROM xhs_notes WHERE userInfo = %s"
            cursor.execute(query, (email,))
        else:
            query = "SELECT DISTINCT keyword FROM xhs_notes"
            cursor.execute(query)
            
        result = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 返回关键字列表
        if result:
            return [row['keyword'] for row in result]
        return []
    except Exception as e:
        logger.error(f"获取关键字失败: {str(e)}")
        return []


def main_handler(event, context):
    """
    云函数入口函数，获取所有小红书笔记关键字
    支持URL参数: email - 按用户邮箱过滤关键字
    示例: ${baseUrl}?email=${encodeURIComponent(email)}
    
    Args:
        event: 触发事件
        context: 函数上下文
        
    Returns:
        JSON格式的关键字列表
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    try:
        # 从请求中获取email参数
        email = None
        if 'queryString' in event and 'email' in event['queryString']:
            email = event['queryString']['email']
        elif 'queryStringParameters' in event and event['queryStringParameters'] and 'email' in event['queryStringParameters']:
            email = event['queryStringParameters']['email']
        
        if email:
            logger.info(f"按email过滤关键字: {email}")
        
        # 获取关键字，可选按email过滤
        keywords = get_all_keywords(email)
        
        # 构建响应
        response = {
            'code': 0,
            'message': 'success',
            'data': keywords
        }
        
        return response
    except Exception as e:
        logger.error(f"处理请求失败: {str(e)}")
        return {
            'code': 1,
            'message': f'获取关键字失败: {str(e)}',
            'data': []
        }


if __name__ == "__main__":
    # 本地测试用
    result = main_handler({}, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))