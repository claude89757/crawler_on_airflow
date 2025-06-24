#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取小红书中指定关键字的笔记

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


def main_handler(event, context):
    """
    云函数入口函数，获取指定关键字的小红书笔记
    
    Args:
        event: 触发事件，包含查询参数
        context: 函数上下文
        
    Returns:
        JSON格式的笔记列表
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    print(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
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
    
    # 提取关键字参数
    keyword = query_params.get('keyword', '')
    email = query_params.get('email', '')
    
    if not keyword:
        return {
            'code': 1,
            'message': '缺少关键字参数',
            'data': None
        }
    
    try:
        # 获取数据库连接
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 处理关键字的空格
        keyword = keyword.strip()
        if keyword.startswith('"') and keyword.endswith('"'):
            keyword = keyword[1:-1]
        
        if email:
            query = "SELECT * FROM xhs_notes WHERE keyword = %s AND userInfo = %s"
            params = (keyword, email)
        else:
            query = "SELECT * FROM xhs_notes WHERE keyword = %s"
            params = (keyword,)
        cursor.execute(query, params)
        notes = cursor.fetchall()

        # 查询总数
        if email:
            count_query = "SELECT COUNT(*) as total FROM xhs_notes WHERE keyword = %s AND userInfo = %s"
            count_params = (keyword, email)
        else:
            count_query = "SELECT COUNT(*) as total FROM xhs_notes WHERE keyword = %s"
            count_params = (keyword,)
        cursor.execute(count_query, count_params)
        total_count = cursor.fetchone()['total']

        # 处理日期时间格式，使其可JSON序列化
        for note in notes:
            for key, value in note.items():
                if isinstance(value, datetime):
                    note[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        # 构建返回结果
        result = {
            "code": 0,
            "message": "success",
            "data": {
                "total": total_count,
                "records": notes
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
            'keyword': '美食',
            'email': 'luyao-operate@lucy.ai'
        }
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))