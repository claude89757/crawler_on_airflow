#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取意向客户数据

Author: by cursor
Date: 2025-05-13
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


def get_customer_intent(keyword=None, intent=None, email=None):
    """
    获取意向客户数据，可按关键词、意向类型和用户邮箱筛选
    
    Args:
        keyword: 关键词筛选
        intent: 意向类型筛选
        email: 用户邮箱筛选，用于过滤特定用户的意向客户数据
        
    Returns:
        list: 意向客户数据列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        where_clauses = []
        params = []
        
        if keyword:
            where_clauses.append("ci.keyword = %s")
            params.append(keyword)
        
        if intent:
            where_clauses.append("ci.intent = %s")
            params.append(intent)
        
        if email:
            where_clauses.append("ci.userInfo = %s")
            params.append(email)
        
        # Modified query to join with comment_manual_reply table and add is_reply field and reply content
        query = """
        SELECT ci.*, 
               CASE WHEN cmr.comment_id IS NOT NULL THEN 1 ELSE 0 END AS is_reply,
               IFNULL(cmr.reply, '未回复') AS reply
        FROM customer_intent ci
        LEFT JOIN comment_manual_reply cmr ON ci.comment_id = cmr.comment_id
        """
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        cursor.execute(query, tuple(params) if params else None)
        customers = cursor.fetchall()
        
        # Format dates for JSON serialization
        for customer in customers:
            for key, value in customer.items():
                if isinstance(value, datetime):
                    customer[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.close()
        conn.close()
        
        return customers
    except Exception as e:
        logger.error(f"获取意向客户数据失败: {str(e)}")
        return []


def get_all_keywords(email=None):
    """
    获取所有不重复的关键词，可按email过滤
    
    Args:
        email: 可选，用户邮箱，用于过滤特定用户的关键词
        
    Returns:
        list: 所有不重复的关键词列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询所有不重复的关键词，可选按email过滤
        if email:
            query = "SELECT DISTINCT keyword FROM customer_intent WHERE userInfo = %s"
            cursor.execute(query, (email,))
        else:
            query = "SELECT DISTINCT keyword FROM customer_intent"
            cursor.execute(query)
            
        result = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 返回关键词列表
        if result:
            return [row['keyword'] for row in result]
        return []
    except Exception as e:
        logger.error(f"获取关键词失败: {str(e)}")
        return []


def get_all_intents(email=None):
    """
    获取所有不重复的意向类型，可按email过滤
    
    Args:
        email: 可选，用户邮箱，用于过滤特定用户的意向类型
        
    Returns:
        list: 所有不重复的意向类型列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询所有不重复的意向类型，可选按email过滤
        if email:
            query = "SELECT DISTINCT intent FROM customer_intent WHERE userInfo = %s"
            cursor.execute(query, (email,))
        else:
            query = "SELECT DISTINCT intent FROM customer_intent"
            cursor.execute(query)
            
        result = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 返回意向类型列表
        if result:
            return [row['intent'] for row in result]
        return []
    except Exception as e:
        logger.error(f"获取意向类型失败: {str(e)}")
        return []


def main_handler(event, context):
    """
    云函数入口函数，获取意向客户数据
    支持URL参数: email - 按用户邮箱过滤意向客户数据
    示例: ${baseUrl}?email=${encodeURIComponent(email)}
    
    Args:
        event: 触发事件，包含查询参数 (keyword, intent, email)
        context: 函数上下文
        
    Returns:
        JSON格式的意向客户数据
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
    
    try:
        # 首先获取email参数，用于过滤所有数据
        email = query_params.get('email', None)
        if email:
            logger.info(f"按email过滤意向客户数据: {email}")
        
        # 检查是否请求关键词列表
        if query_params.get('get_keywords', False) or query_params.get('keywords', False):
            # 根据email过滤关键词
            keywords = get_all_keywords(email)
            return {
                'code': 0,
                'message': 'success',
                'data': keywords
            }
        
        # 检查是否请求意向类型列表
        if query_params.get('get_intents', False) or query_params.get('intents', False):
            # 根据email过滤意向类型
            intents = get_all_intents(email)
            return {
                'code': 0,
                'message': 'success',
                'data': intents
            }
        
        # 获取客户意向数据，带有可选的筛选条件
        keyword = query_params.get('keyword', None)
        intent = query_params.get('intent', None)
        
        # 获取客户意向数据，带有可选的筛选条件
        customers = get_customer_intent(keyword, intent, email)
        total_count = len(customers)
        
        # 构建响应，使用相同的email参数获取过滤器数据
        response = {
            'code': 0,
            'message': 'success',
            'data': {
                'total': total_count,
                'records': customers,
                'filters': {
                    'keywords': get_all_keywords(email),
                    'intents': get_all_intents(email)
                }
            }
        }
        
        return response
    except Exception as e:
        logger.error(f"处理请求失败: {str(e)}")
        return {
            'code': 1,
            'message': f'获取意向客户数据失败: {str(e)}',
            'data': None
        }


if __name__ == "__main__":
    # 本地测试用
    test_event = {
        'queryString': {
            # 'keyword': '美食',
            # 'intent': '高意向',
            # 'get_keywords': True,
            # 'get_intents': True
        }
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))