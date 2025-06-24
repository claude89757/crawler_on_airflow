#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取小红书评论数据

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


def get_xhs_comments_by_keyword(keyword, email=None):
    """
    获取指定关键字的评论
    
    Args:
        keyword: 关键字
        email: 可选，用户邮箱
        
    Returns:
        list: 评论列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询指定关键字的评论
        if email:
            query = "SELECT * FROM xhs_comments WHERE keyword = %s AND userInfo = %s"
            cursor.execute(query, (keyword, email))
        else:
            query = "SELECT * FROM xhs_comments WHERE keyword = %s"
            cursor.execute(query, (keyword,))
        comments = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理日期时间格式，使其可JSON序列化
        for comment in comments:
            for key, value in comment.items():
                if isinstance(value, datetime):
                    comment[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return comments
    except Exception as e:
        logger.error(f"获取评论失败: {str(e)}")
        return []


def get_xhs_comments_by_urls(urls):
    """
    获取指定URL的评论
    
    Args:
        urls: URL列表
        
    Returns:
        list: 评论列表
    """
    try:
        if not urls:
            return []
            
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 构建IN查询的占位符
        placeholders = ", ".join(["%s"] * len(urls))
        
        # 查询指定URL的评论
        query = f"SELECT * FROM xhs_comments WHERE note_url IN ({placeholders})"
        cursor.execute(query, tuple(urls))
        comments = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理日期时间格式，使其可JSON序列化
        for comment in comments:
            for key, value in comment.items():
                if isinstance(value, datetime):
                    comment[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return comments
    except Exception as e:
        logger.error(f"获取评论失败: {str(e)}")
        return []


def get_xhs_comments(limit=100):
    """
    获取评论，带有限制数量
    
    Args:
        limit: 限制数量，默认100条
        
    Returns:
        list: 评论列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 查询评论，带有限制
        query = "SELECT * FROM xhs_comments LIMIT %s"
        cursor.execute(query, (limit,))
        comments = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理日期时间格式，使其可JSON序列化
        for comment in comments:
            for key, value in comment.items():
                if isinstance(value, datetime):
                    comment[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return comments
    except Exception as e:
        logger.error(f"获取评论失败: {str(e)}")
        return []


def main_handler(event, context):
    """
    云函数入口函数，获取小红书评论数据
    
    Args:
        event: 触发事件，包含查询参数
        context: 函数上下文
        
    Returns:
        JSON格式的评论列表
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
        # 根据参数决定使用哪种查询方式
        if 'keyword' in query_params:
            # 按关键字查询
            keyword = query_params.get('keyword')
            email = query_params.get('email')
            comments = get_xhs_comments_by_keyword(keyword, email)
            total_count = len(comments)
            
            result = {
                "code": 0,
                "message": "success",
                "data": {
                    "total": total_count,
                    "records": comments
                }
            }
        else:
            # 使用默认查询，带有可选的limit参数
            limit = int(query_params.get('limit', 100))
            comments = get_xhs_comments(limit)
            total_count = len(comments)
            
            result = {
                "code": 0,
                "message": "success",
                "data": {
                    "total": total_count,
                    "records": comments
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
            'limit': 10
        }
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))