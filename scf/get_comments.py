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


def get_xhs_comments_by_keyword(keyword, email=None, page=1, page_size=1000):
    """
    获取指定关键字的评论
    
    Args:
        keyword: 关键字
        email: 可选，用户邮箱
        page: 页码，默认为1
        page_size: 每页数量，默认为1000
        
    Returns:
        tuple: (评论列表, 总数)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 计算偏移量
        offset = (page - 1) * page_size
        
        # 查询总数
        if email:
            count_query = "SELECT COUNT(*) as total FROM xhs_comments WHERE keyword = %s AND userInfo = %s"
            cursor.execute(count_query, (keyword, email))
        else:
            count_query = "SELECT COUNT(*) as total FROM xhs_comments WHERE keyword = %s"
            cursor.execute(count_query, (keyword,))
        total_count = cursor.fetchone()['total']
        
        # 查询指定关键字的评论，带分页
        if email:
            query = "SELECT * FROM xhs_comments WHERE keyword = %s AND userInfo = %s LIMIT %s OFFSET %s"
            cursor.execute(query, (keyword, email, page_size, offset))
        else:
            query = "SELECT * FROM xhs_comments WHERE keyword = %s LIMIT %s OFFSET %s"
            cursor.execute(query, (keyword, page_size, offset))
        comments = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理日期时间格式，使其可JSON序列化
        for comment in comments:
            for key, value in comment.items():
                if isinstance(value, datetime):
                    comment[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return comments, total_count
    except Exception as e:
        logger.error(f"获取评论失败: {str(e)}")
        return [], 0


def get_xhs_comments_by_urls(urls, page=1, page_size=1000):
    """
    获取指定URL的评论
    
    Args:
        urls: URL列表
        page: 页码，默认为1
        page_size: 每页数量，默认为1000
        
    Returns:
        tuple: (评论列表, 总数)
    """
    try:
        if not urls:
            return [], 0
            
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 构建IN查询的占位符
        placeholders = ", ".join(["%s"] * len(urls))
        
        # 计算偏移量
        offset = (page - 1) * page_size
        
        # 查询总数
        count_query = f"SELECT COUNT(*) as total FROM xhs_comments WHERE note_url IN ({placeholders})"
        cursor.execute(count_query, tuple(urls))
        total_count = cursor.fetchone()['total']
        
        # 查询指定URL的评论，带分页
        query = f"SELECT * FROM xhs_comments WHERE note_url IN ({placeholders}) LIMIT %s OFFSET %s"
        params = tuple(urls) + (page_size, offset)
        cursor.execute(query, params)
        comments = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理日期时间格式，使其可JSON序列化
        for comment in comments:
            for key, value in comment.items():
                if isinstance(value, datetime):
                    comment[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return comments, total_count
    except Exception as e:
        logger.error(f"获取评论失败: {str(e)}")
        return [], 0


def get_xhs_comments(limit=100, page=1, page_size=1000):
    """
    获取评论，带有限制数量
    
    Args:
        limit: 限制数量，默认100条
        page: 页码，默认为1
        page_size: 每页数量，默认为1000
        
    Returns:
        tuple: (评论列表, 总数)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 计算偏移量
        offset = (page - 1) * page_size
        
        # 查询总数，但不超过limit
        count_query = "SELECT COUNT(*) as total FROM xhs_comments LIMIT %s"
        cursor.execute(count_query, (limit,))
        total_count = min(cursor.fetchone()['total'], limit)
        
        # 查询评论，带有限制和分页
        actual_limit = min(page_size, limit - offset)
        if actual_limit <= 0:
            return [], total_count
            
        query = "SELECT * FROM xhs_comments LIMIT %s OFFSET %s"
        cursor.execute(query, (actual_limit, offset))
        comments = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 处理日期时间格式，使其可JSON序列化
        for comment in comments:
            for key, value in comment.items():
                if isinstance(value, datetime):
                    comment[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return comments, total_count
    except Exception as e:
        logger.error(f"获取评论失败: {str(e)}")
        return [], 0


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
    
    # 添加分页参数
    page = int(query_params.get('page', 1))
    page_size = int(query_params.get('page_size', 1000))
    
    # 限制每页最大数量为1000
    page_size = min(page_size, 1000)
    
    try:
        # 根据参数决定使用哪种查询方式
        if 'keyword' in query_params:
            # 按关键字查询
            keyword = query_params.get('keyword')
            email = query_params.get('email')
            comments, total_count = get_xhs_comments_by_keyword(keyword, email, page, page_size)
            
            # 计算总页数
            total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
            
            result = {
                "code": 0,
                "message": "success",
                "data": {
                    "total": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "records": comments
                }
            }
        elif 'urls' in query_params:
            # 按URL列表查询
            urls = query_params.get('urls', [])
            comments, total_count = get_xhs_comments_by_urls(urls, page, page_size)
            
            # 计算总页数
            total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
            
            result = {
                "code": 0,
                "message": "success",
                "data": {
                    "total": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "records": comments
                }
            }
        else:
            # 使用默认查询，带有可选的limit参数
            limit = int(query_params.get('limit', 100))
            comments, total_count = get_xhs_comments(limit, page, page_size)
            
            # 计算总页数
            total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
            
            result = {
                "code": 0,
                "message": "success",
                "data": {
                    "total": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
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
            'keyword': '美食',
            'email': 'luyao-operate@lucy.ai',
            'page': 1,
            'page_size': 20
        }
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))