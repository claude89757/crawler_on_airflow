#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取笔记模板数据

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


def execute_query(query, params):
    """
    执行查询操作
    
    Args:
        query: SQL查询语句
        params: 查询参数
        
    Returns:
        list: 查询结果列表
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 执行查询操作
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # 处理datetime对象，转换为字符串
        for result in results:
            if isinstance(result, dict):
                for key, value in result.items():
                    if isinstance(value, datetime):
                        result[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        logger.error(f"执行查询失败: {str(e)}")
        return []


def get_xhs_note_templates_by_email(email, page=1, page_size=10):
    """
    根据邮箱获取笔记模板（支持分页）
    
    Args:
        email: 用户邮箱
        page: 页码，从1开始
        page_size: 每页数量
        
    Returns:
        list: 笔记模板列表
    """
    offset = (page - 1) * page_size
    query = "SELECT id, title, content, userInfo, author, device_id, img_list, status, created_at FROM xhs_note_templates WHERE userInfo = %s ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params = (email, page_size, offset)
    return execute_query(query, params)


def get_xhs_note_template_by_id(template_id, email):
    """
    根据模板ID和邮箱获取单个笔记模板
    
    Args:
        template_id: 模板ID
        email: 用户邮箱
        
    Returns:
        dict: 笔记模板信息，如果不存在返回None
    """
    query = "SELECT id, title, content, userInfo, author, device_id, img_list, status, created_at FROM xhs_note_templates WHERE id = %s AND userInfo = %s"
    params = (template_id, email)
    results = execute_query(query, params)
    return results[0] if results else None


def get_xhs_note_templates_by_title(email, title_keyword=None, page=1, page_size=10):
    """
    根据邮箱和标题关键词获取笔记模板（支持分页）
    
    Args:
        email: 用户邮箱
        title_keyword: 标题关键词，可选
        page: 页码，从1开始
        page_size: 每页数量
        
    Returns:
        list: 笔记模板列表
    """
    offset = (page - 1) * page_size
    if title_keyword:
        query = "SELECT id, title, content, userInfo, author, device_id, img_list, status, created_at FROM xhs_note_templates WHERE userInfo = %s AND title LIKE %s ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params = (email, f"%{title_keyword}%", page_size, offset)
    else:
        query = "SELECT id, title, content, userInfo, author, device_id, img_list, status, created_at FROM xhs_note_templates WHERE userInfo = %s ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params = (email, page_size, offset)
    return execute_query(query, params)


def get_xhs_note_templates_count(email, title_keyword=None):
    """
    获取用户的笔记模板总数
    
    Args:
        email: 用户邮箱
        title_keyword: 标题关键词，可选
        
    Returns:
        int: 模板总数
    """
    if title_keyword:
        query = "SELECT COUNT(*) as count FROM xhs_note_templates WHERE userInfo = %s AND title LIKE %s"
        params = (email, f"%{title_keyword}%")
    else:
        query = "SELECT COUNT(*) as count FROM xhs_note_templates WHERE userInfo = %s"
        params = (email,)
    results = execute_query(query, params)
    return results[0]['count'] if results else 0


def main_handler(event, context):
    """
    云函数入口函数，获取笔记模板数据
    
    Args:
        event: 触发事件，包含查询参数
        context: 函数上下文
        
    Returns:
        JSON格式的查询结果
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    print(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    params = {}
    
    # 打印原始请求信息便于调试
    print(f"Original event: {json.dumps(event, ensure_ascii=False)}")
    
    if 'queryString' in event and event['queryString']:
        params = event['queryString']
        print(f"Using queryString parameters: {json.dumps(params, ensure_ascii=False)}")
    
    if 'body' in event and event['body']:
        try:
            # 处理body参数
            body_data = None
            if isinstance(event['body'], str):
                body_data = json.loads(event['body'])
                print(f"Parsed body string: {json.dumps(body_data, ensure_ascii=False)}")
            else:
                body_data = event['body']
                print(f"Using body object: {json.dumps(body_data, ensure_ascii=False)}")
            
            # 将body中的参数合并到params
            if body_data and isinstance(body_data, dict):
                params.update(body_data)
        except Exception as e:
            print(f"Error parsing body: {str(e)}")
            logger.error(f"Error parsing body: {str(e)}")
    
    # 打印接收到的参数
    print(f"Received parameters: {json.dumps(params, ensure_ascii=False)}")
    logger.info(f"Received parameters: {json.dumps(params, ensure_ascii=False)}")
    
    # 获取参数
    action = params.get('action', 'get_all')
    email = params.get('email', 'zacks@example.com')
    page = int(params.get('page', 1))
    page_size = int(params.get('page_size', 10))
    
    # 验证分页参数
    if page < 1:
        page = 1
    if page_size < 1 or page_size > 100:
        page_size = 10
    
    try:
        # 根据操作类型执行相应查询
        if action == 'get_all':
            # 获取用户模板（分页）
            templates = get_xhs_note_templates_by_email(email, page, page_size)
            total_count = get_xhs_note_templates_count(email)
            total_pages = (total_count + page_size - 1) // page_size  # 向上取整
            
            return {
                "code": 0,
                "message": "success",
                "data": {
                    "templates": templates,
                    "pagination": {
                        "current_page": page,
                        "page_size": page_size,
                        "total_count": total_count,
                        "total_pages": total_pages,
                        "has_next": page < total_pages,
                        "has_prev": page > 1
                    }
                }
            }
            
        elif action == 'get_by_id':
            # 根据ID获取单个模板
            template_id = params.get('template_id')
            if not template_id:
                return {
                    "code": 1,
                    "message": "模板ID不能为空",
                    "data": None
                }
            
            template = get_xhs_note_template_by_id(template_id, email)
            return {
                "code": 0 if template else 1,
                "message": "success" if template else "模板不存在",
                "data": {
                    "template": template
                }
            }
            
        elif action == 'search':
            # 根据标题关键词搜索模板（分页）
            title_keyword = params.get('title_keyword')
            templates = get_xhs_note_templates_by_title(email, title_keyword, page, page_size)
            total_count = get_xhs_note_templates_count(email, title_keyword)
            total_pages = (total_count + page_size - 1) // page_size  # 向上取整
            
            return {
                "code": 0,
                "message": "success",
                "data": {
                    "templates": templates,
                    "keyword": title_keyword,
                    "pagination": {
                        "current_page": page,
                        "page_size": page_size,
                        "total_count": total_count,
                        "total_pages": total_pages,
                        "has_next": page < total_pages,
                        "has_prev": page > 1
                    }
                }
            }
            
        elif action == 'count':
            # 获取模板总数
            title_keyword = params.get('title_keyword')
            count = get_xhs_note_templates_count(email, title_keyword)
            return {
                "code": 0,
                "message": "success",
                "data": {
                    "count": count,
                    "keyword": title_keyword
                }
            }
            
        else:
            # 未知操作
            return {
                "code": 1,
                "message": f"未知操作类型: {action}",
                "data": None
            }
            
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
        'body': json.dumps({
            'action': 'get_all',
            'email': 'zacks@example.com',
            'page': 1,
            'page_size': 5
        })
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))
    
    # 测试搜索分页
    search_event = {
        'body': json.dumps({
            'action': 'search',
            'email': 'zacks@example.com',
            'title_keyword': '测试',
            'page': 1,
            'page_size': 3
        })
    }
    search_result = main_handler(search_event, {})
    print("\n搜索结果:")
    print(json.dumps(search_result, ensure_ascii=False, indent=2))