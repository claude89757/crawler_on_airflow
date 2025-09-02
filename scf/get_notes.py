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
    
    # 添加分页参数
    page = int(query_params.get('page', 1))
    page_size = int(query_params.get('page_size', 1000))
    
    # 限制每页最大数量为1000
    page_size = min(page_size, 1000)
    
    # 计算偏移量
    offset = (page - 1) * page_size
    
    # 处理时间筛选参数
    start_time = query_params.get('start_time')
    end_time = query_params.get('end_time')
    
    # 时间参数处理：支持整数时间戳和日期时间字符串格式
    if start_time:
        try:
            # 尝试解析为整数时间戳
            start_time = int(start_time)
        except ValueError:
            try:
                # 尝试解析为日期时间字符串
                dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                start_time = int(dt.timestamp())
            except ValueError:
                logger.error(f"无效的start_time格式: {start_time}")
                start_time = None
    
    if end_time:
        try:
            # 尝试解析为整数时间戳
            end_time = int(end_time)
        except ValueError:
            try:
                # 尝试解析为日期时间字符串
                dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                end_time = int(dt.timestamp())
            except ValueError:
                logger.error(f"无效的end_time格式: {end_time}")
                end_time = None
    
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
        
        # 构建WHERE条件
        where_conditions = ["keyword = %s"]
        query_params_list = [keyword]
        
        if email:
            where_conditions.append("userInfo = %s")
            query_params_list.append(email)
        
        # 添加时间筛选条件
        if start_time:
            where_conditions.append("UNIX_TIMESTAMP(collect_time) >= %s")
            query_params_list.append(start_time)
        
        if end_time:
            where_conditions.append("UNIX_TIMESTAMP(collect_time) <= %s")
            query_params_list.append(end_time)
        
        where_clause = " AND ".join(where_conditions)
        
        # 查询总数
        count_query = f"SELECT COUNT(*) as total FROM xhs_notes WHERE {where_clause}"
        cursor.execute(count_query, tuple(query_params_list))
        total_count = cursor.fetchone()['total']
        
        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
        
        # 添加分页限制的查询
        query = f"SELECT * FROM xhs_notes WHERE {where_clause} LIMIT %s OFFSET %s"
        query_params_list.extend([page_size, offset])
        cursor.execute(query, tuple(query_params_list))
        notes = cursor.fetchall()

        # 处理日期时间格式，使其可JSON序列化
        for note in notes:
            for key, value in note.items():
                if isinstance(value, datetime):
                    note[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        # 构建返回结果，包含分页信息
        result = {
            "code": 0,
            "message": "success",
            "data": {
                "total": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
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
            'email': 'luyao-operate@lucy.ai',
            'page': 1,
            'page_size': 20
            # 'start_time': '2025-06-01 00:00:00',  # 可选：支持日期时间字符串格式
            # 'end_time': '2025-06-30 23:59:59'     # 可选：支持日期时间字符串格式
        }
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))