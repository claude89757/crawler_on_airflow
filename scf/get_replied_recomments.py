#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取小红书回复评论数据

Author: by cursor
Date: 2025-01-20
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


def get_replied_recomments(page=1, page_size=100):
    """
    获取回复评论数据
    
    Args:
        page: 页码，从1开始
        page_size: 每页数量，最大100
        
    Returns:
        tuple: (回复评论列表, 总数量)
    """
    connection = None
    try:
        connection = get_db_connection()
        
        with connection.cursor() as cursor:
            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 查询总数
            count_sql = "SELECT COUNT(*) as total FROM xhs_recomment_list"
            cursor.execute(count_sql)
            total_count = cursor.fetchone()['total']
            
            # 查询数据
            sql = """
            SELECT 
                id,
                user_name,
                userInfo,
                reply_content,
                device_id,
                reply_time,
                comment_content
            FROM xhs_recomment_list 
            ORDER BY reply_time DESC 
            LIMIT %s OFFSET %s
            """
            
            cursor.execute(sql, (page_size, offset))
            records = cursor.fetchall()
            
            # 格式化时间字段
            for record in records:
                if record['reply_time']:
                    record['reply_time'] = record['reply_time'].strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"查询到 {len(records)} 条回复评论数据")
            return records, total_count
            
    except Exception as e:
        logger.error(f"获取回复评论失败: {str(e)}")
        return [], 0
    finally:
        if connection:
            connection.close()


def get_replied_recomments_by_user(user_name, page=1, page_size=100):
    """
    根据用户名获取回复评论数据
    
    Args:
        user_name: 用户名
        page: 页码，从1开始
        page_size: 每页数量，最大1000
        
    Returns:
        tuple: (回复评论列表, 总数量)
    """
    connection = None
    try:
        connection = get_db_connection()
        
        with connection.cursor() as cursor:
            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 查询总数
            count_sql = "SELECT COUNT(*) as total FROM xhs_recomment_list WHERE user_name LIKE %s"
            cursor.execute(count_sql, (f"%{user_name}%",))
            total_count = cursor.fetchone()['total']
            
            # 查询数据
            sql = """
            SELECT 
                id,
                user_name,
                userInfo,
                reply_content,
                device_id,
                reply_time,
                comment_content
            FROM xhs_recomment_list 
            WHERE user_name LIKE %s
            ORDER BY reply_time DESC 
            LIMIT %s OFFSET %s
            """
            
            cursor.execute(sql, (f"%{user_name}%", page_size, offset))
            records = cursor.fetchall()
            
            # 格式化时间字段
            for record in records:
                if record['reply_time']:
                    record['reply_time'] = record['reply_time'].strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"查询到 {len(records)} 条用户 {user_name} 的回复评论数据")
            return records, total_count
            
    except Exception as e:
        logger.error(f"获取用户回复评论失败: {str(e)}")
        return [], 0
    finally:
        if connection:
            connection.close()


def get_replied_recomments_by_device(device_id, page=1, page_size=100):
    """
    根据设备ID获取回复评论数据
    
    Args:
        device_id: 设备ID
        page: 页码，从1开始
        page_size: 每页数量，最大1000
        
    Returns:
        tuple: (回复评论列表, 总数量)
    """
    connection = None
    try:
        connection = get_db_connection()
        
        with connection.cursor() as cursor:
            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 查询总数
            count_sql = "SELECT COUNT(*) as total FROM xhs_recomment_list WHERE device_id = %s"
            cursor.execute(count_sql, (device_id,))
            total_count = cursor.fetchone()['total']
            
            # 查询数据
            sql = """
            SELECT 
                id,
                user_name,
                userInfo,
                reply_content,
                device_id,
                reply_time,
                comment_content
            FROM xhs_recomment_list 
            WHERE device_id = %s
            ORDER BY reply_time DESC 
            LIMIT %s OFFSET %s
            """
            
            cursor.execute(sql, (device_id, page_size, offset))
            records = cursor.fetchall()
            
            # 格式化时间字段
            for record in records:
                if record['reply_time']:
                    record['reply_time'] = record['reply_time'].strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"查询到 {len(records)} 条设备 {device_id} 的回复评论数据")
            return records, total_count
            
    except Exception as e:
        logger.error(f"获取设备回复评论失败: {str(e)}")
        return [], 0
    finally:
        if connection:
            connection.close()


def get_replied_recomments_by_userInfo(userInfo, page=1, page_size=100):
    """
    根据用户信息获取回复评论数据
    
    Args:
        userInfo: 用户信息关键字
        page: 页码，从1开始
        page_size: 每页数量，最大100
        
    Returns:
        tuple: (回复评论列表, 总数量)
    """
    connection = None
    try:
        connection = get_db_connection()
        
        with connection.cursor() as cursor:
            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 查询总数
            count_sql = "SELECT COUNT(*) as total FROM xhs_recomment_list WHERE userInfo LIKE %s"
            cursor.execute(count_sql, (f"%{userInfo}%",))
            total_count = cursor.fetchone()['total']
            
            # 查询数据
            sql = """
            SELECT 
                id,
                user_name,
                userInfo,
                reply_content,
                device_id,
                reply_time,
                comment_content
            FROM xhs_recomment_list 
            WHERE userInfo LIKE %s
            ORDER BY reply_time DESC 
            LIMIT %s OFFSET %s
            """
            
            cursor.execute(sql, (f"%{userInfo}%", page_size, offset))
            records = cursor.fetchall()
            
            # 格式化时间字段
            for record in records:
                if record['reply_time']:
                    record['reply_time'] = record['reply_time'].strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"查询到 {len(records)} 条包含用户信息 {userInfo} 的回复评论数据")
            return records, total_count
            
    except Exception as e:
        logger.error(f"获取用户信息回复评论失败: {str(e)}")
        return [], 0
    finally:
        if connection:
            connection.close()


def main_handler(event, context):
    """
    云函数入口函数，获取小红书回复评论数据
    
    Args:
        event: 触发事件，包含查询参数
        context: 函数上下文
        
    Returns:
        JSON格式的回复评论列表
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
    page_size = int(query_params.get('page_size', 100))
    
    # 限制每页最大数量为100
    page_size = min(page_size, 100)
    
    try:
        # 根据参数决定使用哪种查询方式
        if 'user_name' in query_params:
            # 按用户名查询
            user_name = query_params.get('user_name')
            records, total_count = get_replied_recomments_by_user(user_name, page, page_size)
        elif 'device_id' in query_params:
            # 按设备ID查询
            device_id = query_params.get('device_id')
            records, total_count = get_replied_recomments_by_device(device_id, page, page_size)
        elif 'userInfo' in query_params:
            # 按用户信息查询
            userInfo = query_params.get('userInfo')
            records, total_count = get_replied_recomments_by_userInfo(userInfo, page, page_size)
        else:
            # 默认查询所有数据
            records, total_count = get_replied_recomments(page, page_size)
        
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
                "records": records
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
            'page': 1,
            'page_size': 20
        }
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))