#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
管理笔记模板数据（增、删、改）

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


def execute_update(query, params):
    """
    执行更新操作
    
    Args:
        query: SQL查询语句
        params: 查询参数
        
    Returns:
        int: 受影响的行数
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 执行更新操作
        affected_rows = cursor.execute(query, params)
        conn.commit()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        return affected_rows
    except Exception as e:
        logger.error(f"执行更新失败: {str(e)}")
        return 0


def insert_many(query, data):
    """
    批量插入数据
    
    Args:
        query: SQL查询语句
        data: 数据列表
        
    Returns:
        int: 受影响的行数
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 执行批量插入
        affected_rows = cursor.executemany(query, data)
        conn.commit()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        return affected_rows
    except Exception as e:
        logger.error(f"批量插入失败: {str(e)}")
        return 0


def add_xhs_note_templates(title, content, email, author=None, device_id=None, img_list=None):
    """
    添加笔记模板
    
    Args:
        title: 笔记标题
        content: 笔记内容
        email: 用户邮箱，默认为zacks@example.com
        author: 作者，可选
        device_id: 发布笔记的设备号，可选
        img_list: 图片列表，可选
        
    Returns:
        int: 受影响的行数
    """
    query = "INSERT INTO xhs_note_templates (title, content, userInfo, author, device_id, img_list, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    params = (title, content, email, author, device_id, img_list, datetime.now())
    return execute_update(query, params)


def add_xhs_note_templatess(templates, email):
    """
    批量添加笔记模板
    
    Args:
        templates: 模板列表，每个模板应包含title和content等字段
        email: 用户邮箱，默认为zacks@example.com
        
    Returns:
        int: 受影响的行数
    """
    if not templates:
        return 0
    query = "INSERT INTO xhs_note_templates (title, content, userInfo, author, device_id, img_list) VALUES (%s, %s, %s, %s, %s, %s)"
    data = []
    for template in templates:
        if isinstance(template, dict):
            data.append((
                template.get('title', ''),
                template.get('content', ''),
                email,
                template.get('author'),
                template.get('device_id'),
                template.get('img_list')
            ))
        else:
            # 兼容旧格式，将字符串作为content处理
            data.append(('', template, email, None, None, None))
    return insert_many(query, data)


def delete_xhs_note_templates(template_id, email):
    """
    删除指定ID的回复模板
    
    Args:
        template_id: 模板ID
        email: 用户邮箱
        
    Returns:
        int: 受影响的行数
    """
    query = "DELETE FROM xhs_note_templates WHERE id = %s AND userInfo = %s"
    params = (template_id, email)
    return execute_update(query, params)


def delete_all_xhs_note_templatess(email):
    """
    删除用户的所有回复模板
    
    Args:
        email: 用户邮箱
        
    Returns:
        int: 受影响的行数
    """
    query = "DELETE FROM xhs_note_templates WHERE userInfo = %s"
    params = (email,)
    return execute_update(query, params)


def update_xhs_note_templates(template_id, email, title=None, content=None, author=None, device_id=None, img_list=None):
    """
    更新指定ID的笔记模板内容
    
    Args:
        template_id: 模板ID
        email: 用户邮箱
        title: 笔记标题，可选
        content: 笔记内容，可选
        author: 作者，可选
        device_id: 发布笔记的设备号，可选
        img_list: 图片列表，可选
        
    Returns:
        int: 受影响的行数
    """
    # 构建动态更新语句
    update_fields = []
    params = []
    
    if title is not None:
        update_fields.append("title = %s")
        params.append(title)
    if content is not None:
        update_fields.append("content = %s")
        params.append(content)
    if author is not None:
        update_fields.append("author = %s")
        params.append(author)
    if device_id is not None:
        update_fields.append("device_id = %s")
        params.append(device_id)
    if img_list is not None:
        update_fields.append("img_list = %s")
        params.append(img_list)
    
    if not update_fields:
        return 0
    
    query = f"UPDATE xhs_note_templates SET {', '.join(update_fields)} WHERE id = %s AND userInfo = %s"
    params.extend([template_id, email])
    return execute_update(query, params)


def main_handler(event, context):
    """
    云函数入口函数，管理回复模板数据
    
    Args:
        event: 触发事件，包含操作参数
        context: 函数上下文
        
    Returns:
        JSON格式的操作结果
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
    action = params.get('action', '')
    email = params.get('email', 'zacks@example.com')  
    
    try:
        # 根据操作类型执行相应操作
        if action == 'add':
            # 添加单个模板
            title = params.get('title', '')
            content = params.get('content', '')
            author = params.get('author')
            device_id = params.get('device_id')
            img_list = params.get('img_list')
            
            if not title:
                return {
                    "code": 1,
                    "message": "笔记标题不能为空",
                    "data": None
                }
            
            if not content:
                return {
                    "code": 1,
                    "message": "笔记内容不能为空",
                    "data": None
                }
            
            affected_rows = add_xhs_note_templates(title, content, email, author, device_id, img_list)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "添加失败",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'add_batch':
            # 批量添加模板
            templates = params.get('templates', [])
            if not templates:
                return {
                    "code": 1,
                    "message": "模板列表不能为空",
                    "data": None
                }
            
            affected_rows = add_xhs_note_templatess(templates, email)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "批量添加失败",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'delete':
            # 删除指定ID的模板
            template_id = params.get('template_id')
            if not template_id:
                return {
                    "code": 1,
                    "message": "模板ID不能为空",
                    "data": None
                }
            
            affected_rows = delete_xhs_note_templates(template_id, email)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "删除失败",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'delete_all':
            # 删除用户的所有模板
            affected_rows = delete_all_xhs_note_templatess(email)
            return {
                "code": 0,
                "message": "success",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'update':
            # 更新模板内容
            template_id = params.get('template_id')
            title = params.get('title')
            content = params.get('content')
            author = params.get('author')
            device_id = params.get('device_id')
            img_list = params.get('img_list')
            
            if not template_id:
                return {
                    "code": 1,
                    "message": "模板ID不能为空",
                    "data": None
                }
            
            # 检查是否至少有一个字段需要更新
            if all(field is None for field in [title, content, author, device_id, img_list]):
                return {
                    "code": 1,
                    "message": "至少需要提供一个要更新的字段",
                    "data": None
                }
            
            affected_rows = update_xhs_note_templates(template_id, email, title, content, author, device_id, img_list)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "更新失败",
                "data": {
                    "affected_rows": affected_rows
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
        logger.error(f"操作失败: {str(e)}")
        return {
            "code": 1,
            "message": f"操作失败: {str(e)}",
            "data": None
        }


if __name__ == "__main__":
    # 本地测试用
    test_event = {
        'body': json.dumps({
            'action': 'add',
            'email': 'zacks@example.com',
            'title': '测试笔记标题',
            'content': '这是一个测试笔记内容',
            'author': '测试作者',
            'device_id': 'test_device_001',
            'img_list': '["image1.jpg", "image2.jpg"]'
        })
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))