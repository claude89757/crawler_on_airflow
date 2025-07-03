#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
管理任务模板数据（增、删、改）

Author: by cursor
Date: 2025-07-03
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


def add_task_template(template_data):
    """
    添加任务模板
    
    Args:
        template_data: 模板数据字典
        
    Returns:
        int: 受影响的行数
    """
    # 处理JSON字段
    if 'template_ids' in template_data and template_data['template_ids']:
        if isinstance(template_data['template_ids'], list):
            template_data['template_ids'] = json.dumps(template_data['template_ids'])
    
    if 'intent_type' in template_data and template_data['intent_type']:
        if isinstance(template_data['intent_type'], list):
            template_data['intent_type'] = json.dumps(template_data['intent_type'])
    
    # 构建SQL语句和参数
    fields = list(template_data.keys())
    placeholders = ['%s'] * len(fields)
    
    query = f"INSERT INTO task_template ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
    params = tuple(template_data.values())
    
    return execute_update(query, params)


def add_task_templates(templates, userInfo):
    """
    批量添加任务模板
    
    Args:
        templates: 模板数据列表
        userInfo: 用户邮箱
        
    Returns:
        int: 受影响的行数
    """
    if not templates:
        return 0
    
    # 确保每个模板都有userInfo字段
    for template in templates:
        if 'userInfo' not in template:
            template['userInfo'] = userInfo
        
        # 处理JSON字段
        if 'template_ids' in template and template['template_ids']:
            if isinstance(template['template_ids'], list):
                template['template_ids'] = json.dumps(template['template_ids'])
        
        if 'intent_type' in template and template['intent_type']:
            if isinstance(template['intent_type'], list):
                template['intent_type'] = json.dumps(template['intent_type'])
    
    # 假设所有模板具有相同的字段
    fields = list(templates[0].keys())
    placeholders = ['%s'] * len(fields)
    
    query = f"INSERT INTO task_template ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
    data = [tuple(template.values()) for template in templates]
    
    return insert_many(query, data)


def delete_task_template(template_id, userInfo):
    """
    删除指定ID的任务模板
    
    Args:
        template_id: 模板ID
        userInfo: 用户邮箱
        
    Returns:
        int: 受影响的行数
    """
    query = "DELETE FROM task_template WHERE id = %s AND userInfo = %s"
    params = (template_id, userInfo)
    return execute_update(query, params)


def delete_all_task_templates(userInfo):
    """
    删除用户的所有任务模板
    
    Args:
        userInfo: 用户邮箱
        
    Returns:
        int: 受影响的行数
    """
    query = "DELETE FROM task_template WHERE userInfo = %s"
    params = (userInfo,)
    return execute_update(query, params)


def update_task_template(template_id, template_data, userInfo):
    """
    更新指定ID的任务模板
    
    Args:
        template_id: 模板ID
        template_data: 新的模板数据
        userInfo: 用户邮箱
        
    Returns:
        int: 受影响的行数
    """
    # 处理JSON字段
    if 'template_ids' in template_data and template_data['template_ids']:
        if isinstance(template_data['template_ids'], list):
            template_data['template_ids'] = json.dumps(template_data['template_ids'])
    
    if 'intent_type' in template_data and template_data['intent_type']:
        if isinstance(template_data['intent_type'], list):
            template_data['intent_type'] = json.dumps(template_data['intent_type'])
    
    # 构建SET部分
    set_clause = ", ".join([f"{field} = %s" for field in template_data.keys()])
    
    # 构建完整的SQL语句
    query = f"UPDATE task_template SET {set_clause} WHERE id = %s AND userInfo = %s"
    
    # 构建参数元组
    params = tuple(template_data.values()) + (template_id, userInfo)
    
    return execute_update(query, params)


def main_handler(event, context):
    """
    云函数入口函数，管理任务模板数据
    
    Args:
        event: 触发事件，包含操作参数
        context: 函数上下文
        
    Returns:
        JSON格式的操作结果
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    # 解析请求参数
    params = {}
    if 'body' in event:
        try:
            if isinstance(event['body'], str):
                params = json.loads(event['body'])
            else:
                params = event['body']
        except:
            pass
    elif 'queryString' in event:
        params = event['queryString']
    
    # 打印参数信息
    print(f"Request parameters: {json.dumps(params, ensure_ascii=False)}")
    logger.info(f"Request parameters: {json.dumps(params, ensure_ascii=False)}")
    
    try:
        # 获取操作类型
        action = params.get('action')
        if not action:
            return {
                "code": 1,
                "message": "缺少操作类型参数",
                "data": None
            }
        
        # 获取用户邮箱
        userInfo = params.get('userInfo')
        if not userInfo and 'email' in params:
            userInfo = params.get('email')
            
        if not userInfo:
            return {
                "code": 1,
                "message": "缺少用户邮箱参数",
                "data": None
            }
        
        # 根据操作类型执行相应的操作
        if action == 'add':
            # 添加单个模板
            template_data = params.copy()
            # 移除操作类型参数
            template_data.pop('action', None)
            
            # 确保必要字段存在
            if 'keyword' not in template_data or not template_data['keyword']:
                return {
                    "code": 1,
                    "message": "关键词不能为空",
                    "data": None
                }
            
            affected_rows = add_task_template(template_data)
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
            
            affected_rows = add_task_templates(templates, userInfo)
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
            
            affected_rows = delete_task_template(template_id, userInfo)
            return {
                "code": 0 if affected_rows > 0 else 1,
                "message": "success" if affected_rows > 0 else "删除失败",
                "data": {
                    "affected_rows": affected_rows
                }
            }
            
        elif action == 'delete_all':
            # 删除用户的所有模板
            affected_rows = delete_all_task_templates(userInfo)
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
            if not template_id:
                return {
                    "code": 1,
                    "message": "模板ID不能为空",
                    "data": None
                }
            
            # 创建更新数据字典
            template_data = params.copy()
            # 移除不需要的字段
            template_data.pop('action', None)
            template_data.pop('template_id', None)
            template_data.pop('userInfo', None)
            template_data.pop('email', None)
            
            if not template_data:
                return {
                    "code": 1,
                    "message": "更新数据不能为空",
                    "data": None
                }
            
            affected_rows = update_task_template(template_id, template_data, userInfo)
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
            'userInfo': 'zacks@example.com',
            'keyword': '网球',
            'max_notes': 20,
            'max_comments': 15,
            'note_type': '图文',
            'time_range': '一周内',
            'search_scope': '全站',
            'sort_by': '综合',
            'profile_sentence': '意向分析示例',
            'template_ids': [1, 2, 3],
            'intent_type': ['购买', '咨询']
        })
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))