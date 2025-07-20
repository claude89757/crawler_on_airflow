#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
获取小红书评论对应的意向

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


def get_comment_intents_by_ids(comment_ids, page=1, page_size=20):
    """
    根据comment_ids列表获取customer_intent表中对应的评论意向
    
    Args:
        comment_ids: 评论ID列表
        page: 页码，默认为1
        page_size: 每页数量，默认为1000
        
    Returns:
        tuple: (评论意向数据列表, 总数)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        print(f'comment_ids: {comment_ids}')
        # 过滤掉空值并转换为字符串
        valid_comment_ids = [int(cid) for cid in comment_ids if cid]
        
        if not valid_comment_ids:
            cursor.close()
            conn.close()
            return []
        
        # 计算偏移量
        offset = (page - 1) * page_size
        
        # 查询总数
        placeholders = ','.join(['%s'] * len(valid_comment_ids))
        count_query = f"SELECT COUNT(*) as total FROM customer_intent WHERE comment_id IN ({placeholders})"
        cursor.execute(count_query, valid_comment_ids)
        total_count = cursor.fetchone()['total']
        
        # 构建分页查询
        query = f"SELECT comment_id, intent FROM customer_intent WHERE comment_id IN ({placeholders}) ORDER BY comment_id LIMIT %s OFFSET %s"
        params = valid_comment_ids + [page_size, offset]
        cursor.execute(query, params)
        result = cursor.fetchall()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        # 创建结果字典，方便查找
        result_dict = {row['comment_id']: row['intent'] for row in result}
        
        # 构建完整结果，确保所有comment_ids都有对应的记录
        complete_result = []
        for comment_id in valid_comment_ids:
            if comment_id in result_dict:
                complete_result.append({
                    'comment_id': comment_id,
                    'intent': result_dict[comment_id]
                })
            else:
                complete_result.append({
                    'comment_id': comment_id,
                    'intent': '未分析'
                })
        
        # 应用分页到完整结果
        start_idx = offset
        end_idx = start_idx + page_size
        paginated_result = complete_result[start_idx:end_idx]
        
        # 处理datetime字段，转换为字符串格式
        for row in paginated_result:
            for key, value in row.items():
                if isinstance(value, datetime):
                    row[key] = value.strftime('%Y-%m-%d %H:%M:%S')
        
        return paginated_result, len(valid_comment_ids)
    except Exception as e:
        logger.error(f"获取评论意向失败: {str(e)}")
        return [], 0


def main_handler(event, context):
    """
    云函数入口函数，获取指定comment_ids的评论意向
    支持URL参数: 
    - comment_ids: 评论ID列表（必需）
    - page: 页码，默认为1
    - page_size: 每页数量，默认为1000
    示例: ${baseUrl}?comment_ids=["id1","id2","id3"]&page=1&page_size=100
    
    Args:
        event: 触发事件
        context: 函数上下文
        
    Returns:
        JSON格式的评论意向数据
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    try:
        # 从请求中获取comment_ids参数
        comment_ids = None
        
       
        if 'body' in event:
            print(event)
            try:
                # 尝试解析body为JSON
                if isinstance(event['body'], str):
                    query_params = json.loads(event['body'])
                    
                    comment_ids = query_params.get('comment_ids')
                else:
                    comment_ids = event['body'].get('comment_ids')
            except Exception as e:
                print(e)
        
        # 从请求中获取参数
        comment_ids = query_params.get('comment_ids')
        page = int(query_params.get('page', 1))
        page_size = int(query_params.get('page_size', 20))
        
        # 限制每页最大数量为500
        page_size = min(page_size, 500)
        
        # comment_ids参数是必需的
        if not comment_ids:
            return {
                'code': 1,
                'message': 'comment_ids参数不能为空',
                'data': []
            }
        
        # 如果comment_ids是字符串，尝试解析为列表
        if isinstance(comment_ids, str):
            try:
                comment_ids = json.loads(comment_ids)
            except:
                return {
                    'code': 1,
                    'message': 'comment_ids参数格式错误，应为JSON数组',
                    'data': []
                }
        
        # 确保comment_ids是列表
        if not isinstance(comment_ids, list):
            return {
                'code': 1,
                'message': 'comment_ids参数应为数组格式',
                'data': []
            }
        
        logger.info(f"获取comment_ids为 {comment_ids} 的评论意向，页码: {page}，每页数量: {page_size}")
        
        # 获取评论意向数据
        intents, total_count = get_comment_intents_by_ids(comment_ids, page, page_size)
        
        # 计算总页数
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
        
        # 构建响应
        response = {
            'code': 0,
            'message': 'success',
            'data': {
                'total': total_count,
                'page': page,
                'page_size': page_size,
                'total_pages': total_pages,
                'records': intents
            }
        }
        
        return response
    except Exception as e:
        logger.error(f"处理请求失败: {str(e)}")
        return {
            'code': 1,
            'message': f'获取评论意向失败: {str(e)}',
            'data': []
        }


if __name__ == "__main__":
    # 本地测试用
    test_event = {
        'queryString': {
            'comment_ids': ['12345', '67890', '11111'],
            'page': 1,
            'page_size': 20
        }
    }
    result = main_handler(test_event, {})
    print(json.dumps(result, ensure_ascii=False, indent=2))