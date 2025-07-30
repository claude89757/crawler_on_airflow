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

def get_reply_num(event, context):
    """
    获取回复数量统计和按时间分组的回复数量
    
    参数:
    - start_date: 可选，开始日期，格式为YYYY-MM-DD
    - end_date: 可选，结束日期，格式为YYYY-MM-DD
    - email: 可选，用户邮箱，用于过滤数据
    
    返回:
    - 包含回复统计数据的JSON对象，格式为：
      {
        "code": 0,
        "message": "success",
        "data": {
          "total": 总回复数,
          "replyTime": [
            {"time": "2023-01-01", "count": 5},
            {"time": "2023-01-02", "count": 10},
            ...
          ]
        }
      }
    """
    logger.info(f"收到请求: {json.dumps(event, ensure_ascii=False)}")
    
    try:
        # 解析请求参数
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
        
        start_date = query_params.get('start_date', None)
        end_date = query_params.get('end_date', None)
        email = query_params.get('email', None)
        
        # 连接数据库
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 构建查询条件
            where_clause = "1=1"
            params = []
            
            if start_date:
                where_clause += " AND DATE(replied_at) >= %s"
                params.append(start_date)
            
            if end_date:
                where_clause += " AND DATE(replied_at) <= %s"
                params.append(end_date)
            
            if email:
                # 直接使用userInfo字段进行过滤
                where_clause += " AND userInfo = %s"
                params.append(email)
            
            # 获取总回复数
            count_query = f"""
                SELECT COUNT(*) as total 
                FROM comment_manual_reply
                WHERE {where_clause}
            """
            
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()['total']
            
            # 按日期分组统计回复数
            time_stats_query = f"""
                SELECT 
                    DATE(replied_at) as reply_date,
                    COUNT(*) as count
                FROM comment_manual_reply
                WHERE {where_clause}
                GROUP BY DATE(replied_at)
                ORDER BY reply_date
            """
            
            cursor.execute(time_stats_query, params)
            time_stats = cursor.fetchall()
            
            # 处理日期时间格式，使其可JSON序列化
            time_stats_formatted = []
            for stat in time_stats:
                time_stats_formatted.append({
                    "time": stat['reply_date'].strftime('%Y-%m-%d'),
                    "count": stat['count']
                })
            
            # 构建返回结果
            response = {
                "code": 0,
                "message": "success",
                "data": {
                    "total": total_count,
                    "replyTime": time_stats_formatted
                }
            }
            
            return response
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"查询失败: {str(e)}")
        return {
            "code": 1,
            "message": f"查询失败: {str(e)}",
            "data": None
        }

def main_handler(event, context):
    """
    云函数入口
    """
    # 解析event
    if isinstance(event, dict):
        pass
    else:
        try:
            event = json.loads(event)
        except:
            return {
                "code": 1,
                "message": "无效的请求参数",
                "data": None
            }
    
    # 调用主函数
    result = get_reply_num(event, context)
    
    # 返回结果
    return result