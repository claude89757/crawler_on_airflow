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

def get_auto_result(event, context):
    """
    获取关键词下的笔记作者、评论内容、评论点赞数、客户意向和手动回复内容
    
    参数:
    - keyword: 必须，关键词
    - email: 可选，用户邮箱，用于过滤数据
    - page: 可选，页码，默认为1
    - page_size: 可选，每页记录数，默认为20，最大500
    
    返回:
    - 包含查询结果的JSON对象
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
        
        keyword = query_params.get('keyword', '')
        email = query_params.get('email', None)
        page = int(query_params.get('page', 1))
        page_size = int(query_params.get('page_size', 20))
        
        # 参数验证
        if not keyword:
            return {
                "code": 1,
                "message": "关键词不能为空",
                "data": None
            }
        
        # 处理关键字的空格
        keyword = keyword.strip()
        if keyword.startswith('"') and keyword.endswith('"'):
            keyword = keyword[1:-1]
        
        # 限制每页最大记录数为500
        page_size = min(page_size, 500)
        
        # 计算偏移量
        offset = (page - 1) * page_size
        
        # 连接数据库
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 构建查询条件
            where_clause = "n.keyword = %s"
            params = [keyword]
            
            if email:
                where_clause += " AND n.userInfo = %s"
                params.append(email)
            
            # 计算总记录数
            count_query = f"""
                SELECT COUNT(*) as total 
                FROM xhs_notes n
                LEFT JOIN xhs_comments c ON n.note_url = c.note_url
                LEFT JOIN customer_intent ci ON c.id = ci.comment_id
                WHERE {where_clause}
            """
            
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()['total']
            
            # 计算总页数
            total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 0
            
            # 构建主查询
            if email:
                # 普通用户查询
                query = f"""
                    SELECT 
                        c.author as author,
                        c.content as comment_content,
                        c.likes as comment_likes,
                        c.comment_time as comment_time,
                        ci.intent as intent,
                        IFNULL(cmr.reply, '未回复') as reply_content
                    FROM xhs_notes n
                    LEFT JOIN xhs_comments c ON n.note_url = c.note_url
                    LEFT JOIN customer_intent ci ON c.id = ci.comment_id
                    LEFT JOIN comment_manual_reply cmr ON ci.comment_id = cmr.comment_id
                    WHERE {where_clause}
                    LIMIT %s OFFSET %s
                """
            else:
                # 管理员查询
                query = f"""
                    SELECT 
                        c.userInfo as email,
                        c.author as author,
                        c.content as comment_content,
                        c.likes as comment_likes,
                        c.comment_time as comment_time,
                        ci.intent as intent,
                        IFNULL(cmr.reply, '未回复') as reply_content
                    FROM xhs_notes n
                    LEFT JOIN xhs_comments c ON n.note_url = c.note_url
                    LEFT JOIN customer_intent ci ON c.id = ci.comment_id
                    LEFT JOIN comment_manual_reply cmr ON ci.comment_id = cmr.comment_id
                    WHERE {where_clause}
                    LIMIT %s OFFSET %s
                """
            
            # 添加分页参数
            params.extend([page_size, offset])
            
            # 执行查询
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # 处理日期时间格式，使其可JSON序列化
            for result in results:
                for key, value in result.items():
                    if isinstance(value, datetime):
                        result[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            
            # 构建返回结果
            response = {
                "code": 0,
                "message": "success",
                "data": {
                    "total": total_count,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "records": results
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
    result = get_auto_result(event, context)
    
    # 返回结果
    return result
