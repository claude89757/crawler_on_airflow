from datetime import datetime
import time
import random
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

from utils.xhs_utils import cos_to_device_via_host

from utils.xhs_appium import XHSOperator

def get_reply_templates_from_db(email=None):
    """从数据库获取回复模板
    
    Args:
        email: 用户邮箱，如果指定则只获取该用户的模板，否则获取所有模板
        
    Returns:
        包含模板内容和图片URL的字典列表，每个字典包含content和image_urls字段
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    # 查询回复模板
    if email:
        print(f"根据用户邮箱 {email} 查询模板")
        cursor.execute("SELECT id,userInfo, content, image_urls FROM reply_template WHERE userInfo = %s", (email,))
        templates_data = cursor.fetchall()
        templates = [{"content": row[1], "image_urls": row[2]} for row in templates_data]
    else:
        print("查询所有模板")
        cursor.execute("SELECT id,userInfo, content, image_urls FROM reply_template")
        templates_data = cursor.fetchall()
        templates = [{"content": row[1], "image_urls": row[2]} for row in templates_data]

    cursor.close()
    db_conn.close()

    if not templates:
        print("警告：未找到回复模板，请确保reply_template表中有数据")
        # 返回一个默认模板，避免程序崩溃
        return [{"content": "谢谢您的评论，我们会继续努力！", "image_urls": None}]
    
    print(f"成功获取 {len(templates)} 条回复模板")
    return templates

def get_reply_contents_from_db(comment_ids: list, max_comments: int = 10):
    """从xhs_comments表获取需要回复的评论
    Args:
        comment_ids: 评论ID列表，对应xhs_comments表中的id字段
        max_comments: 本次要回复的评论数量
    Returns:
        包含note_url的字典列表，其中comment_id对应xhs_comments表的id
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    # 确保comment_manual_reply表存在（用于后续插入回复记录）
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment_manual_reply (
        id INT AUTO_INCREMENT PRIMARY KEY,
        comment_id INT NOT NULL,
        note_url VARCHAR(512),
        author VARCHAR(255),
        userInfo TEXT,
        content TEXT,
        reply TEXT NOT NULL,
        replied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_comment (comment_id)
    )
    """)
    db_conn.commit()

    # 从xhs_comments表中获取评论记录
    # 如果ids列表不为空，添加ID限制条件
    if comment_ids:
        placeholders = ','.join(['%s'] * len(comment_ids))
        cursor.execute(
            f"SELECT id, note_url, author, userInfo, content FROM xhs_comments WHERE id IN ({placeholders})",
            comment_ids
        )
    else:
        cursor.execute(
            "SELECT id, note_url, author, userInfo, content FROM xhs_comments LIMIT %s",
            (max_comments,)
        )

    results = [{'comment_id': row[0], 'note_url': row[1], 'author': row[2], 'userInfo': row[3], 'content': row[4]} for row in cursor.fetchall()]

    cursor.close()
    db_conn.close()

    return results

def insert_manual_reply(comment_id: int, note_url: str, author: str, userInfo: str, content: str, reply: str):
    """将回复记录插入到comment_manual_reply表
    Args:
        comment_id: 评论ID
        note_url: 笔记URL
        author: 评论作者
        userInfo: 用户信息（邮箱）
        content: 评论内容
        reply: 回复内容
    """
    try:
        # 使用get_hook函数获取数据库连接
        db_hook = BaseHook.get_connection("xhs_db").get_hook()
        db_conn = db_hook.get_conn()
        cursor = db_conn.cursor()
        
        # 确保comment_manual_reply表存在
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS comment_manual_reply (
            id INT AUTO_INCREMENT PRIMARY KEY,
            comment_id INT NOT NULL,
            note_url VARCHAR(512),
            author VARCHAR(255),
            userInfo TEXT,
            content TEXT,
            reply TEXT NOT NULL,
            replied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_comment (comment_id)
        )
        """)
        db_conn.commit()
        
        # 插入回复记录
        cursor.execute(
            """
            INSERT INTO comment_manual_reply 
            (comment_id, note_url, author, userInfo, content, reply) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            note_url = VALUES(note_url),
            author = VALUES(author),
            userInfo = VALUES(userInfo),
            content = VALUES(content),
            reply = VALUES(reply),
            replied_at = CURRENT_TIMESTAMP
            """,
            (comment_id, note_url, author, userInfo, content, reply)
        )
        
        # 提交事务
        db_conn.commit()
        print(f"成功插入评论 {comment_id} 的回复记录到comment_manual_reply表")
        
    except Exception as e:
        print(f"插入回复记录失败: {str(e)}")
        if 'db_conn' in locals():
            db_conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db_conn' in locals():
            db_conn.close()

# 移除了对comment_reply表的更新操作

def reply_with_template(comments_to_process:list, device_index: int = 0,email: str = None,id_list: list = None):       
    """使用模板自动回复评论
    Args:
        comments_to_process: 需要回复的评论列表
        device_index: 设备索引
        email：用户邮箱（用于查找设备信息）
    """
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 根据email查找设备信息
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if device_info:
        print(f"device_info: {device_info}")
    else:
        raise ValueError("email参数不能为空或设备信息不存在")
    
    # 获取设备信息
    try:
        device_ip = device_info.get('device_ip')
        host_port=device_info.get('port')
        device_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
        username = device_info.get('username')
        password = device_info.get('password')
        appium_server_url = f"http://{device_ip}:{device_port}"
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")
    
    print(f"使用Appium服务器: {appium_server_url}")
    print(f"使用设备ID: {device_id}")
    print(f"准备处理 {len(comments_to_process)} 条评论")
    
    # 初始化小红书操作器
    xhs = None
    successful_replies = 0
    failed_replies = 0
    # 使用email参数获取用户的回复模板
    reply_templates = get_reply_templates_from_db(email=email)
    if id_list is None or not id_list:
        reply_templates=[item for item in reply_templates if item["id"] in id_list]
    try:
        # 初始化小红书操作器（带重试机制）
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        
        # 处理每条分配的评论
        previous_url = None  # 跟踪上一个处理的URL
        for i, comment in enumerate(comments_to_process):
            try:
                note_url = comment['note_url']
                author = comment['author']
                comment_content = comment['content']
                comment_id = comment['comment_id']
                
                print(f"设备 {device_id} 正在处理第 {i+1}/{len(comments_to_process)} 条评论 - 作者: {author}")
                
                #随机选择一条回复模板
                reply_template = random.choice(reply_templates)
                print(reply_templates)
                reply_content = reply_template['content']
                image_urls = reply_template['image_urls']
                has_image=image_urls is not None and image_urls != "null" and image_urls!=""
                print(f"选择的回复模板: {reply_content}, has_image: {has_image}, image_urls: {image_urls}")
                
                
                # 判断是否需要跳过URL打开（如果与上一个URL相同）
                skip_url_open = (previous_url == note_url)
                if skip_url_open:
                    print(f"检测到相同URL，跳过重新打开: {note_url}在当前笔记继续搜索待回复评论")
                else:
                    print(f"新的URL，需要重新打开: {note_url}")
                
                if has_image:
                    print('图片url:',image_urls)
                    cos_to_device_via_host(cos_url=image_urls,host_address=device_ip,host_username=username,device_id=device_id,host_password=password,host_port=host_port)
                
                # 调用评论回复功能
                success = xhs.comments_reply(
                    note_url=note_url,
                    author=author,
                    comment_content=comment_content,
                    reply_content=reply_content,
                    has_image=has_image,
                    skip_url_open=skip_url_open
                )
                
                # 更新上一个URL
                previous_url = note_url
                
                if success:
                    print(f"设备 {device_id} 成功回复评论: {comment_content}")
                    # 插入到manual_reply表
                    try:
                        # 使用DAG配置中的email作为userInfo
                        insert_manual_reply(
                            comment_id=comment_id,
                            note_url=note_url,
                            author=author,
                            userInfo=email,
                            content=comment_content,
                            reply=reply_content
                        )
                    except Exception as db_err:
                        print(f"插入回复记录到数据库失败: {str(db_err)}")
                    
                    successful_replies += 1
                else:
                    print(f"设备 {device_id} 回复评论失败: {comment_content}")
                    failed_replies += 1
                
                # 添加延时，避免操作过快
                time.sleep(2)  # 增加延时，确保有足够时间处理下一条评论
                
            except Exception as e:
                print(f"设备 {device_id} 处理评论时出错: {str(e)}")
                failed_replies += 1
                # 出错后等待时间稍长一些，避免连续失败
                time.sleep(3)
                continue
        
        print(f"设备 {device_id} 完成评论回复任务，成功回复: {successful_replies}/{len(comments_to_process)}，失败: {failed_replies}")
        return successful_replies
        
    except Exception as e:
        print(f"运行任务出错: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return successful_replies  # 即使整体任务出错，也返回已成功处理的评论数
    finally:
        # 确保关闭小红书操作器
        if xhs is not None:
            print(f"关闭设备 {device_id} 的控制器")
            try:
                xhs.close()
            except Exception as close_err:
                print(f"关闭设备控制器出错: {str(close_err)}")

def reply_xhs_comments(device_index: int = 0, **context):
    """回复小红书评论
    Args:
        device_index: 设备索引
        **context: Airflow上下文参数字典
    """
    print(f"dag_run_conf: {context['dag_run'].conf}")
    
    # 从DAG运行配置中获取参数，如果没有则使用默认值
    email = context['dag_run'].conf.get('email')
    comment_ids = context['dag_run'].conf.get('comment_ids') 
    max_comments = context['dag_run'].conf.get('max_comments') 
    id_list = context['dag_run'].conf.get('id_list', [])
    if not email:
        raise ValueError("email参数不能为空")
    
   
    
    # 获取设备列表以确定实际可用的设备数量
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    
    if not device_info:
        print(f"跳过当前任务，因为找不到email为 {email} 的设备信息")
        raise AirflowSkipException("找不到设备信息")
        
    # 确定实际可用的设备数量
    available_devices = len(device_info.get('phone_device_list', []))
    print(f"可用设备数量: {available_devices}")
    # 获取评论内容
    initial_contents = get_reply_contents_from_db(comment_ids=comment_ids, max_comments=max_comments)
    if available_devices == 0:
        print(f"跳过当前任务，因为没有可用的设备")
        raise AirflowSkipException("没有可用的设备")
    if initial_contents:
        # 将评论列表分配给不同设备
        device_urls = distribute_urls(initial_contents, device_index, available_devices)
        if not device_urls:
            print(f"设备索引 {device_index}: 没有分配到评论，跳过")
            raise AirflowSkipException(f"设备索引 {device_index} 没有分配到评论")

        print(f"设备索引 {device_index}: 分配到 {len(device_urls)} 个评论进行回复")
        return reply_with_template(device_urls, device_index, email,id_list)
    else:
        # 从数据库获取评论内容
        comments_data = get_reply_contents_from_db(comment_ids=comment_ids, max_comments=max_comments)
        # 提取评论ID列表
        all_comment_ids = [comment['comment_id'] for comment in comments_data]
        # 分配评论ID给当前设备
        device_urls = distribute_urls(all_comment_ids, device_index, available_devices)
        if not device_urls:
            print(f"设备索引 {device_index}: 没有分配到评论，跳过")
            raise AirflowSkipException(f"设备索引 {device_index} 没有分配到评论")

        print(f"设备索引 {device_index}: 分配到 {len(device_urls)} 个评论进行回复")
        return reply_with_template(device_urls, device_index, email,id_list)


def distribute_urls(urls: list, device_index: int, total_devices: int) -> list:
    """将URL列表分配给特定设备
    Args:
        urls: 所有URL列表
        device_index: 当前设备索引
        total_devices: 设备总数
    Returns:
        分配给当前设备的URL列表
    """
    if not urls or total_devices <= 0:
        return []
    
    # 计算每个设备应处理的URL数量
    urls_per_device = len(urls) // total_devices
    remainder = len(urls) % total_devices
    
    # 计算当前设备的起始和结束索引
    start_index = device_index * urls_per_device + min(device_index, remainder)
    # 如果设备索引小于余数，则多分配一个URL
    end_index = start_index + urls_per_device + (1 if device_index < remainder else 0)
    
    # 返回分配给当前设备的URL
    return urls[start_index:end_index]

# DAG 定义
with DAG(
    dag_id='xhs_comments_template_replier',
    default_args={
        'owner': 'yuchangongzhu',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1)
    },
    description='使用模板自动回复评论',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=5,
) as dag:


    # 创建多个任务，每个任务使用不同的设备索引
    for index in range(10):
        PythonOperator(
            task_id=f'reply_with_template_{index}',
            python_callable=reply_xhs_comments,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True
            
        )