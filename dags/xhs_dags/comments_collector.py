from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
import requests
import base64
from utils.xhs_appium import XHSOperator
import time
xhs = None
  
def get_note_url(keyword: str = None, **context):
    """从数据库获取笔记URL和关键词
    Args:
        keyword: 筛选的关键词
    Returns:
        包含note_url和keyword的字典列表
    """
    # 使用get_hook函数获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    # 根据是否有关键词来构建不同的SQL查询
    if keyword:
        cursor.execute("SELECT note_url, keyword FROM xhs_notes WHERE keyword = %s", (keyword,))
    else:
        cursor.execute("SELECT note_url, keyword FROM xhs_notes")
    results = [{'note_url': row[0], 'keyword': row[1]} for row in cursor.fetchall()]
    
    cursor.close()
    db_conn.close()
    
    return results

def get_author_by_url(note_url: str):
    """根据笔记URL从数据库获取作者
    Args:
        note_url: 笔记URL
    Returns:
        str: 作者名称，如果未找到则返回None
    """
    try:
        db_hook = BaseHook.get_connection("xhs_db").get_hook()
        db_conn = db_hook.get_conn()
        cursor = db_conn.cursor()
        
        # 查询笔记作者
        cursor.execute("SELECT author FROM xhs_notes WHERE note_url = %s", (note_url,))
        result = cursor.fetchone()
        
        cursor.close()
        db_conn.close()
        
        if result:
            return result[0]  # 返回作者名称
        return None
    except Exception as e:
        print(f"获取作者信息失败: {str(e)}")
        return None

def save_comments_to_db(comments: list, note_url: str, keyword: str = None, email: str = None):
    """保存评论到数据库
    Args:
        comments: 评论列表
        note_url: 笔记URL
        keyword: 笔记关键词
    """
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    try:
        # 检查表是否存在，如果不存在则创建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS xhs_comments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            author TEXT,
            userInfo TEXT,
            content TEXT,
            likes INT DEFAULT 0,
            note_url VARCHAR(512),
            keyword VARCHAR(255) NOT NULL,
            comment_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            collect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            location TEXT
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句
        insert_sql = """
        INSERT INTO xhs_comments 
        (note_url, author, userInfo, content, likes, keyword, comment_time, collect_time, location) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 批量插入评论数据
        insert_data = []
        for comment in comments:
            insert_data.append((
                note_url,
                comment.get('author', ''),
                email,  # 添加email信息到userInfo字段
                comment.get('content', ''),
                comment.get('likes', 0),
                keyword,
                comment.get('comment_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                comment.get('collect_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                comment.get('location', '')
            ))

        cursor.executemany(insert_sql, insert_data)

        # 更新xhs_notes表中的last_comments_collected_at字段
        update_sql = """
        UPDATE xhs_notes 
        SET last_comments_collected_at = NOW() 
        WHERE note_url = %s
        """
        cursor.execute(update_sql, (note_url,))
        
        db_conn.commit()
        
        print(f"成功保存 {len(comments)} 条评论到数据库，并更新笔记评论收集时间")
    except Exception as e:
        db_conn.rollback()
        print(f"保存评论到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()

def get_notes_by_url_list(note_urls: list, keyword: str = None, device_index: int = 0, email: str = None, max_comments: int = 10):
    """根据传入的笔记URL列表收集评论
    Args:
        note_urls: 笔记URL列表
        keyword: 关键词（可选）
        device_index: 设备索引
        email: 用户邮箱（用于查找设备信息）
    Returns:
        所有评论的列表
    """
    # 获取设备列表
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    
    # 根据email查找设备信息
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if device_info:
        print(f"device_info: {device_info}")
    else:
        raise ValueError("email参数不能为空")
    
    # 获取设备信息
    try:
        device_ip = device_info.get('device_ip')
        device_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
        
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")
    
    appium_server_url = f"http://{device_ip}:{device_port}"
    print(f"开始收集{len(note_urls)}条笔记的评论...")
    print(f"使用Appium服务器: {appium_server_url}")
    print(f"使用设备ID: {device_id}")
    

    try:
        # 初始化小红书操作器（带重试机制）
        global xhs
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)

        all_comments = []
        total_comments = 0
        for note_url in note_urls:
            try:
                # 收集评论
                if len(note_url) == 34:  # 链接长度34则为短链接
                    full_url = xhs.get_redirect_url(note_url)
                    print(f"处理笔记URL: {full_url}")
                else:
                    full_url = note_url  # 长链不需要处理直接使用

                # 获取笔记作者
                origin_author = get_author_by_url(note_url)
                print(f"获取到笔记作者: {origin_author if origin_author else '未知'}")
                
                # 收集评论，传递原作者信息
                comments = xhs.collect_comments_by_url(full_url, max_comments=max_comments, origin_author=origin_author)
                # 保存评论到数据库
                if comments:
                    save_comments_to_db(comments, note_url, keyword, email)
                    total_comments += len(comments)
                all_comments.extend(comments)
            except Exception as e:
                print(f"处理笔记 {note_url} 时出错: {str(e)}")
                continue
        
        print(f"成功收集 {total_comments} 条评论")
        return all_comments
    except Exception as e:
        print(f"运行出错: {str(e)}")
        raise e
    finally:
        if 'xhs' in locals() and xhs is not None:
            try:
                xhs.close()
            except Exception as e:
                print(f"关闭XHSOperator时出错: {e}")

def collect_xhs_comments(device_index: int = 0, **context):
    """收集小红书评论
    Args:
        device_index: 设备索引
        **context: Airflow上下文参数字典
    """
    print(f"dag_run_conf: {context['dag_run'].conf}")
    
    email = context['dag_run'].conf.get('email')
    keyword = context['dag_run'].conf.get('keyword')
    note_urls = context['dag_run'].conf.get('note_urls')
    max_comments = context['dag_run'].conf.get('max_comments', 10)  # 默认收集10条评论

    if not keyword and not note_urls:
        raise ValueError("keyword和note_urls参数不能同时为空")
    
    # 获取设备总数，用于分配URL
    device_info_list = Variable.get("XHS_DEVICE_INFO_LIST", default_var=[], deserialize_json=True)
    device_info = next((device for device in device_info_list if device.get('email') == email), None)
    if not device_info:
        print(f"跳过当前任务，因为找不到email为 {email} 的设备信息")
        raise AirflowSkipException("找不到设备信息")
    
    total_devices = len(device_info.get('phone_device_list', []))
    if total_devices == 0:
        print(f"跳过当前任务，因为没有可用的设备")
        raise AirflowSkipException("没有可用的设备")
    
    if note_urls:
        # 将URL列表分配给不同设备
        device_urls = distribute_urls(note_urls, device_index, total_devices)
        if not device_urls:
            print(f"设备索引 {device_index}: 没有分配到笔记URL，跳过")
            raise AirflowSkipException(f"设备索引 {device_index} 没有分配到笔记URL")
        
        print(f"设备索引 {device_index}: 分配到 {len(device_urls)} 个笔记URL进行收集")
        return get_notes_by_url_list(device_urls, keyword, device_index, email,max_comments)
    else:
        # 从数据库获取笔记URL和关键词
        notes_data = get_note_url(keyword)
        # 提取URL列表
        all_note_urls = [note['note_url'] for note in notes_data]
        # 分配URL给当前设备
        device_urls = distribute_urls(all_note_urls, device_index, total_devices)
        if not device_urls:
            print(f"设备索引 {device_index}: 没有分配到笔记URL，跳过")
            raise AirflowSkipException(f"设备索引 {device_index} 没有分配到笔记URL")
        
        print(f"设备索引 {device_index}: 分配到 {len(device_urls)} 个笔记URL进行收集")
        return get_notes_by_url_list(device_urls, keyword, device_index, email,max_comments)

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
    dag_id='comments_collector',
    default_args={
        'owner': 'yuchangongzhu', 
        'depends_on_past': False, 
        'start_date': datetime(2024, 1, 1)
    },
    description='收集小红书笔记评论',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=5,
) as dag:

    # 创建多个任务，每个任务使用不同的设备索引
    for index in range(10):
        PythonOperator(
            task_id=f'collect_xhs_comments_{index}',
            python_callable=collect_xhs_comments,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True,
            
        )
