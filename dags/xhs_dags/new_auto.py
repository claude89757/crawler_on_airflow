#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
import requests
import base64
from utils.xhs_appium import XHSOperator
from utils.xhs_utils import cos_to_device_via_host
import time
import json
import random
from appium.webdriver.common.appiumby import AppiumBy

# 全局变量
xhs = None

# 回复评论相关函数
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
        templates = [{"id":row[0],"content": row[2], "image_urls": row[3]} for row in templates_data]
    else:
        print("查询所有模板")
        cursor.execute("SELECT id,userInfo, content, image_urls FROM reply_template")
        templates_data = cursor.fetchall()
        templates = [{"id":row[0],"content": row[2], "image_urls": row[3]} for row in templates_data]

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


# 评论意向分析相关函数
def analyze_comment_intent(comment_content, profile_sentence):
    """
    使用OpenRouter API（Deepseek模型）分析单条评论的用户意向
    
    :param comment_content: 评论内容
    :param profile_sentence: 行业及客户定位描述
    :return: 意向等级（"高意向"、"中意向"、"低意向"）
    """
    # 构建prompt
    prompt = f"""
你是一个专业的用户意向分析师。请根据以下信息分析用户的购买意向：

**角色**：专业的用户意向分析师
**目标**：准确判断用户基于评论内容的购买意向等级
**指令**：分析用户评论，判断其对产品/服务的购买意向

**推理步骤**：
1. 仔细阅读用户评论内容
2. 结合行业背景和客户定位进行分析
3. 识别关键意向信号（询价、咨询、表达需求等）
4. 综合判断意向等级

**输出格式**：只输出以下三个选项之一
- 高意向
- 中意向  
- 低意向

**上下文信息**：
行业及客户定位：{profile_sentence}
用户评论：{comment_content}

请直接输出意向等级，不要包含其他内容：
"""
    
    # API配置
    api_url = "https://openrouter.ai/api/v1/chat/completions"
    api_key = Variable.get("OPENROUTER_API_KEY", default_var="")
    
    if not api_key:
        print("警告: 未找到OpenRouter API密钥，返回默认意向")
        return "中意向"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "deepseek/deepseek-chat",
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.1,
        "max_tokens": 50
    }
    
    # 重试机制
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(api_url, headers=headers, json=data, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                intent = result['choices'][0]['message']['content'].strip()
                
                # 验证返回结果
                valid_intents = ["高意向", "中意向", "低意向"]
                if intent in valid_intents:
                    return intent
                else:
                    print(f"API返回了无效的意向等级: {intent}，使用默认值")
                    return "中意向"
            else:
                print(f"API请求失败，状态码: {response.status_code}，响应: {response.text}")
                if attempt < max_retries - 1:
                    print(f"第{attempt + 1}次重试...")
                    time.sleep(2 ** attempt)  # 指数退避
                    continue
                else:
                    return "中意向"
        except Exception as e:
            print(f"处理评论时发生异常: {str(e)}")
            return "中意向"  # 异常情况下的默认结果

def analyze_comments_intent(comments, profile_sentence):
    """
    批量分析评论意向
    
    :param comments: 评论列表
    :param profile_sentence: 行业及客户定位描述
    :return: 包含意向分析结果的评论列表
    """
    print(f"[意向分析] 开始批量分析评论意向，共 {len(comments)} 条评论")
    print(f"[意向分析] 使用的客户定位描述: {profile_sentence}")
    
    results = []
    processed_count = 0
    empty_count = 0
    error_count = 0
    
    for i, comment in enumerate(comments, 1):
        try:
            content = comment.get('content', '')
            print(f"[意向分析] 处理第 {i}/{len(comments)} 条评论")
            
            if not content.strip():
                # 空评论默认为低意向
                comment['intent'] = '低意向'
                empty_count += 1
                print(f"[意向分析] 第 {i} 条评论内容为空，设置为低意向")
            else:
                print(f"[意向分析] 第 {i} 条评论内容: {content[:50]}{'...' if len(content) > 50 else ''}")
                # 分析评论意向
                intent = analyze_comment_intent(content, profile_sentence)
                comment['intent'] = intent
                print(f"[意向分析] 第 {i} 条评论分析结果: {intent}")
            
            results.append(comment)
            processed_count += 1
            
        except Exception as e:
            error_count += 1
            print(f"[意向分析] 第 {i} 条评论分析时出错: {str(e)}")
            print(f"[意向分析] 错误评论内容: {comment.get('content', 'N/A')}")
            comment['intent'] = '中意向'  # 出错时的默认值
            results.append(comment)
    
    print(f"[意向分析] 批量分析完成 - 总计: {len(comments)}, 成功: {processed_count}, 空评论: {empty_count}, 错误: {error_count}")
    return results

def save_analysis_results_to_db(results, profile_sentence):
    """
    将分析结果保存到数据库的customer_intent表中
    
    :param results: 分析结果列表，每个元素包含评论信息和意向分析
    :param profile_sentence: 用于分析的行业及客户定位描述
    :return: 成功保存的记录数
    """
    # 使用Airflow的BaseHook获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    
    saved_count = 0
    errors = 0
    
    try:
        # 确保customer_intent表存在
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_intent (
            id INT AUTO_INCREMENT PRIMARY KEY,
            comment_id INT NOT NULL,
            author VARCHAR(255),
            userInfo TEXT,
            note_url VARCHAR(512),
            intent VARCHAR(50) NOT NULL,
            profile_sentence TEXT,
            keyword VARCHAR(255) NOT NULL,
            content TEXT NOT NULL,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_comment (comment_id)
        )
        """)
        db_conn.commit()
        
        # 插入或更新记录
        for result in results:
            try:
                comment_id = result.get('id')
                if not comment_id:
                    print(f"警告: 跳过没有ID的评论记录")
                    continue
                
                # 使用INSERT...ON DUPLICATE KEY UPDATE确保更新已存在的记录
                query = """
                INSERT INTO customer_intent 
                (comment_id, author, userInfo, note_url, intent, profile_sentence, keyword, content)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                author = VALUES(author),
                userInfo = VALUES(userInfo),
                note_url = VALUES(note_url),
                intent = VALUES(intent),
                profile_sentence = VALUES(profile_sentence),
                keyword = VALUES(keyword),
                content = VALUES(content),
                analyzed_at = CURRENT_TIMESTAMP
                """
                
                # 准备参数
                params = (
                    comment_id,
                    result.get('author', ''),
                    result.get('userInfo', ''),
                    result.get('note_url', ''),
                    result.get('intent', '未知'),
                    profile_sentence,
                    result.get('keyword', ''),
                    result.get('content', '')
                )
                
                # 执行插入/更新
                cursor.execute(query, params)
                saved_count += 1
                
            except Exception as e:
                print(f"保存评论ID {result.get('id', 'unknown')} 时出错: {str(e)}")
                errors += 1
        
        # 提交事务
        db_conn.commit()
        print(f"成功保存 {saved_count} 条记录到customer_intent表，{errors} 条失败")
        
    except Exception as e:
        db_conn.rollback()
        print(f"数据库操作失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()
    
    return saved_count

def save_note_to_db(note):
    """
    保存单条笔记到数据库
    """
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()

    try:
        # 检查表是否存在，如果不存在则创建
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS xhs_notes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            keyword TEXT,
            title TEXT NOT NULL,
            author TEXT,
            userInfo TEXT,
            content TEXT,
            likes INT DEFAULT 0,
            collects INT DEFAULT 0,
            comments INT DEFAULT 0,
            note_url VARCHAR(512) DEFAULT NULL,
            collect_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            note_type TEXT,
            note_location TEXT,
            last_comments_collected_at TIMESTAMP NULL
        )
        """)
        db_conn.commit()
        
        # 准备插入数据的SQL语句 - 使用INSERT IGNORE避免重复插入
        insert_sql = """
        INSERT IGNORE INTO xhs_notes
        (keyword, title, author, userInfo, content, likes, collects, comments, note_url, collect_time, note_time, note_type, note_location)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 获取URL - 优先使用note_url，如果不存在则尝试使用video_url
        note_url = note.get('note_url', note.get('video_url', ''))
        
        insert_data = (
            note.get('keyword', ''),
            note.get('title', ''),
            note.get('author', ''),
            note.get('userInfo', ''),
            note.get('content', ''),
            note.get('likes', 0),
            note.get('collects', 0),
            note.get('comments', 0),
            note_url,  # 使用统一处理后的URL
            note.get('collect_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            note.get('note_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
            note.get('note_type', ''),
            note.get('note_location', ''),
        )

        # 执行插入操作
        cursor.execute(insert_sql, insert_data)
        
        # 获取实际插入的记录数
        affected_rows = cursor.rowcount
        
        db_conn.commit()
        
        if affected_rows > 0:
            print(f"成功保存笔记到数据库: {note.get('title', '')}")
            return True
        else:
            print(f"笔记已存在，跳过: {note.get('title', '')}")
            return False
        
    except Exception as e:
        db_conn.rollback()
        print(f"保存笔记到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()

def save_comments_to_db(comments, note_url, keyword=None, email=None):
    """
    保存评论到数据库
    Args:
        comments: 评论列表
        note_url: 笔记URL
        keyword: 笔记关键词
        email: 用户邮箱
    Returns:
        list: 插入的评论ID列表
    """
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()
    comment_ids = []

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
        
        # 逐条插入评论数据以获取每条记录的ID
        for comment in comments:
            insert_data = (
                note_url,
                comment.get('author', ''),
                email,  # 添加email信息到userInfo字段
                comment.get('content', ''),
                comment.get('likes', 0),
                keyword,
                comment.get('comment_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                comment.get('collect_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                comment.get('location', '')
            )
            
            cursor.execute(insert_sql, insert_data)
            # 获取刚插入记录的ID
            comment_id = cursor.lastrowid
            comment_ids.append(comment_id)

        # 更新xhs_notes表中的last_comments_collected_at字段
        update_sql = """
        UPDATE xhs_notes 
        SET last_comments_collected_at = NOW() 
        WHERE note_url = %s
        """
        cursor.execute(update_sql, (note_url,))
        
        db_conn.commit()
        
        print(f"成功保存 {len(comments)} 条评论到数据库，ID: {comment_ids}")
        
        return comment_ids
        
    except Exception as e:
        db_conn.rollback()
        print(f"保存评论到数据库失败: {str(e)}")
        raise
    finally:
        cursor.close()
        db_conn.close()


def collect_note_and_comments_immediately(xhs, note_card, keyword, email, max_comments, profile_sentence, collected_titles):
    """
    进入笔记后立即收集该笔记的评论和意向分析
    
    Args:
        xhs: XHSOperator实例
        note_card: 笔记卡片元素
        keyword: 搜索关键词
        email: 用户邮箱
        max_comments: 最大评论数
        profile_sentence: 意向分析用的行业描述
        collected_titles: 已收集的笔记标题列表
    
    Returns:
        dict: 包含笔记信息、评论数量、评论ID和意向分析结果的字典
    """
    try:
        # 获取笔记标题和作者
        title_element = note_card.find_element(
            by=AppiumBy.XPATH,
            value=".//android.widget.TextView[contains(@text, '')]"
        )
        note_title_and_text = title_element.text
        
        author_element = note_card.find_element(
            by=AppiumBy.XPATH,
            value=".//android.widget.LinearLayout/android.widget.TextView[1]"
        )
        author = author_element.text
        
        # 检查是否已收集过该笔记
        if note_title_and_text in collected_titles:
            print(f"笔记已收集过，跳过: {note_title_and_text}")
            return None
        
        print(f"开始处理笔记: {note_title_and_text}, 作者: {author}")
        
        # 检查元素位置并点击
        try:
            screen_size = xhs.driver.get_window_size()
            element_location = title_element.location
            screen_height = screen_size['height']
            element_y = element_location['y']
            
            # 检查元素是否位于屏幕合适位置
            if element_y > screen_height * 0.25:
                print(f"元素位置正常，位于屏幕{element_y/screen_height:.2%}处，执行点击")
                title_element.click()
                time.sleep(1)  # 等待页面加载
            else:
                print(f"元素位置过高，位于屏幕{element_y/screen_height:.2%}处，跳过")
                return None
        except Exception as e:
            print(f"检测元素位置失败: {str(e)}")
            return None
        
        # 获取笔记详细数据
        note_data = xhs.get_note_data(note_title_and_text)
        # if not note_data:
        #     print(f"获取笔记数据失败: {note_title_and_text}")
        #     # 返回上一页
        #     try:
        #         back_btn = xhs.driver.find_element(
        #             by=AppiumBy.XPATH,
        #             value="//android.widget.Button[@content-desc='返回']"
        #         )
        #         back_btn.click()
        #         time.sleep(0.5)
        #     except:
        #         pass
        #     return None
        
        # 添加关键词和用户信息
        note_data['keyword'] = keyword
        note_data['userInfo'] = email
        
        # 保存笔记到数据库
        note_saved = save_note_to_db(note_data)
        if not note_saved:
            print(f"笔记已存在于数据库中: {note_title_and_text}")
        
        # 立即收集该笔记的评论
        print(f"开始收集笔记评论，最大数量: {max_comments}")
        comments = []
        comment_ids = []
        analysis_results = []
        
        try:
            # 收集评论
            comments = xhs.collect_comments_by_url('', max_comments=max_comments)
            
            if comments:
                # 保存评论到数据库
                comment_ids = save_comments_to_db(comments, note_data['note_url'], keyword, email)
                print(f"成功收集并保存 {len(comments)} 条评论，ID: {comment_ids}")
                
                # 如果提供了profile_sentence，进行意向分析
                if  comment_ids:
                    print(f"开始进行意向分析...")
                    # 为评论添加ID信息以便分析
                    for i, comment in enumerate(comments):
                        if i < len(comment_ids):
                            comment['id'] = comment_ids[i]
                            comment['note_url'] = note_data['note_url']
                            comment['keyword'] = keyword
                            comment['userInfo'] = email
                    
                    # 进行意向分析
                    analysis_results = analyze_comments_intent(comments, profile_sentence)
                    
                    # 保存分析结果
                    if analysis_results:
                        save_analysis_results_to_db(analysis_results, profile_sentence)
                        print(f"完成 {len(analysis_results)} 条评论的意向分析")
            else:
                print(f"该笔记没有评论")
                
        except Exception as e:
            print(f"收集评论时出错: {str(e)}")
        
        # 返回上一页
        try:
            back_btn = xhs.driver.find_element(
                by=AppiumBy.XPATH,
                value="//android.widget.Button[@content-desc='返回']"
            )
            back_btn.click()
            time.sleep(0.5)
        except Exception as e:
            print(f"返回上一页失败: {str(e)}")
        
        # 记录已收集的标题
        collected_titles.append(note_title_and_text)
        
        return {
            'note_data': note_data,
            'comments_count': len(comments),
            'comment_ids': comment_ids,
            'analysis_results': analysis_results
        }
        
    except Exception as e:
        print(f"处理笔记卡片失败: {str(e)}")
        # 尝试返回上一页
        try:
            back_btn = xhs.driver.find_element(
                by=AppiumBy.XPATH,
                value="//android.widget.Button[@content-desc='返回']"
            )
            back_btn.click()
            time.sleep(0.5)
        except:
            pass
        return None

def collect_notes_and_comments_immediately(device_index: int = 0,**context):
    """
    收集小红书笔记并立即收集每条笔记的评论
    """
    # 从dag run配置获取参数
    email = context['dag_run'].conf.get('email')
    keyword = context['dag_run'].conf.get('keyword')
    max_notes = int(context['dag_run'].conf.get('max_notes', 10))
    max_comments = int(context['dag_run'].conf.get('max_comments', 10))
    note_type = context['dag_run'].conf.get('note_type', '图文')
    time_range = context['dag_run'].conf.get('time_range', '')
    search_scope = context['dag_run'].conf.get('search_scope', '')
    sort_by = context['dag_run'].conf.get('sort_by', '综合')
    profile_sentence = context['dag_run'].conf.get('profile_sentence', '')  # 意向分析参数
    
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
        appium_port = device_info.get('available_appium_ports')[device_index]
        device_id = device_info.get('phone_device_list')[device_index]
        username = device_info.get('username')
        password = device_info.get('password')
        host_port=device_info.get('port')
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")

    # 获取appium_server_url
    appium_server_url = f"http://{device_ip}:{appium_port}"
    
    print(f"选择设备 {device_id}, appium_server_url: {appium_server_url}")
    print(f"开始收集关键词 '{keyword}' 的小红书笔记和评论，笔记数量: {max_notes}，每条笔记最大评论数: {max_comments}")
    
    global xhs
    try:
        # 初始化小红书操作器
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        
        # 统计变量
        collected_notes = []
        collected_titles = []
        total_comments = 0
        all_comment_ids = []
        all_analysis_results = []
        
        # 搜索关键词
        print(f"搜索关键词: {keyword}, 笔记类型: {note_type}")
        
        if note_type == '视频':
            print("暂不支持视频笔记的即时评论收集，请使用图文笔记")
            return {
                'notes_count': 0,
                'comments_count': 0,
                'comment_ids': [],
                'analysis_results': []
            }
        else:
            # 使用图文搜索方法
            print(f"使用图文搜索方法搜索关键词: {keyword}")
            xhs.search_keyword(keyword, filters={
                "note_type": note_type,
                "time_range": time_range,
                "search_scope": search_scope,
                "sort_by": sort_by
            })
            
            print(f"开始收集图文笔记和评论，计划收集{max_notes}条笔记...")
            print("---------------card----------------")
            xhs.print_all_elements()
            
            # 收集笔记和评论
            while len(collected_notes) < max_notes:
                try:
                    print("获取所有笔记卡片元素")
                    note_cards = []
                    try:
                        note_cards = xhs.driver.find_elements(
                            by=AppiumBy.XPATH,
                            value="//android.widget.FrameLayout[@resource-id='com.xingin.xhs:id/-' and @clickable='true']"
                        )
                        print(f"获取笔记卡片成功，共{len(note_cards)}个")
                    except Exception as e:
                        print(f"获取笔记卡片失败: {e}")
                        break
                    
                    # 处理每个笔记卡片
                    for note_card in note_cards:
                        if len(collected_notes) >= max_notes:
                            break
                        
                        # 收集笔记和评论
                        result = collect_note_and_comments_immediately(
                            xhs, note_card, keyword, email, max_comments, 
                            profile_sentence, collected_titles
                        )
                        
                         # 提取笔记URL列表
                        note_urls = [note.get('note_url', '') for note in collected_notes if note.get('note_url', '')]
                        
                        if result:
                            collected_notes.append(result['note_data'])
                            total_comments += result['comments_count']
                            all_comment_ids.extend(result['comment_ids'])
                            all_analysis_results.extend(result['analysis_results'])
                            
                            print(f"完成笔记处理，当前进度: {len(collected_notes)}/{max_notes}")
                            print(f"累计评论数: {total_comments}，累计评论ID数: {len(all_comment_ids)}")
                        # 执行评论回复逻辑
                        reply_count = 0
                        if all_comment_ids:  # 只有在有评论ID时才执行回复
                            print("\n========== 开始执行评论回复 ==========")
                            try:
                                # 获取回复模板
                                templates = get_reply_templates_from_db(email=email)
                                if not templates:
                                    print("没有找到可用的回复模板，跳过评论回复")
                                else:
                                    print(f"找到 {len(templates)} 个回复模板")
                                    
                                    # 获取高意向和中意向的评论进行回复
                                    # high_intent_comments = [result for result in all_analysis_results if result.get('intent') in ['高意向', '中意向']]
                                    high_intent_comments=all_analysis_results
                                    if high_intent_comments:
                                        print(f"找到 {len(high_intent_comments)} 条高/中意向评论需要回复")
                                        
                                        # 执行回复逻辑
                                        previous_url = None  # 跟踪上一个处理的URL
                                        for comment_result in high_intent_comments:
                                            
                                            try:
                                                template = random.choice(templates)
                                                template_content = template.get('content', '')
                                                
                                                comment_id = comment_result.get('id')
                                                if not comment_id:
                                                    continue
                                                skip_url_open = (previous_url == comment_result.get('note_url', ''))
                                                image_urls = template['image_urls']
                                                has_image = image_urls is not None and image_urls != "null" and image_urls != ""
                                                if has_image:
                                                    print('图片url:',image_urls)
                                                    cos_to_device_via_host(cos_url=image_urls,host_address=device_ip,host_username=username,device_id=device_id,host_password=password,host_port=host_port)
                                
                                            
                                                # 执行回复（这里使用现有的XHS操作器）
                                                success = xhs.comments_reply(
                                                    comment_result.get('note_url', ''),
                                                    comment_result.get('author', ''),
                                                    comment_content=comment_result.get('content', ''),
                                                    reply_content=template_content,
                                                    has_image=has_image,
                                                    skip_url_open=skip_url_open
                                                )
                                                previous_url = comment_result.get('note_url', '')
                                                if success:
                                                    # 记录回复到数据库
                                                    insert_manual_reply(
                                                    comment_id=comment_id,
                                                    note_url=comment_result.get('note_url', ''),
                                                    author=comment_result.get('author', ''),
                                                    userInfo=email,
                                                    content=comment_result.get('content', ''),
                                                    reply=template_content
                                                    )
                                                    reply_count += 1
                                                    print(f"成功回复评论ID: {comment_id}，内容: {template_content[:50]}...")
                                                else:
                                                    print(f"回复评论ID {comment_id} 失败")
                                                    
                                                # 添加延迟避免操作过快
                                                if not skip_url_open and previous_url:
                                                    # 返回上一页
                                                    try:
                                                        back_btn = xhs.driver.find_element(
                                                                by=AppiumBy.XPATH,
                                                                value="//android.widget.Button[@content-desc='返回']"
                                                            )
                                                        
                                                        back_btn.click()
                                                        time.sleep(0.5)
                                                    except Exception as e:
                                                        print(f"返回上一页失败: {str(e)}")
                                                time.sleep(random.uniform(2, 5))
                                                
                                            except Exception as e:
                                                print(f"回复评论时出错: {str(e)}")
                                                continue
                                    else:
                                        print("没有找到需要回复的高/中意向评论")
                                        
                            except Exception as e:
                                print(f"执行评论回复时出错: {str(e)}")
                        else:
                            print("跳过评论回复：没有评论ID或未进行意向分析")
                            
                        print(f"\n========== 评论回复完成，共回复 {reply_count} 条评论 ==========")
                        
                        
                        
                    
                    # 如果还没收集够，滚动页面
                    if len(collected_notes) < max_notes:
                        print("滚动页面获取更多笔记...")
                        xhs.scroll_down()
                        time.sleep(1)
                    
                except Exception as e:
                    print(f"收集笔记失败: {str(e)}")
                    import traceback
                    print(traceback.format_exc())
                    break
        
        # 总结
        print("\n========== 任务完成总结 ==========")
        print(f"设备索引: {device_index}")
        print(f"关键词: {keyword}")
        print(f"收集笔记数量: {len(collected_notes)}")
        print(f"收集评论总数: {total_comments}")
        print(f"评论数据库ID总数: {len(all_comment_ids)}")
        print(f"意向分析结果数量: {len(all_analysis_results)}")
        return {
                            'notes_count': len(collected_notes),
                            'comments_count': total_comments,
                            'comment_ids': all_comment_ids,
                            'note_urls': note_urls,
                            'analysis_results': all_analysis_results,
                            'reply_count': reply_count
                        }
       
            
    except Exception as e:
        error_msg = f"收集小红书笔记和评论失败: {str(e)}"
        print(error_msg)
        raise
    finally:
        # 确保关闭小红书操作器
        if xhs:
            xhs.close()


# DAG 定义
with DAG(
    dag_id='new_auto_notes_and_comments_collector',
    default_args={
        'owner': 'yuchangongzhu', 
        'depends_on_past': False, 
        'start_date': datetime(2024, 1, 1)
    },
    description='新版自动收集小红书笔记和评论 (进入每条笔记后立即收集评论)',
    schedule_interval=None,
    tags=['小红书', '自动化', '即时评论'],
    catchup=False,
    max_active_runs=5,
) as dag:

    # 创建多个任务，每个任务使用不同的设备索引
    for index in range(10):
        PythonOperator(
            task_id=f'new_auto_collect_notes_and_comments_{index}',
            python_callable=collect_notes_and_comments_immediately,
            provide_context=True,
            op_kwargs={
                'device_index': index,
            },
            retries=3,
            retry_delay=timedelta(seconds=10)
        )