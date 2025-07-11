import os
import requests
from typing import List, Dict
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# OpenRouter API 基础 URL
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# 在运行时初始化 OpenRouter 客户端
def get_openrouter_key():
    """获取 OpenRouter API key"""
    # 优先从 Airflow 变量中获取
    try:
        from airflow.models import Variable
        api_key = Variable.get("OPENROUTER_API_KEY", default_var=None)
    except Exception as e:
        print(f"从Airflow Variable获取API key失败: {e}")
        api_key = None
    
    # 如果Airflow变量中没有，尝试从环境变量获取（作为备选方案）
    if not api_key:
        api_key = os.getenv("OPENROUTER_API_KEY")
    
    if not api_key:
        raise ValueError("在Airflow变量和环境变量中均未找到OpenRouter API key")
        
    return api_key

def analyze_comments_intent(comments: List[Dict[str, str]], profile_sentence: str) -> List[Dict[str, str]]:
    """
    批量分析多个评论的用户意向，返回包含原评论和意向分析结果的列表。
    
    :param comments: 评论列表，每个评论是包含 'author' 和 'content' 键的字典
    :param profile_sentence: 要分析的句子，如 "我是做xxx行业的，我要寻找xxx类型客户"
    :return: 带有意向级别的评论列表
    """
    import time
    results = []
    
    total_comments = len(comments)
    print(f"准备分析 {total_comments} 条评论...")
    
    for i, comment in enumerate(comments, 1):
        try:
            print(f"正在分析第 {i}/{total_comments} 条评论...")
            author = comment.get('author', '')
            content = comment.get('content', '')
            
            # 添加断点继续功能 - 每10个评论打印进度并则进行短暂停
            if i > 1 and i % 10 == 0:
                print(f"完成 {i}/{total_comments} 条评论的分析，短暂停后继续...")
                time.sleep(1)  # 每10个评论后暂停1秒，避免过快请求API
            
            intent = _analyze_single_comment(content, author, profile_sentence)
            
            # 将原始评论信息和分析结果合并
            result = comment.copy()
            result['intent'] = intent
            results.append(result)
            
        except Exception as e:
            print(f"分析评论时出错: {str(e)}")
            # 当单个评论分析出错时，添加默认结果并继续分析其他评论
            result = comment.copy()
            result['intent'] = "中意向"  # 默认结果
            result['error'] = str(e)  # 记录错误信息
            results.append(result)
            
            # 出错后暂停一会再继续
            time.sleep(0.5)
    
    print(f"完成全部 {total_comments} 条评论的分析")
    return results

def _analyze_single_comment(content: str, author: str, profile_sentence: str) -> str:
    """
    使用 OpenRouter 分析单个用户评论意向，返回"高意向"、"中意向"或"低意向"。
    这是内部辅助函数，不建议直接调用。
    
    :param content: 评论内容
    :param author: 评论作者
    :param profile_sentence: 要分析的句子
    :return: 意向级别
    """
    import time
    from requests.exceptions import RequestException
    
    # 如果评论为空，直接返回低意向
    if not content or content.strip() == "":
        print(f"评论内容为空，自动判定为低意向")
        return "低意向"
    
    industry='医美'
    
    prompt = f"""
# Role and Objective
你是一个资深的用户意向判断器，工作在{industry}行业，该行业的【理想客户画像】为{profile_sentence}。你的任务是根据用户在小红书的行业相关的帖子下的一条评论留言，分析该用户和【理想客户画像】的贴近程度，以此判断该用户能够成为你的客户的可能性，并最终直接返回"高意向"、"中意向"或"低意向"。
====
# Instructions
1. 分析输入内容包括：
-【用户名】：{author}
-【评论内容】：{content}
2. 结合【用户名】和【评论内容】进行多层分析：
 a. 根据用户名和评论内容综合判断该用户是否为同行。同行通过在评论中传播行业资源或展示服务以邀请私聊或对某家机构做出宣传，其评论内容与客户的核心区别在于同行是在提供资源而客户是在请求资源。同行用户为你的竞争对手，不会成为你的客户，对同行应直接判断为“低意向”，并不再进行后续的判断与调整。
 b. 解析评论内容表达的情绪与需求，关注是否包含如下典型客户意向特征之一：
    - 较为夸赞或认可行业相关产品/服务的质量和效果，但注意评论对象应为不带有具体机构名称的行业产品，如果带有具体的机构名称，则划分到中意向；
    - 询问购买方式、库存情况、服务细节、价格、优惠等信息，包括其它对行业相关的任何询问；
    - 给出个人信息，请求推荐适合自己的产品或服务，例如询问xx地点或xx价位附近能否提供相关服务；
    - 呼吁或者表达愿意进一步互动、私聊、咨询，与同行的区别在于客户是在请求被服务；
    - 表示考虑或计划购买、复购、使用相关服务。
 这类评论体现用户对行业相关产品或服务的兴趣和购买潜力，判定为“高意向”。
 c. 对于中性表达且与【理想客户画像】契合度较低的，如果不是同行，判定为“中意向”。
 d. 对于对整个行业的负面批评、与行业完全无关的闲聊、或明显表现为与【理想客户画像】不契合的，则判定为“低意向”。注意如果只是对某厂家的负面评价，说明客户其实是有相关产品使用需求的，因此应该划分到“中意向”。
3. 结合用户名和评论内容的综合分析结果，以及与【理想客户画像】的契合程度，输出最终意向等级：“高意向”、“中意向”或“低意向”，允许大胆给出“高意向”的评价，但要避免因同行伪装产生误判。
====
#Reasoning steps
1. 读取输入【用户名】及【评论内容】，分析用户名是否存在同行特征。
2. 深度理解评论语义，提炼情感倾向和潜在需求。
3. 根据需求类型与【理想客户画像】匹配度，判断意向强弱。
4. 权衡用户名与评论内容综合信息，做出最终意向判定。
5. 返回一词结果，规范输出格式，不含多余格式和提示语。
====
#output format
仅输出一词：“高意向”或“中意向”或“低意向”
====
#Context
【用户名】：{author}
【评论内容】：{content}
====
#Final instructions and prompt to think step by step
务必按照上述逻辑流程逐步分析，保证输出仅包含“高意向”或“中意向”或“低意向”一词。禁止包含任何格式标记和额外说明文本。请严格遵守分析要求和输出格式。
"""
    # 获取 OpenRouter API key
    api_key = get_openrouter_key()
    
    # 设置请求头
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        # "HTTP-Referer": "https://your-app-domain.com",  # 替换为你的应用域名
        # "X-Title": "XHS Comments Analyzer"  # 你的应用名称
    }
    
    # 请求体
    data = {
        "model": "deepseek/deepseek-chat",  # 使用 Deepseek 聊天模型
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0,
        "max_tokens": 50  # 限制生成的文本数量，加快响应时间
    }
    
    # 最多尝试3次
    max_retries = 3
    retry_delay = 2  # 初始等待时间（秒）
    
    for attempt in range(max_retries):
        try:
            # 发送请求
            print(f"发送请求到 OpenRouter ({attempt + 1}/{max_retries})")
            response = requests.post(OPENROUTER_API_URL, headers=headers, json=data, timeout=30)
            response.raise_for_status()  # 如果请求失败，抛出异常
            
            # 解析响应
            result = response.json()["choices"][0]["message"]["content"].strip()
            
            # 检查结果是否在预期的三种结果之一，如果不是，则使用简单的分类逻辑
            expected_results = ["高意向", "中意向", "低意向"]
            if result not in expected_results:
                # 尝试从结果中提取正确的意向分类
                for expected in expected_results:
                    if expected in result:
                        print(f"模型返回了非标准的结果，已从 '{result}' 中提取为 '{expected}'")
                        return expected
                
                # 如果仍然无法提取，则根据内容长度返回默认级别
                print(f"无法解析模型返回的结果 '{result}'，使用默认级别代替")
                return "中意向"
            return result
            
        except RequestException as e:
            # 打印错误信息
            print(f"请求失败 ({attempt + 1}/{max_retries}): {str(e)}")
            
            # 如果还有重试机会，则等待后重试
            if attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # 指数退避策略
                print(f"在 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                print("到最大重试次数，返回默认结果")
                return "中意向"  # 所有重试失败后的默认结果
        except Exception as e:
            # 捕获其他类型的异常（如JSON解析错误）
            print(f"处理评论时发生异常: {str(e)}")
            return "中意向"  # 异常情况下的默认结果

def save_results_to_db(results, profile_sentence):
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

def get_comments_from_db(comment_ids=None, limit=100):
    """
    从数据库获取评论数据
    
    :param comment_ids: 可选，评论 ID 列表，如果提供则仅获取这些 ID 的评论
    :param limit: 获取记录的最大数量（当comment_ids为空时使用）
    :return: 评论列表
    """
    # 使用Airflow的BaseHook获取数据库连接
    db_hook = BaseHook.get_connection("xhs_db").get_hook()
    db_conn = db_hook.get_conn()
    cursor = db_conn.cursor()  # 标准游标
    
    comments = []
    try:
        # 构建查询
        if comment_ids and len(comment_ids) > 0:
            # 如果提供了具体的comment_ids，则只获取这些评论
            # 注意：将 comment_ids 列表展平传递给 SQL 查询
            format_strings = ','.join(['%s'] * len(comment_ids))
            query = f"SELECT id, author, userInfo, content, note_url, keyword FROM xhs_comments WHERE id IN ({format_strings})"
            # 确保 params 是一个元组
            params = tuple(comment_ids)
        else:
            # 如果没有提供具体的comment_ids，则获取最新的一定数量评论
            query = f"SELECT id, author, userInfo, content, note_url, keyword FROM xhs_comments ORDER BY id DESC LIMIT {limit}"
            params = []
        
        # 执行查询
        cursor.execute(query, params)
        result = cursor.fetchall()
        
        # 获取列名
        columns = [desc[0] for desc in cursor.description]
        
        # 将元组结果转换为字典列表
        comments = []
        for row in result:
            comment = dict(zip(columns, row))
            comments.append(comment)
        
        print(f"从数据库获取了 {len(comments)} 条评论，是{comments}")
        
    except Exception as e:
        print(f"数据库查询失败: {str(e)}")
    finally:
        cursor.close()
        db_conn.close()
    
    return comments

def run_comments_analysis(**context):
    """
    Airflow任务：运行评论意向分析
    """
    try:
        # 从dag run配置获取参数，使用默认值作为备选
        profile_sentence = context['dag_run'].conf.get('profile_sentence')
        comment_ids = context['dag_run'].conf.get('comment_ids')
        
        # 获取评论数据
        if comment_ids:
            print(f"正在获取指定的 {len(comment_ids)} 条评论数据...")
        else:
            print("未指定评论 ID，将获取最新的评论数据...")
            
        comments = get_comments_from_db(comment_ids=comment_ids, limit=100)
        
        if not comments:
            print("没有找到符合条件的评论数据")
            return
        
        print(f"共获取到 {len(comments)} 条评论，开始分析...")
        
        # 分析评论意向
        results = analyze_comments_intent(comments, profile_sentence)
        
        # 输出分析结果
        intent_counts = {'高意向': 0, '中意向': 0, '低意向': 0}
        
        print("\n===== 评论意向分析结果 =====\n")
        for result in results:
            intent = result.get('intent', '未知')
            intent_counts[intent] = intent_counts.get(intent, 0) + 1
            
            print(f"作者: {result['author']}")
            print(f"评论: {result['content']}")
            print(f"意向: {intent}")
            print("----------------------")
        
        print("\n===== 统计结果 =====\n")
        total = len(results)
        for intent, count in intent_counts.items():
            percentage = (count / total) * 100 if total > 0 else 0
            print(f"{intent}: {count}条 ({percentage:.1f}%)")
        
        # 将分析结果传递到下一个任务
        context['ti'].xcom_push(key='analysis_results', value=results)
        context['ti'].xcom_push(key='profile_sentence', value=profile_sentence)
        
        return results
        
    except Exception as e:
        error_msg = f"评论意向分析失败: {str(e)}"
        print(error_msg)
        raise

def save_analysis_results(**context):
    """
    Airflow任务：保存分析结果到数据库
    """
    try:
        # 从上一个任务获取结果
        ti = context['ti']
        results = ti.xcom_pull(task_ids='analyze_comments', key='analysis_results')
        profile_sentence = ti.xcom_pull(task_ids='analyze_comments', key='profile_sentence')
        
        if not results:
            print("没有找到分析结果，无法保存到数据库")
            return 0
        
        print(f"准备保存 {len(results)} 条分析结果到数据库...")
        
        # 保存结果到数据库
        saved_count = save_results_to_db(results, profile_sentence)
        
        return saved_count
        
    except Exception as e:
        error_msg = f"保存分析结果失败: {str(e)}"
        print(error_msg)
        raise

# DAG 定义
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='comments_analyzer',
    default_args=default_args,
    description='使用Deepseek模型分析小红书评论的用户意向',
    schedule_interval=None,
    tags=['小红书', 'AI分析', 'Deepseek'],
    catchup=False,
)

analyze_comments_task = PythonOperator(
    task_id='analyze_comments',
    python_callable=run_comments_analysis,
    provide_context=True,
    dag=dag,
)

save_results_task = PythonOperator(
    task_id='save_results',
    python_callable=save_analysis_results,
    provide_context=True,
    dag=dag,
)

# 设置任务依赖关系
analyze_comments_task >> save_results_task