#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
import time
import json
from utils.xhs_appium import XHSOperator
from utils.xhs_utils import cos_to_device_via_host

# 导入auto.py中的相关函数
from xhs_dags.auto import (
    save_note_to_db,
    save_comments_to_db,
    analyze_comments_intent,
    save_analysis_results_to_db,
    collect_notes_and_comments_immediately
)

# 注意：现在每个任务都会独立管理XHSOperator实例，不再使用全局变量

def execute_single_keyword_task(keyword, email, max_notes, max_comments, note_type, 
                               time_range, search_scope, sort_by, profile_sentence, 
                               template_ids, intent_type, device_index):
    """
    执行单个关键词的自动化任务
    
    Args:
        keyword: 搜索关键词
        email: 用户邮箱
        max_notes: 最大笔记数量
        max_comments: 每条笔记最大评论数
        note_type: 笔记类型
        time_range: 时间范围
        search_scope: 搜索范围
        sort_by: 排序方式
        profile_sentence: 行业及客户定位描述
        template_ids: 模板ID列表
        intent_type: 意向类型
        device_index: 设备索引
    
    Returns:
        dict: 执行结果统计
    """
    print(f"[关键词: {keyword}] 开始执行任务")
    print(f"[关键词: {keyword}] 参数: max_notes={max_notes}, max_comments={max_comments}")
    print(f"[关键词: {keyword}] 笔记类型: {note_type}, 排序方式: {sort_by}")
    
    try:
        # 构造模拟的context参数，用于调用collect_note_and_comments_immediately
        mock_context = {
            'dag_run': type('MockDagRun', (), {
                'conf': {
                    'email': email,
                    'keyword': keyword,
                    'max_notes': max_notes,
                    'max_comments': max_comments,
                    'note_type': note_type,
                    'time_range': time_range,
                    'search_scope': search_scope,
                    'sort_by': sort_by,
                    'profile_sentence': profile_sentence,
                    'template_ids': template_ids,
                    'intent_type': intent_type
                }
            })()
        }
        
        # 直接调用auto.py中的完整函数
        result = collect_notes_and_comments_immediately(device_index=device_index, **mock_context)
        
        print(f"[关键词: {keyword}] 任务完成！")
        print(f"[关键词: {keyword}] 总计收集笔记: {result.get('notes_count', 0)} 条")
        print(f"[关键词: {keyword}] 总计收集评论: {result.get('comments_count', 0)} 条")
        print(f"[关键词: {keyword}] 评论回复数量: {result.get('reply_count', 0)} 条")
        
        # 返回统一格式的结果
        return {
            'keyword': keyword,
            'notes_count': result.get('notes_count', 0),
            'comments_count': result.get('comments_count', 0),
            'comment_ids': result.get('comment_ids', []),
            'analysis_results': result.get('analysis_results', []),
            'reply_count': result.get('reply_count', 0),
            'note_urls': result.get('note_urls', [])
        }
        
    except Exception as e:
        print(f"[关键词: {keyword}] 执行任务时发生错误: {str(e)}")
        raise

def execute_multi_keyword_tasks(device_index: int = 0, **context):
    """
    执行多关键词顺序自动化任务
    """
    
    # 从dag run配置获取参数
    email = context['dag_run'].conf.get('email')
    keywords = context['dag_run'].conf.get('keywords', [])  # 关键词列表
    max_notes = int(context['dag_run'].conf.get('max_notes', 10))
    max_comments = int(context['dag_run'].conf.get('max_comments', 10))
    note_type = context['dag_run'].conf.get('note_type', '图文')
    time_range = context['dag_run'].conf.get('time_range', '')
    search_scope = context['dag_run'].conf.get('search_scope', '')
    sort_by = context['dag_run'].conf.get('sort_by', '综合')
    profile_sentence = context['dag_run'].conf.get('profile_sentence', '')
    template_ids = context['dag_run'].conf.get('template_ids', [])
    intent_type = context['dag_run'].conf.get('intent_type', [])
    
    # 验证参数
    if not email:
        raise ValueError("email参数不能为空")
    
    if not keywords or not isinstance(keywords, list):
        raise ValueError("keywords参数必须是非空列表")
    
    print(f"开始执行多关键词任务，设备索引: {device_index}")
    print(f"用户邮箱: {email}")
    print(f"关键词列表: {keywords}")
    print(f"每个关键词收集笔记数: {max_notes}，每条笔记最大评论数: {max_comments}")
    
    # 总体统计
    total_results = []
    total_notes = 0
    total_comments = 0
    all_comment_ids = []
    
    try:
        # 顺序执行每个关键词的任务
        for idx, keyword in enumerate(keywords):
            print(f"\n=== 开始执行第 {idx+1}/{len(keywords)} 个关键词: {keyword} ===")
            
            try:
                # 执行单个关键词任务
                result = execute_single_keyword_task(
                    keyword=keyword,
                    email=email,
                    max_notes=max_notes,
                    max_comments=max_comments,
                    note_type=note_type,
                    time_range=time_range,
                    search_scope=search_scope,
                    sort_by=sort_by,
                    profile_sentence=profile_sentence,
                    template_ids=template_ids,
                    intent_type=intent_type,
                    device_index=device_index
                )
                
                # 累计统计
                total_results.append(result)
                total_notes += result['notes_count']
                total_comments += result['comments_count']
                all_comment_ids.extend(result['comment_ids'])
                
                print(f"=== 关键词 {keyword} 执行完成 ===")
                print(f"本轮收集笔记: {result['notes_count']} 条，评论: {result['comments_count']} 条")
                
                # 关键词之间的休息时间
                if idx < len(keywords) - 1:  # 不是最后一个关键词
                    print(f"休息 3 秒后执行下一个关键词...")
                    time.sleep(3)
                    
            except Exception as e:
                print(f"执行关键词 {keyword} 时发生错误: {str(e)}")
                # 记录失败的关键词
                total_results.append({
                    'keyword': keyword,
                    'notes_count': 0,
                    'comments_count': 0,
                    'comment_ids': [],
                    'analysis_results': [],
                    'error': str(e)
                })
                continue
        
        # 输出总体统计
        print(f"\n=== 多关键词任务执行完成 ===")
        print(f"总计处理关键词: {len(keywords)} 个")
        print(f"总计收集笔记: {total_notes} 条")
        print(f"总计收集评论: {total_comments} 条")
        print(f"总计评论ID: {len(all_comment_ids)} 个")
        
        # 返回详细结果
        return {
            'total_keywords': len(keywords),
            'total_notes': total_notes,
            'total_comments': total_comments,
            'total_comment_ids': len(all_comment_ids),
            'keyword_results': total_results,
            'all_comment_ids': all_comment_ids
        }
        
    except Exception as e:
        print(f"多关键词任务执行失败: {str(e)}")
        raise

# DAG 定义
with DAG(
    dag_id='xhs_multi_keyword_loop',
    default_args={
        'owner': 'yuchangongzhu',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1)
    },
    description='多关键词顺序执行小红书自动化任务',
    schedule_interval=None,
    tags=['小红书', '多关键词', '顺序执行', '自动化'],
    catchup=False,
    max_active_runs=15,
    max_active_tasks=15,
) as dag:

    # 创建多个任务，每个任务使用不同的设备索引
    for index in range(15):
        PythonOperator(
            task_id=f'xhs_multi_keyword_loop_{index}',
            python_callable=execute_multi_keyword_tasks,
            provide_context=True,
            op_kwargs={
                'device_index': index,
            },
            retries=3,
            retry_delay=timedelta(seconds=10)
        )