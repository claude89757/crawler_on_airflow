#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.exceptions import AirflowSkipException

from appium.webdriver.common.appiumby import AppiumBy
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.xhs_appium import XHSOperator

import time
import re

def browse_xhs_notes(device_index=0, **context) -> None:
    """
    浏览小红书笔记
    Args:
        device_index: 设备索引
        **context: Airflow上下文参数字典
            - keyword: 搜索关键词
            - max_notes: 最大浏览笔记数量
            - email: 用户邮箱
            - note_type: 笔记类型，可选值为 '图文' 或 '视频'，默认为 '图文'
    
    Returns:
        None
    """
    # 获取输入参数
    keyword = context['dag_run'].conf.get('keyword') 
    max_notes = int(context['dag_run'].conf.get('max_notes'))
    email = context['dag_run'].conf.get('email')
    note_type = context['dag_run'].conf.get('note_type')  # 默认为图文类型
    time_range = context['dag_run'].conf.get('time_range')
    search_scope=context['dag_run'].conf.get('search_scope')
    sort_by=context['dag_run'].conf.get('sort_by')

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
    except Exception as e:
        print(f"获取设备信息失败: {e}")
        print(f"跳过当前任务，因为获取设备信息失败")
        raise AirflowSkipException("设备信息获取失败")

    # 获取appium_server_url
    appium_server_url = f"http://{device_ip}:{appium_port}"
    
    print(f"选择设备 {device_id}, appium_server_url: {appium_server_url}")
    print(f"开始浏览关键词 '{keyword}' 的小红书笔记... ，数量为'{max_notes}'")
    
    xhs = None
    try:
        # 初始化小红书操作器（带重试机制）
        xhs = XHSOperator(appium_server_url=appium_server_url, force_app_launch=True, device_id=device_id)
        
        # 搜索关键词，并且开始收集
        print(f"搜索关键词: {keyword}, 笔记类型: {note_type}")
        collected_notes = [] 
        collected_titles = []
        
        if note_type == '视频':
            # 使用视频搜索方法
            print(f"使用视频搜索方法搜索关键词: {keyword}")
            # search_keyword_of_video 方法内部已经处理了视频的收集和处理
            # 该方法会返回收集到的视频列表
            print(f"开始收集视频笔记,计划收集{max_notes}条...")
            collected_videos = xhs.search_keyword_of_video(keyword, max_videos=max_notes)
            
            # 处理收集到的视频数据
            if collected_videos:
                for video in collected_videos:
                    # 添加关键词信息
                    video['keyword'] = keyword
                    # 使用相同的处理函数处理视频数据
                    collected_notes.append(video)
            else:
                print(f"未找到关于 '{keyword}' 的视频笔记")
                
        else:
            # 使用默认搜索方法（图文）
            print(f"使用图文搜索方法搜索关键词: {keyword}")
            xhs.search_keyword(keyword, filters={
                "note_type": note_type,
                "time_range":time_range,
                "search_scope":search_scope,
                "sort_by":sort_by
            })
            
            print(f"开始浏览图文笔记,计划浏览{max_notes}条...--------------------------------------")
            xhs.print_all_elements()
            
            get_note_card(xhs, collected_notes, collected_titles, max_notes, keyword)

        
        if not collected_notes:
            print(f"未找到关于 '{keyword}' 的笔记")
            return
            
        # 打印收集结果
        print("\n收集完成!")
        print(f"共收集到 {len(collected_notes)} 条笔记")
        
        return collected_notes
            
    except Exception as e:
        error_msg = f"浏览小红书笔记失败: {str(e)}"
        print(error_msg)
        raise
    finally:
        # 确保关闭小红书
        if xhs:
            xhs.close()

def get_note_card(xhs, collected_notes, collected_titles, max_notes, keyword):
    """
    收集小红书笔记卡片
    """
    import random  # 导入random模块用于随机选择
    
    # 设置随机浏览的概率 (可以根据需求调整，这里设置为40%的概率会点开一篇笔记)
    browse_probability = 0.4
    
    while len(collected_notes) < max_notes:
        try:
            print("获取所有笔记卡片元素")
            note_cards = []
            try:
                # 只保留原始方法
                note_cards = xhs.driver.find_elements(
                    by=AppiumBy.XPATH,
                    value="//android.widget.FrameLayout[@resource-id='com.xingin.xhs:id/-' and @clickable='true']"
                )
                print(f"获取笔记卡片成功，共{len(note_cards)}个")
            except Exception as e:
                print(f"获取笔记卡片失败: {e}")
            for note_card in note_cards:
                if len(collected_notes) >= max_notes:
                    break
                try:
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
                    
                    # 随机决定是否浏览这篇笔记
                    should_browse = random.random() < browse_probability
                    
                    if note_title_and_text not in collected_titles and should_browse:
                        print(f"随机选择浏览笔记: {note_title_and_text}, 作者: {author}, 当前收集数量: {len(collected_notes)}")
                        
                        # 获取屏幕尺寸和元素位置
                        try:
                            screen_size = xhs.driver.get_window_size()
                            element_location = title_element.location
                            screen_height = screen_size['height']
                            element_y = element_location['y']
                            
                            # 检查元素是否位于屏幕高度的3/4以上
                            if element_y > screen_height * 0.25:
                                # 点击标题元素而不是整个卡片
                                print(f"元素位置正常，位于屏幕{element_y/screen_height:.2%}处，执行点击")
                                title_element.click()
                                time.sleep(0.5)
                            else:
                                print(f"元素位置过高，位于屏幕{element_y/screen_height:.2%}处，跳过点击")
                                continue
                        except Exception as e:
                            error_msg = str(e)
                            print(f"检测元素位置失败: {error_msg}")

                            # 默认点击标题元素
                            # title_element.click()
                            time.sleep(0.5)
                        note_data = get_note_data(xhs,note_title_and_text)
                        # time.sleep(0.5)
                        # xhs.bypass_share()
                        if note_data:
                            note_data['keyword'] = keyword
                            collected_titles.append(note_title_and_text)
                            collected_notes.append(note_data)
                        back_btn = xhs.driver.find_element( by=AppiumBy.XPATH,
                        value="//android.widget.Button[@content-desc='返回']")
                        back_btn.click()
                        time.sleep(0.5)
                    elif note_title_and_text not in collected_titles and not should_browse:
                        # 记录跳过的笔记
                        print(f"随机跳过浏览笔记: {note_title_and_text}, 作者: {author}")
                except Exception as e:
                    print(f"处理笔记卡片失败: {str(e)}")
                    continue
            if len(collected_notes) < max_notes:
                xhs.scroll_down()
                time.sleep(0.5)
        except Exception as e:
            print(f"收集笔记失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            break

def get_note_data(xhs: XHSOperator, note_title_and_text: str):
        """
        获取笔记内容和评论
        Args:
            note_title_and_text: 笔记标题和内容
        Returns:
            dict: 笔记数据
        """
        try:
            print('---------------note--------------------')
            xhs.print_all_elements()
            print(f"正在获取笔记内容: {note_title_and_text}")
            
            # 等待笔记内容加载
            time.sleep(0.5)
            
            # 获取笔记作者
            try:
                # 尝试查找作者元素
                author_element = None
                max_scroll_attempts = 3
                scroll_attempts = 0
                
                while not author_element and scroll_attempts < max_scroll_attempts:
                    try:
                        author_element = WebDriverWait(xhs.driver, 2).until(
                            EC.presence_of_element_located((AppiumBy.ID, "com.xingin.xhs:id/nickNameTV"))
                        )
                        print(f"找到作者信息元素: {author_element.text}")
                        author = author_element.text
                        break
                    except:
                        # 向下滚动一小段距离
                        xhs.scroll_down()
                        scroll_attempts += 1
                        time.sleep(1)
                
                if not author_element:
                    author = ""
                    print("未找到作者信息元素")
                    
            except Exception as e:
                author = ""
                print(f"获取作者信息失败: {str(e)}")
            
            # 获取笔记内容 - 需要滑动查找
            content = ""
            max_scroll_attempts = 5  # 最大滑动次数
            scroll_count = 0
            
            note_title = ""
            note_content = ""
           
            #修改标题定位逻辑，一般进入笔记时就可定位到标题，无需滑动，嵌套在循环中会导致二次定位成错误元素
            # 尝试获取标题 
            try:
                # 如果失败，使用原来的方法
                title_element = xhs.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.TextView[contains(@text, '') and string-length(@text) > 3 and not(contains(@text, '1/')) and not(contains(@text, 'LIVE')) and not(contains(@text, '试试文字发笔记')) and not(contains(@text, '关注')) and not(contains(@text, '分享')) and not(contains(@text, '作者')) and not(@resource-id='com.xingin.xhs:id/nickNameTV')]"
                )
                note_title = title_element.text
                print(f"找到标题: {note_title}")
                        
            except:
                # 尝试使用resource-id匹配标题
                title_element = xhs.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.TextView[contains(@resource-id, 'com.xingin.xhs:id/') and string-length(@text) > 0 and string-length(@text) < 50]"
                )
                note_title = title_element.text
                print(f"通过resource-id找到标题: {note_title}")
            while scroll_count < max_scroll_attempts:
                #查找笔记编辑时间
                note_time_exists = False
                if note_time_exists == False:
                    try:
                        note_time_element = xhs.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.view.View[contains(@content-desc, '-') or contains(@content-desc, ':') or contains(@content-desc, '编辑于')]"
                        )
                        time_content = note_time_element.get_attribute("content-desc")
                        print(f"找到笔记修改时间: {time_content}")
                        format_time=xhs.process_time_string(time_content)['timestamp']
                        format_location=xhs.process_time_string(time_content)['location'].replace("编辑于","")
                        print(f"时间格式化为: {format_time},地区格式化为: {format_location}")
                        note_time_exists = True
                    except:
                        print(f"未找到笔记修改时间")
                    
                try:
                   
                    # 尝试获取正文内容 - 优先匹配长文本
                    try:
                        # 首先尝试匹配长文本
                        content_element = xhs.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[string-length(@text) > 100]"
                        )
                        note_content = content_element.text
                        print(f"通过长文本找到正文内容: {len(note_content)} 字符")
                    except:
                        # 如果失败，尝试使用resource-id匹配
                        content_element = xhs.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[contains(@resource-id, 'com.xingin.xhs:id/') and string-length(@text) > 50]"
                        )
                        note_content = content_element.text
                        print(f"通过resource-id找到正文内容: {len(note_content)} 字符")
                    
                    if note_content and note_title:
                        print("找到正文内容和标题")
                        print(f"标题: {note_title}")
                        print(f"正文前100字符: {note_content[:100]}...")
                        if note_time_exists == True:
                            #修改时间位于正文下方，找到时间后再退出循环
                            break
                        else:
                            xhs.scroll_down()
                            time.sleep(0.5)
                            scroll_count += 1
                except:
                    print(f"第 {scroll_count + 1} 次滑动查找正文...")
                    # 向下滑动
                    xhs.scroll_down()
                    time.sleep(0.5)
                    scroll_count += 1

                        # 获取互动数据 - 分别处理每个数据
            likes = "0"
            try:
                # 获取点赞数 - 基于图片中的元素结构
                try:
                    # 首先尝试使用resource-id和content-desc结合查找
                    likes_btn = xhs.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '点赞') and contains(@resource-id, 'com.xingin.xhs:id/')]"
                    )
                    # 尝试获取文本内容
                    likes_text = likes_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过resource-id和content-desc找到点赞数: {likes_text}")
                except:
                    # 如果失败，尝试只使用content-desc
                    likes_btn = xhs.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '点赞')]"
                    )
                    likes_text = likes_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过content-desc找到点赞数: {likes_text}")
                
                # 如果获取到的是纯文本"点赞"或数字后跟有文本，则提取数字部分
                if likes_text == "点赞":
                    likes = "0"
                else:
                    # 尝试提取数字部分
                    
                    digits = re.findall(r'\d+', likes_text)
                    likes = digits[0] if digits else "0"
                print(f"最终点赞数: {likes}")
            except Exception as e:
                print(f"获取点赞数失败: {str(e)}")

            collects = "0"
            try:
                # 获取收藏数 - 基于图片中的元素结构
                try:
                    # 首先尝试使用resource-id和content-desc结合查找
                    collects_btn = xhs.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '收藏') and contains(@resource-id, 'com.xingin.xhs:id/')]"
                    )
                    # 尝试获取文本内容
                    collects_text = collects_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过resource-id和content-desc找到收藏数: {collects_text}")
                except:
                    # 如果失败，尝试只使用content-desc
                    collects_btn = xhs.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '收藏')]"
                    )
                    collects_text = collects_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过content-desc找到收藏数: {collects_text}")
                
                # 如果获取到的是纯文本"收藏"或数字后跟有文本，则提取数字部分
                if collects_text == "收藏":
                    collects = "0"
                else:
                    # 尝试提取数字部分
                    
                    digits = re.findall(r'\d+', collects_text)
                    collects = digits[0] if digits else "0"
                print(f"最终收藏数: {collects}")
            except Exception as e:
                print(f"获取收藏数失败: {str(e)}")

            comments = "0"
            try:
                # 获取评论数 - 基于图片中的元素结构
                try:
                    # 首先尝试使用resource-id和content-desc结合查找
                    comments_btn = xhs.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '评论') and contains(@resource-id, 'com.xingin.xhs:id/')]"
                    )
                    # 尝试获取文本内容
                    comments_text = comments_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过resource-id和content-desc找到评论数: {comments_text}")
                except:
                    # 如果失败，尝试只使用content-desc
                    comments_btn = xhs.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '评论')]"
                    )
                    comments_text = comments_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过content-desc找到评论数: {comments_text}")
                
                # 如果获取到的是纯文本"评论"或数字后跟有文本，则提取数字部分
                if comments_text == "评论":
                    comments = "0"
                else:
                    # 尝试提取数字部分
                    
                    digits = re.findall(r'\d+', comments_text)
                    comments = digits[0] if digits else "0"
                print(f"最终评论数: {comments}")
            except Exception as e:
                print(f"获取评论数失败: {str(e)}")

            note_data = {
                "title": note_title,
                "content": note_content,
                "author": author,
                "likes": int(likes),
                "collects": int(collects),
                "comments": int(comments),
                "note_url": note_url,
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "note_time": format_time,
                "note_location": format_location
            }
            
            print(f"获取笔记数据: {note_data}")
            return note_data
            
        except Exception as e:
            import traceback
            print(f"获取笔记内容失败: {str(e)}")
            print("异常堆栈信息:")
            print(traceback.format_exc())
            xhs.print_all_elements()
            return {
                "title": note_title,
                "error": str(e),
                "url": "",
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
    


with DAG(
    dag_id='notes_browser',
    default_args={'owner': 'yuchangongzhu', 'depends_on_past': False, 'start_date': datetime(2024, 1, 1)},
    description='浏览小红书笔记 (支持图文和视频)',
    schedule_interval=None,
    tags=['小红书'],
    catchup=False,
    max_active_runs=5,
) as dag:

    for index in range(10):
        PythonOperator(
            task_id=f'browse_xhs_notes_{index}',
            python_callable=browse_xhs_notes,
            op_kwargs={
                'device_index': index,
            },
            provide_context=True,
            retries=3,
            retry_delay=timedelta(seconds=10)
        
        )
