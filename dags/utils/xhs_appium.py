#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小红书自动化操作SDK
提供基于Appium的小红书自动化操作功能,包括:
- 自动搜索关键词
- 收集笔记内容
- 获取笔记详情等

Author: claude89757
Date: 2025-01-09
"""
from datetime import datetime, timedelta
import os
import json
import time
import subprocess

import requests
import re


from appium.webdriver.webdriver import WebDriver as AppiumWebDriver
from appium.webdriver.common.appiumby import AppiumBy
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from appium.options.android import UiAutomator2Options
from xml.etree import ElementTree


def get_adb_devices():
    """
    获取当前连接的Android设备列表
    Returns:
        list: 设备序列号列表
    """
    try:
        # 执行adb devices命令
        result = subprocess.run(['adb', 'devices'], capture_output=True, text=True)
        # 解析输出，获取设备列表
        devices = []
        for line in result.stdout.split('\n')[1:]:  # 跳过第一行标题
            if '\tdevice' in line:  # 只获取已授权的设备
                devices.append(line.split('\t')[0])
        return devices
    except Exception as e:
        print(f"获取adb设备列表失败: {str(e)}")
        return []


class XHSOperator:
    def __init__(self, appium_server_url: str, force_app_launch: bool = False, device_id: str = None):
        """
        初始化小红书操作器
        Args:
            appium_server_url: Appium服务器URL
            force_app_launch: 是否强制重启应用
            device_id: 指定的设备ID
            system_port: Appium服务指定的本地端口，用来转发数据给安卓设备
        """
        
        # 使用指定的设备
        if not device_id:
            raise Exception("未指定设备ID")
        device_name = device_id
        print(f"使用设备: {device_name}")

        capabilities = dict(
            platformName='Android',
            automationName='uiautomator2',
            deviceName=device_name,
            udid=device_name,  # 添加udid参数，值与deviceName相同
            appPackage='com.xingin.xhs',
            appActivity='com.xingin.xhs.index.v2.IndexActivityV2',
            noReset=True,  # 保留应用数据
            fullReset=False,  # 不完全重置
            forceAppLaunch=force_app_launch,  # 是否强制重启应用
            autoGrantPermissions=True,  # 自动授予权限
            newCommandTimeout=60,  # 命令超时时间
            unicodeKeyboard=False,  # 禁用 Unicode 输入法
            resetKeyboard=False,  # 禁用重置输入法
        )

        print('当前capabilities配置:', json.dumps(capabilities, ensure_ascii=False, indent=2))
        print('正在初始化小红书控制器...',appium_server_url)
        self.driver: AppiumWebDriver = AppiumWebDriver(
            command_executor=appium_server_url,
            options=UiAutomator2Options().load_capabilities(capabilities)
        )
        print('控制器初始化完成。')
        
    def search_keyword(self, keyword: str, filters: dict = None):
        """
        搜索关键词并应用筛选条件
        Args:
            keyword: 要搜索的关键词
            filters: 筛选条件字典，可包含以下键值:
                sort_by: 排序方式 - '综合', '最新', '最多点赞', '最多评论', '最多收藏'
                note_type: 笔记类型 - '不限', '视频', '图文', '直播'
                time_range: 发布时间 - '不限', '一天内', '一周内', '半年内'
                search_scope: 搜索范围 - '不限', '已看过', '未看过', '已关注'
        """
        print(f"准备搜索关键词: {keyword}")
        try:
            # 通过 content-desc="搜索" 定位搜索按钮
            search_btn = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.Button[@content-desc='搜索']"))
            )
            search_btn.click()
            
            # 等待输入框出现并输入
            search_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((AppiumBy.CLASS_NAME, "android.widget.EditText"))
            )
            search_input.send_keys(keyword)
            
            # 点击键盘上的搜索按钮
            self.driver.press_keycode(66)  # 66 是 Enter 键的 keycode
            print(f"已搜索关键词: {keyword}")
            
            # 等待搜索结果加载
            time.sleep(1)

            if filters:
                try:
                    # 先尝试点击筛选按钮
                    all_btn = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.TextView[contains(@text, '全部') or contains(@content-desc, '全部')]"))
                        )
                    all_btn.click()
                    time.sleep(0.5)
                except:
                    # 如果找不到筛选按钮，尝试点击全部按钮
                    try:
                        filter_btn = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.TextView[contains(@text, '筛选') or contains(@content-desc, '筛选')]"))
                    )
                        filter_btn.click()
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"找不到筛选按钮和全部按钮: {str(e)}")
                        return

                # 应用排序方式
                if filters.get('sort_by'):
                    sort_option = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((
                            AppiumBy.XPATH, 
                            f"//android.widget.TextView[@text='{filters['sort_by']}']"
                        ))
                    )
                    sort_option.click()
                    time.sleep(0.5)

                # 应用笔记类型
                if filters.get('note_type'):
                    note_type = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((
                            AppiumBy.XPATH, 
                            f"//android.widget.TextView[@text='{filters['note_type']}']"
                        ))
                    )
                    note_type.click()
                    time.sleep(0.5)

                # 应用发布时间
                if filters.get('time_range'):
                    time_range = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((
                            AppiumBy.XPATH, 
                            f"//android.widget.TextView[@text='{filters['time_range']}']"
                        ))
                    )
                    time_range.click()
                    time.sleep(0.5)

                # 应用搜索范围
                if filters.get('search_scope'):
                    search_scope = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((
                            AppiumBy.XPATH, 
                            f"//android.widget.TextView[@text='{filters['search_scope']}']"
                        ))
                    )
                    search_scope.click()
                    time.sleep(0.5)

                # 点击收起按钮
                collapse_btn = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.TextView[@text='收起']"))
                )
                collapse_btn.click()
                time.sleep(0.5)

            print("筛选条件应用完成")
            
        except Exception as e:
            print(f"搜索或筛选失败: {str(e)}")
            raise
        
    def collect_notes_by_keyword_sony(self, keyword: str, max_notes: int = 10, filters: dict = None):
        """
        根据关键词收集笔记
        """
        # 搜索关键词
        print(f"搜索关键词: {keyword}")
        self.search_keyword(keyword, filters=filters)
        
        print(f"开始收集笔记,计划收集{max_notes}条...")
        collected_notes = []
        collected_titles = []
        while len(collected_notes) < max_notes:
            try:
                # 获取所有笔记标题元素
                note_titles = self.driver.find_elements(
                    by=AppiumBy.ID,
                    value="com.xingin.xhs:id/g6_"
                )
                
                for note_element in note_titles:
                    note_title_and_text = note_element.text
                    if note_title_and_text not in collected_titles:
                        print(f"收集笔记: {note_title_and_text}, 当前收集数量: {len(collected_notes)}")

                        # 点击笔记
                        note_element.click()
                        time.sleep(1)

                        # 获取笔记内容
                        note_data = self.get_note_data(note_title_and_text)

                        # 如果笔记数据不为空，则添加到列表中
                        if note_data:
                            note_data['keyword'] = keyword
                            collected_notes.append(note_data)
                            collected_titles.append(note_title_and_text)

                        # 返回上一页
                        self.driver.press_keycode(4)  # Android 返回键
                        time.sleep(1)

                        if len(collected_notes) >= max_notes:
                            break
                    else:
                        print(f"笔记已收集过: {note_title_and_text}")
                
                # 滑动到下一页
                if len(collected_notes) < max_notes:
                    self.scroll_down()
                    time.sleep(1)
            
            except Exception as e:
                print(f"收集笔记失败: {str(e)}")
                import traceback
                print(traceback.format_exc())
                break

        # 打印所有笔记数据
        for note in collected_notes:
            print("-" * 120)
            print(json.dumps(note, ensure_ascii=False, indent=2))
            print("-" * 120)

        return collected_notes

    def collect_notes_by_keyword(self, keyword: str, max_notes: int = 10, filters: dict = None): #xiaomi尝试
        """
        根据关键词收集笔记
        """
        # 搜索关键词
        print(f"搜索关键词: {keyword}")
        self.search_keyword(keyword, filters=filters)
        
        print(f"开始收集笔记,计划收集{max_notes}条...")
        collected_notes = []
        collected_titles = []
        
        while len(collected_notes) < max_notes:
            try:
                # 获取所有笔记卡片元素
                print("获取所有笔记卡片元素")
                note_cards = self.driver.find_elements(
                    by=AppiumBy.XPATH,
                    value="//android.widget.FrameLayout[@resource-id='com.xingin.xhs:id/0_resource_name_obfuscated' and @clickable='true']"
                )
                print(f"获取所有笔记卡片元素成功,共{len(note_cards)}个")
                
                for note_card in note_cards:
                    try:
                        # 获取笔记标题
                        print("卡片",note_card)
                        title_element = note_card.find_element(
                            by=AppiumBy.XPATH,
                            value=".//android.widget.TextView[contains(@text, '')]"
                        )
                        note_title_and_text = title_element.text
                        
                        # 获取作者信息
                        author_element = note_card.find_element(
                            by=AppiumBy.XPATH,
                            value=".//android.widget.LinearLayout/android.widget.TextView[1]"
                        )
                        author = author_element.text
                        
                        if note_title_and_text not in collected_titles:
                            print(f"收集笔记: {note_title_and_text}, 作者: {author}, 当前收集数量: {len(collected_notes)}")

                            # 点击笔记
                            note_card.click()
                            time.sleep(1)

                            # 获取笔记内容
                            note_data = self.get_note_data(note_title_and_text)
                            
                            # 如果笔记数据不为空，则添加到列表中
                            if note_data:
                                note_data['keyword'] = keyword
                                collected_notes.append(note_data)
                                collected_titles.append(note_title_and_text)

                            # 返回上一页
                            self.driver.press_keycode(4)  # Android 返回键
                            time.sleep(1)

                            if len(collected_notes) >= max_notes:
                                break
                        else:
                            print(f"笔记已收集过: {note_title_and_text}")
                            
                    except Exception as e:
                        print(f"处理笔记卡片失败: {str(e)}")
                        continue
                
                # 滑动到下一页
                if len(collected_notes) < max_notes:
                    self.scroll_down()
                    time.sleep(1)
            
            except Exception as e:
                print(f"收集笔记失败: {str(e)}")
                import traceback
                print(traceback.format_exc())
                break

        # 打印所有笔记数据
        for note in collected_notes:
            print("-" * 120)
            print(json.dumps(note, ensure_ascii=False, indent=2))
            print("-" * 120)

        return collected_notes
    
    def get_note_data_sony(self, note_title_and_text: str):
        """
        获取笔记内容和评论
        Args:
            note_title_and_text: 笔记标题和内容
        Returns:
            dict: 笔记数据
        """
        try:
            print(f"正在获取笔记内容: {note_title_and_text}")
            
            # 等待笔记内容加载
            time.sleep(1)
            
            # 获取笔记作者
            try:
                # 尝试查找作者元素
                author_element = None
                max_scroll_attempts = 3
                scroll_attempts = 0
                
                while not author_element and scroll_attempts < max_scroll_attempts:
                    try:
                        author_element = WebDriverWait(self.driver, 2).until(
                            EC.presence_of_element_located((AppiumBy.ID, "com.xingin.xhs:id/nickNameTV"))
                        )
                        print(f"找到作者信息元素: {author_element.text}")
                        author = author_element.text
                        break
                    except:
                        # 向下滚动一小段距离
                        self.scroll_down()
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
            max_scroll_attempts = 3  # 最大滑动次数
            scroll_count = 0
            
            note_title = ""
            note_content = ""
            while scroll_count < max_scroll_attempts:
                try:
                    # 尝试获取标题
                    title_element = self.driver.find_element(
                        by=AppiumBy.ID,
                        value="com.xingin.xhs:id/g6b"
                    )
                    note_title = title_element.text
                    print(f"找到标题: {note_title}")

                    # 尝试获取正文内容
                    content_element = self.driver.find_element(
                        by=AppiumBy.ID,
                        value="com.xingin.xhs:id/dod"
                    )
                    note_content = content_element.text
                    if note_content and note_title:
                        print("找到正文内容和标题")
                        break
                except:
                    print(f"第 {scroll_count + 1} 次滑动查找正文...")
                    # 向下滑动
                    self.scroll_down()
                    time.sleep(0.5)
                    scroll_count += 1

            # 获取互动数据 - 分别处理每个数据
            likes = "0"
            try:
                # 获取点赞数
                likes_btn = self.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.Button[contains(@content-desc, '点赞')]"
                )
                likes_text = likes_btn.find_element(
                    by=AppiumBy.ID,
                    value="com.xingin.xhs:id/g5i"
                ).text
                # 如果获取到的是"点赞"文本，则设为0
                print(f"获取到点赞数: {likes_text}")
                likes = "0" if likes_text == "点赞" else likes_text
            except Exception as e:
                print(f"获取点赞数失败: {str(e)}")

            collects = "0"
            try:
                # 获取收藏数
                collects_btn = self.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.Button[contains(@content-desc, '收藏')]"
                )
                collects_text = collects_btn.find_element(
                    by=AppiumBy.ID,
                    value="com.xingin.xhs:id/g3s"
                ).text
                # 如果获取到的是"收藏"文本，则设为0
                print(f"获取到收藏数: {collects_text}")
                collects = "0" if collects_text == "收藏" else collects_text
            except Exception as e:
                print(f"获取收藏数失败: {str(e)}")

            comments = "0"
            try:
                # 获取评论数
                comments_btn = self.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.Button[contains(@content-desc, '评论')]"
                )
                comments_text = comments_btn.find_element(
                    by=AppiumBy.ID,
                    value="com.xingin.xhs:id/g41"
                ).text
                # 如果获取到的是"评论"文本，则设为0
                print(f"获取到评论数: {comments_text}")
                comments = "0" if comments_text == "评论" else comments_text
            except Exception as e:
                print(f"获取评论数失败: {str(e)}")
            
            # # 收集评论数据
            # comments = []
            # if include_comments:
            #     print(f"\n开始收集评论 (目标数量: {max_comments})")
            #     print("-" * 50)
            #     if int(total_comments) > 0:
            #         # 循环滑动收集评论
            #         no_new_comments_count = 0  # 连续没有新评论的次数
            #         max_no_new_comments = 3  # 最大连续无新评论次数
            #         page_num = 1  # 当前页码
                    
            #         while True:
            #             print(f"\n正在处理第 {page_num} 页评论...")
                        
            #             # 检查是否到底或无评论
            #             try:
            #                 # 检查"还没有评论哦"文本
            #                 no_comments = self.driver.find_element(
            #                     by=AppiumBy.ID,
            #                     value="com.xingin.xhs:id/es1"
            #                 )
            #                 if no_comments.text in ["还没有评论哦", "- 到底了 -"]:
            #                     print(f">>> 遇到终止条件: {no_comments.text}")
            #                     break
            #             except:
            #                 pass
                        
            #             # 获取当前可见的评论元素
            #             comment_elements = self.driver.find_elements(
            #                 by=AppiumBy.ID,
            #                 value="com.xingin.xhs:id/j9m"
            #             )
            #             print(f"当前页面发现 {len(comment_elements)} 条评论")
                        
            #             current_page_has_new = False  # 当前页面是否有新评论
                        
            #             for idx, comment_elem in enumerate(comment_elements, 1):
            #                 try:
            #                     # 只获取评论内容
            #                     comment_text = comment_elem.text
                                
            #                     # 检查评论内容是否已存在
            #                     if comment_text not in comments:
            #                         comments.append(comment_text)
            #                         current_page_has_new = True
            #                         print(f"[{len(comments)}/{max_comments}] 新评论: {comment_text}")
                                    
            #                         # 检查是否达到最大评论数
            #                         if max_comments and len(comments) >= max_comments:
            #                             print(">>> 已达到目标评论数量")
            #                             break
            #                 except Exception as e:
            #                     print(f"处理第 {idx} 条评论出错: {str(e)}")
            #                     continue
                        
            #             # 如果达到最大评论数，退出循环
            #             if max_comments and len(comments) >= max_comments:
            #                 break
                            
            #             # 如果当前页面有新评论，重置计数器
            #             if current_page_has_new:
            #                 print(f"第 {page_num} 页发现新评论，继续收集")
            #                 no_new_comments_count = 0
            #             else:
            #                 no_new_comments_count += 1
            #                 print(f"第 {page_num} 页未发现新评论 ({no_new_comments_count}/{max_no_new_comments})")
                        
            #             # 如果连续多次没有新评论，认为已到底
            #             if no_new_comments_count >= max_no_new_comments:
            #                 print(">>> 连续多次未发现新评论，停止收集")
            #                 break
                        
            #             # 向下滑动
            #             print("向下滑动加载更多评论...")
            #             self.scroll_down()
            #             time.sleep(0.5)
            #             page_num += 1
                    
            #         # 返回笔记详情页
            #         print("\n评论收集完成，返回笔记详情页")
            #     print(f"共收集到 {len(comments)} 条评论")
            #     print("-" * 50)
            # else:
            #     print("不收集评论")

            # 5. 最后获取分享链接
            note_url = ""
            try:
                # 点击分享按钮
                share_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.Button[@content-desc='分享']"
                    ))
                )
                share_btn.click()
                time.sleep(1)
                
                # 点击复制链接
                copy_link_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.TextView[@text='复制链接']"
                    ))
                )
                copy_link_btn.click()
                time.sleep(1)
                
                # 获取剪贴板内容
                clipboard_data = self.driver.get_clipboard_text()
                share_text = clipboard_data.strip()
                
                # 从分享文本中提取URL
                url_start = share_text.find('http://')
                if url_start == -1:
                    url_start = share_text.find('https://')
                url_end = share_text.find('，', url_start) if url_start != -1 else -1
                
                if url_start != -1:
                    note_url = share_text[url_start:url_end] if url_end != -1 else share_text[url_start:]
                    print(f"提取到笔记URL: {note_url}")
                else:
                    note_url = "未知"
                    print(f"未能从分享链接中提取URL: {url_start}")
            
            except Exception as e:
                print(f"获取分享链接失败: {str(e)}")
                note_url = "未知"

            note_data = {
                "title": note_title,
                "content": note_content,
                "author": author,
                "likes": int(likes),
                "collects": int(collects),
                "comments": int(comments),
                "note_url": note_url,
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            print(f"获取笔记数据: {note_data}")
            return note_data
            
        except Exception as e:
            import traceback
            print(f"获取笔记内容失败: {str(e)}")
            print("异常堆栈信息:")
            print(traceback.format_exc())
            self.print_all_elements()
            return {
                "title": note_title,
                "error": str(e),
                "url": "",
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
    
    def get_note_data(self, note_title_and_text: str):
        """
        获取笔记内容和评论
        Args:
            note_title_and_text: 笔记标题和内容
        Returns:
            dict: 笔记数据
        """
        try:
            print('---------------note--------------------')
            self.print_all_elements()
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
                        author_element = WebDriverWait(self.driver, 2).until(
                            EC.presence_of_element_located((AppiumBy.ID, "com.xingin.xhs:id/nickNameTV"))
                        )
                        print(f"找到作者信息元素: {author_element.text}")
                        author = author_element.text
                        break
                    except:
                        # 向下滚动一小段距离
                        self.scroll_down()
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
            max_scroll_attempts = 3  # 最大滑动次数
            scroll_count = 0
            
            note_title = ""
            note_content = ""
            while scroll_count < max_scroll_attempts:
                try:
                    # 尝试获取标题 - 使用resource-id模式匹配
                    try:
                        # 首先尝试使用resource-id匹配标题
                        title_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[contains(@resource-id, 'com.xingin.xhs:id/') and string-length(@text) > 0 and string-length(@text) < 50]"
                        )
                        note_title = title_element.text
                        print(f"通过resource-id找到标题: {note_title}")
                    except:
                        # 如果失败，使用原来的方法
                        title_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[contains(@text, '') and string-length(@text) > 0 and not(contains(@text, '关注')) and not(contains(@text, '分享')) and not(contains(@text, '作者')) and not(@resource-id='com.xingin.xhs:id/nickNameTV')]"
                        )
                        note_title = title_element.text
                        print(f"找到标题: {note_title}")

                    # 尝试获取正文内容 - 优先匹配长文本
                    try:
                        # 首先尝试匹配长文本
                        content_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[string-length(@text) > 100]"
                        )
                        note_content = content_element.text
                        print(f"通过长文本找到正文内容: {len(note_content)} 字符")
                    except:
                        # 如果失败，尝试使用resource-id匹配
                        content_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[contains(@resource-id, 'com.xingin.xhs:id/') and string-length(@text) > 50]"
                        )
                        note_content = content_element.text
                        print(f"通过resource-id找到正文内容: {len(note_content)} 字符")
                    
                    if note_content and note_title:
                        print("找到正文内容和标题")
                        print(f"标题: {note_title}")
                        print(f"正文前100字符: {note_content[:100]}...")
                        break
                except:
                    print(f"第 {scroll_count + 1} 次滑动查找正文...")
                    # 向下滑动
                    self.scroll_down()
                    time.sleep(0.5)
                    scroll_count += 1

                        # 获取互动数据 - 分别处理每个数据
            likes = "0"
            try:
                # 获取点赞数 - 基于图片中的元素结构
                try:
                    # 首先尝试使用resource-id和content-desc结合查找
                    likes_btn = self.driver.find_element(
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
                    likes_btn = self.driver.find_element(
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
                    import re
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
                    collects_btn = self.driver.find_element(
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
                    collects_btn = self.driver.find_element(
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
                    import re
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
                    comments_btn = self.driver.find_element(
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
                    comments_btn = self.driver.find_element(
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
                    import re
                    digits = re.findall(r'\d+', comments_text)
                    comments = digits[0] if digits else "0"
                print(f"最终评论数: {comments}")
            except Exception as e:
                print(f"获取评论数失败: {str(e)}")

            # 5. 最后获取分享链接
            note_url = ""
            try:
                # 点击分享按钮
                share_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.Button[@content-desc='分享']"
                    ))
                )
                share_btn.click()
                time.sleep(1)
                
                # 点击复制链接
                copy_link_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.TextView[@text='复制链接']"
                    ))
                )
                copy_link_btn.click()
                time.sleep(1)
                
                # 获取剪贴板内容
                clipboard_data = self.driver.get_clipboard_text()
                share_text = clipboard_data.strip()
                
                # 从分享文本中提取URL
                url_start = share_text.find('http://')
                if url_start == -1:
                    url_start = share_text.find('https://')
                url_end = share_text.find('，', url_start) if url_start != -1 else -1
                
                if url_start != -1:
                    note_url = share_text[url_start:url_end] if url_end != -1 else share_text[url_start:]
                    print(f"提取到笔记URL: {note_url}")
                else:
                    note_url = "未知"
                    print(f"未能从分享链接中提取URL: {url_start}")
            
            except Exception as e:
                print(f"获取分享链接失败: {str(e)}")
                note_url = "未知"

            note_data = {
                "title": note_title,
                "content": note_content,
                "author": author,
                "likes": int(likes),
                "collects": int(collects),
                "comments": int(comments),
                "note_url": note_url,
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            print(f"获取笔记数据: {note_data}")
            return note_data
            
        except Exception as e:
            import traceback
            print(f"获取笔记内容失败: {str(e)}")
            print("异常堆栈信息:")
            print(traceback.format_exc())
            self.print_all_elements()
            return {
                "title": note_title,
                "error": str(e),
                "url": "",
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
    

    def get_note_data_init(self, note_title_and_text: str):
        """
        获取笔记内容和评论
        Args:
            note_title_and_text: 笔记标题和内容
        Returns:
            dict: 笔记数据
        """
        try:
            print('---------------note--------------------')
            self.print_all_elements()
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
                        author_element = WebDriverWait(self.driver, 2).until(
                            EC.presence_of_element_located((AppiumBy.ID, "com.xingin.xhs:id/nickNameTV"))
                        )
                        print(f"找到作者信息元素: {author_element.text}")
                        author = author_element.text
                        break
                    except:
                        # 向下滚动一小段距离
                        self.scroll_down()
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
            max_scroll_attempts = 3  # 最大滑动次数
            scroll_count = 0
            
            note_title = ""
            note_content = ""
            while scroll_count < max_scroll_attempts:
                try:
                    # 尝试获取标题 - 使用resource-id模式匹配
                    try:
                        # 首先尝试使用resource-id匹配标题
                        title_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[contains(@resource-id, 'com.xingin.xhs:id/') and string-length(@text) > 0 and string-length(@text) < 50]"
                        )
                        note_title = title_element.text
                        print(f"通过resource-id找到标题: {note_title}")
                    except:
                        # 如果失败，使用原来的方法
                        title_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[contains(@text, '') and string-length(@text) > 0 and not(contains(@text, '关注')) and not(contains(@text, '分享')) and not(contains(@text, '作者')) and not(@resource-id='com.xingin.xhs:id/nickNameTV')]"
                        )
                        note_title = title_element.text
                        print(f"找到标题: {note_title}")

                    # 尝试获取正文内容 - 优先匹配长文本
                    try:
                        # 首先尝试匹配长文本
                        content_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[string-length(@text) > 100]"
                        )
                        note_content = content_element.text
                        print(f"通过长文本找到正文内容: {len(note_content)} 字符")
                    except:
                        # 如果失败，尝试使用resource-id匹配
                        content_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.widget.TextView[contains(@resource-id, 'com.xingin.xhs:id/') and string-length(@text) > 50]"
                        )
                        note_content = content_element.text
                        print(f"通过resource-id找到正文内容: {len(note_content)} 字符")
                    
                    if note_content and note_title:
                        print("找到正文内容和标题")
                        print(f"标题: {note_title}")
                        print(f"正文前100字符: {note_content[:100]}...")
                        break
                except:
                    print(f"第 {scroll_count + 1} 次滑动查找正文...")
                    # 向下滑动
                    self.scroll_down()
                    time.sleep(0.5)
                    scroll_count += 1

            # 获取互动数据 - 分别处理每个数据
            likes = "0"
            try:
                # 获取点赞数
                likes_btn = self.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.Button[contains(@content-desc, '点赞')]"
                )
                likes_text = likes_btn.find_element(
                    by=AppiumBy.XPATH,
                    value=".//android.widget.TextView"
                ).text
                # 如果获取到的是"点赞"文本，则设为0
                print(f"获取到点赞数: {likes_text}")
                likes = "0" if likes_text == "点赞" else likes_text
            except Exception as e:
                print(f"获取点赞数失败: {str(e)}")

            collects = "0"
            try:
                # 获取收藏数
                collects_btn = self.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.Button[contains(@content-desc, '收藏')]"
                )
                collects_text = collects_btn.find_element(
                    by=AppiumBy.XPATH,
                    value=".//android.widget.TextView"
                ).text
                # 如果获取到的是"收藏"文本，则设为0
                print(f"获取到收藏数: {collects_text}")
                collects = "0" if collects_text == "收藏" else collects_text
            except Exception as e:
                print(f"获取收藏数失败: {str(e)}")

            comments = "0"
            try:
                # 获取评论数
                comments_btn = self.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.Button[contains(@content-desc, '评论')]"
                )
                comments_text = comments_btn.find_element(
                    by=AppiumBy.XPATH,
                    value=".//android.widget.TextView"
                ).text
                # 如果获取到的是"评论"文本，则设为0
                print(f"获取到评论数: {comments_text}")
                comments = "0" if comments_text == "评论" else comments_text
            except Exception as e:
                print(f"获取评论数失败: {str(e)}")

            # 5. 最后获取分享链接
            note_url = ""
            try:
                # 点击分享按钮
                share_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.Button[@content-desc='分享']"
                    ))
                )
                share_btn.click()
                time.sleep(1)
                
                # 点击复制链接
                copy_link_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.TextView[@text='复制链接']"
                    ))
                )
                copy_link_btn.click()
                time.sleep(1)
                
                # 获取剪贴板内容
                clipboard_data = self.driver.get_clipboard_text()
                share_text = clipboard_data.strip()
                
                # 从分享文本中提取URL
                url_start = share_text.find('http://')
                if url_start == -1:
                    url_start = share_text.find('https://')
                url_end = share_text.find('，', url_start) if url_start != -1 else -1
                
                if url_start != -1:
                    note_url = share_text[url_start:url_end] if url_end != -1 else share_text[url_start:]
                    print(f"提取到笔记URL: {note_url}")
                else:
                    note_url = "未知"
                    print(f"未能从分享链接中提取URL: {url_start}")
            
            except Exception as e:
                print(f"获取分享链接失败: {str(e)}")
                note_url = "未知"

            note_data = {
                "title": note_title,
                "content": note_content,
                "author": author,
                "likes": int(likes),
                "collects": int(collects),
                "comments": int(comments),
                "note_url": note_url,
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            print(f"获取笔记数据: {note_data}")
            return note_data
            
        except Exception as e:
            import traceback
            print(f"获取笔记内容失败: {str(e)}")
            print("异常堆栈信息:")
            print(traceback.format_exc())
            self.print_all_elements()
            return {
                "title": note_title,
                "error": str(e),
                "url": "",
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
    

    def scroll_down(self):
        """
        向下滑动页面
        """
        try:
            screen_size = self.driver.get_window_size()
            start_x = screen_size['width'] * 0.5
            start_y = screen_size['height'] * 0.8
            end_y = screen_size['height'] * 0.4
            
            self.driver.swipe(start_x, start_y, start_x, end_y, 1000)
            time.sleep(0.5)  # 等待内容加载
        except Exception as e:
            print(f"页面滑动失败: {str(e)}")
            raise
        
    def return_to_home_page(self):
        """
        返回小红书首页
        """
        try:
            # 尝试点击返回按钮直到回到首页
            max_attempts = 6
            for _ in range(max_attempts):
                try:
                    # 使用更通用的定位方式
                    back_btn = self.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.ImageView[@content-desc='返回']"
                    )
                    back_btn.click()
                    time.sleep(0.5)
                    
                    # 检查是否已经回到首页
                    if self.is_at_xhs_home_page():
                        print("已返回首页")
                        return
                except:
                    break
            
            # 通过点击首页按钮返回首页
            try:
                home_btn =self.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.TextView[contains(@text, '首页')]"
                )
                home_btn.click()
                time.sleep(0.5)
                if self.is_at_xhs_home_page():
                    print("已返回首页")
                    return
            except:
                pass
                
            # 如果还没回到首页，使用Android返回键
            self.driver.press_keycode(4)
            time.sleep(0.5)
            
            if not self.is_at_xhs_home_page():
                raise Exception("无法返回首页")
            
        except Exception as e:
            print(f"返回首页失败: {str(e)}")
            raise
            
    def is_at_xhs_home_page(self):
        """
        检查是否在首页
        """
        try:
            # 检查首页特有元素
            elements = [
                "//android.widget.TextView[contains(@text, '首页')]",
                "//android.widget.TextView[contains(@text, '发现')]",
                "//android.widget.TextView[contains(@text, '关注')]",
                "//android.widget.TextView[contains(@text, '购物')]",
                "//android.widget.TextView[contains(@text, '消息')]"
            ]
            
            for xpath in elements:
                self.driver.find_element(AppiumBy.XPATH, xpath)
            return True
        except:
            return False
        
    def save_notes(self, notes: list, output_file: str):
        """
        保存笔记到文件
        Args:
            notes: 笔记列表
            output_file: 输出文件路径
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(notes, f, ensure_ascii=False, indent=2)
            print(f"笔记已保存到: {output_file}")
        except Exception as e:
            print(f"保存笔记失败: {str(e)}")
            raise
        
    def print_current_page_source(self):
        """
        打印当前页面的XML结构，用于调试
        """
        print(self.driver.page_source)

    def print_all_elements(self, element_type: str = 'all'):
        """
        打印当前页面所有元素的属性和值,使用XML解析优化性能
        """
        # 一次性获取页面源码
        page_source = self.driver.page_source
        root = ElementTree.fromstring(page_source)
        
        print("\n页面元素列表:")
        print("-" * 120)
        print("序号 | 文本内容 | 类名 | 资源ID | 描述 | 可点击 | 可用 | 已选中 | 坐标 | 包名")
        print("-" * 120)
        
        for i, element in enumerate(root.findall(".//*"), 1):
            try:
                # 从XML属性中直接获取值，避免多次网络请求
                attrs = element.attrib
                text = attrs.get('text', '无')
                class_name = attrs.get('class', '无')
                resource_id = attrs.get('resource-id', '无')
                content_desc = attrs.get('content-desc', '无')
                clickable = attrs.get('clickable', '否')
                enabled = attrs.get('enabled', '否')
                selected = attrs.get('selected', '否')
                bounds = attrs.get('bounds', '无')
                package = attrs.get('package', '无')
                
                if element_type == 'note' and '笔记' in content_desc:
                    print(f"{i:3d} | {text[:20]:20s} | {class_name:30s} | {resource_id:30s} | {content_desc:20s} | "
                          f"{clickable:4s} | {enabled:4s} | {selected:4s} | {bounds:15s} | {package}")
                elif element_type == 'video' and '视频' in content_desc:
                    print(f"{i:3d} | {text[:20]:20s} | {class_name:30s} | {resource_id:30s} | {content_desc:20s} | "
                          f"{clickable:4s} | {enabled:4s} | {selected:4s} | {bounds:15s} | {package}")
                elif element_type == 'all':
                    print(f"{i:3d} | {text[:20]:20s} | {class_name:30s} | {resource_id:30s} | {content_desc:20s} | "
                          f"{clickable:4s} | {enabled:4s} | {selected:4s} | {bounds:15s} | {package}")
                elif element_type == 'text' and text != '':
                    print(f"{i:3d} | {text[:20]:20s} | {class_name:30s} | {resource_id:30s} | {content_desc:20s} | "
                          f"{clickable:4s} | {enabled:4s} | {selected:4s} | {bounds:15s} | {package}")
                else:
                    raise Exception(f"元素类型错误: {element_type}")
                
            except Exception as e:
                continue
                
        print("-" * 120)

    def find_elements_by_resource_id(self, resource_id: str):
        """
        查找笔记元素
        """
        # 获取页面源码并解析
        root = ElementTree.fromstring(self.driver.page_source)
        
        # 查找所有匹配resource-id的元素
        note_elements = []
        for element in root.findall(".//*"):
            if element.attrib.get('resource-id') == resource_id:
                note_elements.append(element)
        return note_elements
    
    def print_xml_structure(self):
        """
        打印当前页面的XML结构,便于分析页面层级
        """
        # 获取页面源码
        page_source = self.driver.page_source
        
        # 解析XML
        root = ElementTree.fromstring(page_source)
        
        def print_element(element, level=0):
            # 打印当前元素
            indent = "  " * level
            attrs = element.attrib
            class_name = attrs.get('class', '')
            text = attrs.get('text', '')
            resource_id = attrs.get('resource-id', '')
            
            # 构建元素描述
            element_desc = f"{class_name}"
            if text:
                element_desc += f" [text='{text}']"
            if resource_id:
                element_desc += f" [id='{resource_id}']"
                
            print(f"{indent}├─ {element_desc}")
            
            # 递归打印子元素
            for child in element:
                print_element(child, level + 1)
                
        print("\n页面XML结构:")
        print("根节点")
        print_element(root)
    
    def close(self):
        """
        关闭小红书操作器
        """
        if self.driver:
            self.driver.quit()
            print('控制器已关闭。')

    def scroll_in_element(self, element):
        """
        在指定元素内滑动
        """
        try:
            # 获取元素位置和大小
            location = element.location
            size = element.size
            
            # 计算滑动起点和终点
            start_x = location['x'] + size['width'] * 0.5
            start_y = location['y'] + size['height'] * 0.8
            end_y = location['y'] + size['height'] * 0.2
            
            # 执行滑动
            self.driver.swipe(start_x, start_y, start_x, end_y, 1000)
            time.sleep(0.5)  # 等待内容加载
        except Exception as e:
            print(f"元素内滑动失败: {str(e)}")
            raise

    def scroll_to_bottom(self):
        """
        滑动到页面底部
        """
        last_page_source = None
        max_attempts = 5
        attempts = 0
        
        while attempts < max_attempts:
            # 获取当前页面源码
            current_page_source = self.driver.page_source
            
            # 如果页面没有变化，说明已经到底
            if current_page_source == last_page_source:
                break
            
            # 保存当前页面源码
            last_page_source = current_page_source
            
            # 向下滑动
            self.scroll_down()
            time.sleep(0.5)
            attempts += 1

    def publish_note(self, title: str, content: str):

        """
        发布笔记
        """
        pass



    def get_redirect_url(self, short_url):
        try:
            # 添加请求头，模拟浏览器请求
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
            # 发送请求，禁止自动重定向
            response = requests.get(short_url, headers=headers, allow_redirects=False)
            # 检查响应状态码
            if response.status_code in [301, 302, 307]:  # 处理重定向状态码
                redirect_url = response.headers['Location']  # 获取重定向链接
                return redirect_url
            else:
                return "无法获取重定向链接，状态码: {}".format(response.status_code)
        except Exception as e:
            return "请求失败: {}".format(str(e))

    def collect_comments_by_url(self, note_url: str, max_comments: int = 10, max_attempts: int = 10) -> list:
        """
        根据帖子 URL 获取并解析评论信息
        Args:
            note_url: 帖子 URL
            max_attempts: 最大滑动次数
        Returns:
            list: 解析后的评论列表
        """
        try:
            print(f"开始获取并解析评论，帖子 URL: {note_url}")

            # 打开帖子页面
            self.driver.get(note_url)
            # time.sleep(0.5)  # 等待页面加载

            # 等待评论区加载
            print("等待评论区加载...")
            self.scroll_down()
            try:
                # 查找评论列表
                try:
                    WebDriverWait(self.driver, 0.3).until(
                        EC.presence_of_element_located((
                            AppiumBy.XPATH,
                            "//android.widget.TextView[contains(@text, '')]"
                        ))
                    )
                    print("找到评论区")
                except:
                    print("未找到评论区")
                    return []
            except Exception as e:
                print(f"等待评论区加载失败: {str(e)}")
                return []

            last_page_source = None
            all_comments = []  # 用于存储解析后的评论
            seen_comments = set()  # 用于去重
            max_comments = max_comments  # 最大评论数
            is_first_comment = True  # 标记是否是第一条评论

            for attempt in range(max_attempts):

                print(f"第 {attempt + 1} 次滑动加载评论...")

                # 获取当前页面源码
                page_source = self.driver.page_source

                # 如果页面没有变化，说明已经到底
                if page_source == last_page_source:
                    print("页面未发生变化，可能已到底")
                    break
                
                # 解析当前页面的评论
                try:
                    # 查找评论元素
                    comment_elements = self.driver.find_elements(
                        by=AppiumBy.XPATH,
                        value="//android.widget.TextView[contains(@text, '')]"
                    )
                    
                    # 过滤出可能是评论的元素（排除非评论文本）
                    comment_elements = [
                        e for e in comment_elements 
                        if e.text and 
                        len(e.text) > 1 and 
                        "Say something" not in e.text and
                        "说点什么" not in e.text and
                        "还没有评论哦" not in e.text and
                        "到底了" not in e.text and
                        "评论" not in e.text and
                        not e.text.endswith("comments") and  # 排除评论数
                        "First comment" not in e.text and  # 排除小红书标识
                        not re.search(r'#\S+', e.text)  # 排除包含话题标签的文本（通常是笔记正文）
                    ]
                    
                    # 跳过笔记正文（通常是第一条）
                    if comment_elements and re.search(r'#\S+', comment_elements[0].text):
                        comment_elements = comment_elements[1:]
                    
                    print(f"找到 {len(comment_elements)} 个可能的评论元素")
                    
                    # 解析每个评论元素
                    for comment_elem in comment_elements:
                        
                        #打印当前元素的属性
                        print("当前元素属性:", {
                            "text": comment_elem.text,
                            "resource-id": comment_elem.get_attribute("resource-id"),
                            "class": comment_elem.get_attribute("class"),
                            "bounds": comment_elem.get_attribute("bounds"),
                            "content-desc": comment_elem.get_attribute("content-desc")
                        })
                        
                        try:
                            # 获取评论内容
                            comment_text = comment_elem.text.strip()
                            
                            # 移除日期和回复后缀，并提取时间信息
                            time_pattern = r'(?P<date>\d{4}-\d{2}-\d{2})|(?P<short_date>\d{2}-\d{2})|(?P<yesterday>昨天\s*(?P<yesterday_time>\d{2}:\d{2}))|(?P<relative>(?P<value>\d+)\s*(?P<unit>小时|分钟)前)(?:\s*\n?\s*(?P<location>[^\s]+))?\s*回复'
                            time_match = re.search(time_pattern, comment_text)
                            collect_time = None
                            
                            if time_match:
                                if time_match.group('date'):
                                    # 标准日期格式，只保留日期
                                    collect_time = time_match.group('date')
                                elif time_match.group('short_date'):
                                    # 短日期格式，添加当前年份
                                    current_year = datetime.now().year
                                    date_str = time_match.group('short_date')
                                    collect_time = f"{current_year}-{date_str}"
                                elif time_match.group('yesterday'):
                                    # 昨天格式，获取当前日期并减一天
                                    yesterday = datetime.now() - timedelta(days=1)
                                    collect_time = yesterday.strftime('%Y-%m-%d')
                                elif time_match.group('relative'):
                                    # 相对时间格式（X小时/分钟前）
                                    now = datetime.now()
                                    value = int(time_match.group('value'))  # 直接使用捕获的数字
                                    unit = time_match.group('unit')
                                    if unit == '小时':
                                        collect_time = (now - timedelta(hours=value)).strftime('%Y-%m-%d')
                                    else:  # 分钟
                                        collect_time = (now - timedelta(minutes=value)).strftime('%Y-%m-%d')
                            
                            # 移除时间信息和回复后缀
                            comment_text = re.sub(time_pattern, '', comment_text)
                            # 额外清理可能的回复后缀
                            comment_text = re.sub(r'\s*回复\s*$', '', comment_text)
                            comment_text = comment_text.strip()
                            
                            # 跳过第一条评论（文章内容）
                            if is_first_comment:
                                is_first_comment = False
                                continue
                            
                            # 如果评论不为空且未见过，则添加到结果中
                            if comment_text and comment_text not in seen_comments and collect_time :
                                # 获取点赞数
                                try:
                                    # 获取评论元素的位置
                                    comment_loc = comment_elem.location
                                    if not comment_loc:
                                        continue

                                    # 获取评论元素下方一定范围内的所有TextView
                                    all_text_views = self.driver.find_elements(
                                        by=AppiumBy.XPATH,
                                        value="//android.widget.TextView[string-length(@text) > 0]"
                                    )

                                    # 找到评论下方最近的纯数字文本
                                    closest_likes = None
                                    min_distance = float('inf')

                                    for text_view in all_text_views:
                                        try:
                                            # 获取元素位置
                                            loc = text_view.location
                                            if not loc:
                                                continue
                                                
                                            # 计算垂直距离（只考虑下方的元素）
                                            distance = loc['y'] - comment_loc['y']
                                            
                                            # 检查是否是纯数字且在下方的合理范围内
                                            if (0 < distance < 200 and  # 在评论下方200像素内
                                                text_view.text.strip().isdigit() and  # 是纯数字
                                                distance < min_distance):  # 是最近的
                                                closest_likes = text_view
                                                min_distance = distance
                                        except:
                                            continue

                                    # 获取赞数
                                    likes = 0
                                    if closest_likes:
                                        likes = int(closest_likes.text.strip())
                                except:
                                    likes = 0

                                # 尝试获取评论者信息
                                author = "未知作者"
                                try:
                                    # 获取评论元素的位置
                                    comment_loc = comment_elem.location
                                    if not comment_loc:
                                        print("无法获取评论元素位置")
                                        continue
                                    
                                    # 在整个页面中查找作者信息
                                    # 作者通常位于评论上方，且是独立的TextView
                                    author_elements = self.driver.find_elements(
                                        by=AppiumBy.XPATH,
                                        value="//android.widget.TextView[not(contains(@text, '评论')) and not(contains(@text, '回复')) and not(contains(@text, '点赞')) and not(contains(@text, '收藏'))]"
                                    )
                                    
                                    if not author_elements:
                                        print("未找到任何可能的作者元素")
                                        continue
                                    
                                    # 找到在评论上方的最近的作者元素
                                    closest_author = None
                                    min_distance = float('inf')
                                    
                                    for author_elem in author_elements:
                                        try:
                                            author_loc = author_elem.location
                                            if not author_loc:
                                                continue
                                            
                                            # 计算垂直距离
                                            distance = comment_loc['y'] - author_loc['y']
                                            # 只考虑上方的元素，且距离要合理（比如不超过200像素）
                                            if 0 < distance < 200 and distance < min_distance:
                                                closest_author = author_elem
                                                min_distance = distance
                                        except Exception as e:
                                            print(f"处理作者元素时出错: {str(e)}")
                                            continue
                                    
                                    if closest_author:
                                        author = closest_author.text.strip()
                                        if not author:
                                            print("找到的作者元素文本为空")
                                            author = "未知作者"
                                    else:
                                        print("未找到合适的作者元素")
                                except Exception as e:
                                    print(f"获取作者信息时出错: {str(e)}")
                                
                            
                                
                                # 构建评论数据
                                comment_data = {
                                    "author": author,
                                    "content": comment_text,
                                    "likes": likes,
                                    "collect_time": collect_time
                                }
                                
                                all_comments.append(comment_data)
                                seen_comments.add(comment_text)
                                print(f"发现新评论: {comment_text[:50]}...")
                                
                                # 如果达到最大评论数，退出循环
                                if len(all_comments) >= max_comments:
                                    print(f"已达到最大评论数 {max_comments}，停止收集")
                                    return all_comments
                                
                        except Exception as e:
                            print(f"解析单个评论失败: {str(e)}")
                            continue
                except Exception as e:
                    print(f"解析页面评论失败: {str(e)}")
                    continue

                # 更新最后的页面源码
                last_page_source = page_source

                # 模拟滑动加载更多评论
                self.scroll_down()

            print(f"评论获取完成，共收集到 {len(all_comments)} 条评论")
            return all_comments

        except Exception as e:
            print(f"获取评论失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return []

    def comments_reply(self, note_url: str, author: str, comment_content: str, reply_content: str):
        """
        回复评论
        Args:
            note_url: 帖子URL
            author: 评论者
            comment_content: 评论内容
            reply_content: 回复内容
        Returns:
            bool: 是否回复成功
        """
        try:
            # 获取完整URL（处理短链接）
            full_url = self.get_redirect_url(note_url)
            print(f"处理笔记URL: {full_url}")
            
            # 打开笔记
            self.driver.get(full_url)
            time.sleep(1)  # 等待页面加载
            
            # 等待评论区加载
            print("等待评论区加载...")
            try:
                # 查找评论列表
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((
                            AppiumBy.XPATH,
                            "//android.widget.TextView[contains(@text, '')]"
                        ))
                    )
                    print("找到评论区")
                except:
                    print("未找到评论区")
                    return False
            except Exception as e:
                print(f"等待评论区加载失败: {str(e)}")
                return False
            
            # 查找目标评论
            # 先找到评论内容
            comment_found = False
            comment_element = None
            max_scroll_attempts = 10
            scroll_attempt = 0
            
            while scroll_attempt < max_scroll_attempts:
                try:
                    # 查找所有评论内容元素
                    comment_elements = self.driver.find_elements(
                        by=AppiumBy.XPATH,
                        value="//android.widget.TextView[contains(@text, '')]"
                    )
                    
                    # 过滤出可能是评论的元素
                    comment_elements = [
                        e for e in comment_elements 
                        if e.text and 
                        len(e.text) > 10 and 
                        "Say something" not in e.text and
                        "说点什么" not in e.text and
                        "还没有评论哦" not in e.text and
                        "到底了" not in e.text and
                        "评论" not in e.text and
                        not e.text.endswith("comments") and
                        "First comment" not in e.text and
                        not re.search(r'View \d+ replies', e.text)
                    ]
                    
                    # 遍历评论元素，查找匹配的评论
                    for elem in comment_elements:
                        # 获取评论文本并清理
                        elem_text = elem.text.strip()
                        # 移除表情符号和特殊字符
                        elem_text = re.sub(r'\[.*?\]', '', elem_text)  # 移除表情符号
                        elem_text = re.sub(r'\s+', ' ', elem_text)  # 合并多个空格
                        elem_text = elem_text.strip()
                        
                        # 清理目标评论内容
                        target_content = re.sub(r'\[.*?\]', '', comment_content)  # 移除表情符号
                        target_content = re.sub(r'\s+', ' ', target_content)  # 合并多个空格
                        target_content = target_content.strip()
                        
                        # 使用部分匹配
                        if target_content in elem_text or elem_text in target_content:
                            print(f"找到匹配评论: {elem_text}")
                            # 找到评论内容后，向上查找作者
                            comment_loc = elem.location
                            if not comment_loc:
                                continue
                                
                            # 如果作者是"未知用户"，直接匹配评论内容
                            if author == "未知用户":
                                comment_element = elem
                                comment_found = True
                                print("作者为未知用户，直接匹配评论内容")
                                break
                                
                            # 在整个页面中查找作者信息
                            author_elements = self.driver.find_elements(
                                by=AppiumBy.XPATH,
                                value="//android.widget.TextView[not(contains(@text, '评论')) and not(contains(@text, '回复')) and not(contains(@text, '点赞')) and not(contains(@text, '收藏'))]"
                            )
                            
                            if not author_elements:
                                continue
                            
                            # 找到在评论上方的最近的作者元素
                            closest_author = None
                            min_distance = float('inf')
                            
                            for author_elem in author_elements:
                                try:
                                    author_loc = author_elem.location
                                    if not author_loc:
                                        continue
                                    
                                    # 计算垂直距离
                                    distance = comment_loc['y'] - author_loc['y']
                                    # 只考虑上方的元素，且距离要合理（比如不超过200像素）
                                    if 0 < distance < 200 and distance < min_distance:
                                        closest_author = author_elem
                                        min_distance = distance
                                except Exception as e:
                                    print(f"处理作者元素时出错: {str(e)}")
                                    continue
                            
                            if closest_author and closest_author.text.strip() == author:
                                comment_element = elem
                                comment_found = True
                                print(f"找到匹配的作者: {author}")
                                break
                    
                    if comment_found:
                        break
                        
                    # 如果没找到，向下滚动
                    self.driver.swipe(500, 1000, 500, 500, 1000)
                    scroll_attempt += 1
                    
                except Exception as e:
                    print(f"查找评论时出错: {str(e)}")
                    # 如果没找到，向下滚动
                    self.driver.swipe(500, 1000, 500, 500, 1000)
                    scroll_attempt += 1
            
            if not comment_found:
                print(f"未找到评论: {comment_content}")
                return False
            
            # 点击评论
            comment_element.click()
            
            # 等待输入框出现并输入内容
            try:
                # 等待输入框出现
                reply_input = WebDriverWait(self.driver, 1).until(
                    EC.presence_of_element_located((
                        AppiumBy.CLASS_NAME,
                        "android.widget.EditText"
                    ))
                )
                # 输入回复内容
                reply_input.clear()
                reply_input.send_keys(reply_content)
                
                # 点击发送按钮
                send_button = WebDriverWait(self.driver, 1).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.TextView[@text='发送']"
                    ))
                )
                send_button.click()
                
                print(f"成功回复评论: {reply_content}")
                return True
                
            except Exception as e:
                print(f"输入回复内容失败: {str(e)}")
                return False
            
        except Exception as e:
            print(f"回复评论失败: {str(e)}")
            return False


# 测试代码
if __name__ == "__main__":
    # 加载.env文件
    from dotenv import load_dotenv
    import os

    # 获取当前文件所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 加载.env文件
    load_dotenv(os.path.join(current_dir, '.env'))
    print(os.path.join(current_dir, '.env'))
    
    # 获取Appium服务器URL
    appium_server_url = os.getenv('APPIUM_SERVER_URL', 'http://localhost:4723')

    print(appium_server_url)
    # 初始化小红书操作器
    xhs = XHSOperator(
        appium_server_url=appium_server_url,
        force_app_launch=True,
        device_id="c2c56d1b0107",
        system_port=8200
    )

    try:
        # 1 测试收集文章
        # print("\n开始测试收集文章...")
        # notes = xhs.collect_notes_by_keyword(
        #     keyword="龙图",
        #     max_notes=10,
        #     filters={
        #         "note_type": "图文",  # 只收集图文笔记
        #         "sort_by": "最新"  # 按最新排序
        #     }
        # )
        
        # print(f"\n共收集到 {len(notes)} 条笔记:")
        # for i, note in enumerate(notes, 1):
        #     print(f"\n笔记 {i}:")
        #     print(f"标题: {note.get('title', 'N/A')}")
        #     print(f"作者: {note.get('author', 'N/A')}")
        #     print(f"内容: {note.get('content', 'N/A')[:100]}...")  # 只显示前100个字符
        #     print(f"URL: {note.get('note_url', 'N/A')}")
        #     print(f"点赞: {note.get('likes', 'N/A')}")
        #     print(f"收藏: {note.get('collects', 'N/A')}")
        #     print(f"评论: {note.get('comments', 'N/A')}")
        #     print(f"收集时间: {note.get('collect_time', 'N/A')}")
        #     print("-" * 50) 

        # 2 测试收集评论
        print("\n开始测试收集评论...")
        note_url = "http://xhslink.com/a/KARbkJb1qGSbb"
        full_url = xhs.get_redirect_url(note_url)
        print(f"帖子 URL: {full_url}")
        
        comments = xhs.collect_comments_by_url(full_url,max_comments=10)
        print(f"\n共收集到 {len(comments)} 条评论:")
        for i, comment in enumerate(comments, 1):
            print(f"\n评论 {i}:")
            print(f"作者: {comment['author']}")
            print(f"内容: {comment['content']}")
            print(f"点赞: {comment['likes']}")
            print(f"时间: {comment['collect_time']}")
            print("-" * 50)

        #3 测试根据评论者id和评论内容定位该条评论并回复
        # note_url = "http://xhslink.com/a/Hr4QFxdhrNrbb"
        # author = "小红薯65C0511B"  # 替换为实际的评论者ID
        # comment_content = "靠，首付6万，两年零息，这也太爽了吧，说的我也想换了"  # 替换为实际的评论内容
        # reply_content = "有兴趣的私哦"  # 替换为要回复的内容
        
        # print("\n开始测试评论回复功能...")
        # success = xhs.comments_reply(
        #     note_url=note_url,
        #     author=author,
        #     comment_content=comment_content,
        #     reply_content=reply_content
        # )
        
        # if success:
        #     print("评论回复成功！")
        # else:
        #     print("评论回复失败！")
            
        # print("-" * 50)


    except Exception as e:
        print(f"运行出错: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        # 关闭操作器
        xhs.close()
