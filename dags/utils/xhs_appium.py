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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
import os
import json
import time
import subprocess
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
from airflow.models import Variable

from appium.webdriver.webdriver import WebDriver as AppiumWebDriver
from appium.webdriver.common.appiumby import AppiumBy
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from appium.options.android import UiAutomator2Options
from airflow.models.variable import Variable
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
            time.sleep(5)
            
            raise


    def search_keyword_of_video(self, keyword, max_videos=10):
        """
        搜索关键词视频并收集视频卡片信息
        
        参数:
            keyword (str): 搜索关键词
            max_videos (int): 最大收集视频数量，默认10个
            
        返回:
            list: 收集到的视频数据列表
        """
        collected_videos = []
        collected_titles = []
        
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
                    return collected_videos
            try:
                sort_option = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((
                                AppiumBy.XPATH, 
                                f"//android.widget.TextView[@text='视频']"
                            ))
                        )
                sort_option.click()
                time.sleep(0.5)
                collapse_btn = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.TextView[@text='收起']"))
                )
                collapse_btn.click()
                time.sleep(0.5)
            except Exception as e:
                print(f"找不到视频选项: {str(e)}")
            
            # 开始收集视频卡片
            while len(collected_videos) < max_videos:
                try:
                    print("获取所有视频卡片元素")
                    video_cards = []
                    try:
                        # 获取视频卡片元素
                        video_cards = self.driver.find_elements(
                            by=AppiumBy.XPATH,
                            value="//android.widget.FrameLayout[@resource-id='com.xingin.xhs:id/-' and @clickable='true']"
                        )
                        print(f"获取视频卡片成功，共{len(video_cards)}个")
                    except Exception as e:
                        print(f"获取视频卡片失败: {e}")
                    
                    for video_card in video_cards:
                        if len(collected_videos) >= max_videos:
                            break
                        try:
                            title_element = video_card.find_element(
                                by=AppiumBy.XPATH,
                                value=".//android.widget.TextView[contains(@text, '')]"
                            )
                            video_title_and_text = title_element.text
                            author_element = video_card.find_element(
                                by=AppiumBy.XPATH,
                                value=".//android.widget.LinearLayout/android.widget.TextView[1]"
                            )
                            author = author_element.text
                            
                            if video_title_and_text not in collected_titles:
                                print(f"收集视频: {video_title_and_text}, 作者: {author}, 当前收集数量: {len(collected_videos)}")
                                
                                # 获取屏幕尺寸和元素位置
                                try:
                                    screen_size = self.driver.get_window_size()
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
                                    print(f"检测元素位置失败: {str(e)}，不执行点击")
                                    time.sleep(0.5)
                                
                                # 获取视频数据
                                video_data = self.get_video_note_data(video_title_and_text)
                                if video_data:
                                    video_data['keyword'] = keyword
                                    video_data['content_type'] = 'video'  # 标记为视频类型
                                    collected_titles.append(video_title_and_text)
                                    collected_videos.append(video_data)
                                
                                # 返回上一页
                                try:
                                    back_btn = self.driver.find_element(
                                        by=AppiumBy.XPATH,
                                        value="//android.widget.ImageView[@content-desc='返回']"
                                    )
                                    back_btn.click()
                                    time.sleep(0.5)
                                except Exception as e:
                                    print(f"返回失败: {str(e)}")
                                    # 尝试使用返回键
                                    self.driver.press_keycode(4)
                                    time.sleep(0.5)
                        except Exception as e:
                            print(f"处理视频卡片失败: {str(e)}")
                            continue
                    
                    # 如果还没收集够，向下滚动
                    if len(collected_videos) < max_videos:
                        self.scroll_down()
                        time.sleep(0.5)
                        
                except Exception as e:
                    print(f"收集视频失败: {str(e)}")
                    import traceback
                    print(traceback.format_exc())
                    break
            
            print(f"视频收集完成，共收集到 {len(collected_videos)} 个视频")
            return collected_videos
            
        except Exception as e:
            print(f"搜索或筛选失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return collected_videos


    def process_time_string(self, input_str):
        """
        完整处理时间字符串，支持多种时间格式
        返回标准化时间戳和地区信息
        
        参数:
            input_str (str): 原始时间地点字符串
            
        返回:
            dict: 包含 'timestamp' (YYYY-MM-DD HH:MM:SS) 和 'location' 的字典
        """
        # 获取当前日期和时间
        now = datetime.now()
        current_date = now.date()
        current_year = now.year
        
        # 初始化结果
        result = {
            "timestamp": "",
            "location": "无地区"
        }
        
        # 0. 预处理 - 统一全角半角符号
        input_str = input_str.replace("：", ":")  # 全角冒号转半角
        input_str = input_str.replace("Ｘ", "X")  # 全角X转半角
        
        # 1. 处理"x小时前"、"x分钟前"、"x秒前"格式
        # =================================================================
        time_ago_match = re.search(r"(\d+)\s*(小时|分钟|秒)[以之]?前", input_str)
        if time_ago_match:
            num = int(time_ago_match.group(1))
            unit = time_ago_match.group(2)
            
            # 根据单位减去相应时间
            if unit == "小时":
                target_time = now - timedelta(hours=num)
            elif unit == "分钟":
                target_time = now - timedelta(minutes=num)
            else:  # 秒
                target_time = now - timedelta(seconds=num)
            
            result["timestamp"] = target_time.strftime("%Y-%m-%d %H:%M:%S")
            
            # 提取地点
            location = re.sub(r"(\d+)\s*(小时|分钟|秒)[以之]?前\s*", "", input_str).strip()
            if location:
                result["location"] = location
            return result
        
        # 2. 处理"今天"格式（新增对"今天 10:39山东"格式的专门处理）
        # =================================================================
        today_match = re.search(r"今天\s*(\d{1,2}:\d{1,2})", input_str)
        if today_match:
            # 提取时间部分 (如 "10:39")
            time_str = today_match.group(1)
            try:
                # 尝试解析时间
                time_part = datetime.strptime(time_str, "%H:%M").time()
                result["timestamp"] = datetime.combine(current_date, time_part).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                # 时间解析失败，使用中午12点
                result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
            
            # 提取地点 - 移除"今天"和时间部分
            location = re.sub(r"今天\s*\d{1,2}:\d{1,2}\s*", "", input_str).strip()
            if location:
                result["location"] = location
            return result
        
        # 3. 处理"昨天"格式
        # =================================================================
        if "昨天" in input_str:
            # 提取时间部分 (如 "16:59")
            time_match = re.search(r"(\d{1,2}:\d{1,2})", input_str)
            target_date = current_date - timedelta(days=1)  # 减去一天
            
            if time_match:
                # 使用昨天日期 + 提取的时间
                time_str = time_match.group(1)
                try:
                    # 尝试解析时间
                    time_part = datetime.strptime(time_str, "%H:%M").time()
                    result["timestamp"] = datetime.combine(target_date, time_part).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    # 时间解析失败，使用中午12点
                    result["timestamp"] = target_date.strftime("%Y-%m-%d 12:00:00")
            else:
                # 没有时间部分，使用中午12点
                result["timestamp"] = target_date.strftime("%Y-%m-%d 12:00:00")
            
            # 提取地点
            location = re.sub(r"昨天\s*\d{1,2}:\d{1,2}\s*", "", input_str)
            location = re.sub(r"昨天\s*", "", location).strip()
            if location:
                result["location"] = location
            return result
        
        # 4. 处理"编辑于"格式
        # =================================================================
        if "编辑于" in input_str:
            # 先尝试匹配完整的YYYY-MM-DD格式
            date_match_full = re.search(r"编辑于(\d{4}-\d{2}-\d{2})", input_str)
            if date_match_full:
                date_str = date_match_full.group(1)
                try:
                    # 尝试解析完整日期
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    result["timestamp"] = date_obj.strftime("%Y-%m-%d 12:00:00")
                except ValueError:
                    # 日期无效，使用当前日期
                    result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
                    
                # 提取地点
                location = re.sub(r"编辑于\d{4}-\d{2}-\d{2}\s*", "", input_str).strip()
                if location:
                    result["location"] = location
                return result
            
            # 如果没有匹配到完整格式，尝试匹配HH:MM时间格式
            time_match = re.search(r"编辑于(\d{1,2}:\d{2})", input_str)
            if time_match:
                time_str = time_match.group(1)
                try:
                    # 尝试解析时间
                    time_part = datetime.strptime(time_str, "%H:%M").time()
                    result["timestamp"] = datetime.combine(current_date, time_part).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    # 时间解析失败，使用中午12点
                    result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
                    
                # 提取地点
                location = re.sub(r"编辑于\d{1,2}:\d{2}\s*", "", input_str).strip()
                if location:
                    result["location"] = location
                return result
            
            # 如果没有匹配到时间格式，尝试匹配MM-DD格式
            date_match = re.search(r"编辑于(\d{2}-\d{2})", input_str)
            if date_match:
                date_str = date_match.group(1)
                try:
                    # 尝试解析日期
                    date_obj = datetime.strptime(f"{current_year}-{date_str}", "%Y-%m-%d")
                    result["timestamp"] = date_obj.strftime("%Y-%m-%d 12:00:00")
                except ValueError:
                    # 日期无效，使用当前日期
                    result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
            else:
                result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
            
            # 提取地点
            location = re.sub(r"编辑于\d{2}-\d{2}\s*", "", input_str).strip()
            location = re.sub(r"编辑于\s*", "", location).strip()
            if location:
                result["location"] = location
            return result
        
        # 5. 处理 YYYY-MM-DD 格式
        # =================================================================
        if re.match(r"\d{4}-\d{2}-\d{2}", input_str):
            try:
                # 尝试解析完整日期
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", input_str)
                if date_match:
                    date_obj = datetime.strptime(date_match.group(1), "%Y-%m-%d")
                    result["timestamp"] = date_obj.strftime("%Y-%m-%d 12:00:00")
            except ValueError:
                # 日期无效，使用当前日期
                result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
            
            # 提取地点
            location = re.sub(r"\d{4}-\d{2}-\d{2}\s*", "", input_str).strip()
            if location:
                result["location"] = location
            return result
        
        # 6. 处理 MM-DD 格式
        # =================================================================
        if re.match(r"\d{2}-\d{2}", input_str):
            date_match = re.search(r"(\d{2}-\d{2})", input_str)
            if date_match:
                date_str = date_match.group(1)
                try:
                    # 尝试解析日期
                    date_obj = datetime.strptime(f"{current_year}-{date_str}", "%Y-%m-%d")
                    result["timestamp"] = date_obj.strftime("%Y-%m-%d 12:00:00")
                except ValueError:
                    # 日期无效，使用当前日期
                    result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
            else:
                result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
            
            # 提取地点
            location = re.sub(r"\d{2}-\d{2}\s*", "", input_str).strip()
            if location:
                result["location"] = location
            return result
        
        # 7. 处理仅时间格式 (如 "16:59北京")
        # =================================================================
        time_match = re.search(r"(\d{1,2}:\d{1,2})", input_str)
        if time_match:
            # 使用当前日期 + 提取的时间
            time_str = time_match.group(1)
            try:
                # 尝试解析时间
                time_part = datetime.strptime(time_str, "%H:%M").time()
                result["timestamp"] = datetime.combine(current_date, time_part).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                # 时间解析失败，使用中午12点
                result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
            
            # 提取地点
            location = re.sub(r"\d{1,2}:\d{1,2}\s*", "", input_str).strip()
            if location:
                result["location"] = location
            return result
        
        # 8. 默认处理 - 当所有格式都不匹配时
        # =================================================================
        result["timestamp"] = current_date.strftime("%Y-%m-%d 12:00:00")
        
        # 尝试提取任何可能的地点信息
        # 移除数字和特殊字符，保留中文字符
        location = re.sub(r"[\d\s\-:]+", "", input_str).strip()
        if location:
            result["location"] = location
        
        return result
#解析评论中包含的时间和地点信息，适配多种格式
    def extract_time_location_from_text(self,text):
        
        now = datetime.now()
        current_year = now.year
        
        # 初始化结果
        timestamp = ""
        location = "无地区"  # 默认为"无地区"
        cleaned_text = text
        
        # 1. 处理完整的YYYY-MM-DD格式 (优先处理，避免误匹配)
        date_match_full = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)

        if date_match_full:
        
            year = date_match_full.group(1)
            month = date_match_full.group(2)
            day = date_match_full.group(3)
            date_str = f"{year}-{month}-{day}"
            timestamp = f"{date_str} 00:00:00"
            
            # 提取地区信息 (通常在日期后面)
            location_match = re.search(f"{re.escape(date_str)}\s+([^\s]+)(?=\s+回复|$)", text)
            if location_match:
                location = location_match.group(1)
                # 只删除时间戳和地区标记的组合，不影响评论内容中的地区名称
                pattern = f"\s*{re.escape(date_str)}\s+{re.escape(location)}(?=\s+回复|$)"
                cleaned_text = re.sub(pattern, "", text)
                # 删除末尾的回复字样
                cleaned_text = re.sub(r"\s+回复$", "", cleaned_text).strip()
            else:
                # 如果没有找到地区信息，只删除日期和回复字样
                pattern = f"\s*{re.escape(date_str)}(?=\s+回复|$)"
                cleaned_text = re.sub(pattern, "", text)
                # 删除末尾的回复字样
                cleaned_text = re.sub(r"\s+回复$", "", cleaned_text).strip()
                # 确保地区为"无地区"
                location = "无地区"
        
        # 2. 处理"昨天 HH:MM [地区]"格式（地区可选）
        elif yesterday_match := re.search(r"昨天\s+(\d{1,2}):(\d{2})(?:\s+([^\s]+))?(?=\s+回复|$)", text):
            hour = yesterday_match.group(1)
            minute = yesterday_match.group(2)
            location_part = yesterday_match.group(3) if yesterday_match.group(3) else None
            
            if location_part:
                location = location_part
                time_str = f"昨天 {hour}:{minute} {location}"
            else:
                location = "无地区"
                time_str = f"昨天 {hour}:{minute}"
            
            # 计算昨天的日期和时间
            yesterday = now - timedelta(days=1)
            timestamp = yesterday.replace(hour=int(hour), minute=int(minute), second=0).strftime("%Y-%m-%d %H:%M:%S")
            
            # 删除时间戳和地区标记的组合
            pattern = f"\s*{re.escape(time_str)}(?=\s+回复|$)"
            cleaned_text = re.sub(pattern, "", text)
            # 删除末尾的回复字样
            cleaned_text = re.sub(r"\s+回复$", "", cleaned_text).strip()
        
        # 3. 处理"x分钟前 [地区]"格式（地区可选）
        elif minutes_ago_match := re.search(r"(\d+)\s*分钟前(?:\s+([^\s]+))?(?=\s+回复|$)", text):
            minutes_ago = int(minutes_ago_match.group(1))
            location_part = minutes_ago_match.group(2) if minutes_ago_match.group(2) else None
            
            if location_part:
                location = location_part
                time_str = f"{minutes_ago}分钟前 {location}"
            else:
                location = "无地区"
                time_str = f"{minutes_ago}分钟前"
            
            timestamp = (now - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")
            
            # 删除时间戳和地区标记的组合
            pattern = f"\s*{re.escape(time_str)}(?=\s+回复|$)"
            cleaned_text = re.sub(pattern, "", text)
            # 删除末尾的回复字样
            cleaned_text = re.sub(r"\s+回复$", "", cleaned_text).strip()
        
        # 4. 处理"x小时前 [地区]"格式（地区可选）
        elif time_ago_match := re.search(r"(\d+)\s*小时前(?:\s+([^\s]+))?(?=\s+回复|$)", text):
            hours_ago = int(time_ago_match.group(1))
            location_part = time_ago_match.group(2) if time_ago_match.group(2) else None
            
            if location_part:
                location = location_part
                time_str = f"{hours_ago}小时前 {location}"
            else:
                location = "无地区"
                time_str = f"{hours_ago}小时前"
            
            timestamp = (now - timedelta(hours=hours_ago)).strftime("%Y-%m-%d %H:%M:%S")
            
            # 删除时间戳和地区标记的组合
            pattern = f"\s*{re.escape(time_str)}(?=\s+回复|$)"
            cleaned_text = re.sub(pattern, "", text)
            # 删除末尾的回复字样
            cleaned_text = re.sub(r"\s+回复$", "", cleaned_text).strip()
        
        # 5. 处理MM-DD格式
        elif date_match := re.search(r"(\d{2})-(\d{2})", text):
            month = date_match.group(1)
            day = date_match.group(2)
            date_str = f"{month}-{day}"
            timestamp = f"{current_year}-{month}-{day} 00:00:00"
            
            # 提取地区信息 (通常在日期后面)
            location_match = re.search(f"{re.escape(date_str)}\s+([^\s]+)(?=\s+回复|$)", text)
            if location_match:
                location = location_match.group(1)
                # 只删除时间戳和地区标记的组合，不影响评论内容中的地区名称
                pattern = f"\s*{re.escape(date_str)}\s+{re.escape(location)}(?=\s+回复|$)"
                cleaned_text = re.sub(pattern, "", text)
                # 删除末尾的回复字样
                cleaned_text = re.sub(r"\s+回复$", "", cleaned_text).strip()
            else:
                # 如果没有找到地区信息，只删除日期和回复字样
                pattern = f"\s*{re.escape(date_str)}(?=\s+回复|$)"
                cleaned_text = re.sub(pattern, "", text)
                # 删除末尾的回复字样
                cleaned_text = re.sub(r"\s+回复$", "", cleaned_text).strip()
                # 确保地区为"无地区"
                location = "无地区"
        
        # 如果没有找到时间信息，但有回复字样，删除回复字样
        else:
            cleaned_text = re.sub(r"\s+回复$", "", text).strip()
            # 确保地区为"无地区"
            location = "无地区"
        
        # 移除表情符号 (简单处理，移除方括号内的内容)
        cleaned_text = re.sub(r"\[.*?\]", "", cleaned_text)
        
        # 移除多余空格
        cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()
        
        return {
            "cleaned_text": cleaned_text.replace("翻译", ""),
            "timestamp": timestamp if timestamp else now.strftime("%Y-%m-%d %H:%M:%S"),
            "location": location.replace("回复", "").strip()
        }
    

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
                            time.sleep(1)
                            
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
            max_scroll_attempts = 5  # 最大滑动次数
            scroll_count = 0
            
            note_title = ""
            note_content = ""
           
            #修改标题定位逻辑，一般进入笔记时就可定位到标题，无需滑动，嵌套在循环中会导致二次定位成错误元素
            # 尝试获取标题 
            try:
                # 如果失败，使用原来的方法
                title_element = self.driver.find_element(
                    by=AppiumBy.XPATH,
                    value="//android.widget.TextView[contains(@text, '') and string-length(@text) > 3 and not(contains(@text, '1/')) and not(contains(@text, 'LIVE')) and not(contains(@text, '试试文字发笔记')) and not(contains(@text, '关注')) and not(contains(@text, '分享')) and not(contains(@text, '作者')) and not(@resource-id='com.xingin.xhs:id/nickNameTV')]"
                )
                note_title = title_element.text
                print(f"找到标题: {note_title}")
                        
            except:
                # 尝试使用resource-id匹配标题
                title_element = self.driver.find_element(
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
                        note_time_element = self.driver.find_element(
                            by=AppiumBy.XPATH,
                            value="//android.view.View[contains(@content-desc, '-') or contains(@content-desc, ':') or contains(@content-desc, '编辑于')]"
                        )
                        time_content = note_time_element.get_attribute("content-desc")
                        print(f"找到笔记修改时间: {time_content}")
                        format_time=self.process_time_string(time_content)['timestamp']
                        format_location=self.process_time_string(time_content)['location'].replace("编辑于","")
                        print(f"时间格式化为: {format_time},地区格式化为: {format_location}")
                        note_time_exists = True
                    except:
                        print(f"未找到笔记修改时间")
                    
                try:
                   
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
                        if note_time_exists == True:
                            #修改时间位于正文下方，找到时间后再退出循环
                            break
                        else:
                            self.scroll_down()
                            time.sleep(0.5)
                            scroll_count += 1
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
                    note_url = self.get_redirect_url(note_url.replace("复制本条信息", "").strip())
                    print(f"重定向后的笔记URL: {note_url}")
                    
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
            self.print_all_elements()
            return {
                "title": note_title,
                "error": str(e),
                "url": "",
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
    
    def get_video_note_data(self, video_title_and_text: str):
        """
        获取视频笔记内容和互动数据
        Args:
            video_title_and_text: 视频标题和内容
        Returns:
            dict: 视频数据
        """
        try:
            print('---------------video--------------------')
            self.print_all_elements()
            print(f"正在获取视频内容: {video_title_and_text}")
            
            # 等待视频内容加载
            time.sleep(0.5)
            
            # 获取视频标题 - 使用指定的XPath
            video_title = ""
            try:
                title_element = self.driver.find_element(
                    by=AppiumBy.ID,
                    value="com.xingin.xhs:id/noteContentLayout"
                )
                video_title = title_element.get_attribute("content-desc")
                print(f"找到视频标题: {video_title}")
            except Exception as e:
                print(f"获取视频标题失败: {str(e)}")
                video_title = video_title_and_text  # 使用传入的标题作为备选
            
            # 获取视频作者
            author = ""
            
            try:
                author_element = WebDriverWait(self.driver, 2).until(
                    EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.Button[contains(@content-desc, '作者')]"))
                )
                author = author_element.get_attribute("content-desc")
                print(f"找到作者信息: {author}")
            except Exception as e:
                print(f"获取作者信息失败: {str(e)}")
            
            # 获取点赞数
            likes = "0"
            try:
                try:
                    likes_btn = self.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '点赞')]"
                    )
                    likes_text = likes_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过content-desc找到点赞数: {likes_text}")
                except:
                    print("未找到点赞数")
                
                if likes_text == "点赞":
                    likes = "0"
                else:
                    
                    digits = re.findall(r'\d+', likes_text)
                    likes = digits[0] if digits else "0"
                print(f"最终点赞数: {likes}")
            except Exception as e:
                print(f"获取点赞数失败: {str(e)}")
            
            # 获取评论数
            comments = "0"
            try:
                try:
                    comments_btn = self.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '评论')]"
                    )
                    comments_text = comments_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过resource-id和content-desc找到评论数: {comments_text}")
                except:
                    print("未找到评论数")
                    
                
                if comments_text == "评论":
                    comments = "0"
                else:
                   
                    digits = re.findall(r'\d+', comments_text)
                    comments = digits[0] if digits else "0"
                print(f"最终评论数: {comments}")
            except Exception as e:
                print(f"获取评论数失败: {str(e)}")
            
            # 获取分享数（收藏数）
            shares = "0"
            try:
                try:
                    shares_btn = self.driver.find_element(
                        by=AppiumBy.XPATH,
                        value="//android.widget.Button[contains(@content-desc, '收藏')]"
                    )
                    shares_text = shares_btn.find_element(
                        by=AppiumBy.XPATH,
                        value=".//android.widget.TextView"
                    ).text
                    print(f"通过resource-id和content-desc找到收藏数: {shares_text}")
                except:
                    print("未找到收藏数")
                   
                if shares_text == "收藏":
                    shares = "0"
                else:
                    
                    digits = re.findall(r'\d+', shares_text)
                    shares = digits[0] if digits else "0"
                print(f"最终收藏数: {shares}")
            except Exception as e:
                print(f"获取收藏数失败: {str(e)}")
            
            # 获取分享链接
            video_url = ""
            try:
                share_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.Button[contains(@content-desc, '分享')]"
                    ))
                )
                share_btn.click()
                time.sleep(1)
                
                # 先定位到指定的LinearLayout元素
                target_element = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.LinearLayout[@resource-id='com.xingin.xhs:id/-']/android.widget.LinearLayout"
                    ))
                )
                
                # 在target_element上向左滑动半个屏幕宽度
                element_location = target_element.location
                element_size = target_element.size
                
                # 计算元素中心点
                element_center_x = element_location['x'] + element_size['width'] / 2
                element_center_y = element_location['y'] + element_size['height'] / 2
                
                # 获取屏幕宽度用于计算滑动距离
                screen_size = self.driver.get_window_size()
                swipe_distance = screen_size['width'] * 0.5  # 半个屏幕宽度
                
                # 在元素上向左滑动
                start_x = element_center_x + swipe_distance / 2
                end_x = element_center_x - swipe_distance / 2
                
                self.driver.swipe(start_x, element_center_y, end_x, element_center_y, 500)
                time.sleep(1)
                
                # 然后点击复制链接按钮
                copy_link_btn = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.Button[@content-desc='复制链接']"
                    ))
                )
                copy_link_btn.click()
                time.sleep(1)
                
                clipboard_data = self.driver.get_clipboard_text()
                share_text = clipboard_data.strip()
                
                url_start = share_text.find('http://')
                if url_start == -1:
                    url_start = share_text.find('https://')
                url_end = share_text.find('，', url_start) if url_start != -1 else -1
                
                if url_start != -1:
                    video_url = share_text[url_start:url_end] if url_end != -1 else share_text[url_start:]
                    print(f"提取到视频URL: {video_url}")
                    video_url = self.get_redirect_url(video_url.replace('复制本条信息','').strip())
                    print(f"重定向后的视频URL: {video_url}")
                else:
                    video_url = "未知"
                    print(f"未能从分享链接中提取URL")
            except Exception as e:
                print(f"获取分享链接失败: {str(e)}")
                video_url = "未知"
            
            video_data = {
                "title": video_title,
                "author": author.replace("作者", "").strip(),
                "likes": int(likes),
                "comments": int(comments),
                "shares": int(shares),
                "video_url": video_url,
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "content_type": "video"
            }
            
            print(f"获取视频数据: {video_data}")
            return video_data
            
        except Exception as e:
            import traceback
            print(f"获取视频内容失败: {str(e)}")
            print("异常堆栈信息:")
            print(traceback.format_exc())
            self.print_all_elements()
            return {
                "title": video_title_and_text,
                "error": str(e),
                "url": "",
                "collect_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "content_type": "video"
            }

    
#改动
    def scroll_down(self):
        """
        向下滑动页面
        """
        attempts = 0
        
        try:
            if attempts >= 3:
                print("已尝试多次滑动页面，放弃滑动")
                return
            screen_size = self.driver.get_window_size()
            start_x = screen_size['width'] * 0.5
            start_y = screen_size['height'] * 0.8
            end_y = screen_size['height'] * 0.35
            self.driver.swipe(start_x, start_y, start_x, end_y, 800)
            time.sleep(1)  # 等待内容加载
        except Exception as e:
            print(f"页面滑动失败: {str(e)}")
            time.sleep(1)  # 等待一段时间后重试
            attempts += 1
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
            #更改滑动距离,提高总体效率和稳定性
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
        max_attempts = 10
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
    def get_redirect_url(self, short_url: str, max_retries: int = 3) :
        """
        带重试机制的重定向URL获取
        """
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 用户代理池
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        
        for attempt in range(max_retries):
            try:
                headers = {
                    "User-Agent": random.choice(user_agents),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Connection": "keep-alive"
                }
                
                response = session.get(
                    short_url,
                    headers=headers,
                    allow_redirects=False,
                    timeout=(10, 30),
                    verify=False
                )
                
                if response.status_code in [301, 302, 303, 307, 308]:
                    redirect_url = response.headers.get('Location')
                    if redirect_url:
                        return redirect_url
                
                # 如果不是重定向，等待后重试
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                    
            except Exception as e:
                print(f"第 {attempt + 1} 次尝试失败: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
        
        return  f"经过 {max_retries} 次重试后仍然失败"

    def collect_comments_by_url(self, note_url: str, max_comments: int = 10, max_attempts: int = 10, origin_author: str = None) -> list:
        """
        根据帖子 URL 获取并解析评论信息
        Args:
            note_url: 帖子 URL
            max_comments: 最大评论数量
            max_attempts: 最大滑动次数1
            origin_author: 原始作者名称（需要过滤掉）
        Returns:
            list: 解析后的评论列表
        """
        try:
            if len(note_url) == 34:  # 链接长度34则为短链接
                full_url = self.get_redirect_url(note_url)
                print(f"处理笔记URL: {full_url}")
            else:
                full_url = note_url  # 长链不需要处理直接使用

            print(f"开始获取并解析评论，帖子 URL: {full_url}")
            #检查笔记是否存在
            note_status=self.is_note_404(full_url)
            # 打开帖子页面
            if not note_status or full_url == "":
                if full_url!='':
                    self.driver.get(full_url)
                # 等待评论区加载
                print("等待评论区加载...")
                if full_url != "":
                    self.scroll_down()
                # 等待页面加载
                time.sleep(1)  
                # 修改寻找评论区逻辑，避免正文过长导致评论区不能正常加载
                for i in range(10):
                    try:
                        # 查找评论列表
                        try:
                            WebDriverWait(self.driver, 0.3).until(
                                EC.presence_of_element_located((
                    AppiumBy.XPATH,
                    "//androidx.recyclerview.widget.RecyclerView[@resource-id='com.xingin.xhs:id/-']/android.widget.FrameLayout/android.widget.LinearLayout"
                ))
                
                #更换了评论区定位元素
                            )
                            print("找到评论区")
                            break
                        except:
                            print("未找到评论区,再次滑动页面...")
                            self.scroll_down()
                            time.sleep(1)
                    except Exception as e:
                        print(f"等待评论区加载失败: {str(e)}")
                        return []

                last_page_source = None
                all_comments = []  # 用于存储解析后的评论
                seen_comments = set()  # 用于去重
                max_comments = max_comments  # 最大评论数
                is_first_comment = True  # 标记是否是第一条评论

                for attempt in range(max_attempts):


                    # 获取翻页前源码
                    before_scroll_page_source = self.driver.page_source

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
                                "content-desc": comment_elem.get_attribute("content-desc"),
                                "long-clickable": comment_elem.get_attribute("long-clickable")
                            })
                            
                            try:
                                # 获取评论内容
                                comment_text = comment_elem.text.strip()
                                content_copy=comment_text
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
                                            if author==origin_author or author=="作者":
                                                continue
                                            if not author:
                                                print("找到的作者元素文本为空")
                                                author = "未知作者"
                                        else:
                                            print("未找到合适的作者元素")
                                    except Exception as e:
                                        print(f"获取作者信息时出错: {str(e)}")
                                    
                                    #去除无用信息后的评论文本和时间、地点信息
                                    info_of_comment=self.extract_time_location_from_text(content_copy)
                                    # 构建评论数据
                                    comment_data = {
                                        "author": author,
                                        # "content": info_of_comment['cleaned_text'].replace('回复',''), #
                                        'content':comment_text.split('\n')[0].replace(info_of_comment.get('location', '未知'),''),
                                        "likes": likes,
                                        # "comment_time": info_of_comment.get('timestamp', collect_time), #评论时间
                                        'comment_time':collect_time,
                                        "collect_time": time.strftime("%Y-%m-%d %H:%M:%S"), #评论收集时间
                                        "location": info_of_comment.get('location', '未知'), #评论地区
                                    }
                                    print(f"解析到评论: {comment_data}")
                                    # 添加到结果列表
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
                    # last_page_source = page_source

                    # 模拟滑动加载更多评论
                    self.scroll_down()
                    print(f"第 {attempt + 1} 次滑动加载评论...")
                    #防止机器卡顿页面未更新
                    time.sleep(1) 
                    #将判断页面是否变动的逻辑调整为前后对比的方式,页面滑动前后分别获取页面源码,进行对比
                    after_scroll_page_source=self.driver.page_source
                    
                    if before_scroll_page_source == after_scroll_page_source:
                        print("页面未发生变化，可能已到底")
                        break
                    
                    # page_source = self.driver.page_source
                    # if page_source == last_page_source:
                    #     print("页面未发生变化，可能已到底")
                    #     break

                print(f"评论获取完成，共收集到 {len(all_comments)} 条评论")
                return all_comments
            else:
                print(f"笔记不存在或已被删除: {full_url}")
                return []
        except Exception as e:
            print(f"获取评论失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return []
    
        #回评时候回复图片的操作
    
    def collect_video_comments(self, note_url: str, max_comments: int = 10, max_attempts: int = 10, origin_author: str = None) -> list:
        """
        根据视频帖子 URL 获取并解析评论信息
        Args:
            note_url: 帖子 URL
            max_comments: 最大评论数量
            max_attempts: 最大滑动次数
            origin_author: 原始作者名称（需要过滤掉）
        Returns:
            list: 解析后的评论列表
        """
        try:
            if len(note_url) == 34:  # 链接长度34则为短链接
                full_url = self.get_redirect_url(note_url)
                print(f"处理笔记URL: {full_url}")
            else:
                full_url = note_url  # 长链不需要处理直接使用

            print(f"开始获取并解析视频评论，帖子 URL: {full_url}")
            #检查笔记是否存在
            note_status=self.is_note_404(full_url)
            # 打开帖子页面
            if not note_status or full_url == "":
                if full_url!='':
                    self.driver.get(full_url)
                    time.sleep(2)  # 等待页面加载
                    
                #如果没有评论则跳过收集逻辑
                try:
                    self.driver.find_element(
                                    by=AppiumBy.XPATH,
                                    value="//android.widget.Button[contains(@content-desc, '评论0')]"
                                )
                    print(f"笔记没有评论,跳过收集评论逻辑")
                    return []
                except Exception as e:
                    print(f"笔记有评论,继续收集评论逻辑")
                
                #有评论执行收集逻辑
                #打开评论区
                self.driver.find_element(
                                by=AppiumBy.XPATH,
                                value="//android.widget.Button[contains(@content-desc, '评论')]"
                            ).click()
                
                # 等待评论区加载
                time.sleep(2)
                
                last_page_source = None
                all_comments = []  # 用于存储解析后的评论
                seen_comments = set()  # 用于去重
                max_comments = max_comments  # 最大评论数
                is_first_comment = True  # 标记是否是第一条评论

                for attempt in range(max_attempts):
                    # 获取翻页前源码
                    before_scroll_page_source = self.driver.page_source

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
                                "content-desc": comment_elem.get_attribute("content-desc"),
                                "long-clickable": comment_elem.get_attribute("long-clickable")
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
                                            if author==origin_author or author=="作者":
                                                continue
                                            if not author:
                                                print("找到的作者元素文本为空")
                                                author = "未知作者"
                                        else:
                                            print("未找到合适的作者元素")
                                    except Exception as e:
                                        print(f"获取作者信息时出错: {str(e)}")
                                    
                                    #去除无用信息后的评论文本和时间、地点信息
                                    info_of_comment=self.extract_time_location_from_text(comment_text)
                                    # 构建评论数据
                                    comment_data = {
                                        "author": author,
                                        "content": info_of_comment['cleaned_text'].replace('回复',''), #去除无用信息后的评论
                                        "likes": likes,
                                        "comment_time": info_of_comment.get('timestamp', collect_time), #评论时间
                                        "collect_time": time.strftime("%Y-%m-%d %H:%M:%S"), #评论收集时间
                                        "location": info_of_comment.get('location', '未知'), #评论地区
                                    }
                                    print(f"解析到评论: {comment_data}")
                                    # 添加到结果列表
                                    all_comments.append(comment_data)
                                    seen_comments.add(comment_text)
                                    
                                    print(f"发现新评论: {comment_text[:50]}...")
                                    
                                    # 如果达到最大评论数，退出循环
                                    if len(all_comments) >= max_comments:
                                        print(f"已达到最大评论数 {max_comments}，停止收集")
                                        #关闭评论区
                                        self.driver.find_element(by=AppiumBy.XPATH,
                                                                value="(//android.widget.ImageView[@resource-id='com.xingin.xhs:id/-'])[1]").click()
                                        print('关闭评论区')
                                        #关闭视频笔记
                                        self.driver.find_element(by=AppiumBy.XPATH,
                                                                value="//android.widget.ImageView[@content-desc='返回']").click()
                                        print('关闭视频笔记')
                                        return all_comments
                                    
                            except Exception as e:
                                print(f"解析单个评论失败: {str(e)}")
                                continue
                    except Exception as e:
                        print(f"解析页面评论失败: {str(e)}")
                        continue

                    # 模拟滑动加载更多评论
                    self.scroll_down()
                    print(f"第 {attempt + 1} 次滑动加载评论...")
                    #防止机器卡顿页面未更新
                    time.sleep(1) 
                    #将判断页面是否变动的逻辑调整为前后对比的方式,页面滑动前后分别获取页面源码,进行对比
                    after_scroll_page_source=self.driver.page_source
                    
                    if before_scroll_page_source == after_scroll_page_source:
                        print("页面未发生变化，可能已到底")
                        #关闭评论区
                        self.driver.find_element(by=AppiumBy.XPATH,
                                                value="(//android.widget.ImageView[@resource-id='com.xingin.xhs:id/-'])[1]").click()
                        print('关闭评论区')
                        time.sleep(2)  # 等待关闭动画
                        #关闭视频笔记
                        self.driver.find_element(by=AppiumBy.XPATH,
                                                value="//android.widget.ImageView[@content-desc='返回']").click()
                        print('关闭视频笔记')
                        break

                print(f"视频评论获取完成，共收集到 {len(all_comments)} 条评论")
                
                return all_comments
            else:
                print(f"笔记不存在或已被删除: {full_url}")
                return []
        except Exception as e:
            print(f"获取视频评论失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return []
        
    def comments_reply_image(self):
        #定位到选择回复图片的元素
        WebDriverWait(self.driver, 2).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "(//android.widget.ImageView[@resource-id='com.xingin.xhs:id/-'])[3]"
                    ))
                ).click()
        
        try:
            # 执行点击操作
            self.driver.tap([(674, 258)])
            print('选择第一张照片成功')
        except Exception as e:
            print(f"点击屏幕失败: {str(e)}")
        #单选版本的点击图片后会自动返回评论界面
        try:
            choice_btn=WebDriverWait(self.driver, 2).until(
                    EC.presence_of_element_located((
                        AppiumBy.XPATH,
                        "//android.widget.TextView[@resource-id='com.xingin.xhs:id/-' and  contains(@text,'完成')]"
                    ))
                )
            if choice_btn:
                choice_btn.click()
            print('图片选择完成')
        except Exception as e:
            print('选择成功，返回评论界面')
            #返回评论界面
           
            
    def comments_reply(self, note_url: str, author: str, comment_content: str, reply_content: str, skip_url_open: bool = False,has_image:bool=False,note_type:str='图文'):
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
        if comment_content is None or comment_content == "":
            print("评论内容为空，无法回复")
            return False
        try:
            # 获取完整URL（处理短链接）
            if len(note_url) == 34:  # 连接长度34则为短链接
                full_url = self.get_redirect_url(note_url)
                print(f"处理笔记URL: {full_url}")
            else:
                full_url = note_url #长连不需要处理直接使用
            note_status=self.is_note_404(full_url)
            # 打开帖子页面
            if not note_status:
                # 只有在不跳过URL打开时才重新打开笔记
                if not skip_url_open:
                    print(f"打开新的笔记URL: {full_url}")
                    self.driver.get(full_url)
                    time.sleep(1)  # 等待页面加载
                else:
                    print(f"跳过URL打开，继续在当前页面查找评论: {full_url}")
                
                # 只有在不跳过URL打开时才需要等待评论区加载
                if not skip_url_open:
                    if note_type=='图文':
                        print("等待评论区加载...")
                        for i in range(10):
                            try:
                                # 查找评论列表
                                try:
                                    WebDriverWait(self.driver, 0.3).until(
                                        EC.presence_of_element_located((
                            AppiumBy.XPATH,
                            "//androidx.recyclerview.widget.RecyclerView[@resource-id='com.xingin.xhs:id/-']/android.widget.FrameLayout/android.widget.LinearLayout"
                        ))
                        
                        #更换了评论区定位元素
                                    )
                                    print("找到评论区")
                                    break
                                except:
                                    print("未找到评论区,再次滑动页面...")
                                    self.scroll_down()
                                    time.sleep(2)
                            except Exception as e:
                                print(f"等待评论区加载失败: {str(e)}")
                                return []
                    elif note_type=='视频':
                        try:
                            self.driver.find_element(
                                            by=AppiumBy.XPATH,
                                            value="//android.widget.Button[contains(@content-desc, '评论0')]"
                                        )
                            print(f"笔记没有评论,跳过收集评论逻辑")
                            return []
                        except Exception as e:
                            print(f"笔记有评论,继续收集评论逻辑")
                        
                        #有评论执行收集逻辑
                        #打开评论区
                        self.driver.find_element(
                                        by=AppiumBy.XPATH,
                                        value="//android.widget.Button[contains(@content-desc, '评论')]"
                                    ).click()
                        # 等待评论区加载
                        time.sleep(2)
                else:
                    print("跳过评论区定位，直接查找目标评论")
                
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
                            print(f'elem_text: {elem_text}， comment_content: {comment_content}， target_content: {target_content}')
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
                    reply_input = WebDriverWait(self.driver, 2).until(
                        EC.presence_of_element_located((
                            AppiumBy.CLASS_NAME,
                            "android.widget.EditText"
                        ))
                    )
                    # 输入回复内容
                    reply_input.clear()
                    reply_input.send_keys(reply_content)

                    if has_image:
                        self.comments_reply_image()
                    
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
                
                
            else:
                print(f"笔记不存在或已被删除: {full_url}")
                return False
        except Exception as e:
            print(f"回复评论失败: {str(e)}")
            return False
    def check_unreplied_messages(self,device_id,email):
        """
        检查未回复的私信，返回私信的用户名称和未回复总数
        Returns:
            dict: 包含未回复私信信息的字典
                {
                    'total_unreplied': int,  # 未回复总数（按用户计算）
                    'unreplied_users': list  # 未回复用户列表，每个用户一条记录
                }
        """
        unreplied_msg_list = []
        total_unreplied = 0
        
        try:
            # 点击"消息"，跳转到私信页面
            msg_btn = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.TextView[contains(@resource-id,'com.xingin.xhs:id/-') and contains(@text, '消息')]"))
            )
            msg_btn.click()
            time.sleep(1)  # 等待页面加载
            
            # 检查是否存在回评
            try:
                reply_frame = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.RelativeLayout[contains(@content-desc,'评论和@')]"))
                )
                recomment_text=reply_frame.get_attribute("content-desc")
                recomment_cnt=''.join(filter(str.isdigit, recomment_text))

                print(f"回评数量: {recomment_cnt}")
            except Exception as e:
                print(f"没有回评: {e}")
                reply_frame = None
            # 第一套逻辑----检查陌生人私信
            try:
                stranger_msg_frame = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.RelativeLayout[contains(@content-desc,'陌生人')]"))
                )
                stranger_msg_frame.click()
                time.sleep(0.5)  # 等待页面加载
                
                # 在陌生人消息列表界面检查未回复消息
                processed_users = set()  # 用于避免重复处理同一用户
                
                for i in range(10):
                    try:
                        # 获取当前页面所有陌生人私信
                        msg_frames = self.driver.find_elements(
                            by=AppiumBy.XPATH,
                            value="//android.widget.RelativeLayout[@resource-id='com.xingin.xhs:id/-']"
                        )
                        
                        if not msg_frames:
                            print(f"第{i+1}次检查：当前页面没有陌生人私信")
                            break
                        
                        current_page_found = False
                        
                        for msg_frame in msg_frames:
                            try:
                                msg_author = msg_frame.find_element(
                                    by=AppiumBy.XPATH, 
                                    value=".//android.widget.TextView[@resource-id='com.xingin.xhs:id/-']"
                                ).text
                                
                                # 避免重复处理同一用户
                                if msg_author not in processed_users:
                                    processed_users.add(msg_author)
                                    
                                    # 添加到未回复列表
                                    unreplied_msg_list.append({
                                        'username': msg_author,
                                        'message_type': '陌生人私信',
                                        'reply_status': 0  # 0表示未回复
                                    })
                                    total_unreplied += 1
                                    print(f"发现未回复陌生人私信: {msg_author}")
                                    current_page_found = True
                                    
                            except Exception as e:
                                print(f"解析陌生人私信失败: {str(e)}")
                                continue
                        
                        # 获取滚动前的页面源码
                        before_scroll_page_source = self.driver.page_source
                        
                        # 滚动查找更多
                        print(f"第{i+1}次检查：滚动查找更多陌生人私信")
                        self.scroll_down()
                        time.sleep(0.5)
                        
                        # 获取滚动后的页面源码
                        after_scroll_page_source = self.driver.page_source
                        
                        # 比较页面源码判断是否到达底部
                        if before_scroll_page_source == after_scroll_page_source:
                            print(f"第{i+1}次检查：页面未发生变化，已到达陌生人私信列表底部")
                            break
                        
                    except Exception as e:
                        print(f"陌生人私信检查完毕: {e}")
                        break
                
                # 返回到私信列表
                back_btn = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.ImageView[@resource-id='com.xingin.xhs:id/-']"))
                )
                back_btn.click()
                time.sleep(0.5)
                
            except Exception as e:
                print(f"没有陌生人私信: {e}")
            
            # 第二套逻辑----检查正常私信
            try:
                processed_normal_users = set()  # 用于避免重复处理同一用户
                
                for index in range(10):
                    try:
                        # 定位未回复私信（包含"条未读"的消息）
                        normal_msg_frames = self.driver.find_elements(
                            by=AppiumBy.XPATH,
                            value="//android.widget.RelativeLayout[@resource-id='com.xingin.xhs:id/-' and contains(@content-desc,'条未读')and not(contains(@content-desc,'赞和收藏'))and not(contains(@content-desc,'评论和'))]"
                        )
                        
                        current_page_found = False
                        
                        # 处理未读私信
                        if normal_msg_frames:
                            for msg_frame in normal_msg_frames:
                                try:
                                    # 获取用户名
                                    msg_author = msg_frame.find_element(
                                        by=AppiumBy.XPATH,
                                        value=".//android.widget.TextView[@resource-id='com.xingin.xhs:id/-']"
                                    ).text
                                    
                                    # 避免重复处理同一用户
                                    if msg_author not in processed_normal_users and msg_author!= "陌生人消息":
                                        processed_normal_users.add(msg_author)
                                        
                                        # 添加到未回复列表
                                        unreplied_msg_list.append({
                                            'username': msg_author,
                                            'message_type': '正常私信',
                                            'reply_status': 0  # 0表示未回复
                                        })
                                        total_unreplied += 1
                                        print(f"发现未回复私信: {msg_author}")
                                        current_page_found = True
                                    
                                except Exception as e:
                                    print(f"解析私信信息失败: {str(e)}")
                                    continue
                        
                        # 获取滚动前的页面源码
                        before_scroll_page_source = self.driver.page_source
                        
                        # 滚动查找更多
                        print(f"第{index+1}次检查：滚动查找更多正常私信")
                        self.scroll_down()
                        time.sleep(0.5)
                        
                        # 获取滚动后的页面源码
                        after_scroll_page_source = self.driver.page_source
                        
                        # 比较页面源码判断是否到达底部
                        if before_scroll_page_source == after_scroll_page_source:
                            print(f"第{index+1}次检查：页面未发生变化，已到达正常私信列表底部")
                            break
                            
                    except Exception as e:
                        print(f"检查正常私信失败: {str(e)}")
                        break
                        
            except Exception as e:
                print(f"检查正常私信出错: {str(e)}")
            
            # 返回结果
            result = {
                "userInfo": email,
                "device_id": device_id,
                "total_unreplied": total_unreplied,
                "unreplied_users": unreplied_msg_list,
                "check_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "recomment_cnt": int(recomment_cnt) if recomment_cnt != '' else 0
            }
            
            # 将结果存储到Airflow Variable中（多设备合并存储）
            try:
                
                
                # 获取现有的设备消息列表
                existing_data = Variable.get("XHS_DEVICES_MSG_LIST", default_var=[], deserialize_json=True)
                if not isinstance(existing_data, list):
                    existing_data = []
                
                # 查找是否已存在当前设备的数据
                device_found = False
                for i, item in enumerate(existing_data):
                    if item.get("device_id") == device_id:
                        existing_data[i] = result
                        device_found = True
                        break
                
                # 如果没有找到当前设备，则添加新的设备数据
                if not device_found:
                    existing_data.append(result)
                
                # 保存更新后的数据
                Variable.set("XHS_DEVICES_MSG_LIST", existing_data, serialize_json=True,description=f"多设备未回复私信检查结果，最后更新时间: {result['check_time']}")
                print(f"设备 {device_id} 的检查结果已存储到Airflow Variable: XHS_DEVICES_MSG_LIST")
            except Exception as e:
                print(f"存储到Airflow Variable失败: {str(e)}")
            
            print(f"未回复私信检查完成，共发现 {total_unreplied} 条未回复消息，涉及 {len(unreplied_msg_list)} 个用户")
            return result
            
        except Exception as e:
            print(f"检查未回复私信失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            
            error_result = {
                'userInfo': email,
                'device_id': device_id,
                'total_unreplied': 0,
                'unreplied_users': [],
                'check_time': time.strftime("%Y-%m-%d %H:%M:%S"),
                "recomment_cnt": int(recomment_cnt) if recomment_cnt != '' else 0,
                'error': str(e)
            }
            
            return error_result
    #检查笔记是否存在
    def is_note_404(self,short_url):
        try:
            # 添加请求头，模拟浏览器请求
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
            # 发送请求，禁止自动重定向
            response = requests.get(short_url, headers=headers)
            # 检查响应状态码
            
            if "你访问的页面不见了" in response.text :  # 处理重定向状态码
                print('短链接已失效或不存在')
                return True
            else:
                print('短链接有效')
                return False    
        except Exception as e:
            return "请求失败: {}".format(str(e))

    #私信回复
    def reply_to_msg(self,msg):
        """
        回复消息
        Args:
            msg: 回复的消息内容
        """

        unreplyed_msg_list = []
        # 点击“消息”，跳转到私信页面
        replyed_msg_list = []
        msg_btn = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.TextView[contains(@resource-id,'com.xingin.xhs:id/-') and contains(@text, '消息')]"))
        )
        
        msg_btn.click()
        time.sleep(1)  # 等待页面加载
        #第一套逻辑----陌生人
        #点击陌生人消息，执行回复陌生人消息逻辑
        try:
                stranger_msg_frame = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.RelativeLayout[contains(@content-desc,'陌生人')]"))
                )
                stranger_msg_frame.click()
                time.sleep(0.5)  # 等待页面加载
                #在陌生人消息列表界面进行的操作
                for i in range(10):
                        try:
                                try:    #定位到陌生人私信
                                        msg_frame = WebDriverWait(self.driver, 10).until(
                                                EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.RelativeLayout[@resource-id='com.xingin.xhs:id/-']"))
                                        )
                                        msg_author=msg_frame.find_element(by=AppiumBy.XPATH,value=".//android.widget.TextView[@resource-id='com.xingin.xhs:id/-']").text
                                        #点击进入聊天界面
                                        unreplyed_msg_list.append({'msg_author':msg_author, 'msg_content':"未回复"})
                                        msg_frame.click()
                                        time.sleep(0.5)  # 等待页面加载
                                        print(f"正在回复: {msg_author}的私信")
                                except Exception as e:
                                        print(f"陌生人私信已全部回复: {e}")
                                        back_btn = WebDriverWait(self.driver, 10).until(
                                                EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.ImageView[@resource-id='com.xingin.xhs:id/-']"))
                                        )       
                                        #返回到私信列表
                                        back_btn.click()
                                        time.sleep(0.5)
                                        break
                                #定位输入框
                                chat_frame = WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((AppiumBy.CLASS_NAME, "android.widget.EditText"))
                                )
                                #输入消息
                                chat_frame.send_keys(msg)
                                time.sleep(1)  # 等待输入完成
                                # 点击发送按钮
                                send_frame = WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.TextView[@resource-id='com.xingin.xhs:id/-' and @text='发送']"))
                                )
                                send_frame.click()
                                print(f'{msg_author}的私信回复成功')
                                #将回复的私信信息添加到返回列表
                                replyed_msg_list.append({'msg_author':msg_author, 'msg_content':msg})
                                #返回到陌生人私信列表
                                back_btn = WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.ImageView[@resource-id='com.xingin.xhs:id/-']"))
                                )
                                back_btn.click()
                        except Exception as e:
                                print(f"回复私信失败: {str(e)}")
        except Exception as e:
                print(f"没有陌生人私信，执行回复正常私信逻辑")
        #第二套逻辑----正常私信
        try:    
            for index,i in enumerate(range(5)):
                try:    #定位未回复私信
                    normal_msg_frames = self.driver.find_elements(by=AppiumBy.XPATH,value="//android.widget.RelativeLayout[@resource-id='com.xingin.xhs:id/-' and contains(@content-desc,'条未读')and not(contains(@content-desc,'赞和收藏'))and not(contains(@content-desc,'评论和'))]")
                    if normal_msg_frames:
                        for msg_frame in normal_msg_frames:
                            msg_author=msg_frame.find_element(by=AppiumBy.XPATH,value=".//android.widget.TextView[@resource-id='com.xingin.xhs:id/-']").text
                            print(f"正在回复: {msg_author}的私信")
                            #打开聊天页面
                            msg_frame.click()
                            try:
                                #定位输入框
                                chat_frame = WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((AppiumBy.CLASS_NAME, "android.widget.EditText"))
                                )
                                #输入消息
                                chat_frame.send_keys(msg)
                                time.sleep(1)  # 等待输入完成
                                #点击发送按钮
                                send_frame = WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.TextView[@resource-id='com.xingin.xhs:id/-' and @text='发送']"))
                                )
                                send_frame.click()
                                print(f'{msg_author}的私信回复成功')
                                replyed_msg_list.append({'msg_author':msg_author, 'msg_content':msg})
                                #返回到正常私信列表
                                msg_frame = WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located((AppiumBy.XPATH, "//android.widget.ImageView[@resource-id='com.xingin.xhs:id/-']"))
                                )
                                msg_frame.click()
                            except Exception as e:
                                    print(f"回复私信失败: {str(e)}")
                except Exception as e:
                        print(f"当前页面没有待回复的私信,执行第{index+1}次滚动")
                        self.scroll_down()
        except Exception as e:
                print(f"{str(e)}")

        return replyed_msg_list        
                
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
        force_app_launch=False,
        device_id="ZY22FX4H65",
        # system_port=8200
    )

    try:
        # 1 测试收集文章
        # print("\n开始测试收集文章...")
        # notes = xhs.collect_video_comments('http://xhslink.com/a/coeq2hYSGC6fb')
        
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
        note_url = "https://www.xiaohongshu.com/discovery/item/687e016d000000002201dbbe?app_platform=android&ignoreEngage=true&app_version=8.72.0&share_from_user_hidden=true&xsec_source=app_share&type=normal&xsec_token=CBOO9x2tD5d4Ne8eSR9P2Dc8XJPInR8xznMv95xoBm_hM%3D&author_share=1&xhsshare=CopyLink&shareRedId=OD43Q0dKN0s2NzUyOTgwNjdIOTo9OzlC&apptime=1753159608&share_id=ddd2c38bf52c4994a678ef3af0154cf8&share_channel=copy_link"
        # full_url = xhs.get_redirect_url(note_url)
        print(f"帖子 URL: {note_url}")
        
        comments = xhs.collect_comments_by_url(note_url,max_comments=10)
        print(f"\n共收集到 {len(comments)} 条评论:")
        for i, comment in enumerate(comments, 1):
            print(f"\n评论 {i}:")
            print(f"作者: {comment['author']}")
            print(f"内容: {comment['content']}")
            print(f"点赞: {comment['likes']}")
            print(f"地区: {comment['location']}")
            print(f"时间: {comment['collect_time']}")
            print("-" * 50)

        # 3 测试检查未回复私信功能
        # print("\n开始测试检查未回复私信功能...")
        # unreplied_result = xhs.check_unreplied_messages()
        
        # print(f"\n未回复私信检查结果:")
        # print(f"检查时间: {unreplied_result.get('check_time', 'N/A')}")
        # print(f"未回复总数: {unreplied_result.get('total_unreplied', 0)}")
        # print(f"涉及用户数: {len(unreplied_result.get('unreplied_users', []))}")
        
        # if unreplied_result.get('error'):
        #     print(f"检查过程中出现错误: {unreplied_result['error']}")
        
        # unreplied_users = unreplied_result.get('unreplied_users', [])
        # if unreplied_users:
        #     print("\n未回复私信详情:")
        #     for i, user_info in enumerate(unreplied_users, 1):
        #         print(f"  {i}. 用户: {user_info.get('username', 'N/A')}")
        #         print(f"     类型: {user_info.get('message_type', 'N/A')}")
        #         print("-" * 30)
        # else:
        #     print("\n没有发现未回复的私信")
        
        # # 验证Airflow Variable存储结果（多设备格式）
        # try:
            
        #     stored_data = Variable.get("XHS_DEVICES_MSG_LIST", default_var=None, deserialize_json=True)
        #     if stored_data and isinstance(stored_data, list):
        #         print("\nAirflow Variable存储验证（多设备格式）:")
        #         print(f"总设备数: {len(stored_data)}")
                
        #         # 查找当前设备的数据
        #         current_device_data = None
        #         for device_item in stored_data:
        #             if device_item.get("device_id") == xhs.device_id:
        #                 current_device_data = device_item.get("data", {})
        #                 break
                
        #         if current_device_data:
        #             print(f"当前设备 {xhs.device_id} 的存储数据:")
        #             print(f"  存储时间: {current_device_data.get('check_time', 'N/A')}")
        #             print(f"  存储的未回复总数: {current_device_data.get('total_unreplied', 0)}")
        #             print(f"  存储的涉及用户数: {len(current_device_data.get('unreplied_users', []))}")
        #             print("存储成功!")
        #         else:
        #             print(f"未找到设备 {xhs.device_id} 的存储数据")
                    
        #         # 显示所有设备的概览
        #         print("\n所有设备概览:")
        #         for i, device_item in enumerate(stored_data, 1):
        #             device_id = device_item.get("device_id", "未知")
        #             device_data = device_item.get("data", {})
        #             unreplied_count = device_data.get("total_unreplied", 0)
        #             check_time = device_data.get("check_time", "未知")
        #             print(f"  {i}. 设备 {device_id}: {unreplied_count} 条未回复, 检查时间: {check_time}")
        #     else:
        #         print("\n未找到有效的Airflow Variable存储数据")
        # except Exception as e:
        #     print(f"\n注意: 无法验证Airflow Variable存储 (可能在非Airflow环境运行): {str(e)}")
            
        # print("-" * 50)

        # 4 测试根据评论者id和评论内容定位该条评论并回复（已注释）
        # note_url = "http://xhslink.com/a/obpDqQ0omk7db"
        # author = "爱吃的jerry"  # 替换为实际的评论者ID
        # comment_content = "生日还是在那里过的"  # 替换为实际的评论内容
        # reply_content = "哈哈哈"  # 替换为要回复的内容
        
        # 测试搜集笔记信息
        # print("\n开始测试搜集笔记信息...")
        
        # # 测试搜索关键词视频并收集信息
        # print("\n=== 测试视频搜索和收集 ===")
        # videos = xhs.search_keyword_of_video('小猫老师', max_videos=50)
        
        # if videos:
        #     print(f"\n共收集到 {len(videos)} 个视频:")
        #     for i, video in enumerate(videos, 1):
        #         print(f"\n视频 {i}:")
        #         print(f"标题: {video.get('title', 'N/A')}")
        #         print(f"作者: {video.get('author', 'N/A')}")
        #         print(f"点赞数: {video.get('likes', 'N/A')}")
        #         print(f"评论数: {video.get('comments', 'N/A')}")
        #         print(f"收藏数: {video.get('shares', 'N/A')}")
        #         print(f"视频URL: {video.get('video_url', 'N/A')}")
        #         print(f"收集时间: {video.get('collect_time', 'N/A')}")
        #         print(f"内容类型: {video.get('content_type', 'N/A')}")
        #         print("-" * 50)
        # else:
        #     print("未收集到视频信息")
    
    except Exception as e:
        print(f"运行出错: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        # 关闭操作器
        xhs.close()
