# -*- coding: utf-8 -*-
"""
小红书元素定位优化工具类
提供更精确、更稳定的元素定位方法
"""

import re
from typing import List, Optional

class XhsElementUtils:
    """小红书元素定位优化工具类"""
    
    @staticmethod
    def _is_valid_comment_text(text: str) -> bool:
        """
        验证文本是否为有效的评论内容
        
        Args:
            text: 待验证的文本
            
        Returns:
            bool: 是否为有效评论
        """
        if not text or len(text.strip()) < 2:
            return False
            
        text = text.strip()
        
        # 排除的无效文本模式
        invalid_patterns = [
            r'^\d+$',  # 纯数字
            r'^\d+[wkW万千]$',  # 数字+单位
            r'^\d{1,2}:\d{2}$',  # 时间格式
            r'^\d+小时前$',  # 相对时间
            r'^\d+分钟前$',  # 相对时间
            r'^\d+天前$',  # 相对时间
            r'^昨天\s*\d{1,2}:\d{2}$',  # 昨天时间
            r'^\d{4}-\d{2}-\d{2}$',  # 日期格式
            r'^\d{2}-\d{2}$',  # 短日期格式
        ]
        
        # 检查是否匹配无效模式
        for pattern in invalid_patterns:
            if re.match(pattern, text):
                return False
        
        # 排除的关键词
        invalid_keywords = [
            'Say something', '说点什么', '还没有评论哦', '到底了',
            'First comment', '评论', 'comments', '点赞', '回复',
            '收藏', '分享', '关注', '作者', 'LIVE', '试试文字发笔记'
        ]
        
        # 检查是否包含无效关键词
        for keyword in invalid_keywords:
            if keyword in text:
                return False
        
        # 排除包含话题标签的文本（通常是笔记正文）
        if re.search(r'#\S+', text):
            return False
            
        # 排除过长的文本（可能是笔记正文）
        if len(text) > 500:
            return False
            
        return True
    
    @staticmethod
    def _is_valid_author_name(text: str) -> bool:
        """
        验证文本是否为有效的作者名
        
        Args:
            text: 待验证的文本
            
        Returns:
            bool: 是否为有效作者名
        """
        if not text or len(text.strip()) == 0:
            return False
            
        text = text.strip()
        
        # 作者名长度限制
        if len(text) > 20 or len(text) < 1:
            return False
        
        # 排除的无效作者名模式
        invalid_patterns = [
            r'^\d+$',  # 纯数字
            r'^\d+[wkW万千]$',  # 数字+单位
            r'^\d{1,2}:\d{2}$',  # 时间格式
            r'^\d+小时前$',  # 相对时间
            r'^\d+分钟前$',  # 相对时间
            r'^\d+天前$',  # 相对时间
            r'^昨天\s*\d{1,2}:\d{2}$',  # 昨天时间
            r'^\d{4}-\d{2}-\d{2}$',  # 日期格式
            r'^\d{2}-\d{2}$',  # 短日期格式
        ]
        
        # 检查是否匹配无效模式
        for pattern in invalid_patterns:
            if re.match(pattern, text):
                return False
        
        # 排除的关键词
        invalid_keywords = [
            '点赞', '回复', '收藏', '分享', '关注', '评论',
            'comments', '小时', '分钟', '天前', '昨天'
        ]
        
        # 检查是否包含无效关键词
        for keyword in invalid_keywords:
            if keyword in text:
                return False
                
        return True
    
    @staticmethod
    def _parse_number_with_unit(text: str) -> int:
        """
        解析带单位的数字文本
        
        Args:
            text: 数字文本，如 "123", "1.2k", "1万"
            
        Returns:
            int: 解析后的数字
        """
        if not text:
            return 0
            
        text = text.strip().lower()
        
        # 纯数字
        if text.isdigit():
            return int(text)
        
        # 带单位的数字
        match = re.match(r'^([\d.]+)([wkW万千]?)$', text)
        if match:
            number_str, unit = match.groups()
            try:
                number = float(number_str)
                
                # 单位转换
                if unit in ['k', 'K', '千']:
                    return int(number * 1000)
                elif unit in ['w', 'W', '万']:
                    return int(number * 10000)
                else:
                    return int(number)
            except ValueError:
                return 0
        
        return 0
    
    @staticmethod
    def get_optimized_comment_xpath() -> List[str]:
        """
        获取优化的评论元素XPath列表
        
        Returns:
            List[str]: XPath表达式列表，按优先级排序
        """
        return [
            # 策略1: 通过评论容器定位
            "//android.widget.RelativeLayout[contains(@content-desc,'评论') or @resource-id='com.xingin.xhs:id/comment_item']",
            
            # 策略2: 通过特定的resource-id
            "//android.widget.TextView[@resource-id='com.xingin.xhs:id/commentContent']",
            
            # 策略3: 基于内容长度的文本元素
            "//android.widget.TextView[string-length(@text) > 5 and string-length(@text) < 500]",
            
            # 策略4: 传统方法
            "//android.widget.TextView[contains(@text, '')]"
        ]
    
    @staticmethod
    def get_optimized_author_xpath() -> List[str]:
        """
        获取优化的作者元素XPath列表
        
        Returns:
            List[str]: XPath表达式列表，按优先级排序
        """
        return [
            # 策略1: 通过resource-id
            "//android.widget.TextView[@resource-id='com.xingin.xhs:id/nickNameTV']",
            "//android.widget.TextView[@resource-id='com.xingin.xhs:id/authorName']",
            
            # 策略2: 通过content-desc
            "//android.widget.TextView[contains(@content-desc,'用户名') or contains(@content-desc,'作者')]",
            
            # 策略3: 基于长度和内容的文本元素
            "//android.widget.TextView[string-length(@text) > 0 and string-length(@text) < 20 and not(contains(@text,'点赞')) and not(contains(@text,'回复'))]"
        ]
    
    @staticmethod
    def get_optimized_likes_xpath() -> List[str]:
        """
        获取优化的点赞数元素XPath列表
        
        Returns:
            List[str]: XPath表达式列表，按优先级排序
        """
        return [
            # 策略1: 通过resource-id
            "//android.widget.TextView[@resource-id='com.xingin.xhs:id/likeCount']",
            
            # 策略2: 通过content-desc
            "//android.widget.TextView[contains(@content-desc,'点赞')]",
            
            # 策略3: 点赞按钮内的文本
            "//android.widget.Button[contains(@content-desc,'点赞')]//android.widget.TextView",
            
            # 策略4: 基于数字模式的文本元素
            "//android.widget.TextView[string-length(@text) > 0 and string-length(@text) < 10]"
        ]