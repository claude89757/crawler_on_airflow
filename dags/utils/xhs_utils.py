#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小红书工具函数模块
提供各种辅助功能，包括：
- 从腾讯云COS下载图片到主机
- 从主机传输图片到手机

Author: claude89757
Date: 2025-06-18
"""

import os
import subprocess
import logging
import paramiko
from typing import Optional, List, Dict, Union, Tuple

# 配置日志
logger = logging.getLogger(__name__)


def download_cos_to_host(
    cos_url: str,
    host_address: str,
    host_username: str,
    host_password: Optional[str] = None,
    host_key_path: Optional[str] = None,
    host_port: int =None,
    host_save_path: Optional[str] = None
) -> Optional[str]:
    """
    通过SSH在主机上执行命令，直接从腾讯云COS下载文件到主机
    
    Args:
        cos_url: COS对象的URL（带签名的临时URL）
        host_address: 主机IP地址
        host_username: 主机用户名
        host_password: 主机密码（与host_key_path二选一）
        host_key_path: SSH密钥路径（与host_password二选一）
        host_port: SSH端口，默认为22
        host_save_path: 主机上的保存路径
        
    Returns:
        str: 主机上的文件路径，失败则返回None
    """
    try:
        # 创建SSH客户端
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # 连接主机
        if host_key_path:
            private_key = paramiko.RSAKey.from_private_key_file(host_key_path)
            ssh.connect(host_address, username=host_username, pkey=private_key,port=host_port)
            logger.info(f"使用SSH密钥连接到主机 {host_address}")
        else:
            ssh.connect(host_address, username=host_username, password=host_password,port=host_port)
            logger.info(f"使用密码连接到主机 {host_address}")
        
        # 确定保存路径
        if not host_save_path:
            # 获取用户主目录
            stdin, stdout, stderr = ssh.exec_command("echo $HOME")
            home_dir = stdout.read().decode().strip()
            host_save_path = os.path.join(home_dir, "cos_downloads")
            
            # 确保目录存在
            ssh.exec_command(f"mkdir -p {host_save_path}")
            logger.info(f"在主机上创建目录: {host_save_path}")
        
        # 提取文件名
        filename = cos_url.split('/')[-1].split('?')[0]
        file_path = os.path.join(host_save_path, filename)
        
        # 使用curl下载
        command = f"curl -s --globoff -o {file_path} '{cos_url}'"
        logger.info(f"在主机上执行: {command}")
        stdin, stdout, stderr = ssh.exec_command(command)
        
        # 检查是否成功
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            error = stderr.read().decode()
            logger.error(f"下载失败: {error}")
            raise Exception(f"下载失败: {error}")
        
        # 确认文件存在
        stdin, stdout, stderr = ssh.exec_command(f"ls -l {file_path}")
        if not stdout.read():
            logger.error(f"文件下载失败，{file_path}不存在")
            raise Exception(f"文件下载失败，{file_path}不存在")
        
        # 关闭连接
        ssh.close()
        
        logger.info(f"文件已成功下载到主机: {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"下载文件到主机失败: {str(e)}")
        return None


def transfer_from_host_to_device(
    host_address: str,
    host_username: str,
    device_id: str,
    host_file_path: str,
    device_path: str = "/sdcard/Pictures/",
    host_password: Optional[str] = None,
    host_key_path: Optional[str] = None,
    host_port: int=None
) -> Optional[str]:
    """
    通过SSH在主机上执行ADB命令，将文件传输到手机
    
    Args:
        host_address: 主机IP地址
        host_username: 主机用户名
        device_id: 设备ID
        host_file_path: 主机上的文件路径
        device_path: 手机上的保存路径，默认为/sdcard/Pictures/
        host_password: 主机密码（与host_key_path二选一）
        host_key_path: 主机SSH密钥路径（与host_password二选一）
        host_port: SSH端口，默认为22
        
    Returns:
        str: 手机上的文件路径，失败则返回None
    """
    try:
        # 创建SSH客户端
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # 连接主机
        if host_key_path:
            private_key = paramiko.RSAKey.from_private_key_file(host_key_path)
            ssh.connect(host_address, username=host_username, pkey=private_key,port=host_port)
            logger.info(f"使用SSH密钥连接到主机 {host_address}")
        else:
            ssh.connect(host_address, username=host_username, password=host_password,port=host_port)
            logger.info(f"使用密码连接到主机 {host_address}")
        
        # 构建ADB命令
        filename = os.path.basename(host_file_path)
        target_path = f"{device_path}{filename}"
        push_command = f"adb -s {device_id} push {host_file_path} {target_path}"
        refresh_command=f"adb -s {device_id} shell am broadcast -a android.intent.action.MEDIA_SCANNER_SCAN_FILE -d file:///sdcard/DCIM/Camera/"
        
        # 执行ADB命令
        logger.info(f"在主机上执行上传命令: {push_command}")
        stdin, stdout, stderr = ssh.exec_command(push_command)
        
        # 检查结果
        exit_status = stdout.channel.recv_exit_status()
        error = stderr.read().decode()
        if exit_status != 0 or ("error" in error.lower() and len(error) > 0):
            logger.error(f"上传失败: {error}")
            raise Exception(f"上传失败: {error}")
        
        # 执行刷新命令
        logger.info(f"在主机上执行刷新命令: {refresh_command}")
        refresh_stdin, refresh_stdout, refresh_stderr = ssh.exec_command(refresh_command)
        
        # 检查刷新结果
        refresh_exit_status = refresh_stdout.channel.recv_exit_status()
        refresh_error = refresh_stderr.read().decode()
        if refresh_exit_status != 0 or ("error" in refresh_error.lower() and len(refresh_error) > 0):
            logger.error(f"刷新失败: {refresh_error}")
            raise Exception(f"刷新失败: {refresh_error}")
        
        # 关闭连接
        ssh.close()
        
        logger.info(f"文件已成功传输到手机: {target_path}")
        return target_path
        
    except Exception as e:
        logger.error(f"传输文件到手机失败: {str(e)}")
        return None


def cos_to_device_via_host(
    cos_url: str,
    host_address: str,
    host_username: str,
    device_id: str,
    host_password: Optional[str] = None,
    host_key_path: Optional[str] = None,
    host_port: int =None,
    host_save_path: Optional[str] = None,
    device_path: str = "/sdcard/DCIM/Camera/",
    delete_after_transfer: bool = False
) -> Optional[str]:
    """
    完整流程：从腾讯云COS下载到主机，再从主机传输到手机
    
    Args:
        cos_url: COS预签名URL
        host_address: 主机IP地址
        host_username: 主机用户名
        device_id: 设备ID
        host_password: 主机密码（与host_key_path二选一）
        host_key_path: SSH密钥路径（与host_password二选一）
        host_save_path: 主机上的保存路径
        device_path: 手机上的保存路径，默认为/sdcard/Pictures/
        delete_after_transfer: 传输后是否删除主机上的文件
        
    Returns:
        str: 手机上的文件路径，失败则返回None
    """
    try:
        print(f"从COS下载到主机: {cos_url}",host_address,host_username,host_password,host_key_path,host_port,host_save_path)
        # 步骤1：从COS下载到主机
        host_file_path = download_cos_to_host(
            cos_url=cos_url,
            host_address=host_address,
            host_username=host_username,
            host_password=host_password,
            host_key_path=host_key_path,
            host_port=host_port,
            host_save_path=host_save_path
        )
        
        if not host_file_path:
            logger.error("从COS下载到主机失败")
            return None
        
        # 步骤2：从主机传输到手机
        device_file_path = transfer_from_host_to_device(
            host_address=host_address,
            host_username=host_username,
            device_id=device_id,
            host_file_path=host_file_path,
            device_path=device_path,
            host_password=host_password,
            host_key_path=host_key_path,
            host_port=host_port
        )
        
        if not device_file_path:
            logger.error("从主机传输到手机失败")
            return None
        
        # 步骤3：如果需要，删除主机上的文件
        if delete_after_transfer:
            try:
                # 创建SSH客户端
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                # 连接主机
                if host_key_path:
                    private_key = paramiko.RSAKey.from_private_key_file(host_key_path)
                    ssh.connect(host_address, port=host_port, username=host_username, pkey=private_key)
                else:
                    ssh.connect(host_address, port=host_port, username=host_username, password=host_password)
                
                # 删除文件
                ssh.exec_command(f"rm {host_file_path}")
                logger.info(f"已删除主机上的文件: {host_file_path}")
                
                # 关闭连接
                ssh.close()
            except Exception as e:
                logger.warning(f"删除主机上的文件失败: {str(e)}")
        
        return device_file_path
        
    except Exception as e:
        logger.error(f"完整传输流程失败: {str(e)}")
        return None


def batch_cos_to_device(
    cos_urls: List[str],
    host_address: str,
    host_username: str,
    device_id: str,
    host_password: Optional[str] = None,
    host_key_path: Optional[str] = None,
    host_port: int = 22,
    host_save_path: Optional[str] = None,
    device_path: str = "/sdcard/Pictures/",
    delete_after_transfer: bool = False
) -> Dict[str, str]:
    """
    批量处理：将多个COS图片传输到手机
    
    Args:
        cos_urls: COS预签名URL列表
        host_address: 主机IP地址
        host_username: 主机用户名
        device_id: 设备ID
        host_password: 主机密码（与host_key_path二选一）
        host_key_path: SSH密钥路径（与host_password二选一）
        host_save_path: 主机上的保存路径
        device_path: 手机上的保存路径，默认为/sdcard/Pictures/
        delete_after_transfer: 传输后是否删除主机上的文件
        
    Returns:
        Dict[str, str]: URL到设备路径的映射，失败的项为None
    """
    results = {}
    
    for cos_url in cos_urls:
        try:
            device_path = cos_to_device_via_host(
                cos_url=cos_url,
                host_address=host_address,
                host_username=host_username,
                device_id=device_id,
                host_password=host_password,
                host_key_path=host_key_path,
                host_port=host_port,
                host_save_path=host_save_path,
                device_path=device_path,
                delete_after_transfer=delete_after_transfer
            )
            results[cos_url] = device_path
        except Exception as e:
            logger.error(f"处理URL {cos_url} 失败: {str(e)}")
            results[cos_url] = None
    
    return results


# 示例用法
if __name__ == "__main__":
    # 设置日志
    logging.basicConfig(level=logging.INFO)
    
    # 示例参数
    cos_url = "https://wx-records-1347723456.cos.ap-guangzhou.myqcloud.com/wx%E5%9B%9E%E5%A4%8D%E5%9B%BE%E7%89%87%E6%A8%A1%E6%9D%BF/986468573387963.mp4"
    host_address = "42.193.193.179"
    host_username = "lucy002"
    host_password = "lucyai"
    device_id = "ZD222H3QDX"
    host_port=10003
    
    # 测试完整流程
    result = cos_to_device_via_host(
        cos_url=cos_url,
        host_address=host_address,
        host_username=host_username,
        device_id=device_id,
        host_password=host_password,
        host_port=host_port
    )
    
    print(f"传输结果: {result}")