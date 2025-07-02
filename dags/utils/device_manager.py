from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Callable
import threading
from queue import Queue
import time
from utils.xhs_appium import XHSOperator, get_adb_devices
from dotenv import load_dotenv
import os
import json
from appium.webdriver.common.appiumby import AppiumBy
import subprocess

def start_appium_servers(devices, base_port=6001):
    """
    启动与设备数量对应的Appium服务，每个设备一个端口。
    :param devices: 设备ID列表
    :param base_port: 起始端口号
    :return: 端口号列表
    """
    ports = []
    for idx, device_id in enumerate(devices):
        port = base_port + idx
        ports.append(port)
        # 检查端口是否已被占用（可选）
        # 启动Appium服务
        cmd = [
            "appium",
            "-p", str(port),
            "--session-override"
        ]
        # 只在主进程启动，不阻塞
        #print(cmd)
        #subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        env = os.environ.copy()  # 获取当前环境变量
        #print(env)
        subprocess.Popen(cmd, env=env, shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"已为设备 {device_id} 启动 Appium 服务，端口 {port}")
        time.sleep(1)  # 给Appium服务一点启动时间
    return ports


def get_device_pool(port=6001, system_port=8200):
    """
    设备启动参数管理池。含启动参数和对应的端口号
    :param port: appium服务的端口号。每一个设备对应一个。
    :param system_port: appium服务指定的本地端口，用来转发数据给安卓设备。每一个设备对应一个。
    :return: 所有已连接设备的启动参数和appium端口号。
    """
    # 获取当前连接的所有设备
    devices = get_adb_devices()
    if not devices:
        return []
    # 启动Appium服务
    start_appium_servers(devices, base_port=port)
    devs_pool = []
    for idx, device_id in enumerate(devices):
        dev_port = port + idx
        dev_system_port = system_port + idx * 4
        new_dict = {
            "device_id": device_id,
            "port": dev_port,
            "system_port": dev_system_port,
            "appium_server_url": f"http://localhost:{dev_port}"
        }
        devs_pool.append(new_dict)
    return devs_pool

class DeviceManager:
    """
    设备管理器，负责管理所有可用的设备
    Args:
            devices_pool: 设备池，包含设备信息的字典列表
            每个设备字典应包含以下信息：
                {
                'device_id': str,  # 设备ID
                'port': int,  # Appium服务器端口
                'system_port': int,  # 系统端口
                'appium_server_url': str  # Appium服务器URL
            }
    """
    def __init__(self, devices_pool: List[Dict]):
        self.devices_pool = devices_pool
        self.device_status = {}
        self.lock = threading.Lock()
        
        # 初始化设备状态
        for device in devices_pool:
            self.device_status[device['device_id']] = {
                'status': 'idle',
                'last_used': 0,  # 初始化为0
                'current_task': None
            }
    
    def update_devices(self, devices: List[str]):
        """更新设备列表"""
        with self.lock:
            # 更新设备池中的设备状态
            for device in self.devices_pool:
                if device['device_id'] in devices:
                    if device['device_id'] not in self.device_status:
                        self.device_status[device['device_id']] = {
                            'status': 'idle',
                            'last_used': 0,
                            'current_task': None
                        }
                else:
                    if device['device_id'] in self.device_status:
                        del self.device_status[device['device_id']]
    
    def get_available_device(self) -> Optional[Dict]:
        """获取一个空闲的设备及其配置信息"""
        with self.lock:
            current_time = time.time()
            for device in self.devices_pool:
                device_id = device['device_id']
                status_info = self.device_status[device_id]
                
                # 检查设备状态
                if status_info['status'] == 'idle':
                    self.device_status[device_id] = {
                        'status': 'busy',
                        'last_used': current_time,
                        'current_task': None
                    }
                    return device
                
                # 检查设备是否超时（5分钟）
                if (status_info['status'] == 'busy' and 
                    current_time - status_info['last_used'] > 300):
                    print(f"设备 {device_id} 任务超时，重置状态")
                    self.device_status[device_id] = {
                        'status': 'idle',
                        'last_used': 0,
                        'current_task': None
                    }
                    return device
                    
        return None
    
    def release_device(self, device_id: str):
        """释放设备"""
        with self.lock:
            if device_id in self.device_status:
                self.device_status[device_id] = {
                    'status': 'idle',
                    'last_used': 0,
                    'current_task': None
                }

class TaskDistributor:
    """任务分配器，负责分配任务给设备"""
    def __init__(self, device_manager: DeviceManager):
        self.device_manager = device_manager
        self.task_queue = Queue()
        self.results = []
        self.lock = threading.Lock()
        self.active_workers = 0
        self.max_workers = len(device_manager.devices_pool)
        self.task_status = {}  # 记录任务状态
        self.collected_urls = set()  # 所有设备共享的URL集合
        self.url_lock = threading.Lock()  # URL集合的锁
    
    def add_task(self, task_data: Dict):
        """添加任务到队列"""
        task_id = task_data['task_id']
        
        # 根据任务类型决定如何处理
        if task_data['type'] == 'collect_notes':
            # 笔记收集任务需要分配笔记数量
            total_notes = task_data['notes_per_device']
            num_devices = len(self.device_manager.devices_pool)
            notes_per_device = max(1, total_notes // num_devices)
            
            # 为每个设备创建子任务
            for device_idx in range(num_devices):
                sub_task = task_data.copy()
                sub_task['task_id'] = f"{task_id}_{device_idx}"
                sub_task['notes_per_device'] = notes_per_device
                sub_task['device_idx'] = device_idx
                self.task_status[sub_task['task_id']] = {
                    'status': 'pending',
                    'device_id': None,
                    'start_time': None
                }
                self.task_queue.put(sub_task)
        else:
            # 其他类型的任务（如评论收集）直接添加到队列
            self.task_status[task_id] = {
                'status': 'pending',
                'device_id': None,
                'start_time': None
            }
            self.task_queue.put(task_data)
    
    def get_task(self) -> Optional[Dict]:
        """获取一个任务"""
        try:
            return self.task_queue.get_nowait()
        except:
            return None
    
    def add_result(self, result):
        """添加结果"""
        with self.lock:
            self.results.append(result)
    
    def add_url(self, url: str) -> bool:
        """
        添加URL到集合，如果URL已存在则返回False
        Args:
            url: 要添加的URL
        Returns:
            bool: 是否成功添加（True表示新URL，False表示重复URL）
        """
        with self.url_lock:
            if url in self.collected_urls:
                return False
            self.collected_urls.add(url)
            return True
    
    def get_collected_urls(self) -> set:
        """获取已收集的URL集合"""
        with self.url_lock:
            return self.collected_urls.copy()
    
    def get_collected_urls_count(self) -> int:
        """获取已收集的URL数量"""
        with self.url_lock:
            return len(self.collected_urls)
    
    def worker(self, task_processor: Callable):
        """工作线程，处理任务队列中的任务"""
        with self.lock:
            self.active_workers += 1
            
        try:
            while True:
                # 获取任务
                task = self.get_task()
                if not task:
                    break
                    
                task_id = task['task_id']
                
                # 获取可用设备
                device_info = self.device_manager.get_available_device()
                if not device_info:
                    print("没有可用设备，等待...")
                    time.sleep(1)
                    continue
                
                # 更新任务状态
                with self.lock:
                    self.task_status[task_id] = {
                        'status': 'running',
                        'device_id': device_info['device_id'],
                        'start_time': time.time()
                    }
                
                try:
                    print(f"使用设备 {device_info['device_id']} 处理任务 {task_id}")
                    
                    # 创建XHSOperator实例
                    xhs = XHSOperator(
                        appium_server_url=device_info['appium_server_url'],
                        force_app_launch=True,
                        device_id=device_info['device_id'],
                        system_port=device_info['system_port']
                    )
                    
                    try:
                        # 执行任务，传入URL集合
                        result = task_processor(task, device_info, xhs, self)
                        self.add_result(result)
                        
                        # 更新任务状态为完成
                        with self.lock:
                            self.task_status[task_id]['status'] = 'completed'
                    finally:
                        xhs.close()
                except Exception as e:
                    print(f"任务 {task_id} 执行失败: {str(e)}")
                    self.add_result({
                        "device": device_info['device_id'],
                        "status": "error",
                        "error": str(e)
                    })
                    # 更新任务状态为失败
                    with self.lock:
                        self.task_status[task_id]['status'] = 'failed'
                finally:
                    # 释放设备
                    self.device_manager.release_device(device_info['device_id'])
        finally:
            with self.lock:
                self.active_workers -= 1
    
    def run_tasks(self, task_processor: Callable, max_workers: int = None):
        """运行所有任务"""
        if max_workers is None:
            max_workers = self.max_workers
            
        print(f"启动 {max_workers} 个工作线程...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for _ in range(max_workers):
                future = executor.submit(self.worker, task_processor)
                futures.append(future)
            
            # 等待所有任务完成
            for future in futures:
                future.result()
        
        return self.results

class TaskProcessorManager:
    """任务处理器管理器，负责管理不同类型的任务处理器"""
    def __init__(self):
        self.processors = {}
    
    def register_processor(self, task_type: str, processor: Callable):
        """注册任务处理器"""
        self.processors[task_type] = processor
    
    def get_processor(self, task_type: str) -> Callable:
        """获取任务处理器"""
        if task_type not in self.processors:
            raise ValueError(f"Unknown task type: {task_type}")
        return self.processors[task_type]
    
    def process_task(self, task: Dict, device_info: Dict, xhs: XHSOperator, task_distributor: TaskDistributor) -> Dict:
        """处理任务"""
        processor = self.get_processor(task['type'])
        return processor(task, device_info, xhs, task_distributor)

# 定义任务处理器
def collect_notes_processor(task: Dict, device_info: Dict, xhs: XHSOperator, collected_urls: set = None) -> Dict:
    """收集笔记任务处理器"""
    print(f"正在设备 {device_info['device_id']}上收集笔记")
    max_retries = 3
    retry_count = 0
    
    # 获取目标URL数量
    target_url_count = task.get('target_url_count', 20)
    device_idx = task.get('device_idx', 0)
    
    # 初始化已收集URL集合
    if collected_urls is None:
        collected_urls = set()
    
    while retry_count < max_retries:
        try:
            # 检查是否已达到目标URL数量
            if len(collected_urls) >= target_url_count:
                print(f"已达到目标URL数量 {target_url_count}，停止收集")
                break
            
            try:
                # 使用XHSOperator的collect_notes_by_keyword方法收集笔记
                notes = xhs.collect_notes_by_keyword(
                    keyword=task['keyword'],
                    max_notes=task['notes_per_device'],
                    filters={
                        "note_type": "图文"  # 只收集图文笔记
                    }
                )
            except Exception as e:
                print(f"设备 {device_info['device_id']} 收集笔记时出错: {str(e)}")
                # 如果是元素未找到错误，等待后重试
                if "NoSuchElement" in str(e):
                    print(f"元素未找到，等待 1 秒后重试...")
                    time.sleep(1)
                    retry_count += 1
                    continue
                # 如果是其他错误，尝试重新启动应用
                print("尝试重新启动应用...")
                try:
                    # 使用正确的方法重启应用
                    xhs.driver.terminate_app(xhs.driver.current_package)
                    time.sleep(1)  # 等待应用终止
                    xhs.driver.activate_app(xhs.driver.current_package)
                    time.sleep(1)  # 等待应用启动
                except Exception as reset_error:
                    print(f"重启应用失败: {str(reset_error)}")
                retry_count += 1
                continue
            
            if not notes:
                print(f"未找到关于 '{task['keyword']}' 的笔记")
                return {
                    "device": device_info['device_id'],
                    "status": "success",
                    "keyword": task['keyword'],
                    "notes_count": 0,
                    "output_file": None
                }
            
            # 处理收集到的笔记
            valid_notes = []
            for note in notes:
                if note.get('note_url'):
                    # 检查URL是否已存在
                    if note['note_url'] not in collected_urls:
                        collected_urls.add(note['note_url'])
                        valid_notes.append(note)
                        print(f"设备 {device_info['device_id']} 收集到新笔记: {note['note_url']}")
                    else:
                        print(f"设备 {device_info['device_id']} 跳过重复笔记: {note['note_url']}")
            
            return {
                "device": device_info['device_id'],
                "status": "success",
                "notes_count": len(valid_notes),
                "notes": valid_notes,
                "collected_urls": list(collected_urls)
            }
        except Exception as e:
            retry_count += 1
            print(f"设备 {device_info['device_id']} 第 {retry_count} 次尝试失败: {str(e)}")
            if retry_count < max_retries:
                print(f"等待 1 秒后重试...")
                time.sleep(1)
                continue
            else:
                return {
                    "device": device_info['device_id'],
                    "keyword": task['keyword'],
                    "status": "error",
                    "error": str(e)
                }
    
    # 返回最终结果
    return {
        "device": device_info['device_id'],
        "status": "success",
        "notes_count": len(collected_urls),
        "collected_urls": list(collected_urls)
    }

def collect_comments_processor(task: Dict, device_info: Dict, xhs: XHSOperator, collected_comments: set = None) -> Dict:
    """
    评论收集任务处理器
    :param task: 任务信息
    :param device_info: 设备信息
    :param xhs: XHSOperator实例
    :param collected_comments: 已收集的评论集合，用于去重
    :return: 处理结果
    """
    print(f"正在设备 {device_info['device_id']} 上收集评论")
    max_retries = 3
    retry_count = 0
    
    # 获取要处理的URL和关键词
    note_url = task.get('note_url')
    keyword = task.get('keyword')
    if not note_url:
        return {
            "device": device_info['device_id'],
            "status": "error",
            "error": "未提供笔记URL",
            "note_url": note_url,
            "keyword": keyword
        }
    
    # 初始化已收集评论集合
    if collected_comments is None:
        collected_comments = set()
    
    while retry_count < max_retries:
        try:
            # 获取完整URL（处理短链接）
            # if len(note_url) == 34:  # 链接长度34则为短链接
            #     full_url = xhs.get_redirect_url(note_url)
            #     print(f"处理笔记URL: {full_url}")
            # else:
            full_url = note_url #长链不需要处理直接使用

            # 收集评论
            comments = xhs.collect_comments_by_url(full_url)
            
            if not comments:
                print(f"笔记 {note_url} 未找到评论")
                return {
                    "device": device_info['device_id'],
                    "status": "success",
                    "keyword": keyword,
                    "note_url": note_url,
                    "comments_count": 0,
                    "comments": []
                }
            
            # 评论去重
            unique_comments = []
            for comment in comments:
                # 使用评论内容和作者作为唯一标识
                comment_key = f"{comment.get('content', '')}_{comment.get('author', '')}"
                if comment_key not in collected_comments:
                    collected_comments.add(comment_key)
                    unique_comments.append(comment)
            
            print(f"设备 {device_info['device_id']} 从笔记 {note_url} 收集到 {len(unique_comments)} 条新评论")
            
            return {
                "device": device_info['device_id'],
                "status": "success",
                "note_url": note_url,
                "keyword": keyword,
                "comments_count": len(unique_comments),
                "comments": unique_comments,
                "collected_comments": collected_comments  # 返回更新后的评论集合
            }
            
        except Exception as e:
            retry_count += 1
            print(f"设备 {device_info['device_id']} 第 {retry_count} 次尝试失败: {str(e)}")
            if retry_count < max_retries:
                print(f"等待 5 秒后重试...")
                time.sleep(5)
                continue
            else:
                return {
                    "device": device_info['device_id'],
                    "keyword": keyword,
                    "status": "error",
                    "note_url": note_url,
                    "error": str(e)
                }
    
    return {
        "device": device_info['device_id'],
        "status": "success",
        "note_url": note_url,
        "comments_count": 0,
        "comments": []
    }

if __name__ == "__main__":
    # 加载.env文件
    current_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(current_dir, '.env'))
    
    # 获取设备池（会自动启动Appium服务）
    devices_pool = get_device_pool()
    if not devices_pool:
        print("No devices available")
        exit(1)
    print(f"Available devices: {[dev['device_id'] for dev in devices_pool]}")
    
    # 初始化设备管理器
    device_manager = DeviceManager(devices_pool)
    
    # 初始化任务分配器
    task_distributor = TaskDistributor(device_manager)
    
    # 初始化任务处理器管理器
    task_processor_manager = TaskProcessorManager()
    task_processor_manager.register_processor('collect_notes', collect_notes_processor)
    
    # 获取关键词和最大笔记数
    keyword = "可露丽"
    max_notes = 8
    
    print(f"\n开始收集关键词 '{keyword}' 的小红书笔记...")
    
    # 创建收集笔记任务
    task = {
        "task_id": 1,
        "type": "collect_notes",
        "keyword": keyword,
        "notes_per_device": max_notes,
        "target_url_count": max_notes
    }
    
    # 添加任务到分发器
    task_distributor.add_task(task)
    
    # 运行任务
    print("\n开始收集笔记...")
    results = task_distributor.run_tasks(task_processor=task_processor_manager.process_task)
    
    # 打印结果
    print("\n收集结果:")
    for result in results:
        print(f"\n设备: {result['device']}")
        print(f"状态: {result['status']}")
        if result['status'] == 'success':
            print(f"收集到的笔记数量: {result['notes_count']}")
            if 'notes' in result:
                print("\n收集到的笔记:")
                for note in result['notes']:
                    print(f"- 标题: {note.get('title', 'N/A')}")
                    print(f"  作者: {note.get('author', 'N/A')}")
                    print(f"  内容: {note.get('content', 'N/A')[:100]}...")  # 只显示前100个字符
                    print(f"  URL: {note.get('note_url', 'N/A')}")
                    print(f"  点赞: {note.get('likes', 'N/A')}")
                    print(f"  收藏: {note.get('collects', 'N/A')}")
                    print(f"  评论: {note.get('comments', 'N/A')}")
                    print(f"  收集时间: {note.get('collect_time', 'N/A')}")
                    print("  " + "-" * 50)
        else:
            print(f"错误: {result['error']}")
    
    # 打印所有收集到的URL
    print("\n所有收集到的URL:")
    for url in task_distributor.get_collected_urls():
        print(f"- {url}")
    

    #测试并发收集评论
    # 初始化任务处理器管理器
    task_processor_manager = TaskProcessorManager()
    task_processor_manager.register_processor('collect_comments', collect_comments_processor)
    
    # 模拟从数据库获取5条笔记URL
    test_urls = [
        "http://xhslink.com/a/JbkelB4VaZgab",
        "http://xhslink.com/a/lkyC04xkeZgab",
        "http://xhslink.com/a/xd8ommNKgZgab",
        "http://xhslink.com/a/oaivHia0kZgab",
        "http://xhslink.com/a/Gj7iW9kQwdiab"
    ]
    
     # 创建评论收集任务列表
    comment_tasks = []
    for i, url in enumerate(test_urls, 1):
        task = {
            "task_id": i,
            "type": "collect_comments",
            "note_url": url
        }
        comment_tasks.append(task)
        
    print(f"\n准备处理 {len(comment_tasks)} 条笔记的评论...")

    # 添加任务到分发器
    # 添加评论收集任务到分发器
    for task in comment_tasks:
        task_distributor.add_task(task)
    
    # 运行任务
    print("\n开始并发收集评论...")
    results = task_distributor.run_tasks(task_processor=task_processor_manager.process_task)
    
    # 打印评论收集结果
    print("\n评论收集结果:")
    success_count = 0
    total_comments = 0
    failed_urls = []  # Initialize the list here
    
    for result in results:
        print(f"\n设备: {result['device']}")
        print(f"状态: {result['status']}")
        if result['status'] == 'success':
            print(f"笔记URL: {result['note_url']}")
            print(f"评论数量: {result['comments_count']}")
            if result['comments']:
                success_count += 1
                total_comments += result['comments_count']
                print("\n收集到的评论示例:")
                # 只显示前3条评论作为示例
                for comment in result['comments'][:3]:
                    print(f"作者: {comment['author']}")
                    print(f"内容: {comment['content']}")
                    print(f"点赞: {comment['likes']}")
                    print(f"时间: {comment['collect_time']}")
                    print("-" * 50)
        else:
            # 修改这里：从result中安全地获取note_url
            failed_url = result.get('note_url', '未知URL')  # 使用get方法，提供默认值
            print(f"错误: {result.get('error', '未知错误')}")
            failed_urls.append(failed_url)
            
    # 打印统计信息
    print("\n任务统计:")
    print(f"总任务数: {len(comment_tasks)}")
    print(f"成功任务数: {success_count}")
    print(f"失败任务数: {len(failed_urls)}")
    print(f"总收集评论数: {total_comments}")
    
    if failed_urls:
        print("\n失败的URL:")
        for url in failed_urls:
            print(f"- {url}")

    
