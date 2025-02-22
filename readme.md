# xhs-on-airflow

## 项目简介

基于Appium和Airflow的自动化数据爬取方案，用于高效地抓取小红书平台的数据。通过Appium模拟用户操作，结合Airflow调度管理，实现数据抓取的自动化和流程化。

## 项目架构

### 核心组件

1. **Apache Airflow**
   - DAG 任务编排和调度
   - 工作流监控和管理
   - 任务依赖关系处理

2. **Appium**
   - 模拟用户操作
   - 数据抓取
   - 异常处理

3. **安卓手机**
   - 运行小红书\抖音\微信APP

### 部署架构

```
+------------------+     +------------------------+     +----------------------+     +------------+
|      WEB UI      | --> | Airflow 服务器(云服务器) | --> | Appium 设备(本地PC)   | --> |  安卓手机   |
+------------------+     +------------------------+     +----------------------+     +------------+
```

### 项目结构

```
xhs-on-airflow/
├── dags/  # 存放Airflow的DAG流程代码（关键）
├── logs/  # 存放Airflow的日志
├── database/  # 存放Airflow的持久化数据
├── requirements.txt  # 存放项目依赖
├── webhook_server.py  # 存放Webhook服务器代码
├── docker-compose.yml  # 存放Docker Compose配置
├── readme.md  # 存放项目说明
├── .gitignore  # 存放Git忽略文件
├── .env.example  # 存放环境变量示例
```
