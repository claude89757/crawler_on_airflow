import json
import os
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime

def call_dify_api(text, api_key):
    """使用标准库调用Dify API"""
    url = "http://dify.lucyai.sale/v1/chat-messages"
    data = {
        "inputs": {"text": text},
        "query": text,
        "response_mode": "blocking",
        "user": f"scf_user_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    }
    
    req = Request(
        url,
        data=json.dumps(data).encode('utf-8'),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        },
        method="POST"
    )
    
    try:
        with urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))
    except HTTPError as e:
        return {"error": f"API请求失败: {e.code}", "detail": e.read().decode()}
    except URLError as e:
        return {"error": f"网络错误: {e.reason}"}

def main_handler(event, context):
    try:
        # 1. 解析输入参数
        body = json.loads(event["body"]) if "body" in event else event
        text = body.get("text", "")
        
        if not text:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "缺少text参数"}, ensure_ascii=False)
            }

        # 2. 获取环境变量配置
        api_key = os.getenv("DIFY_API_KEY", "app-pQNhb00JrC5b6omhkEzweMb2")
        
        # 3. 调用API
        result = call_dify_api(text, api_key)
        
        if "error" in result:
            return {
                "statusCode": 502,
                "body": json.dumps(result, ensure_ascii=False)
            }

        # 4. 返回润色结果
        return {
            "statusCode": 200,
            "body": json.dumps({
                "original_text": text,
                "polished_text": result.get("answer", "润色失败"),
                "timestamp": datetime.now().isoformat()
            }, ensure_ascii=False)
        }

    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "非法JSON格式"}, ensure_ascii=False)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "内部错误", "detail": str(e)}, ensure_ascii=False)
        }