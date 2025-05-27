#!/usr/bin/env python3
"""
创建安全的批处理输入文件，跳过有问题的图片
"""

import os
import json
import base64
import pandas as pd
import argparse
import logging
from typing import Optional
import re
from src.utils.config_loader import parse_config

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_image_valid(image_path: str) -> tuple[bool, str]:
    """检查图片是否有效"""
    try:
        # 检查文件是否存在
        if not os.path.exists(image_path):
            return False, "文件不存在"
        
        # 检查文件大小
        file_size = os.path.getsize(image_path)
        if file_size > 20 * 1024 * 1024:  # 20MB限制
            return False, f"文件过大: {file_size/1024/1024:.1f}MB"
        
        # 尝试读取和编码
        with open(image_path, "rb") as image_file:
            image_data = image_file.read()
            base64_data = base64.b64encode(image_data).decode('utf-8')
            
            # 检查base64编码后的大小
            if len(base64_data) > 20 * 1024 * 1024:  # 20MB限制
                return False, f"编码后过大: {len(base64_data)/1024/1024:.1f}MB"
        
        return True, "正常"
        
    except Exception as e:
        return False, f"读取错误: {str(e)}"

def encode_image_safe(image_path: str) -> Optional[str]:
    """安全地编码图片"""
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        logger.error(f"编码图片失败 {image_path}: {e}")
        return None

def create_safe_batch_request(custom_id: str, image_path: str, prompt: str, model: str = "gpt-4o-mini") -> Optional[dict]:
    """创建安全的批处理请求"""
    
    # 检查图片是否有效
    is_valid, reason = is_image_valid(image_path)
    if not is_valid:
        logger.warning(f"跳过无效图片 {custom_id}: {reason}")
        return None
    
    # 编码图片
    base64_image = encode_image_safe(image_path)
    if not base64_image:
        logger.warning(f"跳过编码失败的图片 {custom_id}")
        return None
    
    # 检查prompt长度
    if len(prompt) > 4000:  # 限制prompt长度
        logger.warning(f"截断过长的prompt {custom_id}: {len(prompt)} 字符")
        prompt = prompt[:4000] + "..."
    
    # 构建批处理请求
    request = {
        "custom_id": custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 1000,
            "temperature": 0.7
        }
    }
    
    return request

def create_safe_batch_input_file(input_csv: str, output_jsonl: str, model: str = "gpt-4o-mini", 
                                start_row: int = 0, end_row: Optional[int] = None):
    """创建安全的批处理输入文件"""
    
    # 读取CSV文件
    try:
        df = pd.read_csv(input_csv)
        logger.info(f"成功读取CSV文件，共{len(df)}行")
    except Exception as e:
        logger.error(f"读取CSV文件失败: {e}")
        return
    
    # 检查必要的列
    required_columns = ['Image Path', 'Content of P*']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"CSV文件缺少必要的列: {missing_columns}")
        return
    
    # 确定处理范围
    start_idx = max(0, start_row)
    end_idx = min(len(df), end_row) if end_row is not None else len(df)
    
    logger.info(f"处理范围: 第{start_idx+1}行到第{end_idx}行")
    
    # 创建批处理请求
    requests = []
    skipped_count = 0
    valid_count = 0
    
    for idx in range(start_idx, end_idx):
        row = df.iloc[idx]
        image_path = row['Image Path']
        prompt = row['Content of P*']
        
        # 创建自定义ID
        custom_id = f"row_{idx}"
        
        # 显示进度
        if (idx - start_idx + 1) % 10 == 0:
            logger.info(f"处理进度: {idx - start_idx + 1}/{end_idx - start_idx}")
        
        # 创建批处理请求
        request = create_safe_batch_request(custom_id, image_path, prompt, model)
        
        if request:
            requests.append(request)
            valid_count += 1
        else:
            skipped_count += 1
    
    # 写入JSONL文件
    if not requests:
        logger.error("没有有效的请求可以写入")
        return
    
    try:
        with open(output_jsonl, 'w', encoding='utf-8') as f:
            for request in requests:
                f.write(json.dumps(request, ensure_ascii=False) + '\n')
        
        logger.info(f"安全批处理输入文件已创建: {output_jsonl}")
        logger.info(f"有效请求数: {valid_count}")
        logger.info(f"跳过的请求数: {skipped_count}")
        logger.info(f"成功率: {valid_count/(valid_count+skipped_count)*100:.1f}%")
        
        # 保存跳过的行号信息
        if skipped_count > 0:
            skipped_file = output_jsonl.replace('.jsonl', '_skipped.txt')
            with open(skipped_file, 'w') as f:
                f.write(f"跳过的行数: {skipped_count}\n")
                f.write(f"处理范围: {start_idx}-{end_idx}\n")
            logger.info(f"跳过信息已保存: {skipped_file}")
        
    except Exception as e:
        logger.error(f"写入JSONL文件失败: {e}")

def main():
    config = parse_config('config/batch_config.conf')
    parser = argparse.ArgumentParser(description='创建安全的OpenAI Batch API输入文件')
    parser.add_argument('input_csv', help='输入CSV文件路径')
    parser.add_argument('output_jsonl', help='输出JSONL文件路径')
    parser.add_argument('--model', default=config.get('MODEL', 'gpt-4o-mini'), help='使用的模型，默认gpt-4o-mini')
    parser.add_argument('--start-row', type=int, default=0, help='开始处理的行号（0开始），默认0')
    parser.add_argument('--end-row', type=int, help='结束处理的行号（不包含），默认处理到最后')
    args = parser.parse_args()
    
    # 检查输入文件是否存在
    if not os.path.exists(args.input_csv):
        logger.error(f"输入文件不存在: {args.input_csv}")
        return
    
    # 创建安全的批处理输入文件
    create_safe_batch_input_file(
        input_csv=args.input_csv,
        output_jsonl=args.output_jsonl,
        model=args.model,
        start_row=args.start_row,
        end_row=args.end_row
    )

if __name__ == "__main__":
    main()
