#!/usr/bin/env python3
"""
创建OpenAI Batch API输入文件的脚本
将CSV文件转换为JSONL格式，用于批量处理
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

def encode_image(image_path: str) -> Optional[str]:
    """
    将图片编码为base64格式

    Args:
        image_path: 图片文件路径

    Returns:
        base64编码的图片字符串，如果失败返回None
    """
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        logger.error(f"编码图片失败 {image_path}: {e}")
        return None

def create_batch_request(custom_id: str, image_path: str, prompt: str, model: str = "gpt-4o-mini") -> Optional[dict]:
    """
    创建单个批处理请求

    Args:
        custom_id: 自定义ID，用于标识请求
        image_path: 图片路径
        prompt: 文本提示
        model: 使用的模型

    Returns:
        批处理请求字典，如果失败返回None
    """
    # 检查图片文件是否存在
    if not os.path.exists(image_path):
        logger.warning(f"图片文件不存在，跳过: {image_path}")
        return None

    # 编码图片
    base64_image = encode_image(image_path)
    if not base64_image:
        logger.warning(f"无法编码图片，跳过: {image_path}")
        return None

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

def create_batch_input_file(input_csv: str, output_jsonl: str, model: str = "gpt-4o",
                           start_row: int = 0, end_row: Optional[int] = None):
    """
    从CSV文件创建批处理输入文件

    Args:
        input_csv: 输入CSV文件路径
        output_jsonl: 输出JSONL文件路径
        model: 使用的模型
        start_row: 开始处理的行号（0开始）
        end_row: 结束处理的行号（不包含），None表示处理到最后
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(input_csv)
        logger.info(f"成功读取CSV文件，共{len(df)}行")
    except Exception as e:
        logger.error(f"读取CSV文件失败: {e}")
        return

    # 检查必要的列是否存在
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

    for idx in range(start_idx, end_idx):
        row = df.iloc[idx]
        image_path = row['Image Path']
        prompt = row['Content of P*']

        # 创建自定义ID
        custom_id = f"row_{idx}"

        # 创建批处理请求
        request = create_batch_request(custom_id, image_path, prompt, model)

        if request:
            requests.append(request)
            logger.info(f"已创建请求 {idx+1}/{end_idx}: {os.path.basename(image_path)}")
        else:
            skipped_count += 1
            logger.warning(f"跳过第 {idx+1} 行: {image_path}")

    # 写入JSONL文件
    try:
        with open(output_jsonl, 'w', encoding='utf-8') as f:
            for request in requests:
                f.write(json.dumps(request, ensure_ascii=False) + '\n')

        logger.info(f"批处理输入文件已创建: {output_jsonl}")
        logger.info(f"总请求数: {len(requests)}")
        logger.info(f"跳过的行数: {skipped_count}")

    except Exception as e:
        logger.error(f"写入JSONL文件失败: {e}")

def main():
    config = parse_config('config/batch_config.conf')
    parser = argparse.ArgumentParser(description='创建OpenAI Batch API输入文件')
    parser.add_argument('input_csv', help='输入CSV文件路径')
    parser.add_argument('output_jsonl', help='输出JSONL文件路径')
    parser.add_argument('--model', default=config.get('MODEL', 'gpt-4o'), help='使用的模型，默认gpt-4o')
    parser.add_argument('--start-row', type=int, default=0, help='开始处理的行号（0开始），默认0')
    parser.add_argument('--end-row', type=int, help='结束处理的行号（不包含），默认处理到最后')
    args = parser.parse_args()

    # 检查输入文件是否存在
    if not os.path.exists(args.input_csv):
        logger.error(f"输入文件不存在: {args.input_csv}")
        return

    # 创建批处理输入文件
    create_batch_input_file(
        input_csv=args.input_csv,
        output_jsonl=args.output_jsonl,
        model=args.model,
        start_row=args.start_row,
        end_row=args.end_row
    )

if __name__ == "__main__":
    main()
