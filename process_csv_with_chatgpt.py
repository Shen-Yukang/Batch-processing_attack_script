#!/usr/bin/env python3
"""
CSV处理脚本 - 使用ChatGPT API处理图片和文本
将图片地址和prompt传递给ChatGPT，并将响应添加到CSV的新列中
"""

import pandas as pd
import openai
import base64
import requests
import os
import time
import json
from typing import Optional, Dict, Any
import argparse
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChatGPTProcessor:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        """
        初始化ChatGPT处理器

        Args:
            api_key: OpenAI API密钥
            model: 使用的模型名称
        """
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model

    def encode_image(self, image_path: str) -> Optional[str]:
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

    def process_image_and_prompt(self, image_path: str, prompt: str) -> str:
        """
        处理图片和prompt，调用ChatGPT API

        Args:
            image_path: 图片路径
            prompt: 文本提示

        Returns:
            ChatGPT的响应文本
        """
        try:
            # 检查图片文件是否存在
            if not os.path.exists(image_path):
                return f"错误：图片文件不存在 - {image_path}"

            # 编码图片
            base64_image = self.encode_image(image_path)
            if not base64_image:
                return f"错误：无法编码图片 - {image_path}"

            # 构建消息
            messages = [
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
            ]

            # 调用API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=1000,
                temperature=0.7
            )

            return response.choices[0].message.content

        except Exception as e:
            error_msg = f"API调用失败: {str(e)}"
            logger.error(error_msg)
            return error_msg

def process_csv_file(input_file: str, output_file: str, api_key: str,
                    delay: float = 1.0, start_row: int = 0, end_row: Optional[int] = None):
    """
    处理CSV文件

    Args:
        input_file: 输入CSV文件路径
        output_file: 输出CSV文件路径
        api_key: OpenAI API密钥
        delay: API调用之间的延迟（秒）
        start_row: 开始处理的行号（0开始）
        end_row: 结束处理的行号（不包含），None表示处理到最后
    """
    # 读取CSV文件
    try:
        df = pd.read_csv(input_file)
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

    # 添加新列用于存储ChatGPT响应
    if 'ChatGPT Response' not in df.columns:
        df['ChatGPT Response'] = ''

    # 初始化ChatGPT处理器
    processor = ChatGPTProcessor(api_key)

    # 确定处理范围
    start_idx = max(0, start_row)
    end_idx = min(len(df), end_row) if end_row is not None else len(df)

    logger.info(f"开始处理行 {start_idx} 到 {end_idx-1}")

    # 处理每一行
    for idx in range(start_idx, end_idx):
        try:
            row = df.iloc[idx]
            image_path = row['Image Path']
            prompt = row['Content of P*']

            logger.info(f"处理第 {idx+1}/{len(df)} 行: {os.path.basename(image_path)}")

            # 如果已经有响应，跳过
            if pd.notna(row['ChatGPT Response']) and row['ChatGPT Response'].strip():
                logger.info(f"第 {idx+1} 行已有响应，跳过")
                continue

            # 调用ChatGPT API
            response = processor.process_image_and_prompt(image_path, prompt)

            # 保存响应
            df.at[idx, 'ChatGPT Response'] = response

            logger.info(f"第 {idx+1} 行处理完成")

            # 每处理10行保存一次
            if (idx + 1) % 10 == 0:
                df.to_csv(output_file, index=False, encoding='utf-8')
                logger.info(f"已保存进度到第 {idx+1} 行")

            # 延迟以避免API限制
            if delay > 0:
                time.sleep(delay)

        except Exception as e:
            logger.error(f"处理第 {idx+1} 行时出错: {e}")
            df.at[idx, 'ChatGPT Response'] = f"处理错误: {str(e)}"

    # 保存最终结果
    try:
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"处理完成，结果已保存到: {output_file}")
    except Exception as e:
        logger.error(f"保存文件失败: {e}")

def main():
    parser = argparse.ArgumentParser(description='使用ChatGPT API处理CSV文件中的图片和文本')
    parser.add_argument('input_file', help='输入CSV文件路径')
    parser.add_argument('output_file', help='输出CSV文件路径')
    parser.add_argument('--api-key', help='OpenAI API密钥（也可通过环境变量OPENAI_API_KEY设置）')
    parser.add_argument('--delay', type=float, default=1.0, help='API调用之间的延迟（秒），默认1.0')
    parser.add_argument('--start-row', type=int, default=0, help='开始处理的行号（0开始），默认0')
    parser.add_argument('--end-row', type=int, help='结束处理的行号（不包含），默认处理到最后')
    parser.add_argument('--model', default='gpt-4o-mini', help='使用的模型，默认gpt-4o-mini')

    args = parser.parse_args()

    # 获取API密钥
    api_key = args.api_key or os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请提供OpenAI API密钥，通过--api-key参数或OPENAI_API_KEY环境变量")
        return

    # 检查输入文件是否存在
    if not os.path.exists(args.input_file):
        logger.error(f"输入文件不存在: {args.input_file}")
        return

    # 处理CSV文件
    process_csv_file(
        input_file=args.input_file,
        output_file=args.output_file,
        api_key=api_key,
        delay=args.delay,
        start_row=args.start_row,
        end_row=args.end_row
    )

if __name__ == "__main__":
    main()
