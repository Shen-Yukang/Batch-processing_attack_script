#!/usr/bin/env python3
"""
OpenAI Batch API处理器
处理批量请求的完整工作流程：上传文件、创建批处理、监控状态、下载结果
"""

import os
import time
import json
import argparse
import logging
from typing import Optional
import openai

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, api_key: str):
        """
        初始化批处理器

        Args:
            api_key: OpenAI API密钥
        """
        self.client = openai.OpenAI(api_key=api_key)

    def upload_batch_file(self, file_path: str) -> Optional[str]:
        """
        上传批处理输入文件

        Args:
            file_path: JSONL文件路径

        Returns:
            文件ID，如果失败返回None
        """
        try:
            with open(file_path, "rb") as file:
                response = self.client.files.create(
                    file=file,
                    purpose="batch"
                )

            file_id = response.id
            logger.info(f"文件上传成功，文件ID: {file_id}")
            return file_id

        except Exception as e:
            logger.error(f"文件上传失败: {e}")
            return None

    def create_batch(self, input_file_id: str, completion_window: str = "24h") -> Optional[str]:
        """
        创建批处理任务

        Args:
            input_file_id: 输入文件ID
            completion_window: 完成时间窗口（24h）

        Returns:
            批处理ID，如果失败返回None
        """
        try:
            response = self.client.batches.create(
                input_file_id=input_file_id,
                endpoint="/v1/chat/completions",
                completion_window=completion_window
            )

            batch_id = response.id
            logger.info(f"批处理创建成功，批处理ID: {batch_id}")
            logger.info(f"状态: {response.status}")
            return batch_id

        except Exception as e:
            logger.error(f"批处理创建失败: {e}")
            return None

    def get_batch_status(self, batch_id: str) -> Optional[dict]:
        """
        获取批处理状态

        Args:
            batch_id: 批处理ID

        Returns:
            批处理状态信息，如果失败返回None
        """
        try:
            response = self.client.batches.retrieve(batch_id)
            return {
                'id': response.id,
                'status': response.status,
                'created_at': response.created_at,
                'completed_at': response.completed_at,
                'failed_at': response.failed_at,
                'expires_at': response.expires_at,
                'request_counts': response.request_counts,
                'output_file_id': response.output_file_id,
                'error_file_id': response.error_file_id
            }
        except Exception as e:
            logger.error(f"获取批处理状态失败: {e}")
            return None

    def wait_for_completion(self, batch_id: str, check_interval: int = 60) -> bool:
        """
        等待批处理完成

        Args:
            batch_id: 批处理ID
            check_interval: 检查间隔（秒）

        Returns:
            是否成功完成
        """
        logger.info(f"等待批处理完成: {batch_id}")

        while True:
            status_info = self.get_batch_status(batch_id)
            if not status_info:
                logger.error("无法获取批处理状态")
                return False

            status = status_info['status']
            logger.info(f"当前状态: {status}")

            if status == 'completed':
                logger.info("批处理已完成")
                return True
            elif status == 'failed':
                logger.error("批处理失败")
                return False
            elif status == 'expired':
                logger.error("批处理已过期")
                return False
            elif status == 'cancelled':
                logger.error("批处理已取消")
                return False

            # 显示进度信息
            if status_info.get('request_counts'):
                counts = status_info['request_counts']
                total = getattr(counts, 'total', 0)
                completed = getattr(counts, 'completed', 0)
                failed = getattr(counts, 'failed', 0)
                logger.info(f"进度: {completed}/{total} 完成, {failed} 失败")

            logger.info(f"等待 {check_interval} 秒后再次检查...")
            time.sleep(check_interval)

    def download_results(self, batch_id: str, output_dir: str = ".") -> Optional[str]:
        """
        下载批处理结果

        Args:
            batch_id: 批处理ID
            output_dir: 输出目录

        Returns:
            结果文件路径，如果失败返回None
        """
        try:
            # 获取批处理状态
            status_info = self.get_batch_status(batch_id)
            if not status_info or status_info['status'] != 'completed':
                logger.error("批处理未完成或获取状态失败")
                return None

            output_file_id = status_info.get('output_file_id')
            if not output_file_id:
                logger.error("没有输出文件ID")
                return None

            # 下载结果文件
            response = self.client.files.content(output_file_id)

            # 保存到本地文件
            output_path = os.path.join(output_dir, f"batch_results_{batch_id}.jsonl")
            with open(output_path, 'wb') as f:
                f.write(response.content)

            logger.info(f"结果文件已下载: {output_path}")

            # 下载错误文件（如果有）
            error_file_id = status_info.get('error_file_id')
            if error_file_id:
                try:
                    error_response = self.client.files.content(error_file_id)
                    error_path = os.path.join(output_dir, f"batch_errors_{batch_id}.jsonl")
                    with open(error_path, 'wb') as f:
                        f.write(error_response.content)
                    logger.info(f"错误文件已下载: {error_path}")
                except Exception as e:
                    logger.warning(f"下载错误文件失败: {e}")

            return output_path

        except Exception as e:
            logger.error(f"下载结果失败: {e}")
            return None

    def process_batch(self, input_file: str, output_dir: str = ".",
                     completion_window: str = "24h", check_interval: int = 60) -> Optional[str]:
        """
        处理完整的批处理工作流程

        Args:
            input_file: 输入JSONL文件路径
            output_dir: 输出目录
            completion_window: 完成时间窗口
            check_interval: 状态检查间隔

        Returns:
            结果文件路径，如果失败返回None
        """
        logger.info("开始批处理工作流程")

        # 1. 上传文件
        file_id = self.upload_batch_file(input_file)
        if not file_id:
            return None

        # 2. 创建批处理
        batch_id = self.create_batch(file_id, completion_window)
        if not batch_id:
            return None

        # 3. 等待完成
        if not self.wait_for_completion(batch_id, check_interval):
            return None

        # 4. 下载结果
        result_file = self.download_results(batch_id, output_dir)

        if result_file:
            logger.info(f"批处理工作流程完成，结果文件: {result_file}")

        return result_file

def main():
    parser = argparse.ArgumentParser(description='OpenAI Batch API处理器')
    parser.add_argument('input_file', help='输入JSONL文件路径')
    parser.add_argument('--api-key', help='OpenAI API密钥（也可通过环境变量OPENAI_API_KEY设置）')
    parser.add_argument('--output-dir', default='.', help='输出目录，默认当前目录')
    parser.add_argument('--completion-window', default='24h', help='完成时间窗口，默认24h')
    parser.add_argument('--check-interval', type=int, default=60, help='状态检查间隔（秒），默认60')

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

    # 创建输出目录
    os.makedirs(args.output_dir, exist_ok=True)

    # 处理批处理
    processor = BatchProcessor(api_key)
    result_file = processor.process_batch(
        input_file=args.input_file,
        output_dir=args.output_dir,
        completion_window=args.completion_window,
        check_interval=args.check_interval
    )

    if result_file:
        logger.info(f"批处理成功完成！结果文件: {result_file}")
        return 0
    else:
        logger.error("批处理失败")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
