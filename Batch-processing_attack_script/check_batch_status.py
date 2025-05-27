#!/usr/bin/env python3
"""
检查批处理状态的脚本
"""

import os
import sys
import time
import openai
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_batch_status(batch_id: str, api_key: str):
    """检查批处理状态"""
    client = openai.OpenAI(api_key=api_key)
    
    try:
        response = client.batches.retrieve(batch_id)
        
        logger.info(f"批处理ID: {response.id}")
        logger.info(f"状态: {response.status}")
        logger.info(f"创建时间: {response.created_at}")
        logger.info(f"完成时间: {response.completed_at}")
        logger.info(f"过期时间: {response.expires_at}")
        
        # 显示进度信息
        if hasattr(response, 'request_counts') and response.request_counts:
            counts = response.request_counts
            total = getattr(counts, 'total', 0)
            completed = getattr(counts, 'completed', 0)
            failed = getattr(counts, 'failed', 0)
            logger.info(f"进度: {completed}/{total} 完成, {failed} 失败")
        
        # 如果完成，显示输出文件信息
        if response.status == 'completed':
            logger.info(f"输出文件ID: {response.output_file_id}")
            if response.error_file_id:
                logger.info(f"错误文件ID: {response.error_file_id}")
        
        return response.status
        
    except Exception as e:
        logger.error(f"检查批处理状态失败: {e}")
        return None

def download_results(batch_id: str, api_key: str, output_dir: str = "."):
    """下载批处理结果"""
    client = openai.OpenAI(api_key=api_key)
    
    try:
        # 获取批处理状态
        response = client.batches.retrieve(batch_id)
        
        if response.status != 'completed':
            logger.error(f"批处理未完成，当前状态: {response.status}")
            return None
        
        output_file_id = response.output_file_id
        if not output_file_id:
            logger.error("没有输出文件ID")
            return None
        
        # 下载结果文件
        file_response = client.files.content(output_file_id)
        
        # 保存到本地文件
        output_path = os.path.join(output_dir, f"batch_results_{batch_id}.jsonl")
        with open(output_path, 'wb') as f:
            f.write(file_response.content)
        
        logger.info(f"结果文件已下载: {output_path}")
        
        # 下载错误文件（如果有）
        if response.error_file_id:
            try:
                error_response = client.files.content(response.error_file_id)
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

def wait_and_download(batch_id: str, api_key: str, check_interval: int = 60):
    """等待批处理完成并下载结果"""
    logger.info(f"监控批处理: {batch_id}")
    
    while True:
        status = check_batch_status(batch_id, api_key)
        
        if status == 'completed':
            logger.info("批处理已完成！开始下载结果...")
            result_file = download_results(batch_id, api_key)
            if result_file:
                logger.info(f"✅ 批处理成功完成！结果文件: {result_file}")
                return result_file
            else:
                logger.error("❌ 下载结果失败")
                return None
                
        elif status in ['failed', 'expired', 'cancelled']:
            logger.error(f"❌ 批处理失败，状态: {status}")
            return None
            
        elif status in ['validating', 'in_progress', 'finalizing']:
            logger.info(f"批处理进行中，状态: {status}")
            logger.info(f"等待 {check_interval} 秒后再次检查...")
            time.sleep(check_interval)
            
        else:
            logger.warning(f"未知状态: {status}")
            time.sleep(check_interval)

def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python check_batch_status.py BATCH_ID                    # 检查状态")
        print("  python check_batch_status.py BATCH_ID --wait             # 等待完成并下载")
        print("  python check_batch_status.py BATCH_ID --download         # 立即下载（如果已完成）")
        return
    
    batch_id = sys.argv[1]
    
    # 获取API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请设置环境变量 OPENAI_API_KEY")
        return
    
    if len(sys.argv) > 2:
        if sys.argv[2] == '--wait':
            wait_and_download(batch_id, api_key)
        elif sys.argv[2] == '--download':
            download_results(batch_id, api_key)
        else:
            logger.error("无效参数，使用 --wait 或 --download")
    else:
        check_batch_status(batch_id, api_key)

if __name__ == "__main__":
    main()
