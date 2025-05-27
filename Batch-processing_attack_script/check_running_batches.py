#!/usr/bin/env python3
"""
检查所有正在运行的批处理
"""

import os
import openai
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def list_all_batches(api_key: str):
    """列出所有批处理"""
    client = openai.OpenAI(api_key=api_key)
    
    try:
        # 获取批处理列表
        batches = client.batches.list(limit=20)
        
        logger.info("=" * 80)
        logger.info("所有批处理状态")
        logger.info("=" * 80)
        
        if not batches.data:
            logger.info("没有找到任何批处理")
            return
        
        for batch in batches.data:
            logger.info(f"批处理ID: {batch.id}")
            logger.info(f"状态: {batch.status}")
            logger.info(f"创建时间: {datetime.fromtimestamp(batch.created_at)}")
            
            if hasattr(batch, 'request_counts') and batch.request_counts:
                counts = batch.request_counts
                total = getattr(counts, 'total', 0)
                completed = getattr(counts, 'completed', 0)
                failed = getattr(counts, 'failed', 0)
                logger.info(f"进度: {completed}/{total} 完成, {failed} 失败")
            
            if batch.status in ['in_progress', 'validating', 'finalizing']:
                logger.info("🟡 此批处理正在运行中")
            elif batch.status == 'completed':
                logger.info("✅ 此批处理已完成")
            elif batch.status == 'failed':
                logger.info("❌ 此批处理已失败")
            
            logger.info("-" * 40)
        
        # 统计各状态的批处理数量
        status_counts = {}
        for batch in batches.data:
            status = batch.status
            status_counts[status] = status_counts.get(status, 0) + 1
        
        logger.info("状态统计:")
        for status, count in status_counts.items():
            logger.info(f"  {status}: {count}")
        
        # 检查是否有太多正在运行的批处理
        running_count = sum(status_counts.get(status, 0) for status in ['in_progress', 'validating', 'finalizing'])
        if running_count >= 5:
            logger.warning(f"⚠️ 有 {running_count} 个批处理正在运行，可能导致新批处理排队")
        
    except Exception as e:
        logger.error(f"获取批处理列表失败: {e}")

def cancel_stuck_batches(api_key: str):
    """取消卡住的批处理"""
    client = openai.OpenAI(api_key=api_key)
    
    try:
        batches = client.batches.list(limit=20)
        
        stuck_batches = []
        for batch in batches.data:
            # 检查是否是长时间validating状态的批处理
            if batch.status == 'validating':
                created_time = datetime.fromtimestamp(batch.created_at)
                time_diff = datetime.now() - created_time
                
                if time_diff.total_seconds() > 1800:  # 30分钟
                    stuck_batches.append(batch.id)
        
        if stuck_batches:
            logger.warning(f"发现 {len(stuck_batches)} 个可能卡住的批处理:")
            for batch_id in stuck_batches:
                logger.warning(f"  {batch_id}")
            
            response = input("是否要取消这些批处理？(y/N): ")
            if response.lower() == 'y':
                for batch_id in stuck_batches:
                    try:
                        client.batches.cancel(batch_id)
                        logger.info(f"✅ 已取消批处理: {batch_id}")
                    except Exception as e:
                        logger.error(f"❌ 取消批处理失败 {batch_id}: {e}")
        else:
            logger.info("没有发现卡住的批处理")
            
    except Exception as e:
        logger.error(f"检查卡住批处理失败: {e}")

def main():
    # 获取API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请设置环境变量 OPENAI_API_KEY")
        return
    
    # 列出所有批处理
    list_all_batches(api_key)
    
    # 检查并可选择取消卡住的批处理
    print("\n" + "=" * 80)
    cancel_stuck_batches(api_key)

if __name__ == "__main__":
    main()
