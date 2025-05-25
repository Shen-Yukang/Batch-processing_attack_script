#!/usr/bin/env python3
"""
重试失败批次的专用脚本
"""

import os
import json
import logging
from robust_batch_processor import RobustBatchProcessor

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def show_failed_batches(processor: RobustBatchProcessor):
    """显示失败的批次"""
    failed_jobs = [job for job in processor.jobs if job.status in ["failed", "timeout"]]
    
    if not failed_jobs:
        logger.info("✅ 没有失败的批次！")
        return []
    
    logger.info("=" * 80)
    logger.info("失败的批次详情")
    logger.info("=" * 80)
    
    for i, job in enumerate(failed_jobs, 1):
        logger.info(f"{i}. 任务: {job.name}")
        logger.info(f"   行范围: {job.start_row+1}-{job.end_row}")
        logger.info(f"   状态: {job.status}")
        logger.info(f"   尝试次数: {job.attempts}/{job.max_attempts}")
        logger.info(f"   错误信息: {job.error_message}")
        logger.info(f"   创建时间: {job.created_at}")
        logger.info("-" * 40)
    
    return failed_jobs

def retry_specific_batches(processor: RobustBatchProcessor, batch_names: list):
    """重试指定的批次"""
    for batch_name in batch_names:
        job = next((job for job in processor.jobs if job.name == batch_name), None)
        if not job:
            logger.error(f"未找到批次: {batch_name}")
            continue
        
        if job.status == "completed":
            logger.info(f"批次 {batch_name} 已完成，跳过")
            continue
        
        if job.attempts >= job.max_attempts:
            logger.warning(f"批次 {batch_name} 已达到最大重试次数")
            continue
        
        logger.info(f"🔄 重试批次: {batch_name}")
        processor.process_single_job(job)

def main():
    import sys
    
    # 检查状态文件是否存在
    output_dir = "batch_results_20250524_224700"
    status_file = os.path.join(output_dir, "batch_status.json")
    
    if not os.path.exists(status_file):
        logger.error(f"状态文件不存在: {status_file}")
        logger.info("请先运行 robust_batch_processor.py")
        return
    
    # 创建处理器并加载状态
    processor = RobustBatchProcessor(output_dir)
    
    if len(sys.argv) == 1:
        # 显示失败的批次
        failed_jobs = show_failed_batches(processor)
        
        if not failed_jobs:
            return
        
        print("\n选项:")
        print("1. 重试所有失败的批次")
        print("2. 重试特定的批次")
        print("3. 查看详细状态")
        print("4. 退出")
        
        choice = input("\n请选择 (1-4): ").strip()
        
        if choice == "1":
            processor.retry_failed_jobs()
        elif choice == "2":
            print("\n可重试的批次:")
            retryable_jobs = [job for job in failed_jobs if job.attempts < job.max_attempts]
            for i, job in enumerate(retryable_jobs, 1):
                print(f"  {i}. {job.name} (第{job.start_row+1}-{job.end_row}行)")
            
            if retryable_jobs:
                indices = input("请输入要重试的批次编号 (用逗号分隔，如: 1,3,5): ").strip()
                try:
                    selected_indices = [int(x.strip()) - 1 for x in indices.split(",")]
                    selected_batches = [retryable_jobs[i].name for i in selected_indices if 0 <= i < len(retryable_jobs)]
                    
                    if selected_batches:
                        retry_specific_batches(processor, selected_batches)
                    else:
                        logger.error("无效的选择")
                except ValueError:
                    logger.error("输入格式错误")
        elif choice == "3":
            processor.print_summary()
        
    else:
        # 命令行指定批次名称
        batch_names = sys.argv[1:]
        retry_specific_batches(processor, batch_names)

if __name__ == "__main__":
    main()
