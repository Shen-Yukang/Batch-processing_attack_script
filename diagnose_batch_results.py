#!/usr/bin/env python3
"""
诊断批处理结果文件缺失问题
"""

import os
import json
import glob
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_batch_directory(batch_dir):
    """分析批处理目录"""
    logger.info(f"🔍 分析批处理目录: {batch_dir}")
    
    if not os.path.exists(batch_dir):
        logger.error(f"❌ 目录不存在: {batch_dir}")
        return
    
    # 读取状态文件
    status_file = os.path.join(batch_dir, "batch_status.json")
    if not os.path.exists(status_file):
        logger.error(f"❌ 状态文件不存在: {status_file}")
        return
    
    with open(status_file, 'r') as f:
        status_data = json.load(f)
    
    total_jobs = status_data.get('total_jobs', 0)
    jobs = status_data.get('jobs', [])
    
    logger.info(f"📊 总任务数: {total_jobs}")
    
    # 统计各状态的任务
    status_counts = {}
    completed_jobs = []
    failed_jobs = []
    
    for job in jobs:
        status = job.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1
        
        if status == 'completed':
            completed_jobs.append(job)
        elif status in ['failed', 'timeout']:
            failed_jobs.append(job)
    
    logger.info("📈 任务状态统计:")
    for status, count in status_counts.items():
        logger.info(f"  {status}: {count}")
    
    # 检查输入文件
    input_files = glob.glob(os.path.join(batch_dir, "batch_*.jsonl"))
    input_files = [f for f in input_files if not os.path.basename(f).startswith("batch_results_")]
    
    logger.info(f"📁 输入文件数量: {len(input_files)}")
    
    # 检查结果文件
    result_files = glob.glob(os.path.join(batch_dir, "batch_results_*.jsonl"))
    logger.info(f"📄 结果文件数量: {len(result_files)}")
    
    if result_files:
        logger.info("📄 现有结果文件:")
        for result_file in result_files:
            file_size = os.path.getsize(result_file) / 1024  # KB
            mtime = datetime.fromtimestamp(os.path.getmtime(result_file))
            logger.info(f"  - {os.path.basename(result_file)} ({file_size:.1f} KB, {mtime.strftime('%H:%M:%S')})")
            
            # 检查文件内容的行号范围
            try:
                with open(result_file, 'r') as f:
                    first_line = f.readline()
                    if 'custom_id' in first_line and 'row_' in first_line:
                        import re
                        match = re.search(r'row_(\d+)', first_line)
                        if match:
                            start_row = int(match.group(1))
                            
                            # 读取最后几行来找结束行
                            f.seek(0, 2)  # 移到文件末尾
                            file_size = f.tell()
                            f.seek(max(0, file_size - 1000))  # 读取最后1000字符
                            lines = f.readlines()
                            
                            end_row = start_row
                            for line in reversed(lines):
                                if 'row_' in line:
                                    match = re.search(r'row_(\d+)', line)
                                    if match:
                                        end_row = int(match.group(1))
                                        break
                            
                            logger.info(f"    📊 包含行号: {start_row} - {end_row}")
            except Exception as e:
                logger.warning(f"    ⚠️  无法分析文件内容: {e}")
    
    # 分析缺失的结果
    logger.info("\n🔍 分析缺失的结果文件:")
    
    missing_results = []
    for job in completed_jobs:
        job_name = job['name']
        # 查找对应的结果文件
        job_result_files = [f for f in result_files if job_name in os.path.basename(f)]
        
        if not job_result_files:
            missing_results.append(job)
            logger.warning(f"❌ {job_name} (行 {job['start_row']}-{job['end_row']}) 标记为完成但缺少结果文件")
    
    if missing_results:
        logger.error(f"💥 发现 {len(missing_results)} 个缺失结果的任务")
        
        # 检查是否所有结果都在一个文件中
        if len(result_files) == 1:
            logger.info("🔍 检查是否所有结果都合并在一个文件中...")
            result_file = result_files[0]
            
            try:
                with open(result_file, 'r') as f:
                    content = f.read()
                
                found_jobs = []
                for job in missing_results:
                    expected_rows = [f"row_{i}" for i in range(job['start_row'], min(job['start_row'] + 3, job['end_row']))]
                    if any(row in content for row in expected_rows):
                        found_jobs.append(job['name'])
                
                if found_jobs:
                    logger.info(f"✅ 在结果文件中找到了 {len(found_jobs)} 个任务的数据: {found_jobs}")
                else:
                    logger.warning("⚠️  结果文件中未找到缺失任务的数据")
                    
            except Exception as e:
                logger.error(f"❌ 检查结果文件失败: {e}")
    else:
        logger.info("✅ 所有已完成的任务都有对应的结果文件")
    
    # 建议修复方案
    logger.info("\n💡 建议修复方案:")
    if missing_results:
        logger.info("1. 重新运行缺失结果的批次")
        logger.info("2. 检查OpenAI API状态和配额")
        logger.info("3. 验证batch_processor.py的输出目录参数")
        logger.info("4. 检查网络连接和文件权限")
    else:
        logger.info("1. 当前状态正常，继续等待剩余任务完成")
        logger.info("2. 完成后可以进行结果合并")

def main():
    logger.info("🔧 批处理结果诊断工具")
    logger.info("="*60)
    
    # 查找最新的批处理目录
    batch_dirs = glob.glob("output/batch_results_*")
    if not batch_dirs:
        logger.error("❌ 未找到任何批处理目录")
        return
    
    # 按修改时间排序，最新的在前
    batch_dirs.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    
    logger.info(f"📁 找到 {len(batch_dirs)} 个批处理目录")
    
    # 分析最新的目录
    latest_dir = batch_dirs[0]
    logger.info(f"🎯 分析最新目录: {latest_dir}")
    
    analyze_batch_directory(latest_dir)
    
    # 如果有多个目录，简单列出其他的
    if len(batch_dirs) > 1:
        logger.info(f"\n📂 其他批处理目录:")
        for i, batch_dir in enumerate(batch_dirs[1:], 2):
            mtime = datetime.fromtimestamp(os.path.getmtime(batch_dir))
            logger.info(f"  {i}. {batch_dir} ({mtime.strftime('%Y-%m-%d %H:%M:%S')})")

if __name__ == "__main__":
    main()
