#!/usr/bin/env python3
"""
快速完成剩余行的脚本
"""

import os
import glob
import logging
import subprocess
import time

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_completed_rows():
    """检查已完成的行数"""
    result_files = glob.glob("batch_results_20250524_224700/batch_results_*.jsonl")
    
    completed_rows = set()
    
    for file in result_files:
        try:
            with open(file, 'r') as f:
                for line in f:
                    import json
                    data = json.loads(line)
                    custom_id = data.get('custom_id', '')
                    if custom_id.startswith('row_'):
                        row_num = int(custom_id.split('_')[1])
                        completed_rows.add(row_num)
        except Exception as e:
            logger.warning(f"读取文件失败 {file}: {e}")
    
    logger.info(f"已完成的行数: {len(completed_rows)}")
    logger.info(f"已完成的行范围: {min(completed_rows) if completed_rows else 0} - {max(completed_rows) if completed_rows else 0}")
    
    # 找出缺失的行
    all_rows = set(range(172))
    missing_rows = all_rows - completed_rows
    
    if missing_rows:
        missing_ranges = []
        sorted_missing = sorted(missing_rows)
        start = sorted_missing[0]
        end = start
        
        for row in sorted_missing[1:]:
            if row == end + 1:
                end = row
            else:
                missing_ranges.append((start, end))
                start = end = row
        missing_ranges.append((start, end))
        
        logger.info(f"缺失的行数: {len(missing_rows)}")
        logger.info("缺失的行范围:")
        for start, end in missing_ranges:
            if start == end:
                logger.info(f"  第{start+1}行")
            else:
                logger.info(f"  第{start+1}-{end+1}行")
        
        return missing_ranges
    else:
        logger.info("✅ 所有行都已完成！")
        return []

def process_missing_ranges(missing_ranges):
    """处理缺失的行范围"""
    
    for i, (start, end) in enumerate(missing_ranges):
        batch_name = f"final_batch_{i+1}"
        logger.info(f"\n📦 处理缺失范围 {i+1}/{len(missing_ranges)}: 第{start+1}-{end+1}行")
        
        # 创建输入文件
        jsonl_file = f"{batch_name}.jsonl"
        create_cmd = f"python create_safe_batch_input.py new_csv/content_CogAgent.csv {jsonl_file} --model gpt-4o-mini --start-row {start} --end-row {end+1}"
        
        logger.info(f"创建输入文件: {create_cmd}")
        result = subprocess.run(create_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"❌ 创建输入文件失败: {result.stderr}")
            continue
        
        # 处理批次
        process_cmd = f"python batch_processor.py {jsonl_file} --output-dir batch_results_20250524_224700 --check-interval 30"
        
        logger.info(f"处理批次: {process_cmd}")
        
        # 设置5分钟超时
        try:
            result = subprocess.run(process_cmd, shell=True, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info(f"✅ {batch_name} 完成")
            else:
                logger.error(f"❌ {batch_name} 失败: {result.stderr}")
        
        except subprocess.TimeoutExpired:
            logger.warning(f"⚠️ {batch_name} 超时，但可能仍在后台处理")
        
        # 清理临时文件
        if os.path.exists(jsonl_file):
            os.remove(jsonl_file)
        
        # 短暂延迟
        if i < len(missing_ranges) - 1:
            logger.info("等待30秒...")
            time.sleep(30)

def main():
    logger.info("🔍 检查已完成的批次...")
    
    # 检查已完成的行
    missing_ranges = check_completed_rows()
    
    if not missing_ranges:
        logger.info("🎉 所有行都已完成！可以合并结果了:")
        logger.info("python merge_all_results.py batch_results_20250524_224700 new_csv/content_CogAgent.csv final_output.csv")
        return
    
    # 处理缺失的行
    logger.info(f"\n🚀 开始处理 {len(missing_ranges)} 个缺失范围...")
    process_missing_ranges(missing_ranges)
    
    logger.info("\n✅ 处理完成！现在可以合并结果:")
    logger.info("python merge_all_results.py batch_results_20250524_224700 new_csv/content_CogAgent.csv final_output.csv")

if __name__ == "__main__":
    main()
