#!/usr/bin/env python3
"""
处理剩余批次的脚本（更小的批次大小）
"""

import os
import time
import logging
import subprocess
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command_with_timeout(command, description, timeout=600):
    """运行命令并设置超时"""
    logger.info(f"执行: {description}")
    logger.info(f"命令: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
        
        if result.returncode == 0:
            logger.info(f"✅ {description} 成功")
            return True
        else:
            logger.error(f"❌ {description} 失败")
            logger.error(f"错误: {result.stderr.strip()}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {description} 超时 ({timeout}秒)")
        return False
    except Exception as e:
        logger.error(f"❌ {description} 异常: {e}")
        return False

def create_small_batch_configs(start_from=30):
    """创建更小的批次配置（每批20行）"""
    batches = []
    current = start_from
    batch_num = 2
    
    while current < 172:
        end = min(current + 20, 172)
        batches.append({
            "name": f"batch{batch_num}",
            "start": current,
            "end": end
        })
        current = end
        batch_num += 1
    
    return batches

def process_remaining_batches(start_from=30):
    """处理剩余的批次"""
    
    # 检查API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请设置环境变量 OPENAI_API_KEY")
        return False
    
    # 使用现有的输出目录
    output_dir = "batch_results_20250524_224700"
    if not os.path.exists(output_dir):
        logger.error(f"输出目录不存在: {output_dir}")
        return False
    
    # 获取批次配置
    batches = create_small_batch_configs(start_from)
    
    logger.info("=" * 80)
    logger.info(f"处理剩余 {len(batches)} 个批次（每批20行）")
    logger.info("=" * 80)
    
    successful_batches = []
    failed_batches = []
    
    for i, batch_config in enumerate(batches, 1):
        batch_name = batch_config["name"]
        start_row = batch_config["start"]
        end_row = batch_config["end"]
        
        logger.info(f"\n📦 处理批次 {i}/{len(batches)}: {batch_name}")
        logger.info(f"行范围: {start_row+1}-{end_row}")
        
        # 步骤1: 创建批处理输入文件
        jsonl_file = os.path.join(output_dir, f"{batch_name}.jsonl")
        create_cmd = f"python create_safe_batch_input.py new_csv/content_CogAgent.csv {jsonl_file} --model gpt-4o-mini --start-row {start_row} --end-row {end_row}"
        
        if not run_command_with_timeout(create_cmd, f"创建 {batch_name} 输入文件", 120):
            failed_batches.append(batch_name)
            continue
        
        # 检查文件是否创建成功
        if not os.path.exists(jsonl_file):
            logger.error(f"❌ {batch_name} 输入文件未创建")
            failed_batches.append(batch_name)
            continue
        
        # 步骤2: 提交批处理（设置10分钟超时）
        process_cmd = f"python batch_processor.py {jsonl_file} --output-dir {output_dir} --check-interval 30"
        
        if run_command_with_timeout(process_cmd, f"处理 {batch_name} 批次", 600):
            successful_batches.append(batch_name)
            logger.info(f"✅ {batch_name} 批次完成")
        else:
            failed_batches.append(batch_name)
            logger.error(f"❌ {batch_name} 批次失败或超时")
        
        # 在批次之间添加更长的延迟
        if i < len(batches):
            logger.info("等待60秒后处理下一个批次...")
            time.sleep(60)
    
    # 显示最终结果
    logger.info("=" * 80)
    logger.info("剩余批次处理完成总结")
    logger.info("=" * 80)
    logger.info(f"成功批次 ({len(successful_batches)}): {', '.join(successful_batches)}")
    
    if failed_batches:
        logger.warning(f"失败批次 ({len(failed_batches)}): {', '.join(failed_batches)}")
        logger.info("可以手动重试失败的批次")
    
    return len(failed_batches) == 0

def main():
    import sys
    
    start_from = 30  # 从第30行开始（batch2）
    if len(sys.argv) > 1:
        try:
            start_from = int(sys.argv[1])
        except ValueError:
            logger.error("请提供有效的起始行号")
            return
    
    logger.info(f"🚀 从第{start_from+1}行开始处理剩余批次")
    
    success = process_remaining_batches(start_from)
    
    if success:
        logger.info("🎉 所有剩余批次处理成功！")
        logger.info("现在可以合并所有结果:")
        logger.info("python merge_all_results.py batch_results_20250524_224700 new_csv/content_CogAgent.csv final_output.csv")
    else:
        logger.warning("⚠️ 部分批次处理失败，请检查日志")

if __name__ == "__main__":
    main()
