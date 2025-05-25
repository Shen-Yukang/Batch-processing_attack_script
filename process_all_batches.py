#!/usr/bin/env python3
"""
自动化处理所有批次的脚本
"""

import os
import time
import logging
import subprocess
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command, description):
    """运行命令并记录结果"""
    logger.info(f"执行: {description}")
    logger.info(f"命令: {command}")
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"✅ {description} 成功")
            if result.stdout:
                logger.info(f"输出: {result.stdout.strip()}")
            return True
        else:
            logger.error(f"❌ {description} 失败")
            logger.error(f"错误: {result.stderr.strip()}")
            return False
            
    except Exception as e:
        logger.error(f"❌ {description} 异常: {e}")
        return False

def create_batch_configs():
    """定义批次配置"""
    return [
        {"name": "batch1", "start": 0, "end": 30},
        {"name": "batch2", "start": 30, "end": 60},
        {"name": "batch3", "start": 60, "end": 90},
        {"name": "batch4", "start": 90, "end": 120},
        {"name": "batch5", "start": 120, "end": 150},
        {"name": "batch6", "start": 150, "end": 172},
    ]

def process_all_batches():
    """处理所有批次"""
    
    # 检查API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请设置环境变量 OPENAI_API_KEY")
        return False
    
    # 检查输入文件
    input_csv = "new_csv/content_CogAgent.csv"
    if not os.path.exists(input_csv):
        logger.error(f"输入文件不存在: {input_csv}")
        return False
    
    # 创建输出目录
    output_dir = f"batch_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"创建输出目录: {output_dir}")
    
    # 获取批次配置
    batches = create_batch_configs()
    
    logger.info("=" * 80)
    logger.info(f"开始处理 {len(batches)} 个批次")
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
        create_cmd = f"python create_safe_batch_input.py {input_csv} {jsonl_file} --model gpt-4o-mini --start-row {start_row} --end-row {end_row}"
        
        if not run_command(create_cmd, f"创建 {batch_name} 输入文件"):
            failed_batches.append(batch_name)
            continue
        
        # 检查文件是否创建成功
        if not os.path.exists(jsonl_file):
            logger.error(f"❌ {batch_name} 输入文件未创建")
            failed_batches.append(batch_name)
            continue
        
        # 步骤2: 提交批处理
        process_cmd = f"python batch_processor.py {jsonl_file} --output-dir {output_dir}"
        
        if run_command(process_cmd, f"处理 {batch_name} 批次"):
            successful_batches.append(batch_name)
            logger.info(f"✅ {batch_name} 批次完成")
        else:
            failed_batches.append(batch_name)
            logger.error(f"❌ {batch_name} 批次失败")
        
        # 在批次之间添加延迟，避免API限制
        if i < len(batches):
            logger.info("等待30秒后处理下一个批次...")
            time.sleep(30)
    
    # 显示最终结果
    logger.info("=" * 80)
    logger.info("批处理完成总结")
    logger.info("=" * 80)
    logger.info(f"成功批次 ({len(successful_batches)}): {', '.join(successful_batches)}")
    
    if failed_batches:
        logger.warning(f"失败批次 ({len(failed_batches)}): {', '.join(failed_batches)}")
        logger.info("可以手动重试失败的批次")
    
    # 如果有成功的批次，提供合并指令
    if successful_batches:
        logger.info("\n📋 下一步: 合并结果")
        logger.info("等所有批次完成后，运行以下命令合并结果:")
        logger.info(f"python merge_all_results.py {output_dir} {input_csv} final_output.csv")
    
    return len(failed_batches) == 0

def main():
    logger.info("🚀 开始自动化批处理工作流程")
    
    success = process_all_batches()
    
    if success:
        logger.info("🎉 所有批次处理成功！")
    else:
        logger.warning("⚠️ 部分批次处理失败，请检查日志")

if __name__ == "__main__":
    main()
