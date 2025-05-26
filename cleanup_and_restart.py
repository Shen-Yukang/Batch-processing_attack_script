#!/usr/bin/env python3
"""
清理脚本 - 删除所有旧的批处理结果，准备重新执行
"""

import os
import glob
import shutil
import logging
from datetime import datetime

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'cleanup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    return logging.getLogger(__name__)

def cleanup_batch_results(logger, keep_latest=False):
    """清理批处理结果目录"""
    batch_dirs = glob.glob("batch_results_*")
    
    if not batch_dirs:
        logger.info("没有找到批处理结果目录")
        return
    
    logger.info(f"找到 {len(batch_dirs)} 个批处理目录")
    
    # 按修改时间排序
    batch_dirs.sort(key=os.path.getmtime, reverse=True)
    
    if keep_latest and len(batch_dirs) > 0:
        logger.info(f"保留最新目录: {batch_dirs[0]}")
        dirs_to_remove = batch_dirs[1:]
    else:
        dirs_to_remove = batch_dirs
    
    for dir_path in dirs_to_remove:
        try:
            logger.info(f"删除目录: {dir_path}")
            shutil.rmtree(dir_path)
            logger.info(f"✅ 成功删除: {dir_path}")
        except Exception as e:
            logger.error(f"❌ 删除失败 {dir_path}: {e}")

def cleanup_temp_files(logger):
    """清理临时文件"""
    temp_patterns = [
        "*.jsonl",
        "test_*.csv",
        "quick_test_result.csv",
        "current_status.csv",
        "__pycache__"
    ]
    
    for pattern in temp_patterns:
        files = glob.glob(pattern)
        for file_path in files:
            try:
                if os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    logger.info(f"删除目录: {file_path}")
                else:
                    os.remove(file_path)
                    logger.info(f"删除文件: {file_path}")
            except Exception as e:
                logger.warning(f"删除 {file_path} 失败: {e}")

def backup_important_files(logger):
    """备份重要文件"""
    backup_dir = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.makedirs(backup_dir, exist_ok=True)
    
    important_files = [
        "missing_rows.txt",
        "final_output.csv",
        "complete_final_output.csv"
    ]
    
    for file_path in important_files:
        if os.path.exists(file_path):
            try:
                shutil.copy2(file_path, backup_dir)
                logger.info(f"备份文件: {file_path} -> {backup_dir}/")
            except Exception as e:
                logger.warning(f"备份 {file_path} 失败: {e}")
    
    if os.listdir(backup_dir):
        logger.info(f"备份目录创建: {backup_dir}")
    else:
        os.rmdir(backup_dir)
        logger.info("没有文件需要备份")

def show_current_status(logger):
    """显示当前状态"""
    logger.info("="*60)
    logger.info("当前项目状态")
    logger.info("="*60)
    
    # CSV文件
    csv_files = glob.glob("_csvs/*.csv")
    logger.info(f"CSV文件: {len(csv_files)} 个")
    for csv_file in csv_files:
        try:
            import pandas as pd
            df = pd.read_csv(csv_file)
            logger.info(f"  {csv_file}: {len(df)} 行")
        except:
            logger.info(f"  {csv_file}: 无法读取")
    
    # 批处理目录
    batch_dirs = glob.glob("batch_results_*")
    logger.info(f"批处理目录: {len(batch_dirs)} 个")
    for batch_dir in batch_dirs:
        size = sum(os.path.getsize(os.path.join(batch_dir, f)) 
                  for f in os.listdir(batch_dir) 
                  if os.path.isfile(os.path.join(batch_dir, f)))
        logger.info(f"  {batch_dir}: {size/1024/1024:.1f} MB")
    
    # 图片文件
    if os.path.exists("image"):
        image_count = len([f for f in os.listdir("image") 
                          if f.lower().endswith(('.jpg', '.jpeg', '.png'))])
        logger.info(f"图片文件: {image_count} 个")
    
    # 重要文件
    important_files = ["missing_rows.txt", "final_output.csv", "complete_final_output.csv"]
    for file_path in important_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            logger.info(f"重要文件: {file_path} ({size/1024:.1f} KB)")

def main():
    logger = setup_logging()
    
    logger.info("开始清理和重启准备")
    
    # 显示当前状态
    show_current_status(logger)
    
    # 询问用户确认
    print("\n清理选项:")
    print("1. 删除所有批处理结果目录")
    print("2. 删除所有批处理结果目录（保留最新的）")
    print("3. 只清理临时文件")
    print("4. 完全清理（批处理结果 + 临时文件）")
    print("5. 取消")
    
    choice = input("\n请选择 (1-5): ").strip()
    
    if choice == "5":
        logger.info("用户取消清理")
        return
    
    # 备份重要文件
    backup_important_files(logger)
    
    if choice in ["1", "2", "4"]:
        keep_latest = (choice == "2")
        cleanup_batch_results(logger, keep_latest)
    
    if choice in ["3", "4"]:
        cleanup_temp_files(logger)
    
    logger.info("清理完成")
    
    # 显示清理后状态
    logger.info("\n清理后状态:")
    show_current_status(logger)
    
    print("\n下一步:")
    print("运行以下命令开始重新执行:")
    print("python complete_batch_execution.py --csv-file _csvs/content_MMSafeBench_cleaned.csv")

if __name__ == "__main__":
    main()
