#!/usr/bin/env python3
"""
完整的批处理执行脚本 - 从零开始重新执行所有批处理任务
包含完整的日志记录和错误处理
"""

import os
import sys
import json
import time
import shutil
import logging
import argparse
import pandas as pd
from datetime import datetime
from typing import List, Optional
from robust_batch_processor import RobustBatchProcessor

def setup_logging(log_dir: str) -> logging.Logger:
    """设置详细的日志记录"""
    os.makedirs(log_dir, exist_ok=True)
    
    # 创建日志文件名（包含时间戳）
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f"batch_execution_{timestamp}.log")
    
    # 配置日志格式
    log_format = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    # 创建logger
    logger = logging.getLogger('BatchExecution')
    logger.setLevel(logging.DEBUG)
    
    # 清除现有的handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 文件handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # 控制台handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    logger.info(f"日志记录已启动，日志文件: {log_file}")
    return logger

def find_csv_file(logger: logging.Logger) -> Optional[str]:
    """查找正确的CSV文件"""
    possible_files = [
        "_csvs/content_MMSafeBench_cleaned.csv",
        "_csvs/content_FigStep.csv", 
        "_csvs/content_Jailbreak28k.csv"
    ]
    
    for file_path in possible_files:
        if os.path.exists(file_path):
            logger.info(f"找到CSV文件: {file_path}")
            
            # 检查文件内容
            try:
                df = pd.read_csv(file_path)
                logger.info(f"CSV文件 {file_path} 包含 {len(df)} 行数据")
                logger.info(f"列名: {list(df.columns)}")
                
                # 检查是否包含必要的列
                required_columns = ['Image Path', 'Content of P*']
                if all(col in df.columns for col in required_columns):
                    logger.info(f"CSV文件 {file_path} 包含所需的列，将使用此文件")
                    return file_path
                else:
                    logger.warning(f"CSV文件 {file_path} 缺少必要的列: {required_columns}")
            except Exception as e:
                logger.error(f"读取CSV文件 {file_path} 时出错: {e}")
                continue
    
    logger.error("未找到合适的CSV文件")
    return None

def cleanup_old_results(logger: logging.Logger, keep_latest: bool = True):
    """清理旧的批处理结果"""
    import glob
    
    batch_dirs = glob.glob("batch_results_*")
    if not batch_dirs:
        logger.info("没有找到旧的批处理目录")
        return
    
    # 按修改时间排序
    batch_dirs.sort(key=os.path.getmtime, reverse=True)
    
    if keep_latest and len(batch_dirs) > 0:
        logger.info(f"保留最新的批处理目录: {batch_dirs[0]}")
        dirs_to_remove = batch_dirs[1:]
    else:
        dirs_to_remove = batch_dirs
    
    for dir_path in dirs_to_remove:
        try:
            logger.info(f"删除旧的批处理目录: {dir_path}")
            shutil.rmtree(dir_path)
        except Exception as e:
            logger.error(f"删除目录 {dir_path} 失败: {e}")

def validate_environment(logger: logging.Logger) -> bool:
    """验证执行环境"""
    logger.info("验证执行环境...")
    
    # 检查必要的文件
    required_files = [
        'robust_batch_processor.py',
        'create_safe_batch_input.py',
        'batch_processor.py',
        'cost_tracker.py'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        logger.error(f"缺少必要的文件: {missing_files}")
        return False
    
    # 检查图片目录
    if not os.path.exists('image'):
        logger.error("图片目录 'image' 不存在")
        return False
    
    # 检查Python模块
    try:
        import openai
        logger.info("OpenAI模块检查通过")
    except ImportError:
        logger.error("OpenAI模块未安装，请运行: pip install openai")
        return False
    
    logger.info("环境验证通过")
    return True

def execute_batch_processing(csv_file: str, logger: logging.Logger, 
                           start_row: int = 0, end_row: Optional[int] = None,
                           batch_size: int = 20, model: str = "gpt-4o-mini") -> bool:
    """执行完整的批处理流程"""
    
    # 创建新的输出目录
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"batch_results_{timestamp}"
    
    logger.info(f"创建新的批处理目录: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取CSV文件行数
    if end_row is None:
        try:
            df = pd.read_csv(csv_file)
            end_row = len(df)
            logger.info(f"CSV文件包含 {end_row} 行数据")
        except Exception as e:
            logger.error(f"读取CSV文件失败: {e}")
            return False
    
    # 创建批处理器
    logger.info(f"创建批处理器 - 输出目录: {output_dir}, 批次大小: {batch_size}, 模型: {model}")
    processor = RobustBatchProcessor(
        output_dir=output_dir,
        batch_size=batch_size,
        input_csv=csv_file,
        model=model
    )
    
    # 创建批处理任务
    logger.info(f"创建批处理任务 - 起始行: {start_row}, 结束行: {end_row}")
    processor.create_jobs(start_row, end_row)
    
    # 记录任务信息
    logger.info(f"总共创建了 {len(processor.jobs)} 个批处理任务")
    for i, job in enumerate(processor.jobs, 1):
        logger.debug(f"任务 {i}: {job.name} (行 {job.start_row+1}-{job.end_row})")
    
    # 开始处理
    logger.info("开始执行批处理任务...")
    start_time = datetime.now()
    
    try:
        processor.process_all_jobs()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"批处理执行完成，总耗时: {duration}")
        
        # 打印最终总结
        processor.print_summary()
        
        # 检查是否有失败的任务
        failed_jobs = [job for job in processor.jobs if job.status in ["failed", "timeout"]]
        if failed_jobs:
            logger.warning(f"有 {len(failed_jobs)} 个任务失败")
            for job in failed_jobs:
                logger.warning(f"失败任务: {job.name} - {job.error_message}")
            
            # 询问是否重试
            if input(f"\n发现 {len(failed_jobs)} 个失败任务，是否重试？(y/N): ").lower() == 'y':
                logger.info("开始重试失败的任务...")
                processor.retry_failed_jobs()
        
        return True
        
    except Exception as e:
        logger.error(f"批处理执行过程中出现异常: {e}", exc_info=True)
        return False

def main():
    parser = argparse.ArgumentParser(description="完整的批处理执行脚本")
    parser.add_argument("--csv-file", help="指定CSV文件路径")
    parser.add_argument("--start-row", type=int, default=0, help="起始行")
    parser.add_argument("--end-row", type=int, help="结束行")
    parser.add_argument("--batch-size", type=int, default=20, help="批次大小")
    parser.add_argument("--model", default="gpt-4o-mini", help="使用的模型")
    parser.add_argument("--cleanup", action="store_true", help="清理旧的批处理结果")
    parser.add_argument("--keep-latest", action="store_true", help="保留最新的批处理目录")
    parser.add_argument("--log-dir", default="logs", help="日志目录")
    args = parser.parse_args()
    
    # 设置日志
    logger = setup_logging(args.log_dir)
    
    try:
        logger.info("="*80)
        logger.info("开始完整的批处理执行流程")
        logger.info("="*80)
        
        # 验证环境
        if not validate_environment(logger):
            logger.error("环境验证失败，退出")
            return 1
        
        # 清理旧结果
        if args.cleanup:
            cleanup_old_results(logger, args.keep_latest)
        
        # 查找CSV文件
        csv_file = args.csv_file
        if not csv_file:
            csv_file = find_csv_file(logger)
            if not csv_file:
                logger.error("无法找到合适的CSV文件")
                return 1
        elif not os.path.exists(csv_file):
            logger.error(f"指定的CSV文件不存在: {csv_file}")
            return 1
        
        # 执行批处理
        success = execute_batch_processing(
            csv_file=csv_file,
            logger=logger,
            start_row=args.start_row,
            end_row=args.end_row,
            batch_size=args.batch_size,
            model=args.model
        )
        
        if success:
            logger.info("批处理执行成功完成")
            return 0
        else:
            logger.error("批处理执行失败")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("用户中断执行")
        return 1
    except Exception as e:
        logger.error(f"程序执行过程中出现未预期的错误: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
