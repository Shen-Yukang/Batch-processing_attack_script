#!/usr/bin/env python3
"""
完整的批处理工作流程脚本
自动化处理从CSV到批处理结果的完整流程
"""

import os
import argparse
import logging
from datetime import datetime
from create_batch_input import create_batch_input_file
from batch_processor import BatchProcessor
from process_batch_results import merge_results_to_csv

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_batch_workflow(input_csv: str, output_csv: str, api_key: str,
                      model: str = "gpt-4o", start_row: int = 0, end_row: int = None,
                      completion_window: str = "24h", check_interval: int = 60,
                      keep_temp_files: bool = False):
    """
    运行完整的批处理工作流程
    
    Args:
        input_csv: 输入CSV文件路径
        output_csv: 输出CSV文件路径
        api_key: OpenAI API密钥
        model: 使用的模型
        start_row: 开始处理的行号
        end_row: 结束处理的行号
        completion_window: 完成时间窗口
        check_interval: 状态检查间隔
        keep_temp_files: 是否保留临时文件
    
    Returns:
        bool: 是否成功完成
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_dir = f"batch_temp_{timestamp}"
    
    try:
        # 创建临时目录
        os.makedirs(temp_dir, exist_ok=True)
        logger.info(f"创建临时目录: {temp_dir}")
        
        # 步骤1: 创建批处理输入文件
        logger.info("=" * 60)
        logger.info("步骤1: 创建批处理输入文件")
        logger.info("=" * 60)
        
        jsonl_file = os.path.join(temp_dir, "batch_input.jsonl")
        create_batch_input_file(
            input_csv=input_csv,
            output_jsonl=jsonl_file,
            model=model,
            start_row=start_row,
            end_row=end_row
        )
        
        if not os.path.exists(jsonl_file):
            logger.error("批处理输入文件创建失败")
            return False
        
        # 步骤2: 处理批处理
        logger.info("=" * 60)
        logger.info("步骤2: 处理批处理")
        logger.info("=" * 60)
        
        processor = BatchProcessor(api_key)
        result_file = processor.process_batch(
            input_file=jsonl_file,
            output_dir=temp_dir,
            completion_window=completion_window,
            check_interval=check_interval
        )
        
        if not result_file:
            logger.error("批处理失败")
            return False
        
        # 步骤3: 合并结果到CSV
        logger.info("=" * 60)
        logger.info("步骤3: 合并结果到CSV")
        logger.info("=" * 60)
        
        merge_results_to_csv(
            original_csv=input_csv,
            results_file=result_file,
            output_csv=output_csv,
            start_row=start_row,
            end_row=end_row
        )
        
        if not os.path.exists(output_csv):
            logger.error("结果CSV文件创建失败")
            return False
        
        logger.info("=" * 60)
        logger.info("批处理工作流程完成！")
        logger.info("=" * 60)
        logger.info(f"输入文件: {input_csv}")
        logger.info(f"输出文件: {output_csv}")
        
        return True
        
    except Exception as e:
        logger.error(f"批处理工作流程失败: {e}")
        return False
    
    finally:
        # 清理临时文件
        if not keep_temp_files:
            try:
                import shutil
                shutil.rmtree(temp_dir)
                logger.info(f"已清理临时目录: {temp_dir}")
            except Exception as e:
                logger.warning(f"清理临时目录失败: {e}")
        else:
            logger.info(f"临时文件保留在: {temp_dir}")

def estimate_cost(input_csv: str, start_row: int = 0, end_row: int = None):
    """
    估算批处理成本
    
    Args:
        input_csv: 输入CSV文件路径
        start_row: 开始处理的行号
        end_row: 结束处理的行号
    """
    try:
        import pandas as pd
        df = pd.read_csv(input_csv)
        
        start_idx = max(0, start_row)
        end_idx = min(len(df), end_row) if end_row is not None else len(df)
        
        total_requests = end_idx - start_idx
        
        # GPT-4o vision pricing (approximate)
        # Input: $2.50 per 1M tokens, Output: $10.00 per 1M tokens
        # Batch API: 50% discount
        # Estimate ~1000 input tokens per image + prompt, ~200 output tokens
        
        estimated_input_tokens = total_requests * 1000
        estimated_output_tokens = total_requests * 200
        
        # Regular pricing
        regular_input_cost = (estimated_input_tokens / 1_000_000) * 2.50
        regular_output_cost = (estimated_output_tokens / 1_000_000) * 10.00
        regular_total = regular_input_cost + regular_output_cost
        
        # Batch pricing (50% discount)
        batch_total = regular_total * 0.5
        savings = regular_total - batch_total
        
        logger.info("=" * 60)
        logger.info("成本估算")
        logger.info("=" * 60)
        logger.info(f"处理请求数: {total_requests}")
        logger.info(f"估算输入tokens: {estimated_input_tokens:,}")
        logger.info(f"估算输出tokens: {estimated_output_tokens:,}")
        logger.info(f"常规API成本: ${regular_total:.2f}")
        logger.info(f"批处理API成本: ${batch_total:.2f}")
        logger.info(f"节省成本: ${savings:.2f} (50%)")
        logger.info("=" * 60)
        logger.info("注意：这只是粗略估算，实际成本可能有所不同")
        
    except Exception as e:
        logger.error(f"成本估算失败: {e}")

def main():
    parser = argparse.ArgumentParser(description='完整的批处理工作流程')
    parser.add_argument('input_csv', help='输入CSV文件路径')
    parser.add_argument('output_csv', help='输出CSV文件路径')
    parser.add_argument('--api-key', help='OpenAI API密钥（也可通过环境变量OPENAI_API_KEY设置）')
    parser.add_argument('--model', default='gpt-4o', help='使用的模型，默认gpt-4o')
    parser.add_argument('--start-row', type=int, default=0, help='开始处理的行号（0开始），默认0')
    parser.add_argument('--end-row', type=int, help='结束处理的行号（不包含），默认处理到最后')
    parser.add_argument('--completion-window', default='24h', help='完成时间窗口，默认24h')
    parser.add_argument('--check-interval', type=int, default=60, help='状态检查间隔（秒），默认60')
    parser.add_argument('--keep-temp-files', action='store_true', help='保留临时文件')
    parser.add_argument('--estimate-cost', action='store_true', help='仅估算成本，不执行处理')
    
    args = parser.parse_args()
    
    # 检查输入文件是否存在
    if not os.path.exists(args.input_csv):
        logger.error(f"输入文件不存在: {args.input_csv}")
        return
    
    # 如果只是估算成本
    if args.estimate_cost:
        estimate_cost(args.input_csv, args.start_row, args.end_row)
        return
    
    # 获取API密钥
    api_key = args.api_key or os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请提供OpenAI API密钥，通过--api-key参数或OPENAI_API_KEY环境变量")
        return
    
    # 显示成本估算
    estimate_cost(args.input_csv, args.start_row, args.end_row)
    
    # 询问用户确认
    response = input("\n是否继续执行批处理？(y/N): ")
    if response.lower() != 'y':
        logger.info("取消批处理")
        return
    
    # 运行批处理工作流程
    success = run_batch_workflow(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        api_key=api_key,
        model=args.model,
        start_row=args.start_row,
        end_row=args.end_row,
        completion_window=args.completion_window,
        check_interval=args.check_interval,
        keep_temp_files=args.keep_temp_files
    )
    
    if success:
        logger.info("批处理工作流程成功完成！")
    else:
        logger.error("批处理工作流程失败")

if __name__ == "__main__":
    main()
