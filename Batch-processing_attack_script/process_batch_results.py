#!/usr/bin/env python3
"""
处理OpenAI Batch API结果的脚本
将JSONL结果文件合并回CSV文件
"""

import os
import json
import pandas as pd
import argparse
import logging
from typing import Dict, Optional

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_batch_results(results_file: str) -> Dict[str, str]:
    """
    解析批处理结果文件
    
    Args:
        results_file: 结果JSONL文件路径
        
    Returns:
        字典，键为custom_id，值为响应内容
    """
    results = {}
    
    try:
        with open(results_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    result = json.loads(line.strip())
                    
                    custom_id = result.get('custom_id')
                    if not custom_id:
                        logger.warning(f"第{line_num}行缺少custom_id")
                        continue
                    
                    # 提取响应内容
                    response = result.get('response')
                    if not response:
                        logger.warning(f"第{line_num}行缺少response")
                        results[custom_id] = "错误：无响应数据"
                        continue
                    
                    body = response.get('body')
                    if not body:
                        logger.warning(f"第{line_num}行缺少response body")
                        results[custom_id] = "错误：无响应体"
                        continue
                    
                    choices = body.get('choices')
                    if not choices or len(choices) == 0:
                        logger.warning(f"第{line_num}行缺少choices")
                        results[custom_id] = "错误：无选择项"
                        continue
                    
                    message = choices[0].get('message')
                    if not message:
                        logger.warning(f"第{line_num}行缺少message")
                        results[custom_id] = "错误：无消息内容"
                        continue
                    
                    content = message.get('content')
                    if content is None:
                        logger.warning(f"第{line_num}行消息内容为空")
                        results[custom_id] = "错误：消息内容为空"
                        continue
                    
                    results[custom_id] = content
                    
                except json.JSONDecodeError as e:
                    logger.error(f"第{line_num}行JSON解析失败: {e}")
                    continue
                except Exception as e:
                    logger.error(f"第{line_num}行处理失败: {e}")
                    continue
        
        logger.info(f"成功解析 {len(results)} 个结果")
        return results
        
    except Exception as e:
        logger.error(f"读取结果文件失败: {e}")
        return {}

def merge_results_to_csv(original_csv: str, results_file: str, output_csv: str, 
                        start_row: int = 0, end_row: Optional[int] = None):
    """
    将批处理结果合并到CSV文件
    
    Args:
        original_csv: 原始CSV文件路径
        results_file: 批处理结果JSONL文件路径
        output_csv: 输出CSV文件路径
        start_row: 开始处理的行号（0开始）
        end_row: 结束处理的行号（不包含），None表示处理到最后
    """
    # 读取原始CSV文件
    try:
        df = pd.read_csv(original_csv)
        logger.info(f"成功读取原始CSV文件，共{len(df)}行")
    except Exception as e:
        logger.error(f"读取原始CSV文件失败: {e}")
        return
    
    # 检查必要的列是否存在
    required_columns = ['Image Path', 'Content of P*']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"CSV文件缺少必要的列: {missing_columns}")
        return
    
    # 添加ChatGPT Response列（如果不存在）
    if 'ChatGPT Response' not in df.columns:
        df['ChatGPT Response'] = ''
    
    # 解析批处理结果
    results = parse_batch_results(results_file)
    if not results:
        logger.error("没有有效的批处理结果")
        return
    
    # 确定处理范围
    start_idx = max(0, start_row)
    end_idx = min(len(df), end_row) if end_row is not None else len(df)
    
    logger.info(f"合并范围: 第{start_idx+1}行到第{end_idx}行")
    
    # 合并结果
    merged_count = 0
    missing_count = 0
    
    for idx in range(start_idx, end_idx):
        custom_id = f"row_{idx}"
        
        if custom_id in results:
            df.at[idx, 'ChatGPT Response'] = results[custom_id]
            merged_count += 1
            logger.info(f"已合并第 {idx+1} 行")
        else:
            logger.warning(f"第 {idx+1} 行缺少批处理结果: {custom_id}")
            missing_count += 1
    
    # 保存结果
    try:
        df.to_csv(output_csv, index=False, encoding='utf-8')
        logger.info(f"结果已保存到: {output_csv}")
        logger.info(f"成功合并: {merged_count} 行")
        logger.info(f"缺少结果: {missing_count} 行")
        
    except Exception as e:
        logger.error(f"保存CSV文件失败: {e}")

def analyze_batch_results(results_file: str):
    """
    分析批处理结果文件
    
    Args:
        results_file: 结果JSONL文件路径
    """
    logger.info("分析批处理结果...")
    
    results = parse_batch_results(results_file)
    
    if not results:
        logger.error("没有有效的结果可分析")
        return
    
    # 统计信息
    total_results = len(results)
    error_results = sum(1 for content in results.values() if content.startswith("错误："))
    success_results = total_results - error_results
    
    logger.info(f"总结果数: {total_results}")
    logger.info(f"成功结果: {success_results}")
    logger.info(f"错误结果: {error_results}")
    
    # 显示错误结果
    if error_results > 0:
        logger.info("错误结果详情:")
        for custom_id, content in results.items():
            if content.startswith("错误："):
                logger.info(f"  {custom_id}: {content}")
    
    # 显示成功结果示例
    if success_results > 0:
        logger.info("成功结果示例:")
        count = 0
        for custom_id, content in results.items():
            if not content.startswith("错误：") and count < 3:
                preview = content[:100] + "..." if len(content) > 100 else content
                logger.info(f"  {custom_id}: {preview}")
                count += 1

def main():
    parser = argparse.ArgumentParser(description='处理OpenAI Batch API结果')
    parser.add_argument('command', choices=['merge', 'analyze'], help='操作类型：merge（合并到CSV）或analyze（分析结果）')
    parser.add_argument('results_file', help='批处理结果JSONL文件路径')
    parser.add_argument('--original-csv', help='原始CSV文件路径（merge命令必需）')
    parser.add_argument('--output-csv', help='输出CSV文件路径（merge命令必需）')
    parser.add_argument('--start-row', type=int, default=0, help='开始处理的行号（0开始），默认0')
    parser.add_argument('--end-row', type=int, help='结束处理的行号（不包含），默认处理到最后')
    
    args = parser.parse_args()
    
    # 检查结果文件是否存在
    if not os.path.exists(args.results_file):
        logger.error(f"结果文件不存在: {args.results_file}")
        return
    
    if args.command == 'analyze':
        analyze_batch_results(args.results_file)
    
    elif args.command == 'merge':
        if not args.original_csv or not args.output_csv:
            logger.error("merge命令需要--original-csv和--output-csv参数")
            return
        
        if not os.path.exists(args.original_csv):
            logger.error(f"原始CSV文件不存在: {args.original_csv}")
            return
        
        merge_results_to_csv(
            original_csv=args.original_csv,
            results_file=args.results_file,
            output_csv=args.output_csv,
            start_row=args.start_row,
            end_row=args.end_row
        )

if __name__ == "__main__":
    main()
