#!/usr/bin/env python3
"""
合并所有批处理结果的脚本
"""

import os
import sys
import glob
import pandas as pd
import json
import logging
from typing import Dict

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_batch_results(results_file: str) -> Dict[str, str]:
    """解析单个批处理结果文件"""
    results = {}
    
    try:
        with open(results_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    result = json.loads(line.strip())
                    
                    custom_id = result.get('custom_id')
                    if not custom_id:
                        continue
                    
                    # 提取响应内容
                    response = result.get('response')
                    if not response:
                        results[custom_id] = "错误：无响应数据"
                        continue
                    
                    body = response.get('body')
                    if not body:
                        results[custom_id] = "错误：无响应体"
                        continue
                    
                    choices = body.get('choices')
                    if not choices or len(choices) == 0:
                        results[custom_id] = "错误：无选择项"
                        continue
                    
                    message = choices[0].get('message')
                    if not message:
                        results[custom_id] = "错误：无消息内容"
                        continue
                    
                    content = message.get('content')
                    if content is None:
                        results[custom_id] = "错误：消息内容为空"
                        continue
                    
                    results[custom_id] = content
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"文件 {results_file} 第{line_num}行JSON解析失败: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"文件 {results_file} 第{line_num}行处理失败: {e}")
                    continue
        
        logger.info(f"从 {os.path.basename(results_file)} 解析了 {len(results)} 个结果")
        return results
        
    except Exception as e:
        logger.error(f"读取结果文件失败 {results_file}: {e}")
        return {}

def merge_all_results(results_dir: str, original_csv: str, output_csv: str):
    """合并所有批处理结果"""
    
    # 读取原始CSV文件
    try:
        df = pd.read_csv(original_csv)
        logger.info(f"成功读取原始CSV文件，共{len(df)}行")
    except Exception as e:
        logger.error(f"读取原始CSV文件失败: {e}")
        return False
    
    # 添加ChatGPT Response列（如果不存在）
    if 'ChatGPT Response' not in df.columns:
        df['ChatGPT Response'] = ''
    
    # 查找所有结果文件
    result_files = glob.glob(os.path.join(results_dir, "batch_results_*.jsonl"))
    
    if not result_files:
        logger.error(f"在 {results_dir} 中没有找到结果文件")
        return False
    
    logger.info(f"找到 {len(result_files)} 个结果文件:")
    for file in result_files:
        logger.info(f"  - {os.path.basename(file)}")
    
    # 合并所有结果
    all_results = {}
    total_results = 0
    
    for result_file in result_files:
        file_results = parse_batch_results(result_file)
        all_results.update(file_results)
        total_results += len(file_results)
    
    logger.info(f"总共解析了 {total_results} 个结果")
    
    if not all_results:
        logger.error("没有有效的批处理结果")
        return False
    
    # 合并结果到DataFrame
    merged_count = 0
    missing_count = 0
    
    for idx in range(len(df)):
        custom_id = f"row_{idx}"
        
        if custom_id in all_results:
            df.at[idx, 'ChatGPT Response'] = all_results[custom_id]
            merged_count += 1
        else:
            missing_count += 1
    
    # 保存结果
    try:
        df.to_csv(output_csv, index=False, encoding='utf-8')
        logger.info(f"结果已保存到: {output_csv}")
        
        # 显示统计信息
        logger.info("=" * 60)
        logger.info("合并统计")
        logger.info("=" * 60)
        logger.info(f"总行数: {len(df)}")
        logger.info(f"成功合并: {merged_count} 行")
        logger.info(f"缺少结果: {missing_count} 行")
        logger.info(f"完成率: {(merged_count/len(df)*100):.1f}%")
        
        # 显示缺少结果的行
        if missing_count > 0:
            missing_rows = []
            for idx in range(len(df)):
                custom_id = f"row_{idx}"
                if custom_id not in all_results:
                    missing_rows.append(idx + 1)
            
            logger.warning(f"缺少结果的行号: {missing_rows[:10]}")
            if len(missing_rows) > 10:
                logger.warning(f"... 还有 {len(missing_rows)-10} 行")
        
        return True
        
    except Exception as e:
        logger.error(f"保存CSV文件失败: {e}")
        return False

def analyze_results(output_csv: str):
    """分析合并后的结果"""
    try:
        df = pd.read_csv(output_csv)
        
        # 统计响应情况
        total_rows = len(df)
        has_response = df['ChatGPT Response'].notna() & (df['ChatGPT Response'] != '')
        response_count = has_response.sum()
        error_count = df['ChatGPT Response'].str.startswith('错误：', na=False).sum()
        
        logger.info("=" * 60)
        logger.info("结果分析")
        logger.info("=" * 60)
        logger.info(f"总行数: {total_rows}")
        logger.info(f"有响应: {response_count}")
        logger.info(f"错误响应: {error_count}")
        logger.info(f"成功响应: {response_count - error_count}")
        logger.info(f"成功率: {((response_count - error_count)/total_rows*100):.1f}%")
        
        # 显示响应长度统计
        valid_responses = df[has_response & ~df['ChatGPT Response'].str.startswith('错误：', na=False)]
        if len(valid_responses) > 0:
            response_lengths = valid_responses['ChatGPT Response'].str.len()
            logger.info(f"响应长度统计:")
            logger.info(f"  平均长度: {response_lengths.mean():.0f} 字符")
            logger.info(f"  最短响应: {response_lengths.min()} 字符")
            logger.info(f"  最长响应: {response_lengths.max()} 字符")
        
    except Exception as e:
        logger.error(f"分析结果失败: {e}")

def main():
    if len(sys.argv) != 4:
        print("用法: python merge_all_results.py RESULTS_DIR ORIGINAL_CSV OUTPUT_CSV")
        print("例如: python merge_all_results.py batch_results_20250524_142000 new_csv/content_CogAgent.csv final_output.csv")
        return
    
    results_dir = sys.argv[1]
    original_csv = sys.argv[2]
    output_csv = sys.argv[3]
    
    # 检查输入
    if not os.path.exists(results_dir):
        logger.error(f"结果目录不存在: {results_dir}")
        return
    
    if not os.path.exists(original_csv):
        logger.error(f"原始CSV文件不存在: {original_csv}")
        return
    
    # 合并结果
    logger.info("🔄 开始合并所有批处理结果...")
    
    success = merge_all_results(results_dir, original_csv, output_csv)
    
    if success:
        logger.info("✅ 结果合并成功！")
        analyze_results(output_csv)
    else:
        logger.error("❌ 结果合并失败")

if __name__ == "__main__":
    main()
