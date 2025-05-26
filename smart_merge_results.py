#!/usr/bin/env python3
"""
智能合并批处理结果脚本
自动检测所有结果文件并合并为最终输出
"""

import os
import json
import pandas as pd
import logging
from datetime import datetime
import glob
import argparse

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_result_files(batch_dir):
    """查找所有批处理结果文件"""
    logger.info(f"🔍 扫描批处理目录: {batch_dir}")
    
    # 查找所有结果文件
    result_pattern = os.path.join(batch_dir, "batch_results_*.jsonl")
    result_files = glob.glob(result_pattern)
    
    logger.info(f"📄 找到 {len(result_files)} 个结果文件")
    
    # 按文件名排序
    result_files.sort()
    
    for i, file in enumerate(result_files, 1):
        file_size = os.path.getsize(file) / 1024  # KB
        logger.debug(f"  {i:2d}. {os.path.basename(file)} ({file_size:.1f} KB)")
    
    return result_files

def parse_result_file(file_path):
    """解析单个结果文件"""
    results = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    
                    # 提取关键信息
                    custom_id = data.get('custom_id', '')
                    response = data.get('response', {})
                    
                    if response and response.get('body'):
                        choices = response['body'].get('choices', [])
                        if choices:
                            content = choices[0].get('message', {}).get('content', '')
                            
                            # 提取行号
                            row_num = None
                            if custom_id.startswith('request-'):
                                try:
                                    row_num = int(custom_id.split('-')[1])
                                except:
                                    pass
                            
                            results.append({
                                'row_number': row_num,
                                'custom_id': custom_id,
                                'response_content': content,
                                'file_source': os.path.basename(file_path)
                            })
                
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️  {os.path.basename(file_path)} 第{line_num}行JSON解析失败: {e}")
                    continue
    
    except Exception as e:
        logger.error(f"❌ 读取文件失败 {file_path}: {e}")
        return []
    
    logger.debug(f"📊 {os.path.basename(file_path)}: 解析出 {len(results)} 条结果")
    return results

def merge_all_results(batch_dir, csv_file, output_file):
    """合并所有结果"""
    logger.info("🚀 开始智能合并批处理结果")
    logger.info(f"📁 批处理目录: {batch_dir}")
    logger.info(f"📊 原始CSV: {csv_file}")
    logger.info(f"📄 输出文件: {output_file}")
    
    # 1. 读取原始CSV
    logger.info("📖 读取原始CSV文件...")
    try:
        df_original = pd.read_csv(csv_file)
        total_rows = len(df_original)
        logger.info(f"✅ 原始CSV包含 {total_rows} 行数据")
    except Exception as e:
        logger.error(f"❌ 读取CSV文件失败: {e}")
        return False
    
    # 2. 查找所有结果文件
    result_files = find_result_files(batch_dir)
    if not result_files:
        logger.error("❌ 未找到任何结果文件")
        return False
    
    # 3. 解析所有结果文件
    logger.info("🔄 解析所有结果文件...")
    all_results = []
    
    for i, file_path in enumerate(result_files, 1):
        logger.info(f"📄 处理文件 {i}/{len(result_files)}: {os.path.basename(file_path)}")
        file_results = parse_result_file(file_path)
        all_results.extend(file_results)
    
    logger.info(f"📊 总共解析出 {len(all_results)} 条结果")
    
    # 4. 按行号排序并去重
    logger.info("🔄 整理和去重结果...")
    
    # 创建行号到结果的映射
    row_to_result = {}
    duplicate_count = 0
    
    for result in all_results:
        row_num = result['row_number']
        if row_num is not None:
            if row_num in row_to_result:
                duplicate_count += 1
                logger.debug(f"🔄 发现重复行 {row_num}，保留最新结果")
            row_to_result[row_num] = result
    
    logger.info(f"📊 去重后有效结果: {len(row_to_result)} 条")
    if duplicate_count > 0:
        logger.info(f"🔄 去除重复结果: {duplicate_count} 条")
    
    # 5. 创建最终输出
    logger.info("🔗 创建最终输出文件...")
    
    # 复制原始数据
    df_output = df_original.copy()
    df_output['AI_Response'] = ''
    df_output['Processing_Status'] = 'Missing'
    df_output['Source_File'] = ''
    
    # 填入AI响应
    matched_count = 0
    for row_num, result in row_to_result.items():
        # 转换为0-indexed
        idx = row_num - 1
        
        if 0 <= idx < len(df_output):
            df_output.loc[idx, 'AI_Response'] = result['response_content']
            df_output.loc[idx, 'Processing_Status'] = 'Completed'
            df_output.loc[idx, 'Source_File'] = result['file_source']
            matched_count += 1
        else:
            logger.warning(f"⚠️  行号 {row_num} 超出CSV范围")
    
    # 6. 统计和保存
    logger.info("📊 生成统计信息...")
    
    completed_rows = len(df_output[df_output['Processing_Status'] == 'Completed'])
    missing_rows = len(df_output[df_output['Processing_Status'] == 'Missing'])
    completion_rate = (completed_rows / total_rows) * 100
    
    logger.info(f"✅ 成功匹配: {matched_count} 行")
    logger.info(f"📈 完成统计:")
    logger.info(f"   - 总行数: {total_rows}")
    logger.info(f"   - 已完成: {completed_rows}")
    logger.info(f"   - 缺失: {missing_rows}")
    logger.info(f"   - 完成率: {completion_rate:.1f}%")
    
    # 保存结果
    try:
        df_output.to_csv(output_file, index=False, encoding='utf-8')
        file_size = os.path.getsize(output_file) / 1024 / 1024  # MB
        logger.info(f"💾 结果已保存: {output_file}")
        logger.info(f"📄 文件大小: {file_size:.2f} MB")
        
        # 生成缺失行报告
        if missing_rows > 0:
            missing_file = output_file.replace('.csv', '_missing_rows.txt')
            missing_indices = df_output[df_output['Processing_Status'] == 'Missing'].index + 1
            
            with open(missing_file, 'w') as f:
                for idx in missing_indices:
                    f.write(f"{idx}\n")
            
            logger.warning(f"⚠️  缺失行列表已保存: {missing_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 保存文件失败: {e}")
        return False

def show_summary(batch_dir, output_file):
    """显示处理总结"""
    logger.info("\n" + "="*80)
    logger.info("📊 处理总结")
    logger.info("="*80)
    
    # 检查成本信息
    cost_file = os.path.join(batch_dir, "batch_costs.json")
    if os.path.exists(cost_file):
        try:
            with open(cost_file, 'r') as f:
                cost_data = json.load(f)
            
            total_cost = sum(batch.get('batch_cost', 0) for batch in cost_data.get('batches', {}).values())
            logger.info(f"💰 估算总成本: ${total_cost:.4f}")
            
        except Exception as e:
            logger.warning(f"⚠️  无法读取成本信息: {e}")
    
    # 检查输出文件
    if os.path.exists(output_file):
        try:
            df = pd.read_csv(output_file)
            completed = len(df[df['Processing_Status'] == 'Completed'])
            total = len(df)
            
            logger.info(f"📄 最终输出: {output_file}")
            logger.info(f"📊 数据统计: {completed}/{total} ({completed/total*100:.1f}%)")
            
        except Exception as e:
            logger.warning(f"⚠️  无法读取输出文件统计: {e}")

def main():
    parser = argparse.ArgumentParser(description="智能合并批处理结果")
    parser.add_argument("--batch-dir", default="batch_results_20250525_182528", help="批处理目录")
    parser.add_argument("--csv-file", default="_csvs/content_MMSafeBench_cleaned.csv", help="原始CSV文件")
    parser.add_argument("--output-file", default="final_complete_output.csv", help="输出文件")
    args = parser.parse_args()
    
    # 检查输入
    if not os.path.exists(args.batch_dir):
        logger.error(f"❌ 批处理目录不存在: {args.batch_dir}")
        return 1
    
    if not os.path.exists(args.csv_file):
        logger.error(f"❌ CSV文件不存在: {args.csv_file}")
        return 1
    
    # 执行合并
    success = merge_all_results(args.batch_dir, args.csv_file, args.output_file)
    
    if success:
        show_summary(args.batch_dir, args.output_file)
        logger.info("🎉 合并完成！")
        return 0
    else:
        logger.error("💥 合并失败")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
