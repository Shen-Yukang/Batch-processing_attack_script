#!/usr/bin/env python3
"""
智能续传批处理脚本
跳过已完成的行，只处理剩余未完成的行
"""

import os
import json
import pandas as pd
import argparse
import logging
from datetime import datetime
from typing import List, Set
import subprocess

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchResumer:
    def __init__(self, csv_file: str, completed_output_file: str = None, missing_rows_file: str = None):
        """
        初始化续传处理器
        
        Args:
            csv_file: 原始CSV文件路径
            completed_output_file: 已完成的输出文件路径
            missing_rows_file: 缺失行列表文件路径
        """
        self.csv_file = csv_file
        self.completed_output_file = completed_output_file
        self.missing_rows_file = missing_rows_file
        
    def analyze_completion_status(self) -> dict:
        """分析完成状态"""
        logger.info("🔍 分析当前完成状态...")
        
        # 读取原始CSV
        df_original = pd.read_csv(self.csv_file)
        total_rows = len(df_original)
        
        completed_rows = set()
        missing_rows = set()
        
        # 方法1: 从已完成的输出文件中读取
        if self.completed_output_file and os.path.exists(self.completed_output_file):
            logger.info(f"📄 从输出文件分析: {self.completed_output_file}")
            df_completed = pd.read_csv(self.completed_output_file)
            
            # 找出已完成的行（有AI_Response且不为空）
            completed_mask = (df_completed['Processing_Status'] == 'Completed') & \
                           (df_completed['AI_Response'].notna()) & \
                           (df_completed['AI_Response'] != '')
            
            completed_indices = df_completed[completed_mask].index.tolist()
            completed_rows = set(idx + 1 for idx in completed_indices)  # 转换为1-based
            
            missing_indices = df_completed[~completed_mask].index.tolist()
            missing_rows = set(idx + 1 for idx in missing_indices)  # 转换为1-based
        
        # 方法2: 从缺失行文件中读取
        elif self.missing_rows_file and os.path.exists(self.missing_rows_file):
            logger.info(f"📄 从缺失行文件分析: {self.missing_rows_file}")
            with open(self.missing_rows_file, 'r') as f:
                missing_rows = set(int(line.strip()) for line in f if line.strip())
            
            completed_rows = set(range(1, total_rows + 1)) - missing_rows
        
        else:
            logger.warning("⚠️  未找到完成状态文件，将处理所有行")
            missing_rows = set(range(1, total_rows + 1))
            completed_rows = set()
        
        analysis = {
            'total_rows': total_rows,
            'completed_rows': len(completed_rows),
            'missing_rows': len(missing_rows),
            'completion_rate': len(completed_rows) / total_rows * 100,
            'completed_row_numbers': sorted(completed_rows),
            'missing_row_numbers': sorted(missing_rows)
        }
        
        logger.info(f"📊 完成状态分析:")
        logger.info(f"   总行数: {analysis['total_rows']}")
        logger.info(f"   已完成: {analysis['completed_rows']}")
        logger.info(f"   待处理: {analysis['missing_rows']}")
        logger.info(f"   完成率: {analysis['completion_rate']:.1f}%")
        
        return analysis
    
    def create_resume_csv(self, missing_row_numbers: List[int], output_file: str) -> str:
        """创建续传用的CSV文件，只包含未完成的行"""
        logger.info(f"📝 创建续传CSV文件: {output_file}")
        
        # 读取原始CSV
        df_original = pd.read_csv(self.csv_file)
        
        # 提取未完成的行（转换为0-based索引）
        missing_indices = [row_num - 1 for row_num in missing_row_numbers]
        df_resume = df_original.iloc[missing_indices].copy()
        
        # 重置索引但保留原始行号信息
        df_resume['Original_Row_Number'] = missing_row_numbers
        df_resume.reset_index(drop=True, inplace=True)
        
        # 保存续传CSV
        df_resume.to_csv(output_file, index=False)
        
        logger.info(f"✅ 续传CSV已创建: {output_file}")
        logger.info(f"   包含 {len(df_resume)} 行待处理数据")
        
        return output_file
    
    def calculate_batch_plan(self, total_missing: int, batch_size: int = 20) -> List[dict]:
        """计算批处理计划"""
        logger.info(f"📋 计算批处理计划 (每批 {batch_size} 行)...")
        
        batches = []
        for i in range(0, total_missing, batch_size):
            start_row = i
            end_row = min(i + batch_size - 1, total_missing - 1)
            
            batch_info = {
                'batch_name': f"resume_batch_{i//batch_size + 1:03d}",
                'start_row': start_row,
                'end_row': end_row,
                'row_count': end_row - start_row + 1
            }
            batches.append(batch_info)
        
        logger.info(f"📊 批处理计划:")
        logger.info(f"   总批次数: {len(batches)}")
        logger.info(f"   每批大小: {batch_size}")
        logger.info(f"   最后一批: {batches[-1]['row_count']} 行")
        
        return batches
    
    def start_resume_processing(self, model: str = "gpt-4o-mini", batch_size: int = 20, 
                              delay_between_batches: int = 120) -> str:
        """开始续传处理"""
        logger.info("🚀 开始智能续传批处理...")
        
        # 1. 分析完成状态
        analysis = self.analyze_completion_status()
        
        if analysis['missing_rows'] == 0:
            logger.info("🎉 所有行都已完成，无需续传！")
            return None
        
        # 2. 创建续传CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        resume_csv = f"resume_data_{timestamp}.csv"
        self.create_resume_csv(analysis['missing_row_numbers'], resume_csv)
        
        # 3. 创建输出目录
        output_dir = f"output/resume_batch_results_{timestamp}"
        os.makedirs(output_dir, exist_ok=True)
        
        # 4. 计算批处理计划
        batches = self.calculate_batch_plan(analysis['missing_rows'], batch_size)
        
        # 5. 启动批处理
        logger.info(f"🎯 开始处理 {analysis['missing_rows']} 行数据...")
        
        cmd = [
            "python", "robust_batch_processor.py", 
            resume_csv,
            "--model", model,
            "--batch-size", str(batch_size),
            "--delay", str(delay_between_batches),
            "--output-dir", output_dir
        ]
        
        logger.info(f"🖥️  执行命令: {' '.join(cmd)}")
        
        try:
            # 启动批处理（非阻塞）
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            logger.info(f"✅ 续传批处理已启动 (PID: {process.pid})")
            logger.info(f"📁 输出目录: {output_dir}")
            logger.info(f"📄 续传CSV: {resume_csv}")
            
            return output_dir
            
        except Exception as e:
            logger.error(f"❌ 启动续传处理失败: {e}")
            return None

def main():
    parser = argparse.ArgumentParser(description="智能续传批处理脚本")
    parser.add_argument("csv_file", help="原始CSV文件路径")
    parser.add_argument("--completed-output", help="已完成的输出文件路径")
    parser.add_argument("--missing-rows-file", help="缺失行列表文件路径")
    parser.add_argument("--model", default="gpt-4o-mini", help="使用的模型，默认gpt-4o-mini")
    parser.add_argument("--batch-size", type=int, default=20, help="每批处理的行数，默认20")
    parser.add_argument("--delay", type=int, default=120, help="批次间延迟秒数，默认120")
    parser.add_argument("--analyze-only", action="store_true", help="仅分析完成状态，不启动处理")
    
    args = parser.parse_args()
    
    # 检查输入文件
    if not os.path.exists(args.csv_file):
        logger.error(f"❌ CSV文件不存在: {args.csv_file}")
        return 1
    
    # 创建续传处理器
    resumer = BatchResumer(
        csv_file=args.csv_file,
        completed_output_file=args.completed_output,
        missing_rows_file=args.missing_rows_file
    )
    
    if args.analyze_only:
        # 仅分析
        analysis = resumer.analyze_completion_status()
        
        print("\n" + "="*80)
        print("📊 完成状态分析报告")
        print("="*80)
        print(f"📄 原始文件: {args.csv_file}")
        print(f"📊 总行数: {analysis['total_rows']}")
        print(f"✅ 已完成: {analysis['completed_rows']} ({analysis['completion_rate']:.1f}%)")
        print(f"⏳ 待处理: {analysis['missing_rows']}")
        
        if analysis['missing_rows'] > 0:
            print(f"\n💡 续传建议:")
            print(f"   python resume_batch_processing.py {args.csv_file} --missing-rows-file your_missing_file.txt")
        
        print("="*80)
    else:
        # 启动续传处理
        output_dir = resumer.start_resume_processing(
            model=args.model,
            batch_size=args.batch_size,
            delay_between_batches=args.delay
        )
        
        if output_dir:
            print("\n" + "="*80)
            print("🚀 续传批处理已启动")
            print("="*80)
            print(f"📁 输出目录: {output_dir}")
            print(f"🔍 监控进度: tail -f {output_dir}/logs/*.log")
            print(f"📊 查看状态: cat {output_dir}/batch_status.json")
            print("="*80)
        else:
            logger.error("❌ 续传处理启动失败")
            return 1
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
