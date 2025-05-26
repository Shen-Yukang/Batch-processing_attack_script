#!/usr/bin/env python3
"""
正确的合并脚本 - 让用户选择要合并的CSV文件
"""

import os
import sys
import glob
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_csv_files():
    """查找所有可用的CSV文件"""
    csv_files = glob.glob("_csvs/*.csv")
    return sorted(csv_files)

def find_batch_directories():
    """查找所有批处理目录"""
    batch_dirs = glob.glob("batch_results_*")
    return sorted(batch_dirs, reverse=True)  # 最新的在前

def show_options():
    """显示可用选项"""
    print("🔍 发现的CSV文件:")
    csv_files = find_csv_files()
    for i, csv_file in enumerate(csv_files, 1):
        basename = os.path.basename(csv_file)
        lines = sum(1 for line in open(csv_file)) - 1  # 减去标题行
        print(f"  {i}. {basename} ({lines} 行)")
    
    print("\n📁 发现的批处理目录:")
    batch_dirs = find_batch_directories()
    for i, batch_dir in enumerate(batch_dirs, 1):
        result_count = len(glob.glob(os.path.join(batch_dir, "batch_results_*.jsonl")))
        print(f"  {i}. {batch_dir} ({result_count} 个结果文件)")
    
    return csv_files, batch_dirs

def get_user_choice():
    """获取用户选择"""
    csv_files, batch_dirs = show_options()
    
    print("\n" + "="*60)
    print("请选择要合并的文件:")
    
    # 选择CSV文件
    while True:
        try:
            csv_choice = int(input(f"选择CSV文件 (1-{len(csv_files)}): ")) - 1
            if 0 <= csv_choice < len(csv_files):
                selected_csv = csv_files[csv_choice]
                break
            else:
                print("❌ 无效选择，请重试")
        except ValueError:
            print("❌ 请输入数字")
    
    # 选择批处理目录
    while True:
        try:
            batch_choice = int(input(f"选择批处理目录 (1-{len(batch_dirs)}): ")) - 1
            if 0 <= batch_choice < len(batch_dirs):
                selected_batch = batch_dirs[batch_choice]
                break
            else:
                print("❌ 无效选择，请重试")
        except ValueError:
            print("❌ 请输入数字")
    
    # 输出文件名
    csv_basename = os.path.basename(selected_csv).replace('.csv', '')
    default_output = f"final_output_{csv_basename}.csv"
    output_file = input(f"输出文件名 (默认: {default_output}): ").strip()
    if not output_file:
        output_file = default_output
    
    return selected_csv, selected_batch, output_file

def run_merge(csv_file, batch_dir, output_file):
    """执行合并"""
    logger.info(f"🚀 开始合并:")
    logger.info(f"📊 CSV文件: {csv_file}")
    logger.info(f"📁 批处理目录: {batch_dir}")
    logger.info(f"📄 输出文件: {output_file}")
    
    # 使用智能合并脚本
    import subprocess
    
    cmd = [
        "python", "smart_merge_results.py",
        "--batch-dir", batch_dir,
        "--csv-file", csv_file,
        "--output-file", output_file
    ]
    
    try:
        logger.info(f"🖥️  执行命令: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, capture_output=False)
        
        if result.returncode == 0:
            logger.info("✅ 合并完成!")
            return True
        else:
            logger.error(f"❌ 合并失败，返回码: {result.returncode}")
            return False
            
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ 合并过程中出现错误: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ 执行失败: {e}")
        return False

def check_result(output_file):
    """检查合并结果"""
    if os.path.exists(output_file):
        try:
            import pandas as pd
            df = pd.read_csv(output_file)
            
            total_rows = len(df)
            completed_rows = len(df[df['Processing_Status'] == 'Completed'])
            completion_rate = (completed_rows / total_rows) * 100 if total_rows > 0 else 0
            
            logger.info("📊 合并结果统计:")
            logger.info(f"  📄 输出文件: {output_file}")
            logger.info(f"  📊 总行数: {total_rows}")
            logger.info(f"  ✅ 已完成: {completed_rows}")
            logger.info(f"  📈 完成率: {completion_rate:.1f}%")
            
            file_size = os.path.getsize(output_file) / 1024 / 1024  # MB
            logger.info(f"  💾 文件大小: {file_size:.2f} MB")
            
            # 检查缺失行文件
            missing_file = output_file.replace('.csv', '_missing_rows.txt')
            if os.path.exists(missing_file):
                with open(missing_file, 'r') as f:
                    missing_count = len(f.readlines())
                logger.warning(f"  ⚠️  缺失行数: {missing_count}")
                logger.info(f"  📄 缺失行列表: {missing_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 无法分析结果文件: {e}")
            return False
    else:
        logger.error(f"❌ 输出文件不存在: {output_file}")
        return False

def main():
    print("🔧 正确的CSV批处理结果合并工具")
    print("="*60)
    
    try:
        # 获取用户选择
        csv_file, batch_dir, output_file = get_user_choice()
        
        # 确认选择
        print(f"\n📋 您的选择:")
        print(f"  📊 CSV文件: {csv_file}")
        print(f"  📁 批处理目录: {batch_dir}")
        print(f"  📄 输出文件: {output_file}")
        
        confirm = input("\n确认执行合并? (y/N): ").strip().lower()
        if confirm != 'y':
            logger.info("🛑 用户取消操作")
            return 0
        
        # 执行合并
        success = run_merge(csv_file, batch_dir, output_file)
        
        if success:
            # 检查结果
            check_result(output_file)
            logger.info("🎉 操作完成!")
            return 0
        else:
            logger.error("💥 合并失败")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("⚠️  用户中断操作")
        return 1
    except Exception as e:
        logger.error(f"💥 程序执行过程中出现错误: {e}")
        import traceback
        logger.error(f"🔍 错误详情: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
