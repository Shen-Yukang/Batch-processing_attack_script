#!/usr/bin/env python3
"""
修复失败的批处理任务 - 使用正确的模型重新处理
"""

import os
import json
import logging
import subprocess
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_batch_status(batch_dir):
    """修复批处理状态，将错误标记为完成的任务改为失败"""
    status_file = os.path.join(batch_dir, "batch_status.json")
    
    if not os.path.exists(status_file):
        logger.error(f"❌ 状态文件不存在: {status_file}")
        return False
    
    try:
        with open(status_file, 'r') as f:
            status_data = json.load(f)
        
        fixed_count = 0
        for job in status_data.get('jobs', []):
            if job.get('status') == 'completed':
                # 检查是否真的有结果文件
                job_name = job.get('name', '')
                result_files = [f for f in os.listdir(batch_dir) 
                              if f.startswith('batch_results_') and f.endswith('.jsonl')]
                
                # 检查结果文件中是否包含此任务的数据
                has_results = False
                start_row = job.get('start_row', 0)
                end_row = job.get('end_row', 0)
                expected_rows = [f"row_{i}" for i in range(start_row, min(start_row + 3, end_row))]
                
                for result_file in result_files:
                    try:
                        with open(os.path.join(batch_dir, result_file), 'r') as f:
                            content = f.read(1000)  # 读取前1000字符
                            if any(row in content for row in expected_rows):
                                has_results = True
                                break
                    except:
                        continue
                
                if not has_results:
                    # 没有找到结果，标记为失败
                    job['status'] = 'failed'
                    job['error_message'] = '模型不支持批处理API，需要使用gpt-4o-mini重新处理'
                    job['completed_at'] = None
                    fixed_count += 1
                    logger.info(f"🔧 修复任务状态: {job_name} -> failed")
        
        if fixed_count > 0:
            # 保存修复后的状态
            with open(status_file, 'w') as f:
                json.dump(status_data, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ 修复了 {fixed_count} 个任务的状态")
        else:
            logger.info("✅ 所有任务状态正确，无需修复")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 修复状态失败: {e}")
        return False

def recreate_failed_batches(batch_dir, csv_file):
    """重新创建失败的批次，使用正确的模型"""
    status_file = os.path.join(batch_dir, "batch_status.json")
    
    if not os.path.exists(status_file):
        logger.error(f"❌ 状态文件不存在: {status_file}")
        return False
    
    try:
        with open(status_file, 'r') as f:
            status_data = json.load(f)
        
        failed_jobs = [job for job in status_data.get('jobs', []) 
                      if job.get('status') in ['failed', 'timeout']]
        
        if not failed_jobs:
            logger.info("✅ 没有失败的任务需要重新处理")
            return True
        
        logger.info(f"🔄 发现 {len(failed_jobs)} 个失败的任务需要重新处理")
        
        success_count = 0
        for i, job in enumerate(failed_jobs, 1):
            job_name = job.get('name', '')
            start_row = job.get('start_row', 0)
            end_row = job.get('end_row', 0)
            
            logger.info(f"🔄 重新处理任务 {i}/{len(failed_jobs)}: {job_name} (行 {start_row}-{end_row})")
            
            # 重新创建输入文件，使用gpt-4o-mini模型
            jsonl_file = os.path.join(batch_dir, f"{job_name}.jsonl")
            
            create_cmd = [
                "python", "create_safe_batch_input.py",
                csv_file, jsonl_file,
                "--model", "gpt-4o-mini",
                "--start-row", str(start_row),
                "--end-row", str(end_row)
            ]
            
            logger.info(f"📝 重新创建输入文件...")
            try:
                result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=120)
                if result.returncode != 0:
                    logger.error(f"❌ 创建输入文件失败: {result.stderr}")
                    continue
                logger.info(f"✅ 输入文件创建成功")
            except Exception as e:
                logger.error(f"❌ 创建输入文件异常: {e}")
                continue
            
            # 提交批处理
            process_cmd = [
                "python", "batch_processor.py", jsonl_file,
                "--output-dir", batch_dir,
                "--check-interval", "30"
            ]
            
            logger.info(f"🚀 提交批处理...")
            try:
                result = subprocess.run(process_cmd, capture_output=True, text=True, timeout=600)
                if result.returncode == 0:
                    logger.info(f"✅ {job_name} 重新处理成功")
                    
                    # 更新状态
                    job['status'] = 'completed'
                    job['completed_at'] = datetime.now().isoformat()
                    job['error_message'] = ''
                    job['attempts'] = job.get('attempts', 0) + 1
                    
                    success_count += 1
                else:
                    logger.error(f"❌ {job_name} 重新处理失败: {result.stderr}")
                    job['error_message'] = f"重新处理失败: {result.stderr[:200]}"
                    
            except subprocess.TimeoutExpired:
                logger.error(f"⏰ {job_name} 处理超时")
                job['error_message'] = "处理超时"
            except Exception as e:
                logger.error(f"❌ {job_name} 处理异常: {e}")
                job['error_message'] = f"处理异常: {str(e)}"
            
            # 保存状态
            with open(status_file, 'w') as f:
                json.dump(status_data, f, indent=2, ensure_ascii=False)
            
            # 任务间延迟
            if i < len(failed_jobs):
                logger.info("⏸️  等待30秒后处理下一个任务...")
                import time
                time.sleep(30)
        
        logger.info(f"🎯 重新处理完成: 成功 {success_count}/{len(failed_jobs)}")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"❌ 重新处理失败: {e}")
        return False

def main():
    logger.info("🔧 失败批处理修复工具")
    logger.info("="*60)
    
    # 查找最新的批处理目录
    batch_dirs = [d for d in os.listdir('.') if d.startswith('output/batch_results_')]
    if not batch_dirs:
        # 检查output目录
        if os.path.exists('output'):
            batch_dirs = [os.path.join('output', d) for d in os.listdir('output') 
                         if d.startswith('batch_results_')]
    
    if not batch_dirs:
        logger.error("❌ 未找到任何批处理目录")
        return 1
    
    # 选择最新的目录
    latest_dir = max(batch_dirs, key=lambda x: os.path.getmtime(x))
    logger.info(f"🎯 处理目录: {latest_dir}")
    
    # 确定CSV文件
    csv_file = "_csvs/content_Jailbreak28k.csv"  # 根据目录名推断
    if "FigStep" in latest_dir:
        csv_file = "_csvs/content_FigStep.csv"
    elif "MMSafeBench" in latest_dir:
        csv_file = "_csvs/content_MMSafeBench_cleaned.csv"
    
    if not os.path.exists(csv_file):
        logger.error(f"❌ CSV文件不存在: {csv_file}")
        return 1
    
    logger.info(f"📊 使用CSV文件: {csv_file}")
    
    # 步骤1: 修复状态
    logger.info("🔧 步骤1: 修复批处理状态...")
    if not fix_batch_status(latest_dir):
        return 1
    
    # 步骤2: 重新处理失败的批次
    logger.info("🔄 步骤2: 重新处理失败的批次...")
    if not recreate_failed_batches(latest_dir, csv_file):
        return 1
    
    logger.info("🎉 修复完成！")
    logger.info("💡 建议运行诊断脚本检查结果:")
    logger.info("   python diagnose_batch_results.py")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
