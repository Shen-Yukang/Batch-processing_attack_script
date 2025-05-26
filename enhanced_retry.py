#!/usr/bin/env python3
"""
增强版缺失行重试脚本 - 包含详细的实时日志输出
"""

import os
import sys
import logging
from datetime import datetime

def setup_enhanced_logging():
    """设置增强的日志记录"""
    # 创建日志目录
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # 创建日志文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f"enhanced_retry_{timestamp}.log")
    
    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # 清除现有的handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 创建格式化器
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # 文件handler - 记录所有详细信息
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)
    
    # 控制台handler - 显示重要信息
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    root_logger.addHandler(console_handler)
    
    # 创建专门的进度logger
    progress_logger = logging.getLogger('progress')
    progress_handler = logging.StreamHandler(sys.stdout)
    progress_handler.setLevel(logging.INFO)
    progress_handler.setFormatter(logging.Formatter('%(message)s'))
    progress_logger.addHandler(progress_handler)
    progress_logger.propagate = False
    
    print(f"📝 详细日志将保存到: {log_file}")
    print(f"🖥️  控制台显示重要进度信息")
    print(f"🔍 如需查看详细信息，请查看日志文件")
    print("="*80)
    
    return logging.getLogger(__name__)

def check_environment():
    """检查执行环境"""
    logger = logging.getLogger(__name__)
    
    logger.info("🔍 检查执行环境...")
    
    # 检查必要文件
    required_files = [
        'retry_missing_rows.py',
        'robust_batch_processor.py',
        'batch_processor.py',
        'missing_rows.txt',
        '_csvs/content_MMSafeBench_cleaned.csv'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
        else:
            logger.debug(f"✅ 找到文件: {file_path}")
    
    if missing_files:
        logger.error(f"❌ 缺少必要文件: {missing_files}")
        return False
    
    # 检查API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("❌ 未设置 OPENAI_API_KEY 环境变量")
        return False
    elif not api_key.startswith('sk-'):
        logger.error("❌ API密钥格式不正确")
        return False
    else:
        logger.info("✅ API密钥已设置")
    
    # 检查批处理目录
    batch_dir = "batch_results_20250525_182528"
    if not os.path.exists(batch_dir):
        logger.error(f"❌ 批处理目录不存在: {batch_dir}")
        return False
    else:
        logger.info(f"✅ 批处理目录存在: {batch_dir}")
    
    # 检查缺失行文件
    try:
        with open('missing_rows.txt', 'r') as f:
            lines = f.readlines()
        missing_count = len([line for line in lines if line.strip().isdigit()])
        logger.info(f"✅ 缺失行文件包含 {missing_count} 个缺失行")
    except Exception as e:
        logger.error(f"❌ 读取缺失行文件失败: {e}")
        return False
    
    logger.info("✅ 环境检查通过")
    return True

def show_execution_plan():
    """显示执行计划"""
    logger = logging.getLogger(__name__)
    
    logger.info("📋 执行计划:")
    logger.info("  1. 读取缺失行文件 (missing_rows.txt)")
    logger.info("  2. 智能分组缺失行为批次")
    logger.info("  3. 为每个批次创建输入文件")
    logger.info("  4. 提交到OpenAI批处理API")
    logger.info("  5. 监控执行状态")
    logger.info("  6. 下载结果文件")
    logger.info("  7. 更新状态和成本统计")
    
    logger.info("🔧 配置参数:")
    logger.info("  - CSV文件: _csvs/content_MMSafeBench_cleaned.csv")
    logger.info("  - 批处理目录: batch_results_20250525_182528")
    logger.info("  - 批次大小: 20行/批次")
    logger.info("  - 批次间延迟: 60秒")
    logger.info("  - 模型: gpt-4o-mini")

def run_enhanced_retry():
    """运行增强版重试"""
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 开始执行增强版缺失行重试...")
    
    # 导入并运行重试脚本
    try:
        import subprocess
        
        cmd = [
            sys.executable, 'retry_missing_rows.py',
            '--csv-file', '_csvs/content_MMSafeBench_cleaned.csv',
            '--batch-dir', 'batch_results_20250525_182528',
            '--batch-size', '20',
            '--delay', '60'
        ]
        
        logger.info(f"🖥️  执行命令: {' '.join(cmd)}")
        
        # 使用实时输出
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # 实时显示输出
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                # 直接打印输出，不通过logger以避免重复格式化
                print(output.strip())
        
        return_code = process.poll()
        
        if return_code == 0:
            logger.info("✅ 缺失行重试执行完成")
            return True
        else:
            logger.error(f"❌ 缺失行重试执行失败，返回码: {return_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 执行过程中出现异常: {e}")
        import traceback
        logger.error(f"🔍 异常详情: {traceback.format_exc()}")
        return False

def show_final_summary():
    """显示最终总结"""
    logger = logging.getLogger(__name__)
    
    logger.info("="*80)
    logger.info("📊 执行总结")
    logger.info("="*80)
    
    # 检查结果文件
    batch_dir = "batch_results_20250525_182528"
    if os.path.exists(batch_dir):
        import glob
        result_files = glob.glob(os.path.join(batch_dir, "batch_results_*.jsonl"))
        logger.info(f"📄 生成的结果文件数: {len(result_files)}")
        
        # 检查状态文件
        status_file = os.path.join(batch_dir, "batch_status.json")
        if os.path.exists(status_file):
            try:
                import json
                with open(status_file, 'r') as f:
                    status_data = json.load(f)
                
                total_jobs = status_data.get('total_jobs', 0)
                completed_jobs = len([job for job in status_data.get('jobs', []) if job.get('status') == 'completed'])
                failed_jobs = len([job for job in status_data.get('jobs', []) if job.get('status') in ['failed', 'timeout']])
                
                logger.info(f"📈 任务统计:")
                logger.info(f"  - 总任务数: {total_jobs}")
                logger.info(f"  - 已完成: {completed_jobs}")
                logger.info(f"  - 失败/超时: {failed_jobs}")
                logger.info(f"  - 完成率: {completed_jobs/total_jobs*100:.1f}%" if total_jobs > 0 else "  - 完成率: 0%")
                
            except Exception as e:
                logger.warning(f"⚠️  无法读取状态文件: {e}")
    
    logger.info("💡 下一步建议:")
    logger.info("  1. 检查日志文件了解详细执行情况")
    logger.info("  2. 如有失败任务，可以重新运行此脚本")
    logger.info("  3. 执行合并脚本整合所有结果")
    logger.info("  4. 验证最终输出文件的完整性")

def main():
    """主函数"""
    print("🔧 增强版缺失行重试脚本")
    print("="*80)
    
    # 设置增强日志
    logger = setup_enhanced_logging()
    
    try:
        # 检查环境
        if not check_environment():
            logger.error("❌ 环境检查失败，退出")
            return 1
        
        # 显示执行计划
        show_execution_plan()
        
        # 询问是否继续
        response = input("\n🤔 是否开始执行？(y/N): ")
        if response.lower() != 'y':
            logger.info("🛑 用户取消执行")
            return 0
        
        # 执行重试
        success = run_enhanced_retry()
        
        # 显示总结
        show_final_summary()
        
        if success:
            logger.info("🎉 所有步骤执行完成")
            return 0
        else:
            logger.error("💥 执行过程中出现错误")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("⚠️  用户中断执行")
        return 1
    except Exception as e:
        logger.error(f"💥 程序执行过程中出现未预期的错误: {e}")
        import traceback
        logger.error(f"🔍 错误详情: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
