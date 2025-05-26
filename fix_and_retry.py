#!/usr/bin/env python3
"""
修复API密钥问题并重新执行缺失行处理
"""

import os
import sys
import logging
import subprocess
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_api_key():
    """检查API密钥是否设置"""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("❌ OpenAI API密钥未设置")
        return False
    
    # 检查密钥格式
    if not api_key.startswith('sk-'):
        logger.error("❌ API密钥格式不正确，应该以 'sk-' 开头")
        return False
    
    logger.info("✅ API密钥已设置")
    return True

def set_api_key():
    """设置API密钥"""
    print("请设置您的OpenAI API密钥:")
    print("1. 手动输入API密钥")
    print("2. 从文件读取API密钥")
    print("3. 跳过（如果已在其他地方设置）")
    
    choice = input("请选择 (1-3): ").strip()
    
    if choice == "1":
        api_key = input("请输入您的OpenAI API密钥: ").strip()
        if api_key and api_key.startswith('sk-'):
            os.environ['OPENAI_API_KEY'] = api_key
            logger.info("✅ API密钥已设置")
            return True
        else:
            logger.error("❌ 无效的API密钥")
            return False
    
    elif choice == "2":
        key_file = input("请输入包含API密钥的文件路径: ").strip()
        if os.path.exists(key_file):
            try:
                with open(key_file, 'r') as f:
                    api_key = f.read().strip()
                if api_key and api_key.startswith('sk-'):
                    os.environ['OPENAI_API_KEY'] = api_key
                    logger.info("✅ API密钥已从文件设置")
                    return True
                else:
                    logger.error("❌ 文件中的API密钥无效")
                    return False
            except Exception as e:
                logger.error(f"❌ 读取文件失败: {e}")
                return False
        else:
            logger.error("❌ 文件不存在")
            return False
    
    elif choice == "3":
        return check_api_key()
    
    else:
        logger.error("❌ 无效选择")
        return False

def test_api_connection():
    """测试API连接"""
    logger.info("测试API连接...")
    
    try:
        import openai
        client = openai.OpenAI()
        
        # 简单的API测试
        response = client.models.list()
        logger.info("✅ API连接测试成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ API连接测试失败: {e}")
        return False

def fix_previous_failed_jobs():
    """修复之前失败的任务状态"""
    import json
    
    status_file = "batch_results_20250525_182528/batch_status.json"
    if not os.path.exists(status_file):
        logger.warning("状态文件不存在")
        return
    
    try:
        with open(status_file, 'r') as f:
            status_data = json.load(f)
        
        # 找到那些错误标记为"completed"的retry任务
        fixed_count = 0
        for job in status_data['jobs']:
            if job['name'].startswith('retry_missing_batch_') and job['status'] == 'completed':
                # 检查是否真的有结果文件
                result_pattern = f"batch_results_20250525_182528/batch_results_*{job['name']}*.jsonl"
                import glob
                result_files = glob.glob(result_pattern)
                
                if not result_files:
                    # 没有结果文件，标记为失败
                    job['status'] = 'failed'
                    job['error_message'] = 'API密钥未设置，任务实际未执行'
                    job['completed_at'] = None
                    fixed_count += 1
                    logger.info(f"修复任务状态: {job['name']} -> failed")
        
        if fixed_count > 0:
            # 保存修复后的状态
            with open(status_file, 'w') as f:
                json.dump(status_data, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ 修复了 {fixed_count} 个任务的状态")
        else:
            logger.info("没有需要修复的任务状态")
            
    except Exception as e:
        logger.error(f"修复任务状态失败: {e}")

def retry_missing_rows():
    """重新执行缺失行处理"""
    logger.info("开始重新执行缺失行处理...")
    
    # 使用修复后的脚本
    cmd = [
        "python", "retry_missing_rows.py",
        "--csv-file", "_csvs/content_MMSafeBench_cleaned.csv",
        "--batch-dir", "batch_results_20250525_182528",
        "--batch-size", "20",
        "--delay", "60"
    ]
    
    try:
        logger.info(f"执行命令: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=False, capture_output=False, text=True)
        
        if result.returncode == 0:
            logger.info("✅ 缺失行处理完成")
            return True
        else:
            logger.error(f"❌ 缺失行处理失败，返回码: {result.returncode}")
            return False
            
    except Exception as e:
        logger.error(f"❌ 执行失败: {e}")
        return False

def main():
    logger.info("="*80)
    logger.info("修复API密钥问题并重新执行缺失行处理")
    logger.info("="*80)
    
    # 步骤1: 检查和设置API密钥
    if not check_api_key():
        logger.info("需要设置API密钥...")
        if not set_api_key():
            logger.error("API密钥设置失败，退出")
            return 1
    
    # 步骤2: 测试API连接
    if not test_api_connection():
        logger.error("API连接测试失败，请检查密钥是否正确")
        return 1
    
    # 步骤3: 修复之前失败的任务状态
    fix_previous_failed_jobs()
    
    # 步骤4: 重新执行缺失行处理
    success = retry_missing_rows()
    
    if success:
        logger.info("✅ 所有步骤完成")
        return 0
    else:
        logger.error("❌ 执行过程中出现错误")
        return 1

if __name__ == "__main__":
    sys.exit(main())
