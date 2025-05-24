#!/usr/bin/env python3
"""
批处理功能测试脚本
"""

import os
import sys
import logging
from batch_workflow import run_batch_workflow, estimate_cost

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_batch_workflow():
    """测试批处理工作流程"""
    
    # 配置参数
    input_file = "new_csv/content_CogAgent.csv"
    output_file = "test_batch_output.csv"
    
    # 检查文件是否存在
    if not os.path.exists(input_file):
        logger.error(f"测试文件不存在: {input_file}")
        return False
    
    # 从环境变量获取API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请设置环境变量 OPENAI_API_KEY")
        logger.error("例如：export OPENAI_API_KEY='your-api-key-here'")
        return False
    
    # 显示成本估算
    logger.info("=" * 60)
    logger.info("批处理测试 - 处理前3行")
    logger.info("=" * 60)
    
    estimate_cost(input_file, start_row=0, end_row=3)
    
    # 询问用户确认
    response = input("\n是否继续执行批处理测试？(y/N): ")
    if response.lower() != 'y':
        logger.info("取消测试")
        return False
    
    # 运行批处理工作流程（仅处理前3行）
    success = run_batch_workflow(
        input_csv=input_file,
        output_csv=output_file,
        api_key=api_key,
        model="gpt-4o",
        start_row=0,
        end_row=3,  # 仅处理前3行
        completion_window="24h",
        check_interval=30,  # 30秒检查一次
        keep_temp_files=True  # 保留临时文件以便调试
    )
    
    if success:
        logger.info("=" * 60)
        logger.info("批处理测试成功完成！")
        logger.info(f"结果文件: {output_file}")
        logger.info("=" * 60)
        
        # 显示结果预览
        try:
            import pandas as pd
            df = pd.read_csv(output_file)
            logger.info("结果预览:")
            for idx, row in df.head(3).iterrows():
                response = row.get('ChatGPT Response', 'N/A')
                preview = response[:100] + "..." if len(str(response)) > 100 else response
                logger.info(f"第{idx+1}行: {preview}")
        except Exception as e:
            logger.warning(f"无法显示结果预览: {e}")
        
        return True
    else:
        logger.error("批处理测试失败")
        return False

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--estimate-only':
        # 仅显示成本估算
        input_file = "new_csv/content_CogAgent.csv"
        if os.path.exists(input_file):
            estimate_cost(input_file, start_row=0, end_row=3)
        else:
            logger.error(f"测试文件不存在: {input_file}")
    else:
        # 运行完整测试
        test_batch_workflow()

if __name__ == "__main__":
    main()
