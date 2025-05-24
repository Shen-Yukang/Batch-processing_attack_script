#!/usr/bin/env python3
"""
测试OpenAI API访问权限和可用模型
"""

import os
import openai
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_api_access():
    """测试API访问权限"""
    
    # 获取API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请设置环境变量 OPENAI_API_KEY")
        return False
    
    try:
        client = openai.OpenAI(api_key=api_key)
        
        # 1. 测试基本连接 - 获取可用模型列表
        logger.info("测试1: 获取可用模型列表...")
        models = client.models.list()
        
        # 检查可用的视觉模型
        vision_models = []
        for model in models.data:
            if any(keyword in model.id.lower() for keyword in ['gpt-4', 'vision', 'gpt-4o']):
                vision_models.append(model.id)
        
        logger.info(f"找到 {len(vision_models)} 个相关模型:")
        for model in sorted(vision_models):
            logger.info(f"  - {model}")
        
        # 2. 测试GPT-4o访问权限
        logger.info("\n测试2: 测试GPT-4o文本功能...")
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": "Hello, this is a test message."}],
                max_tokens=10
            )
            logger.info("✅ GPT-4o文本功能正常")
            logger.info(f"响应: {response.choices[0].message.content}")
        except Exception as e:
            logger.error(f"❌ GPT-4o文本功能失败: {e}")
        
        # 3. 测试GPT-4o-mini (更便宜的选择)
        logger.info("\n测试3: 测试GPT-4o-mini...")
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "Hello, this is a test message."}],
                max_tokens=10
            )
            logger.info("✅ GPT-4o-mini功能正常")
            logger.info(f"响应: {response.choices[0].message.content}")
        except Exception as e:
            logger.error(f"❌ GPT-4o-mini失败: {e}")
        
        # 4. 测试GPT-3.5 (备用选择)
        logger.info("\n测试4: 测试GPT-3.5-turbo...")
        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": "Hello, this is a test message."}],
                max_tokens=10
            )
            logger.info("✅ GPT-3.5-turbo功能正常")
            logger.info(f"响应: {response.choices[0].message.content}")
        except Exception as e:
            logger.error(f"❌ GPT-3.5-turbo失败: {e}")
        
        # 5. 检查账户信息
        logger.info("\n测试5: 检查账户使用情况...")
        try:
            # 注意：这个API端点可能需要特殊权限
            usage = client.usage.retrieve()
            logger.info(f"账户使用情况: {usage}")
        except Exception as e:
            logger.warning(f"无法获取使用情况: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"API连接失败: {e}")
        return False

def suggest_alternatives():
    """建议替代方案"""
    logger.info("\n=== 建议的解决方案 ===")
    logger.info("1. 如果GPT-4o不可用，可以尝试:")
    logger.info("   - GPT-4o-mini (更便宜，支持视觉)")
    logger.info("   - GPT-4-turbo (如果可用)")
    logger.info("   - GPT-3.5-turbo (仅文本，但很便宜)")
    
    logger.info("\n2. 修改脚本使用不同模型:")
    logger.info("   python process_csv_with_chatgpt.py input.csv output.csv --model gpt-4o-mini")
    logger.info("   python batch_workflow.py input.csv output.csv --model gpt-4o-mini")
    
    logger.info("\n3. 检查账户设置:")
    logger.info("   - 访问 https://platform.openai.com/account/billing")
    logger.info("   - 确认支付方式已添加")
    logger.info("   - 检查使用限制设置")

def main():
    logger.info("OpenAI API访问权限测试")
    logger.info("=" * 50)
    
    success = test_api_access()
    
    if not success:
        logger.error("API测试失败，请检查API密钥和网络连接")
    
    suggest_alternatives()

if __name__ == "__main__":
    main()
