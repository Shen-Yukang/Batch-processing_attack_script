#!/usr/bin/env python3
"""
检查OpenAI批处理失败的原因
"""

import os
import json
import logging
import openai

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_batch_failure():
    """检查最近失败的批处理"""
    
    # 检查API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("❌ OPENAI_API_KEY 环境变量未设置")
        return
    
    client = openai.OpenAI(api_key=api_key)
    
    try:
        # 获取最近的批处理列表
        logger.info("🔍 获取最近的批处理列表...")
        batches = client.batches.list(limit=10)
        
        logger.info(f"📊 找到 {len(batches.data)} 个最近的批处理")
        
        failed_batches = []
        for batch in batches.data:
            logger.info(f"📄 批处理 {batch.id}:")
            logger.info(f"   状态: {batch.status}")
            logger.info(f"   创建时间: {batch.created_at}")
            
            if batch.status == 'failed':
                failed_batches.append(batch)
                logger.error(f"❌ 失败的批处理: {batch.id}")
                
                # 获取详细信息
                try:
                    detailed_batch = client.batches.retrieve(batch.id)
                    logger.info(f"   失败时间: {detailed_batch.failed_at}")
                    
                    # 检查错误文件
                    if detailed_batch.error_file_id:
                        logger.info(f"   错误文件ID: {detailed_batch.error_file_id}")
                        
                        try:
                            error_content = client.files.content(detailed_batch.error_file_id)
                            error_text = error_content.content.decode('utf-8')
                            logger.error(f"   错误内容: {error_text[:500]}...")
                        except Exception as e:
                            logger.warning(f"   无法读取错误文件: {e}")
                    
                    # 检查请求统计
                    if detailed_batch.request_counts:
                        counts = detailed_batch.request_counts
                        logger.info(f"   请求统计:")
                        logger.info(f"     总数: {getattr(counts, 'total', 0)}")
                        logger.info(f"     完成: {getattr(counts, 'completed', 0)}")
                        logger.info(f"     失败: {getattr(counts, 'failed', 0)}")
                        
                except Exception as e:
                    logger.error(f"   获取详细信息失败: {e}")
            
            elif batch.status == 'completed':
                logger.info(f"✅ 成功的批处理: {batch.id}")
                if batch.output_file_id:
                    logger.info(f"   输出文件ID: {batch.output_file_id}")
        
        if not failed_batches:
            logger.info("✅ 没有发现失败的批处理")
        else:
            logger.warning(f"⚠️  发现 {len(failed_batches)} 个失败的批处理")
            
    except Exception as e:
        logger.error(f"❌ 检查批处理失败: {e}")

def check_input_file_format(file_path):
    """检查输入文件格式"""
    logger.info(f"🔍 检查输入文件格式: {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"❌ 文件不存在: {file_path}")
        return
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        logger.info(f"📊 文件包含 {len(lines)} 行")
        
        # 检查前几行的格式
        for i, line in enumerate(lines[:3], 1):
            try:
                data = json.loads(line.strip())
                logger.info(f"✅ 第{i}行格式正确")
                
                # 检查必要字段
                required_fields = ['custom_id', 'method', 'url', 'body']
                missing_fields = [field for field in required_fields if field not in data]
                
                if missing_fields:
                    logger.error(f"❌ 第{i}行缺少字段: {missing_fields}")
                else:
                    logger.info(f"✅ 第{i}行包含所有必要字段")
                
                # 检查body内容
                body = data.get('body', {})
                if 'model' not in body:
                    logger.error(f"❌ 第{i}行body中缺少model字段")
                if 'messages' not in body:
                    logger.error(f"❌ 第{i}行body中缺少messages字段")
                
                # 检查图片大小
                messages = body.get('messages', [])
                for msg in messages:
                    content = msg.get('content', [])
                    if isinstance(content, list):
                        for item in content:
                            if item.get('type') == 'image_url':
                                image_url = item.get('image_url', {}).get('url', '')
                                if image_url.startswith('data:image'):
                                    # 估算base64图片大小
                                    base64_data = image_url.split(',')[1] if ',' in image_url else ''
                                    size_mb = len(base64_data) * 3 / 4 / 1024 / 1024
                                    logger.info(f"📷 第{i}行图片大小约: {size_mb:.2f} MB")
                                    
                                    if size_mb > 20:
                                        logger.warning(f"⚠️  第{i}行图片可能过大 ({size_mb:.2f} MB > 20 MB)")
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ 第{i}行JSON格式错误: {e}")
            except Exception as e:
                logger.error(f"❌ 第{i}行检查失败: {e}")
                
    except Exception as e:
        logger.error(f"❌ 读取文件失败: {e}")

def check_api_quota():
    """检查API配额"""
    logger.info("🔍 检查API配额和模型可用性...")
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("❌ OPENAI_API_KEY 环境变量未设置")
        return
    
    client = openai.OpenAI(api_key=api_key)
    
    try:
        # 检查可用模型
        models = client.models.list()
        available_models = [model.id for model in models.data if 'gpt-4' in model.id]
        logger.info(f"✅ 可用的GPT-4模型: {available_models}")
        
        # 尝试简单的API调用
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Hello"}],
            max_tokens=5
        )
        logger.info("✅ API调用测试成功")
        
    except Exception as e:
        logger.error(f"❌ API检查失败: {e}")
        if "quota" in str(e).lower():
            logger.error("💰 可能是配额不足问题")
        elif "rate" in str(e).lower():
            logger.error("🚦 可能是速率限制问题")

def main():
    logger.info("🔧 OpenAI批处理失败诊断工具")
    logger.info("="*60)
    
    # 检查API配额
    check_api_quota()
    
    print()
    
    # 检查批处理历史
    check_batch_failure()
    
    print()
    
    # 检查最近的输入文件
    input_file = "output/batch_results_content_Jailbreak28k_20250526_100028/batch_001.jsonl"
    if os.path.exists(input_file):
        check_input_file_format(input_file)
    else:
        logger.warning(f"⚠️  输入文件不存在: {input_file}")

if __name__ == "__main__":
    main()
