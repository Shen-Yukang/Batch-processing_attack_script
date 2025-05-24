#!/usr/bin/env python3
"""
调试批处理失败的详细脚本
"""

import os
import sys
import openai
import json
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_batch_details(batch_id: str, api_key: str):
    """获取批处理的详细信息"""
    client = openai.OpenAI(api_key=api_key)
    
    try:
        response = client.batches.retrieve(batch_id)
        
        logger.info("=" * 60)
        logger.info("批处理详细信息")
        logger.info("=" * 60)
        logger.info(f"批处理ID: {response.id}")
        logger.info(f"状态: {response.status}")
        logger.info(f"创建时间: {response.created_at}")
        logger.info(f"完成时间: {response.completed_at}")
        logger.info(f"失败时间: {response.failed_at}")
        logger.info(f"过期时间: {response.expires_at}")
        logger.info(f"输入文件ID: {response.input_file_id}")
        logger.info(f"输出文件ID: {response.output_file_id}")
        logger.info(f"错误文件ID: {response.error_file_id}")
        
        # 显示请求计数
        if hasattr(response, 'request_counts') and response.request_counts:
            counts = response.request_counts
            logger.info(f"总请求数: {getattr(counts, 'total', 0)}")
            logger.info(f"已完成: {getattr(counts, 'completed', 0)}")
            logger.info(f"失败数: {getattr(counts, 'failed', 0)}")
        
        # 显示错误信息
        if hasattr(response, 'errors') and response.errors:
            logger.error("批处理错误:")
            for error in response.errors:
                logger.error(f"  - {error}")
        
        # 如果有错误文件，尝试下载
        if response.error_file_id:
            logger.info("=" * 60)
            logger.info("下载错误文件...")
            try:
                error_content = client.files.content(response.error_file_id)
                error_file = f"batch_errors_{batch_id}.jsonl"
                with open(error_file, 'wb') as f:
                    f.write(error_content.content)
                logger.info(f"错误文件已保存: {error_file}")
                
                # 分析错误文件
                analyze_error_file(error_file)
                
            except Exception as e:
                logger.error(f"下载错误文件失败: {e}")
        
        # 检查输入文件
        if response.input_file_id:
            logger.info("=" * 60)
            logger.info("检查输入文件...")
            try:
                file_info = client.files.retrieve(response.input_file_id)
                logger.info(f"输入文件大小: {file_info.bytes} bytes ({file_info.bytes/1024/1024:.2f} MB)")
                logger.info(f"文件名: {file_info.filename}")
                logger.info(f"文件用途: {file_info.purpose}")
                logger.info(f"文件状态: {getattr(file_info, 'status', 'unknown')}")
            except Exception as e:
                logger.error(f"检查输入文件失败: {e}")
        
        return response
        
    except Exception as e:
        logger.error(f"获取批处理详情失败: {e}")
        return None

def analyze_error_file(error_file: str):
    """分析错误文件内容"""
    try:
        logger.info("分析错误文件内容...")
        
        error_types = {}
        error_examples = []
        
        with open(error_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    error_data = json.loads(line.strip())
                    
                    # 提取错误信息
                    error_msg = "未知错误"
                    if 'error' in error_data:
                        error_info = error_data['error']
                        if isinstance(error_info, dict):
                            error_msg = error_info.get('message', str(error_info))
                        else:
                            error_msg = str(error_info)
                    
                    # 统计错误类型
                    error_type = error_msg[:50] + "..." if len(error_msg) > 50 else error_msg
                    error_types[error_type] = error_types.get(error_type, 0) + 1
                    
                    # 保存前几个错误示例
                    if len(error_examples) < 5:
                        error_examples.append({
                            'line': line_num,
                            'custom_id': error_data.get('custom_id', 'unknown'),
                            'error': error_msg
                        })
                        
                except json.JSONDecodeError:
                    logger.warning(f"第{line_num}行JSON解析失败")
                except Exception as e:
                    logger.warning(f"第{line_num}行处理失败: {e}")
        
        # 显示错误统计
        logger.info("错误类型统计:")
        for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {count}次: {error_type}")
        
        # 显示错误示例
        logger.info("错误示例:")
        for example in error_examples:
            logger.info(f"  第{example['line']}行 ({example['custom_id']}): {example['error']}")
            
    except Exception as e:
        logger.error(f"分析错误文件失败: {e}")

def suggest_solutions(batch_response):
    """根据错误信息建议解决方案"""
    logger.info("=" * 60)
    logger.info("建议的解决方案")
    logger.info("=" * 60)
    
    if not batch_response:
        logger.info("1. 检查API密钥是否正确")
        logger.info("2. 检查网络连接")
        logger.info("3. 重新创建批处理")
        return
    
    if batch_response.status == 'failed':
        logger.info("批处理失败的常见原因和解决方案:")
        logger.info("1. 输入文件格式错误")
        logger.info("   - 检查JSONL文件格式是否正确")
        logger.info("   - 确保每行都是有效的JSON")
        
        logger.info("2. 图片文件问题")
        logger.info("   - 运行: python check_images.py new_csv/content_CogAgent.csv")
        logger.info("   - 检查图片文件是否存在且可读")
        logger.info("   - 确保图片文件不超过20MB")
        
        logger.info("3. 请求内容问题")
        logger.info("   - 检查prompt是否过长")
        logger.info("   - 确保图片base64编码正确")
        
        logger.info("4. 账户限制")
        logger.info("   - 检查账户是否有批处理权限")
        logger.info("   - 确认账户余额充足")
        
        logger.info("5. 分批处理")
        logger.info("   - 将大批次分成小批次处理")
        logger.info("   - 先测试少量数据: --end-row 10")

def main():
    if len(sys.argv) != 2:
        print("用法: python debug_batch_failure.py BATCH_ID")
        print("例如: python debug_batch_failure.py batch_68320de3b25c81908d272d19c1050739")
        return
    
    batch_id = sys.argv[1]
    
    # 获取API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        logger.error("请设置环境变量 OPENAI_API_KEY")
        return
    
    # 获取详细信息
    batch_response = get_batch_details(batch_id, api_key)
    
    # 建议解决方案
    suggest_solutions(batch_response)

if __name__ == "__main__":
    main()
