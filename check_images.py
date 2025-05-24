#!/usr/bin/env python3
"""
检查CSV中图片文件的脚本
"""

import os
import pandas as pd
import base64
from PIL import Image
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_image_file(image_path: str) -> dict:
    """检查单个图片文件"""
    result = {
        'path': image_path,
        'exists': False,
        'readable': False,
        'size_mb': 0,
        'dimensions': None,
        'format': None,
        'base64_size_mb': 0,
        'error': None
    }
    
    try:
        # 检查文件是否存在
        if not os.path.exists(image_path):
            result['error'] = "文件不存在"
            return result
        
        result['exists'] = True
        
        # 检查文件大小
        file_size = os.path.getsize(image_path)
        result['size_mb'] = file_size / (1024 * 1024)
        
        # 检查是否可读取
        try:
            with Image.open(image_path) as img:
                result['readable'] = True
                result['dimensions'] = img.size
                result['format'] = img.format
        except Exception as e:
            result['error'] = f"无法读取图片: {e}"
            return result
        
        # 检查base64编码后的大小
        try:
            with open(image_path, "rb") as f:
                base64_data = base64.b64encode(f.read())
                result['base64_size_mb'] = len(base64_data) / (1024 * 1024)
        except Exception as e:
            result['error'] = f"无法编码为base64: {e}"
            
    except Exception as e:
        result['error'] = f"检查失败: {e}"
    
    return result

def check_all_images(csv_file: str, start_row: int = 0, end_row: int = None):
    """检查CSV中所有图片"""
    
    # 读取CSV文件
    try:
        df = pd.read_csv(csv_file)
        logger.info(f"成功读取CSV文件，共{len(df)}行")
    except Exception as e:
        logger.error(f"读取CSV文件失败: {e}")
        return
    
    # 确定检查范围
    start_idx = max(0, start_row)
    end_idx = min(len(df), end_row) if end_row is not None else len(df)
    
    logger.info(f"检查范围: 第{start_idx+1}行到第{end_idx}行")
    
    # 统计信息
    total_count = 0
    error_count = 0
    large_files = []
    missing_files = []
    unreadable_files = []
    
    # 检查每个图片
    for idx in range(start_idx, end_idx):
        row = df.iloc[idx]
        image_path = row['Image Path']
        
        result = check_image_file(image_path)
        total_count += 1
        
        # 显示进度
        if total_count % 20 == 0:
            logger.info(f"已检查 {total_count}/{end_idx-start_idx} 个文件...")
        
        # 记录问题
        if result['error']:
            error_count += 1
            if not result['exists']:
                missing_files.append((idx+1, image_path))
            elif not result['readable']:
                unreadable_files.append((idx+1, image_path, result['error']))
        
        # 检查文件大小（OpenAI有20MB限制）
        if result['base64_size_mb'] > 15:  # 留一些余量
            large_files.append((idx+1, image_path, result['base64_size_mb']))
    
    # 显示统计结果
    logger.info("=" * 60)
    logger.info("图片检查结果")
    logger.info("=" * 60)
    logger.info(f"总文件数: {total_count}")
    logger.info(f"错误文件数: {error_count}")
    logger.info(f"成功率: {((total_count-error_count)/total_count*100):.1f}%")
    
    # 显示问题详情
    if missing_files:
        logger.warning(f"缺失文件 ({len(missing_files)}个):")
        for row_num, path in missing_files[:10]:  # 只显示前10个
            logger.warning(f"  第{row_num}行: {path}")
        if len(missing_files) > 10:
            logger.warning(f"  ... 还有 {len(missing_files)-10} 个文件")
    
    if unreadable_files:
        logger.warning(f"无法读取的文件 ({len(unreadable_files)}个):")
        for row_num, path, error in unreadable_files[:5]:
            logger.warning(f"  第{row_num}行: {path} - {error}")
    
    if large_files:
        logger.warning(f"过大文件 ({len(large_files)}个, >15MB):")
        for row_num, path, size in large_files:
            logger.warning(f"  第{row_num}行: {path} - {size:.1f}MB")
    
    # 建议
    logger.info("=" * 60)
    logger.info("建议:")
    if missing_files or unreadable_files:
        logger.info("1. 跳过有问题的文件，只处理正常的文件")
        logger.info("2. 或者修复图片文件路径/文件")
    
    if large_files:
        logger.info("3. 压缩过大的图片文件")
    
    if error_count == 0:
        logger.info("✅ 所有图片文件都正常，可以进行批处理")
    else:
        logger.info(f"❌ 有 {error_count} 个问题文件，建议先修复或跳过")

def main():
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python check_images.py CSV_FILE [START_ROW] [END_ROW]")
        print("例如: python check_images.py new_csv/content_CogAgent.csv 0 50")
        return
    
    csv_file = sys.argv[1]
    start_row = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    end_row = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    check_all_images(csv_file, start_row, end_row)

if __name__ == "__main__":
    main()
