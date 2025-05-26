#!/usr/bin/env python3
"""
修复CSV文件中的图片路径问题
"""

import os
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_csv_image_paths(csv_file):
    """检查CSV文件中的图片路径"""
    logger.info(f"🔍 检查CSV文件: {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
        
        if 'Image Path' not in df.columns:
            logger.error("❌ CSV文件中没有 'Image Path' 列")
            return False
        
        total_rows = len(df)
        existing_files = 0
        missing_files = 0
        
        logger.info(f"📊 总行数: {total_rows}")
        
        # 检查前10个文件路径
        sample_paths = df['Image Path'].head(10).tolist()
        
        logger.info("📁 检查前10个图片路径:")
        for i, path in enumerate(sample_paths, 1):
            if pd.isna(path):
                logger.warning(f"  {i:2d}. (空路径)")
                continue
                
            if os.path.exists(path):
                logger.info(f"  {i:2d}. ✅ {path}")
                existing_files += 1
            else:
                logger.warning(f"  {i:2d}. ❌ {path}")
                missing_files += 1
        
        # 统计所有文件
        logger.info("📊 统计所有图片路径...")
        all_existing = 0
        all_missing = 0
        
        for path in df['Image Path']:
            if pd.isna(path):
                all_missing += 1
                continue
                
            if os.path.exists(path):
                all_existing += 1
            else:
                all_missing += 1
        
        logger.info(f"📈 路径统计:")
        logger.info(f"  ✅ 存在的文件: {all_existing}")
        logger.info(f"  ❌ 缺失的文件: {all_missing}")
        logger.info(f"  📊 存在率: {all_existing/total_rows*100:.1f}%")
        
        return all_existing > 0
        
    except Exception as e:
        logger.error(f"❌ 检查CSV文件失败: {e}")
        return False

def suggest_solutions(csv_file):
    """建议解决方案"""
    logger.info("💡 建议的解决方案:")
    
    # 检查可能的图片目录
    possible_dirs = [
        "images",
        "imagellm_transfer_attack", 
        "data/images",
        "../images",
        "assets"
    ]
    
    found_dirs = []
    for dir_name in possible_dirs:
        if os.path.exists(dir_name):
            file_count = len([f for f in os.listdir(dir_name) if f.lower().endswith(('.png', '.jpg', '.jpeg'))])
            found_dirs.append((dir_name, file_count))
            logger.info(f"  📁 找到目录: {dir_name} ({file_count} 个图片文件)")
    
    if found_dirs:
        logger.info("🔧 可能的修复方案:")
        logger.info("  1. 更新CSV文件中的路径")
        logger.info("  2. 移动图片文件到正确位置")
        logger.info("  3. 创建符号链接")
    else:
        logger.warning("⚠️  未找到包含图片的目录")
        logger.info("💡 建议:")
        logger.info("  1. 检查图片文件是否存在")
        logger.info("  2. 下载缺失的图片文件")
        logger.info("  3. 使用其他CSV文件（如content_FigStep.csv）")

def check_alternative_csvs():
    """检查其他可用的CSV文件"""
    logger.info("🔍 检查其他可用的CSV文件:")
    
    csv_files = [
        "_csvs/content_FigStep.csv",
        "_csvs/content_MMSafeBench_cleaned.csv"
    ]
    
    working_files = []
    
    for csv_file in csv_files:
        if os.path.exists(csv_file):
            logger.info(f"\n📊 检查: {csv_file}")
            if check_csv_image_paths(csv_file):
                working_files.append(csv_file)
                logger.info(f"✅ {csv_file} 可以使用")
            else:
                logger.warning(f"⚠️  {csv_file} 存在路径问题")
        else:
            logger.warning(f"❌ {csv_file} 不存在")
    
    if working_files:
        logger.info(f"\n🎯 推荐使用以下文件:")
        for file in working_files:
            logger.info(f"  ✅ {file}")
    else:
        logger.warning("⚠️  没有找到完全可用的CSV文件")

def main():
    logger.info("🔧 CSV图片路径检查工具")
    logger.info("="*60)
    
    # 检查问题文件
    problem_file = "_csvs/content_Jailbreak28k.csv"
    
    if os.path.exists(problem_file):
        logger.info(f"🔍 检查问题文件: {problem_file}")
        check_csv_image_paths(problem_file)
        suggest_solutions(problem_file)
    else:
        logger.error(f"❌ 文件不存在: {problem_file}")
    
    # 检查其他可用文件
    logger.info("\n" + "="*60)
    check_alternative_csvs()
    
    logger.info("\n" + "="*60)
    logger.info("🎯 建议:")
    logger.info("1. 使用 content_FigStep.csv 或 content_MMSafeBench_cleaned.csv")
    logger.info("2. 这些文件的图片路径更可能是正确的")
    logger.info("3. 运行增强版批处理器时选择可用的CSV文件")

if __name__ == "__main__":
    main()
