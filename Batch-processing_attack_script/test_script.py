#!/usr/bin/env python3
"""
测试脚本 - 验证环境和依赖是否正确安装
"""

import sys
import os

def test_imports():
    """测试所有必要的包是否可以导入"""
    print("测试包导入...")
    
    try:
        import pandas as pd
        print("✓ pandas 导入成功")
    except ImportError as e:
        print(f"✗ pandas 导入失败: {e}")
        return False
    
    try:
        import openai
        print("✓ openai 导入成功")
    except ImportError as e:
        print(f"✗ openai 导入失败: {e}")
        return False
    
    try:
        import requests
        print("✓ requests 导入成功")
    except ImportError as e:
        print(f"✗ requests 导入失败: {e}")
        return False
    
    try:
        from PIL import Image
        print("✓ Pillow 导入成功")
    except ImportError as e:
        print(f"✗ Pillow 导入失败: {e}")
        return False
    
    return True

def test_csv_file():
    """测试CSV文件是否存在且格式正确"""
    print("\n测试CSV文件...")
    
    csv_file = "new_csv/content_CogAgent.csv"
    
    if not os.path.exists(csv_file):
        print(f"✗ CSV文件不存在: {csv_file}")
        return False
    
    try:
        import pandas as pd
        df = pd.read_csv(csv_file)
        print(f"✓ CSV文件读取成功，共 {len(df)} 行")
        
        required_columns = ['Image Path', 'Content of P*']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"✗ CSV文件缺少必要的列: {missing_columns}")
            print(f"  现有列: {list(df.columns)}")
            return False
        
        print("✓ CSV文件格式正确")
        
        # 检查前几个图片文件是否存在
        for i in range(min(3, len(df))):
            image_path = df.iloc[i]['Image Path']
            if os.path.exists(image_path):
                print(f"✓ 图片文件存在: {os.path.basename(image_path)}")
            else:
                print(f"⚠ 图片文件不存在: {image_path}")
        
        return True
        
    except Exception as e:
        print(f"✗ 读取CSV文件失败: {e}")
        return False

def test_api_key():
    """测试API密钥是否设置"""
    print("\n测试API密钥...")
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("⚠ 环境变量 OPENAI_API_KEY 未设置")
        print("  请运行: export OPENAI_API_KEY='your-api-key-here'")
        return False
    
    if api_key.startswith('sk-'):
        print("✓ API密钥格式正确")
        return True
    else:
        print("⚠ API密钥格式可能不正确（应该以'sk-'开头）")
        return False

def main():
    print("=" * 50)
    print("CSV ChatGPT 处理脚本 - 环境测试")
    print("=" * 50)
    
    all_tests_passed = True
    
    # 测试包导入
    if not test_imports():
        all_tests_passed = False
        print("\n请运行以下命令安装依赖:")
        print("pip install -r requirements.txt")
    
    # 测试CSV文件
    if not test_csv_file():
        all_tests_passed = False
    
    # 测试API密钥
    if not test_api_key():
        all_tests_passed = False
    
    print("\n" + "=" * 50)
    if all_tests_passed:
        print("✓ 所有测试通过！可以开始使用脚本了。")
        print("\n建议的下一步:")
        print("1. 运行测试处理: python run_example.py")
        print("2. 或直接处理: python process_csv_with_chatgpt.py new_csv/content_CogAgent.csv output.csv")
    else:
        print("✗ 部分测试失败，请解决上述问题后重试。")
    print("=" * 50)

if __name__ == "__main__":
    main()
