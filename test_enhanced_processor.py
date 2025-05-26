#!/usr/bin/env python3
"""
测试增强版批处理器的功能
"""

import os
import sys

def test_csv_discovery():
    """测试CSV文件发现功能"""
    print("🔍 测试CSV文件发现功能...")
    
    # 导入函数
    from robust_batch_processor import find_csv_files, get_csv_line_count
    
    csv_files = find_csv_files()
    print(f"📊 找到 {len(csv_files)} 个CSV文件:")
    
    for i, csv_file in enumerate(csv_files, 1):
        try:
            lines = get_csv_line_count(csv_file)
            file_size = os.path.getsize(csv_file) / 1024 / 1024  # MB
            print(f"  {i:2d}. {csv_file}")
            print(f"      📊 {lines} 行数据, 💾 {file_size:.2f} MB")
        except Exception as e:
            print(f"  {i:2d}. {csv_file} (错误: {e})")

def test_safe_command():
    """测试安全命令执行"""
    print("\n🔒 测试安全命令执行...")
    
    from robust_batch_processor import safe_command_execution
    
    # 测试简单命令
    success, output = safe_command_execution(["echo", "Hello World"], 5)
    print(f"Echo测试: {'✅' if success else '❌'} - {output.strip()}")
    
    # 测试Python命令
    success, output = safe_command_execution(["python", "-c", "print('Python works')"], 5)
    print(f"Python测试: {'✅' if success else '❌'} - {output.strip()}")

def test_output_directory():
    """测试输出目录创建"""
    print("\n📁 测试输出目录...")
    
    output_dir = "output"
    if os.path.exists(output_dir):
        print(f"✅ 输出目录存在: {output_dir}")
        
        # 检查子目录
        subdirs = [d for d in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, d))]
        print(f"📂 子目录数量: {len(subdirs)}")
        
        if subdirs:
            print("📂 现有子目录:")
            for subdir in subdirs[:5]:  # 只显示前5个
                print(f"   - {subdir}")
    else:
        print(f"⚠️  输出目录不存在: {output_dir}")
        print("💡 将在首次运行时自动创建")

def show_usage_examples():
    """显示使用示例"""
    print("\n📋 使用示例:")
    print("="*60)
    
    print("1. 交互式选择CSV文件:")
    print("   python robust_batch_processor.py --interactive")
    
    print("\n2. 指定CSV文件:")
    print("   python robust_batch_processor.py --input-csv _csvs/content_FigStep.csv")
    
    print("\n3. 自定义参数:")
    print("   python robust_batch_processor.py --interactive --batch-size 10 --model gpt-4o-mini")
    
    print("\n4. 处理特定行范围:")
    print("   python robust_batch_processor.py --input-csv _csvs/content_FigStep.csv --start-row 0 --end-row 100")

def main():
    print("🧪 增强版批处理器功能测试")
    print("="*60)
    
    # 测试各项功能
    test_csv_discovery()
    test_safe_command()
    test_output_directory()
    show_usage_examples()
    
    print("\n" + "="*60)
    print("✅ 功能测试完成")
    print("\n💡 现在可以运行增强版批处理器:")
    print("   python robust_batch_processor.py --interactive")

if __name__ == "__main__":
    main()
