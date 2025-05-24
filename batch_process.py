#!/usr/bin/env python3
"""
批量处理所有CSV文件的脚本
"""

import os
import glob
from process_csv_with_chatgpt import process_csv_file

def main():
    # 配置参数
    input_dir = "new_csv"
    output_dir = "new_csv/processed"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 从环境变量获取API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("错误：请设置环境变量 OPENAI_API_KEY")
        print("例如：export OPENAI_API_KEY='your-api-key-here'")
        return
    
    # 查找所有CSV文件
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    csv_files = [f for f in csv_files if not f.endswith('_with_chatgpt.csv')]  # 排除已处理的文件
    
    if not csv_files:
        print(f"在 {input_dir} 目录中没有找到CSV文件")
        return
    
    print(f"找到 {len(csv_files)} 个CSV文件:")
    for i, file in enumerate(csv_files, 1):
        print(f"  {i}. {os.path.basename(file)}")
    
    # 处理参数
    delay = 2.0  # API调用间隔（秒）
    start_row = 0  # 从第一行开始
    end_row = None  # 处理全部行，设置为数字可限制处理行数
    
    print(f"\n配置:")
    print(f"  API延迟: {delay}秒")
    print(f"  处理范围: 全部行" if end_row is None else f"  处理范围: 第{start_row+1}行到第{end_row}行")
    
    # 询问用户确认
    response = input("\n是否开始批量处理？(y/N): ")
    if response.lower() != 'y':
        print("取消处理")
        return
    
    # 处理每个文件
    for i, input_file in enumerate(csv_files, 1):
        filename = os.path.basename(input_file)
        name, ext = os.path.splitext(filename)
        output_file = os.path.join(output_dir, f"{name}_with_chatgpt{ext}")
        
        print(f"\n{'='*60}")
        print(f"处理文件 {i}/{len(csv_files)}: {filename}")
        print(f"输出文件: {os.path.basename(output_file)}")
        print(f"{'='*60}")
        
        try:
            # 调用处理函数
            process_csv_file(
                input_file=input_file,
                output_file=output_file,
                api_key=api_key,
                delay=delay,
                start_row=start_row,
                end_row=end_row
            )
            print(f"✓ 文件 {filename} 处理完成")
            
        except Exception as e:
            print(f"✗ 处理文件 {filename} 时出错: {e}")
            continue
    
    print(f"\n{'='*60}")
    print("批量处理完成！")
    print(f"处理结果保存在: {output_dir}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
