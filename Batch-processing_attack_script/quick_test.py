#!/usr/bin/env python3
"""
快速测试脚本 - 一键测试指定行数
"""

import os
import sys
from process_csv_with_chatgpt import process_csv_file

def main():
    # 从命令行参数获取测试行数，默认测试第1行
    if len(sys.argv) > 1:
        try:
            if sys.argv[1] == 'first':
                # 测试第一行
                start_row = 0
                end_row = 1
                test_name = "第1行"
            elif sys.argv[1].isdigit():
                # 测试前N行
                num_rows = int(sys.argv[1])
                start_row = 0
                end_row = num_rows
                test_name = f"前{num_rows}行"
            else:
                print("用法:")
                print("  python quick_test.py          # 测试第1行")
                print("  python quick_test.py first    # 测试第1行")
                print("  python quick_test.py 5        # 测试前5行")
                return
        except ValueError:
            print("请输入有效的数字")
            return
    else:
        # 默认测试第1行
        start_row = 0
        end_row = 1
        test_name = "第1行"
    
    # 配置参数
    input_file = "new_csv/content_CogAgent.csv"
    output_file = f"quick_test_result.csv"
    
    # 检查文件是否存在
    if not os.path.exists(input_file):
        print(f"错误：输入文件不存在 - {input_file}")
        return
    
    # 从环境变量获取API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("错误：请设置环境变量 OPENAI_API_KEY")
        print("例如：export OPENAI_API_KEY='your-api-key-here'")
        return
    
    print(f"🚀 快速测试 - {test_name}")
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print(f"处理范围: 第{start_row+1}行到第{end_row}行")
    print("开始处理...\n")
    
    # 调用处理函数
    try:
        process_csv_file(
            input_file=input_file,
            output_file=output_file,
            api_key=api_key,
            delay=2.0,  # 2秒延迟
            start_row=start_row,
            end_row=end_row
        )
        print(f"\n✅ 测试完成！结果保存在: {output_file}")
        print(f"可以查看文件内容确认结果")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")

if __name__ == "__main__":
    main()
