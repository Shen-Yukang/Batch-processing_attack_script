#!/usr/bin/env python3
"""
灵活的测试脚本 - 可以轻松控制测试的行数和范围
"""

import os
import pandas as pd
from process_csv_with_chatgpt import process_csv_file

def show_csv_preview(csv_file, num_rows=10):
    """显示CSV文件的预览"""
    try:
        df = pd.read_csv(csv_file)
        print(f"\nCSV文件预览 ({csv_file}):")
        print(f"总行数: {len(df)}")
        print(f"列名: {list(df.columns)}")
        print("\n前几行数据:")
        for i in range(min(num_rows, len(df))):
            image_path = df.iloc[i]['Image Path']
            prompt = df.iloc[i]['Content of P*']
            category = df.iloc[i]['Category'] if 'Category' in df.columns else 'N/A'
            print(f"  第{i+1}行: {category} | {os.path.basename(image_path)} | {prompt[:50]}...")
        return len(df)
    except Exception as e:
        print(f"读取CSV文件失败: {e}")
        return 0

def get_user_choice():
    """获取用户的测试选择"""
    print("\n请选择测试模式:")
    print("1. 测试单行 (指定行号)")
    print("2. 测试前N行 (指定行数)")
    print("3. 测试指定范围 (起始行到结束行)")
    print("4. 查看CSV预览")
    print("5. 退出")

    choice = input("\n请输入选择 (1-5): ").strip()
    return choice

def main():
    # 配置参数
    input_file = "new_csv/content_CogAgent.csv"  # 输入文件

    # 检查文件是否存在
    if not os.path.exists(input_file):
        print(f"错误：输入文件不存在 - {input_file}")
        print("可用的CSV文件:")
        csv_files = [f for f in os.listdir("new_csv") if f.endswith('.csv') and not f.endswith('_with_chatgpt.csv')]
        for i, f in enumerate(csv_files, 1):
            print(f"  {i}. {f}")
        return

    # 从环境变量获取API密钥
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("错误：请设置环境变量 OPENAI_API_KEY")
        print("例如：export OPENAI_API_KEY='your-api-key-here'")
        return

    # 显示CSV预览
    total_rows = show_csv_preview(input_file)
    if total_rows == 0:
        return

    while True:
        choice = get_user_choice()

        if choice == '1':
            # 测试单行
            try:
                row_num = int(input(f"请输入要测试的行号 (1-{total_rows}): ")) - 1
                if 0 <= row_num < total_rows:
                    start_row = row_num
                    end_row = row_num + 1
                    output_file = f"test_single_row_{row_num+1}.csv"
                    break
                else:
                    print(f"行号必须在 1-{total_rows} 之间")
            except ValueError:
                print("请输入有效的数字")

        elif choice == '2':
            # 测试前N行
            try:
                num_rows = int(input(f"请输入要测试的行数 (1-{total_rows}): "))
                if 1 <= num_rows <= total_rows:
                    start_row = 0
                    end_row = num_rows
                    output_file = f"test_first_{num_rows}_rows.csv"
                    break
                else:
                    print(f"行数必须在 1-{total_rows} 之间")
            except ValueError:
                print("请输入有效的数字")

        elif choice == '3':
            # 测试指定范围
            try:
                start = int(input(f"请输入起始行号 (1-{total_rows}): ")) - 1
                end = int(input(f"请输入结束行号 (1-{total_rows}): "))
                if 0 <= start < end <= total_rows:
                    start_row = start
                    end_row = end
                    output_file = f"test_rows_{start+1}_to_{end}.csv"
                    break
                else:
                    print("请确保起始行号 < 结束行号，且在有效范围内")
            except ValueError:
                print("请输入有效的数字")

        elif choice == '4':
            # 查看CSV预览
            show_csv_preview(input_file, 20)
            continue

        elif choice == '5':
            # 退出
            print("退出程序")
            return

        else:
            print("无效选择，请重试")

    # 处理参数
    delay = 2.0  # API调用间隔（秒）

    print(f"\n配置确认:")
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print(f"API延迟: {delay}秒")
    print(f"处理范围: 第{start_row+1}行到第{end_row}行")

    # 最终确认
    confirm = input("\n确认开始处理？(y/N): ").strip().lower()
    if confirm != 'y':
        print("取消处理")
        return

    print("开始处理...")

    # 调用处理函数
    process_csv_file(
        input_file=input_file,
        output_file=output_file,
        api_key=api_key,
        delay=delay,
        start_row=start_row,
        end_row=end_row
    )

    print(f"\n处理完成！结果保存在: {output_file}")

if __name__ == "__main__":
    main()
