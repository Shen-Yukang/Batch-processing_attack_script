#!/usr/bin/env python3
"""
ChatGPT批处理系统主入口脚本
"""

import sys
import os
import argparse
from pathlib import Path
import re
from src.utils.config_loader import parse_config

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

def main():
    config = parse_config('config/batch_config.conf')
    parser = argparse.ArgumentParser(
        description='ChatGPT批处理系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s batch input.csv --output-dir results    # 批处理
  %(prog)s test input.csv                          # 快速测试
  %(prog)s costs                                   # 查看成本
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 批处理命令
    batch_parser = subparsers.add_parser('batch', help='运行批处理')
    batch_parser.add_argument('input_csv', help='输入CSV文件')
    batch_parser.add_argument('--output-dir', help='输出目录', default=config.get('RESULTS_DIR'))
    batch_parser.add_argument('--model', default=config.get('MODEL', 'gpt-4o-mini'), help='使用的模型')
    batch_parser.add_argument('--batch-size', type=int, default=int(config.get('BATCH_SIZE', 20)), help='批次大小')
    
    # 测试命令
    test_parser = subparsers.add_parser('test', help='快速测试')
    test_parser.add_argument('input_csv', help='输入CSV文件')
    test_parser.add_argument('--rows', type=int, default=1, help='测试行数')
    
    # 成本查看命令
    costs_parser = subparsers.add_parser('costs', help='查看成本')
    costs_parser.add_argument('--output-dir', help='批处理结果目录')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        if args.command == 'batch':
            from workflow.batch_workflow import BatchWorkflow
            workflow = BatchWorkflow(
                input_csv=args.input_csv,
                output_dir=args.output_dir,
                model=args.model,
                batch_size=args.batch_size
            )
            return workflow.run()
            
        elif args.command == 'test':
            from scripts.quick_test import run_quick_test
            return run_quick_test(args.input_csv, args.rows)
            
        elif args.command == 'costs':
            from scripts.view_costs import view_costs
            return view_costs(args.output_dir)
            
    except ImportError as e:
        print(f"错误: 无法导入模块 - {e}")
        print("请确保项目结构正确且所有依赖已安装")
        return 1
    except Exception as e:
        print(f"执行错误: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
