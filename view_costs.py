#!/usr/bin/env python3
"""
查看批处理成本的简单脚本
"""

import os
import sys
from cost_tracker import CostTracker

def main():
    # 默认成本文件路径
    cost_file = "batch_results_20250524_224700/batch_costs.json"
    
    if len(sys.argv) > 1:
        cost_file = sys.argv[1]
    
    if not os.path.exists(cost_file):
        print(f"成本文件不存在: {cost_file}")
        print("请先运行批处理任务")
        return
    
    # 创建成本跟踪器并显示报告
    tracker = CostTracker(cost_file)
    tracker.print_cost_report()
    
    # 显示使用建议
    summary = tracker.get_cost_summary()
    if summary['total_cost'] > 0:
        print("\n💡 成本优化建议:")
        if summary['avg_cost_per_request'] > 0.01:
            print("  - 考虑使用更小的批次大小来减少失败重试成本")
        if summary['completed_batches'] < summary['total_batches']:
            print("  - 有未完成的批次，可能产生额外成本")
        print("  - 批处理比实时API节省50%成本")

if __name__ == "__main__":
    main()
