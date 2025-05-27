#!/usr/bin/env python3
"""
批处理成本跟踪系统
"""

import os
import json
import openai
import logging
from datetime import datetime
from typing import Dict, List, Optional

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CostTracker:
    """成本跟踪器"""
    
    # 模型定价 (per 1M tokens)
    PRICING = {
        # GPT-4o 系列
        "gpt-4o": {
            "input": 2.50,   # $2.50 per 1M input tokens  
            "output": 10.00  # $10.00 per 1M output tokens
        },
        "gpt-4o-2024-05-13": {
            "input": 5.00,   # $5.00 per 1M input tokens
            "output": 15.00  # $15.00 per 1M output tokens
        },
        "gpt-4o-mini": {
            "input": 0.15,   # $0.15 per 1M input tokens
            "output": 0.60   # $0.60 per 1M output tokens
        },
        "gpt-4o-mini-realtime-preview": {
            "input": 0.60,   # $0.60 per 1M input tokens
            "output": 2.40   # $2.40 per 1M output tokens
        },
        "gpt-4o-realtime-preview": {
            "input": 5.00,   # $5.00 per 1M input tokens
            "output": 20.00  # $20.00 per 1M output tokens
        },
        
        # GPT-4.1 系列
        "gpt-4.1": {
            "input": 2.00,   # $2.00 per 1M input tokens
            "output": 8.00   # $8.00 per 1M output tokens
        },
        "gpt-4.1-mini": {
            "input": 0.40,   # $0.40 per 1M input tokens
            "output": 1.60   # $1.60 per 1M output tokens
        },
        "gpt-4.1-nano": {
            "input": 0.10,   # $0.10 per 1M input tokens
            "output": 0.40   # $0.40 per 1M output tokens
        },
        
        # GPT-4.5 系列
        "gpt-4.5-preview": {
            "input": 75.00,  # $75.00 per 1M input tokens
            "output": 150.00 # $150.00 per 1M output tokens
        }
    }
    
    # 批处理折扣
    BATCH_DISCOUNT = 0.5  # batch api 50% 折扣
    
    def __init__(self, cost_file: str = "batch_costs.json"):
        self.cost_file = cost_file
        self.costs = self.load_costs()
    
    def load_costs(self) -> Dict:
        """加载成本记录"""
        if os.path.exists(self.cost_file):
            try:
                with open(self.cost_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载成本文件失败: {e}")
        
        return {
            "total_cost": 0.0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "batches": [],
            "last_updated": datetime.now().isoformat()
        }
    
    def save_costs(self):
        """保存成本记录"""
        self.costs["last_updated"] = datetime.now().isoformat()
        
        with open(self.cost_file, 'w', encoding='utf-8') as f:
            json.dump(self.costs, f, indent=2, ensure_ascii=False)
    
    def estimate_batch_cost(self, num_requests: int, model: str = "gpt-4o-mini") -> Dict:
        """估算批次成本"""
        # 估算每个请求的token使用量
        avg_input_tokens = 1200   # 图片(~1000) + 文本(~200)
        avg_output_tokens = 200   # 平均响应长度
        
        total_input_tokens = num_requests * avg_input_tokens
        total_output_tokens = num_requests * avg_output_tokens
        
        # 计算成本
        pricing = self.PRICING.get(model, self.PRICING["gpt-4o-mini"])
        
        input_cost = (total_input_tokens / 1_000_000) * pricing["input"]
        output_cost = (total_output_tokens / 1_000_000) * pricing["output"]
        regular_cost = input_cost + output_cost
        
        # 批处理折扣
        batch_cost = regular_cost * self.BATCH_DISCOUNT
        savings = regular_cost - batch_cost
        
        return {
            "num_requests": num_requests,
            "model": model,
            "estimated_input_tokens": total_input_tokens,
            "estimated_output_tokens": total_output_tokens,
            "regular_cost": regular_cost,
            "batch_cost": batch_cost,
            "savings": savings,
            "discount_rate": self.BATCH_DISCOUNT
        }
    
    def get_actual_batch_cost(self, batch_id: str, api_key: str) -> Optional[Dict]:
        """获取批次的实际成本"""
        try:
            client = openai.OpenAI(api_key=api_key)
            
            # 获取批次信息
            batch = client.batches.retrieve(batch_id)
            
            if batch.status != "completed":
                logger.warning(f"批次 {batch_id} 未完成，无法获取准确成本")
                return None
            
            # 检查是否有使用统计
            if hasattr(batch, 'request_counts') and batch.request_counts:
                completed_requests = getattr(batch.request_counts, 'completed', 0)
                failed_requests = getattr(batch.request_counts, 'failed', 0)
                total_requests = getattr(batch.request_counts, 'total', 0)
                
                logger.info(f"批次 {batch_id}: {completed_requests}/{total_requests} 完成")
                
                # 估算实际成本（基于完成的请求数）
                if completed_requests > 0:
                    cost_estimate = self.estimate_batch_cost(completed_requests)
                    cost_estimate["actual_completed"] = completed_requests
                    cost_estimate["actual_failed"] = failed_requests
                    cost_estimate["batch_id"] = batch_id
                    return cost_estimate
            
            return None
            
        except Exception as e:
            logger.error(f"获取批次成本失败 {batch_id}: {e}")
            return None
    
    def record_batch_cost(self, batch_name: str, batch_id: str, cost_data: Dict):
        """记录批次成本"""
        batch_record = {
            "batch_name": batch_name,
            "batch_id": batch_id,
            "timestamp": datetime.now().isoformat(),
            **cost_data
        }
        
        self.costs["batches"].append(batch_record)
        
        # 更新总计
        self.costs["total_cost"] += cost_data.get("batch_cost", 0)
        self.costs["total_input_tokens"] += cost_data.get("estimated_input_tokens", 0)
        self.costs["total_output_tokens"] += cost_data.get("estimated_output_tokens", 0)
        
        self.save_costs()
        
        logger.info(f"记录批次成本: {batch_name} = ${cost_data.get('batch_cost', 0):.4f}")
    
    def get_cost_summary(self) -> Dict:
        """获取成本总结"""
        total_batches = len(self.costs["batches"])
        completed_batches = len([b for b in self.costs["batches"] if b.get("actual_completed", 0) > 0])
        
        total_requests = sum(b.get("actual_completed", b.get("num_requests", 0)) for b in self.costs["batches"])
        total_cost = self.costs["total_cost"]
        
        avg_cost_per_request = total_cost / total_requests if total_requests > 0 else 0
        
        return {
            "total_batches": total_batches,
            "completed_batches": completed_batches,
            "total_requests": total_requests,
            "total_cost": total_cost,
            "total_input_tokens": self.costs["total_input_tokens"],
            "total_output_tokens": self.costs["total_output_tokens"],
            "avg_cost_per_request": avg_cost_per_request,
            "last_updated": self.costs["last_updated"]
        }
    
    def print_cost_report(self):
        """打印成本报告"""
        summary = self.get_cost_summary()
        
        logger.info("=" * 80)
        logger.info("💰 批处理成本报告")
        logger.info("=" * 80)
        logger.info(f"总批次数: {summary['total_batches']}")
        logger.info(f"已完成批次: {summary['completed_batches']}")
        logger.info(f"总请求数: {summary['total_requests']}")
        logger.info(f"总成本: ${summary['total_cost']:.4f}")
        logger.info(f"平均每请求成本: ${summary['avg_cost_per_request']:.4f}")
        logger.info(f"总输入tokens: {summary['total_input_tokens']:,}")
        logger.info(f"总输出tokens: {summary['total_output_tokens']:,}")
        
        if self.costs["batches"]:
            logger.info("\n📊 各批次详情:")
            for batch in self.costs["batches"]:
                batch_name = batch.get("batch_name", "unknown")
                cost = batch.get("batch_cost", 0)
                requests = batch.get("actual_completed", batch.get("num_requests", 0))
                timestamp = batch.get("timestamp", "")[:19]  # 只显示日期时间部分
                
                logger.info(f"  {batch_name}: ${cost:.4f} ({requests} 请求) - {timestamp}")
        
        # 计算节省的费用
        total_savings = sum(b.get("savings", 0) for b in self.costs["batches"])
        if total_savings > 0:
            logger.info(f"\n💡 批处理节省: ${total_savings:.4f} (相比实时API)")

def update_costs_from_results(results_dir: str, api_key: str):
    """从结果目录更新成本信息"""
    cost_tracker = CostTracker(os.path.join(results_dir, "batch_costs.json"))
    
    # 查找所有结果文件
    import glob
    result_files = glob.glob(os.path.join(results_dir, "batch_results_*.jsonl"))
    
    logger.info(f"找到 {len(result_files)} 个结果文件")
    
    for result_file in result_files:
        # 从文件名提取批次ID
        filename = os.path.basename(result_file)
        if "batch_results_" in filename:
            batch_id = filename.replace("batch_results_", "").replace(".jsonl", "")
            batch_name = f"batch_{len(cost_tracker.costs['batches'])+1:03d}"
            
            # 检查是否已记录
            existing = next((b for b in cost_tracker.costs["batches"] if b.get("batch_id") == batch_id), None)
            if existing:
                logger.info(f"批次 {batch_id} 已记录，跳过")
                continue
            
            # 获取实际成本
            cost_data = cost_tracker.get_actual_batch_cost(batch_id, api_key)
            if cost_data:
                cost_tracker.record_batch_cost(batch_name, batch_id, cost_data)
            else:
                # 如果无法获取实际成本，计算结果文件中的请求数
                try:
                    with open(result_file, 'r') as f:
                        request_count = sum(1 for line in f)
                    
                    estimated_cost = cost_tracker.estimate_batch_cost(request_count)
                    estimated_cost["batch_id"] = batch_id
                    estimated_cost["actual_completed"] = request_count
                    
                    cost_tracker.record_batch_cost(batch_name, batch_id, estimated_cost)
                    
                except Exception as e:
                    logger.error(f"处理结果文件失败 {result_file}: {e}")
    
    # 打印报告
    cost_tracker.print_cost_report()

def main():
    import sys
    
    if len(sys.argv) < 2:
        print("用法:")
        print("  python cost_tracker.py estimate NUM_REQUESTS [MODEL]  # 估算成本")
        print("  python cost_tracker.py update RESULTS_DIR            # 更新实际成本")
        print("  python cost_tracker.py report [COST_FILE]            # 显示成本报告")
        return
    
    command = sys.argv[1]
    
    if command == "estimate":
        if len(sys.argv) < 3:
            print("请提供请求数量")
            return
        
        num_requests = int(sys.argv[2])
        model = sys.argv[3] if len(sys.argv) > 3 else "gpt-4o-mini"
        
        tracker = CostTracker()
        estimate = tracker.estimate_batch_cost(num_requests, model)
        
        print(f"成本估算 ({num_requests} 请求, {model}):")
        print(f"  估算输入tokens: {estimate['estimated_input_tokens']:,}")
        print(f"  估算输出tokens: {estimate['estimated_output_tokens']:,}")
        print(f"  常规API成本: ${estimate['regular_cost']:.4f}")
        print(f"  批处理成本: ${estimate['batch_cost']:.4f}")
        print(f"  节省费用: ${estimate['savings']:.4f} ({estimate['discount_rate']*100}%)")
    
    elif command == "update":
        if len(sys.argv) < 3:
            print("请提供结果目录")
            return
        
        results_dir = sys.argv[2]
        api_key = os.getenv('OPENAI_API_KEY')
        
        if not api_key:
            print("请设置环境变量 OPENAI_API_KEY")
            return
        
        update_costs_from_results(results_dir, api_key)
    
    elif command == "report":
        cost_file = sys.argv[2] if len(sys.argv) > 2 else "batch_costs.json"
        tracker = CostTracker(cost_file)
        tracker.print_cost_report()

if __name__ == "__main__":
    main()
