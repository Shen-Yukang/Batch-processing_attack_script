#!/usr/bin/env python3
"""
健壮的批处理器 - 具有完整的容错和重试机制
"""

import os
import time
import json
import logging
import subprocess
from datetime import datetime
from typing import List, Dict, Optional
from cost_tracker import CostTracker

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BatchJob:
    """批处理任务类"""
    def __init__(self, name: str, start_row: int, end_row: int, status: str = "pending"):
        self.name = name
        self.start_row = start_row
        self.end_row = end_row
        self.status = status  # pending, running, completed, failed, timeout
        self.attempts = 0
        self.max_attempts = 3
        self.error_message = ""
        self.batch_id = ""
        self.created_at = datetime.now()
        self.completed_at = None

    def to_dict(self):
        return {
            "name": self.name,
            "start_row": self.start_row,
            "end_row": self.end_row,
            "status": self.status,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "error_message": self.error_message,
            "batch_id": self.batch_id,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None
        }

class RobustBatchProcessor:
    """健壮的批处理器"""

    def __init__(self, output_dir: str = "batch_results", batch_size: int = 20):
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.jobs: List[BatchJob] = []
        self.status_file = os.path.join(output_dir, "batch_status.json")

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        # 初始化成本跟踪器
        self.cost_tracker = CostTracker(os.path.join(output_dir, "batch_costs.json"))

        # 加载之前的状态（如果存在）
        self.load_status()

    def save_status(self):
        """保存批处理状态"""
        status_data = {
            "last_updated": datetime.now().isoformat(),
            "total_jobs": len(self.jobs),
            "jobs": [job.to_dict() for job in self.jobs]
        }

        with open(self.status_file, 'w', encoding='utf-8') as f:
            json.dump(status_data, f, indent=2, ensure_ascii=False)

    def load_status(self):
        """加载之前的批处理状态"""
        if os.path.exists(self.status_file):
            try:
                with open(self.status_file, 'r', encoding='utf-8') as f:
                    status_data = json.load(f)

                self.jobs = []
                for job_data in status_data.get("jobs", []):
                    job = BatchJob(
                        job_data["name"],
                        job_data["start_row"],
                        job_data["end_row"],
                        job_data["status"]
                    )
                    job.attempts = job_data.get("attempts", 0)
                    job.error_message = job_data.get("error_message", "")
                    job.batch_id = job_data.get("batch_id", "")

                    if job_data.get("created_at"):
                        job.created_at = datetime.fromisoformat(job_data["created_at"])
                    if job_data.get("completed_at"):
                        job.completed_at = datetime.fromisoformat(job_data["completed_at"])

                    self.jobs.append(job)

                logger.info(f"加载了 {len(self.jobs)} 个批处理任务的状态")

            except Exception as e:
                logger.warning(f"加载状态文件失败: {e}")

    def create_jobs(self, start_from: int, end_at: int):
        """创建批处理任务"""
        current = start_from
        job_num = 1

        while current < end_at:
            end = min(current + self.batch_size, end_at)
            job_name = f"batch_{job_num:03d}"

            # 检查是否已存在该任务
            existing_job = next((job for job in self.jobs if job.name == job_name), None)
            if not existing_job:
                job = BatchJob(job_name, current, end)
                self.jobs.append(job)

            current = end
            job_num += 1

        self.save_status()
        logger.info(f"创建了 {len(self.jobs)} 个批处理任务")

    def run_command_with_timeout(self, command: str, timeout: int = 600) -> tuple[bool, str]:
        """运行命令并设置超时"""
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr

        except subprocess.TimeoutExpired:
            return False, f"命令超时 ({timeout}秒)"
        except Exception as e:
            return False, f"命令执行异常: {str(e)}"

    def process_single_job(self, job: BatchJob) -> bool:
        """处理单个批处理任务"""
        job.attempts += 1
        job.status = "running"
        self.save_status()

        logger.info(f"📦 处理任务 {job.name} (第{job.attempts}次尝试)")
        logger.info(f"   行范围: {job.start_row+1}-{job.end_row}")

        # 步骤1: 创建输入文件
        jsonl_file = os.path.join(self.output_dir, f"{job.name}.jsonl")
        create_cmd = f"python create_safe_batch_input.py new_csv/content_CogAgent.csv {jsonl_file} --model gpt-4o-mini --start-row {job.start_row} --end-row {job.end_row}"

        success, output = self.run_command_with_timeout(create_cmd, 120)
        if not success:
            job.status = "failed"
            job.error_message = f"创建输入文件失败: {output}"
            self.save_status()
            logger.error(f"❌ {job.name} 创建输入文件失败: {output}")
            return False

        # 检查文件是否创建
        if not os.path.exists(jsonl_file):
            job.status = "failed"
            job.error_message = "输入文件未创建"
            self.save_status()
            logger.error(f"❌ {job.name} 输入文件未创建")
            return False

        # 步骤2: 提交批处理
        process_cmd = f"python batch_processor.py {jsonl_file} --output-dir {self.output_dir} --check-interval 30"

        success, output = self.run_command_with_timeout(process_cmd, 600)
        if success:
            job.status = "completed"
            job.completed_at = datetime.now()
            job.error_message = ""

            # 记录成本（估算）
            num_requests = job.end_row - job.start_row
            cost_estimate = self.cost_tracker.estimate_batch_cost(num_requests)
            cost_estimate["actual_completed"] = num_requests
            self.cost_tracker.record_batch_cost(job.name, job.batch_id, cost_estimate)

            self.save_status()
            logger.info(f"✅ {job.name} 完成 (估算成本: ${cost_estimate['batch_cost']:.4f})")
            return True
        else:
            if "超时" in output:
                job.status = "timeout"
                job.error_message = f"处理超时: {output}"
            else:
                job.status = "failed"
                job.error_message = f"处理失败: {output}"

            self.save_status()
            logger.error(f"❌ {job.name} 失败: {output}")
            return False

    def process_all_jobs(self, retry_failed: bool = True):
        """处理所有批处理任务"""
        logger.info("=" * 80)
        logger.info(f"开始处理 {len(self.jobs)} 个批处理任务")
        logger.info("=" * 80)

        for i, job in enumerate(self.jobs, 1):
            # 跳过已完成的任务
            if job.status == "completed":
                logger.info(f"⏭️  跳过已完成的任务 {job.name}")
                continue

            # 检查重试次数
            if job.attempts >= job.max_attempts:
                logger.warning(f"⚠️  任务 {job.name} 已达到最大重试次数")
                continue

            logger.info(f"\n处理任务 {i}/{len(self.jobs)}: {job.name}")

            success = self.process_single_job(job)

            # 在任务之间添加延迟
            if i < len(self.jobs):
                delay = 30 if success else 60  # 失败后等待更长时间
                logger.info(f"等待 {delay} 秒后处理下一个任务...")
                time.sleep(delay)

        self.print_summary()

    def retry_failed_jobs(self):
        """重试失败的任务"""
        failed_jobs = [job for job in self.jobs if job.status in ["failed", "timeout"] and job.attempts < job.max_attempts]

        if not failed_jobs:
            logger.info("没有需要重试的失败任务")
            return

        logger.info(f"🔄 重试 {len(failed_jobs)} 个失败的任务")

        for job in failed_jobs:
            logger.info(f"\n重试任务: {job.name}")
            self.process_single_job(job)
            time.sleep(60)  # 重试间隔更长

        self.print_summary()

    def print_summary(self):
        """打印处理总结"""
        completed = [job for job in self.jobs if job.status == "completed"]
        failed = [job for job in self.jobs if job.status in ["failed", "timeout"]]
        pending = [job for job in self.jobs if job.status == "pending"]

        logger.info("=" * 80)
        logger.info("批处理总结")
        logger.info("=" * 80)
        logger.info(f"总任务数: {len(self.jobs)}")
        logger.info(f"已完成: {len(completed)}")
        logger.info(f"失败/超时: {len(failed)}")
        logger.info(f"待处理: {len(pending)}")
        logger.info(f"完成率: {len(completed)/len(self.jobs)*100:.1f}%")

        if failed:
            logger.warning("失败的任务:")
            for job in failed:
                logger.warning(f"  {job.name} (第{job.start_row+1}-{job.end_row}行): {job.error_message}")

        if completed:
            logger.info("可以合并结果:")
            logger.info(f"python merge_all_results.py {self.output_dir} new_csv/content_CogAgent.csv final_output.csv")

            # 显示成本总结
            logger.info("\n" + "=" * 80)
            self.cost_tracker.print_cost_report()

def main():
    import sys

    # 解析命令行参数
    start_from = 0  # 默认从130开始
    end_at = 20      # 默认到172结束
    batch_size = 20   # 默认每批20行

    if len(sys.argv) > 1:
        start_from = int(sys.argv[1])
    if len(sys.argv) > 2:
        end_at = int(sys.argv[2])
    if len(sys.argv) > 3:
        batch_size = int(sys.argv[3])

    # 创建处理器
    processor = RobustBatchProcessor("batch_results_20250524_224700", batch_size)

    # 创建任务（如果还没有）
    if not processor.jobs:
        processor.create_jobs(start_from, end_at)

    # 处理所有任务
    processor.process_all_jobs()

    # 询问是否重试失败的任务
    failed_jobs = [job for job in processor.jobs if job.status in ["failed", "timeout"] and job.attempts < job.max_attempts]
    if failed_jobs:
        response = input(f"\n发现 {len(failed_jobs)} 个失败任务，是否重试？(y/N): ")
        if response.lower() == 'y':
            processor.retry_failed_jobs()

if __name__ == "__main__":
    main()
