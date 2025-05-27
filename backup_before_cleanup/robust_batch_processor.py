#!/usr/bin/env python3
"""
健壮的批处理器 - 具有完整的容错和重试机制
增强版：支持交互式CSV选择、智能合并、安全的命令执行
"""

import os
import time
import json
import logging
import subprocess
import argparse
import glob
import shlex
import pandas as pd
import base64
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from cost_tracker import CostTracker

def setup_enhanced_logging(log_dir: str = "output/logs") -> logging.Logger:
    """设置增强的日志记录"""
    os.makedirs(log_dir, exist_ok=True)

    # 创建日志文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f"batch_processor_{timestamp}.log")

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # 清除现有的handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 创建格式化器
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # 文件handler - 记录所有详细信息
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)

    # 控制台handler - 显示重要信息
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    root_logger.addHandler(console_handler)

    logger = logging.getLogger(__name__)
    logger.info(f"📝 详细日志保存到: {log_file}")

    return logger

# 初始化日志
logger = setup_enhanced_logging()

def get_csv_line_count(csv_path):
    """获取CSV文件的行数"""
    import csv
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # 减去1是因为通常第一行是标题
            return sum(1 for _ in reader) - 1
    except Exception as e:
        logger.error(f"读取CSV文件失败: {e}")
        return 0

def find_csv_files(search_dirs: List[str] = ["_csvs", ".", "data"]) -> List[str]:
    """查找所有可用的CSV文件"""
    csv_files = []

    for search_dir in search_dirs:
        if os.path.exists(search_dir):
            pattern = os.path.join(search_dir, "*.csv")
            found_files = glob.glob(pattern)
            csv_files.extend(found_files)

    # 去重并排序
    csv_files = sorted(list(set(csv_files)))
    return csv_files

def interactive_csv_selection() -> Optional[str]:
    """交互式选择CSV文件"""
    logger.info("🔍 搜索可用的CSV文件...")

    csv_files = find_csv_files()

    if not csv_files:
        logger.error("❌ 未找到任何CSV文件")
        logger.info("💡 请确保CSV文件位于以下目录之一: _csvs/, data/, 或当前目录")
        return None

    logger.info(f"📊 找到 {len(csv_files)} 个CSV文件:")

    # 显示文件列表
    for i, csv_file in enumerate(csv_files, 1):
        try:
            lines = get_csv_line_count(csv_file)
            file_size = os.path.getsize(csv_file) / 1024 / 1024  # MB
            logger.info(f"  {i:2d}. {csv_file}")
            logger.info(f"      📊 {lines} 行数据, 💾 {file_size:.2f} MB")
        except Exception as e:
            logger.warning(f"  {i:2d}. {csv_file} (无法读取: {e})")

    # 用户选择
    while True:
        try:
            choice = input(f"\n请选择CSV文件 (1-{len(csv_files)}) 或输入 'q' 退出: ").strip()

            if choice.lower() == 'q':
                logger.info("🛑 用户取消选择")
                return None

            choice_num = int(choice) - 1
            if 0 <= choice_num < len(csv_files):
                selected_file = csv_files[choice_num]
                logger.info(f"✅ 选择了文件: {selected_file}")
                return selected_file
            else:
                print("❌ 无效选择，请重试")

        except ValueError:
            print("❌ 请输入数字")
        except KeyboardInterrupt:
            logger.info("\n🛑 用户中断选择")
            return None

def safe_command_execution(command_parts: List[str], timeout: int = 600) -> Tuple[bool, str]:
    """安全的命令执行，避免shell注入"""
    try:
        logger.debug(f"🖥️  执行命令: {' '.join(command_parts)}")

        start_time = datetime.now()
        result = subprocess.run(
            command_parts,  # 直接传递列表，不使用shell=True
            capture_output=True,
            text=True,
            timeout=timeout
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.debug(f"⏱️  命令执行耗时: {duration:.1f}秒")

        return result.returncode == 0, result.stdout if result.returncode == 0 else result.stderr

    except subprocess.TimeoutExpired:
        logger.error(f"⏰ 命令执行超时 ({timeout}秒)")
        return False, f"命令超时 ({timeout}秒)"
    except Exception as e:
        logger.error(f"💥 命令执行异常: {str(e)}")
        return False, f"命令执行异常: {str(e)}"

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
    """健壮的批处理器 - 增强版"""

    def __init__(self, input_csv: str, batch_size: int = 50, model: str = "gpt-4o-mini", batch_interval: int = 120, output_base_dir: str = "output"):
        self.input_csv = input_csv
        self.batch_size = batch_size
        self.model = model
        self.batch_interval = batch_interval
        self.output_base_dir = output_base_dir

        # 创建基于CSV文件名和时间戳的输出目录
        csv_basename = os.path.splitext(os.path.basename(input_csv))[0]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_dir = os.path.join(output_base_dir, f"batch_results_{csv_basename}_{timestamp}")

        self.jobs: List[BatchJob] = []
        self.status_file = os.path.join(self.output_dir, "batch_status.json")

        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"📁 输出目录: {self.output_dir}")

        # 初始化成本跟踪器
        self.cost_tracker = CostTracker(os.path.join(self.output_dir, "batch_costs.json"))

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

    def run_safe_command(self, command_parts: List[str], timeout: int = 600) -> Tuple[bool, str]:
        """安全地运行命令并设置超时"""
        logger.info(f"🖥️  执行命令: {' '.join(command_parts)}")
        logger.info(f"⏱️  超时设置: {timeout}秒")

        try:
            start_time = datetime.now()
            logger.info(f"🚀 开始执行命令...")

            result = subprocess.run(
                command_parts,  # 使用列表而不是字符串，避免shell注入
                capture_output=True,
                text=True,
                timeout=timeout
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"⏱️  命令执行耗时: {duration:.1f}秒")
            logger.info(f"🔍 返回码: {result.returncode}")

            # 检查返回码和输出内容
            if result.returncode == 0:
                # 即使返回码为0，也要检查是否有错误信息
                combined_output = result.stdout + result.stderr
                logger.debug(f"📤 标准输出: {result.stdout[:200]}...")
                logger.debug(f"📤 错误输出: {result.stderr[:200]}...")

                # 检查严重错误模式（更精确的匹配）
                critical_error_patterns = [
                    "请提供OpenAI API密钥",
                    "API密钥未设置",
                    "authentication failed",
                    "unauthorized",
                    "invalid api key",
                    "traceback",
                    "exception:",
                    "fatal error",
                    "critical error"
                ]

                # 检查是否有严重错误
                has_critical_error = False
                for pattern in critical_error_patterns:
                    if pattern.lower() in combined_output.lower():
                        logger.error(f"❌ 检测到严重错误: {pattern}")
                        has_critical_error = True
                        break

                # 检查特定的失败模式
                critical_failure_patterns = [
                    "创建输入文件失败",
                    "无法连接到API",
                    "批处理提交失败",
                    "API密钥无效"
                ]

                # 检查可跳过的情况（数据问题，非系统错误）
                skippable_patterns = [
                    "没有有效的请求可以写入"
                ]

                has_critical_failure = False
                for pattern in critical_failure_patterns:
                    if pattern in combined_output:
                        logger.error(f"❌ 检测到严重失败: {pattern}")
                        has_critical_failure = True
                        break

                has_skippable_issue = False
                for pattern in skippable_patterns:
                    if pattern in combined_output:
                        logger.warning(f"⚠️  检测到可跳过的问题: {pattern}")
                        has_skippable_issue = True
                        break

                if has_critical_error or has_critical_failure:
                    return False, combined_output

                # 对于可跳过的问题，如果没有其他严重错误，则认为是成功（但会在日志中记录）
                if has_skippable_issue:
                    logger.info(f"✅ 任务完成，但存在数据问题（已跳过无效数据）")
                    return True, combined_output

                # 检查是否有实际的成功输出
                success_patterns = [
                    "文件上传成功",
                    "批处理创建成功",
                    "批处理已完成",
                    "批处理成功完成",
                    "成功创建批处理输入文件",
                    "成功写入"
                ]
                has_success_indicator = any(pattern in combined_output for pattern in success_patterns)

                if has_success_indicator:
                    logger.info(f"✅ 检测到成功标识")
                    return True, result.stdout
                elif len(combined_output.strip()) == 0:
                    # 输出为空但返回码为0，这是batch_processor.py的正常行为
                    # 它不向stdout输出日志，但会生成结果文件
                    logger.info(f"✅ 命令执行完成，无输出但返回码为0（batch_processor.py正常行为）")
                    return True, result.stdout
                else:
                    # 对于返回码0但没有明确成功标识的情况，检查是否有WARNING但没有ERROR
                    if "WARNING" in combined_output and "ERROR" not in combined_output:
                        logger.info(f"✅ 命令执行完成，有警告但无错误")
                        return True, result.stdout
                    else:
                        logger.info(f"✅ 命令执行完成，返回码为0")
                        return True, result.stdout
            else:
                logger.error(f"❌ 命令执行失败，返回码: {result.returncode}")
                logger.error(f"🔍 错误输出: {result.stderr}")
                return False, result.stderr

        except subprocess.TimeoutExpired:
            logger.error(f"⏰ 命令执行超时 ({timeout}秒)")
            return False, f"命令超时 ({timeout}秒)"
        except Exception as e:
            logger.error(f"💥 命令执行异常: {str(e)}")
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
        create_cmd = [
            "python", "create_safe_batch_input.py",
            self.input_csv, jsonl_file,
            "--model", self.model,
            "--start-row", str(job.start_row),
            "--end-row", str(job.end_row)
        ]

        success, output = self.run_safe_command(create_cmd, 120)
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
        process_cmd = [
            "python", "batch_processor.py", jsonl_file,
            "--output-dir", self.output_dir,
            "--check-interval", "30"
        ]

        success, output = self.run_safe_command(process_cmd, 600)
        if success:
            # 详细记录batch_processor.py的输出
            logger.info(f"📋 batch_processor.py 输出详情:")
            logger.info(f"   输出长度: {len(output)} 字符")

            # 提取batch ID
            import re
            batch_ids = re.findall(r'batch_[a-f0-9]{32}', output)
            current_batch_id = batch_ids[0] if batch_ids else None

            if "批处理成功完成" in output:
                logger.info(f"   ✅ 检测到成功完成标识")
                if current_batch_id:
                    logger.info(f"   🆔 批处理ID: {current_batch_id}")
                    job.batch_id = current_batch_id
            elif "批处理失败" in output:
                logger.warning(f"   ❌ 检测到失败标识")
                if current_batch_id:
                    logger.warning(f"   🆔 失败的批处理ID: {current_batch_id}")
                    job.batch_id = current_batch_id
                    # 自动运行调试分析
                    self._run_batch_debug_analysis(current_batch_id)
            elif current_batch_id:
                logger.info(f"   🆔 检测到批处理ID: {current_batch_id}")
                job.batch_id = current_batch_id

            # 验证结果文件是否真的存在
            result_files = glob.glob(os.path.join(self.output_dir, f"batch_results_*{job.name}*.jsonl"))
            if not result_files:
                # 检查是否有任何新的结果文件
                all_result_files = glob.glob(os.path.join(self.output_dir, "batch_results_*.jsonl"))
                logger.warning(f"⚠️  {job.name} 命令成功但未找到专用结果文件")
                logger.info(f"📁 当前结果文件数量: {len(all_result_files)}")

                # 分析可能的原因
                logger.info(f"🔍 分析可能的原因:")
                if "批处理失败" in output:
                    logger.info(f"   - batch_processor.py 报告批处理失败")
                elif "batch_" not in output:
                    logger.info(f"   - 输出中未找到批处理ID，可能提交失败")
                elif len(output) < 100:
                    logger.info(f"   - 输出过短，可能执行异常")
                else:
                    logger.info(f"   - 可能是OpenAI API端问题或文件命名问题")

                # 检查是否有新生成的结果文件
                if all_result_files:
                    # 检查最新的结果文件是否包含当前批次的数据
                    latest_file = max(all_result_files, key=os.path.getmtime)
                    logger.info(f"📄 检查最新结果文件: {os.path.basename(latest_file)}")

                    # 简单检查文件是否包含当前批次的行号
                    try:
                        with open(latest_file, 'r') as f:
                            content = f.read(1000)  # 读取前1000字符
                            expected_rows = [f"row_{i}" for i in range(job.start_row, min(job.start_row + 5, job.end_row))]
                            found_rows = [row for row in expected_rows if row in content]

                            if found_rows:
                                logger.info(f"✅ 在最新文件中找到当前批次数据: {found_rows}")
                                job.status = "completed"
                                job.completed_at = datetime.now()
                                job.error_message = ""
                            else:
                                logger.error(f"❌ 最新文件中未找到当前批次数据 (期望: {expected_rows[:3]}...)")
                                job.status = "failed"
                                job.error_message = "结果文件中未找到对应数据"
                                self.save_status()
                                return False
                    except Exception as e:
                        logger.error(f"❌ 检查结果文件失败: {e}")
                        job.status = "failed"
                        job.error_message = f"无法验证结果文件: {e}"
                        self.save_status()
                        return False
                else:
                    logger.error(f"❌ {job.name} 未生成任何结果文件")
                    job.status = "failed"
                    job.error_message = "未生成结果文件"
                    self.save_status()
                    return False
            else:
                logger.info(f"✅ 找到结果文件: {[os.path.basename(f) for f in result_files]}")
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
                # 运行详细的失败分析
                self._analyze_batch_failure(job, output)

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
                delay = self.batch_interval if success else self.batch_interval * 2  # 失败后等待更长时间
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
            logger.info("✅ 有已完成的任务，可以进行结果合并")

            # 询问是否自动合并
            try:
                response = input("\n🤔 是否自动合并结果？(Y/n): ").strip().lower()
                if response in ['', 'y', 'yes']:
                    self.smart_merge_results()
                else:
                    logger.info("💡 手动合并命令:")
                    logger.info(f"python merge_all_results.py {self.output_dir} {self.input_csv} final_output.csv")
            except KeyboardInterrupt:
                logger.info("\n💡 手动合并命令:")
                logger.info(f"python merge_all_results.py {self.output_dir} {self.input_csv} final_output.csv")

            # 显示成本总结
            logger.info("\n" + "=" * 80)
            self.cost_tracker.print_cost_report()

    def smart_merge_results(self) -> bool:
        """智能合并批处理结果"""
        logger.info("🔗 开始智能合并批处理结果...")

        try:
            # 创建输出文件名
            csv_basename = os.path.splitext(os.path.basename(self.input_csv))[0]
            output_file = os.path.join(self.output_base_dir, f"final_output_{csv_basename}.csv")

            # 查找所有结果文件
            result_files = self._find_result_files()
            if not result_files:
                logger.error("❌ 未找到任何结果文件")
                return False

            logger.info(f"📄 找到 {len(result_files)} 个结果文件")

            # 读取原始CSV
            logger.info(f"📖 读取原始CSV: {self.input_csv}")
            df_original = pd.read_csv(self.input_csv)
            total_rows = len(df_original)

            # 解析所有结果
            all_results = []
            for file_path in result_files:
                file_results = self._parse_result_file(file_path)
                all_results.extend(file_results)

            logger.info(f"📊 总共解析出 {len(all_results)} 条结果")

            # 创建行号到结果的映射
            row_to_result = {}
            duplicate_count = 0

            for result in all_results:
                row_num = result['row_number']
                if row_num is not None:
                    if row_num in row_to_result:
                        duplicate_count += 1
                        logger.debug(f"🔄 发现重复行 {row_num}，保留最新结果")
                    row_to_result[row_num] = result

            logger.info(f"📊 去重后有效结果: {len(row_to_result)} 条")
            if duplicate_count > 0:
                logger.info(f"🔄 去除重复结果: {duplicate_count} 条")

            # 创建最终输出
            df_output = df_original.copy()
            df_output['AI_Response'] = ''
            df_output['Processing_Status'] = 'Missing'
            df_output['Source_File'] = ''

            # 填入AI响应
            matched_count = 0
            for row_num, result in row_to_result.items():
                # 转换为0-indexed
                idx = row_num - 1

                if 0 <= idx < len(df_output):
                    df_output.loc[idx, 'AI_Response'] = result['response_content']
                    df_output.loc[idx, 'Processing_Status'] = 'Completed'
                    df_output.loc[idx, 'Source_File'] = result['file_source']
                    matched_count += 1
                else:
                    logger.warning(f"⚠️  行号 {row_num} 超出CSV范围")

            # 统计和保存
            completed_rows = len(df_output[df_output['Processing_Status'] == 'Completed'])
            missing_rows = len(df_output[df_output['Processing_Status'] == 'Missing'])
            completion_rate = (completed_rows / total_rows) * 100

            logger.info(f"✅ 成功匹配: {matched_count} 行")
            logger.info(f"📈 完成统计:")
            logger.info(f"   - 总行数: {total_rows}")
            logger.info(f"   - 已完成: {completed_rows}")
            logger.info(f"   - 缺失: {missing_rows}")
            logger.info(f"   - 完成率: {completion_rate:.1f}%")

            # 保存结果
            df_output.to_csv(output_file, index=False, encoding='utf-8')
            file_size = os.path.getsize(output_file) / 1024 / 1024  # MB
            logger.info(f"💾 结果已保存: {output_file}")
            logger.info(f"📄 文件大小: {file_size:.2f} MB")

            # 生成缺失行报告
            if missing_rows > 0:
                missing_file = output_file.replace('.csv', '_missing_rows.txt')
                missing_indices = df_output[df_output['Processing_Status'] == 'Missing'].index + 1

                with open(missing_file, 'w') as f:
                    for idx in missing_indices:
                        f.write(f"{idx}\n")

                logger.warning(f"⚠️  缺失行列表已保存: {missing_file}")

            return True

        except Exception as e:
            logger.error(f"❌ 合并过程中出现错误: {e}")
            import traceback
            logger.error(f"🔍 错误详情: {traceback.format_exc()}")
            return False

    def _run_batch_debug_analysis(self, batch_id: str):
        """运行批处理调试分析"""
        try:
            logger.info(f"🔍 开始调试分析批处理: {batch_id}")

            debug_cmd = [
                "python", "enhanced_batch_debugger.py", batch_id,
                "--output-dir", self.output_dir
            ]

            success, output = self.run_safe_command(debug_cmd, 120)
            if success:
                logger.info(f"✅ 调试分析完成")
                logger.info(f"📋 调试输出: {output}")
            else:
                logger.warning(f"⚠️  调试分析失败: {output}")

        except Exception as e:
            logger.warning(f"⚠️  调试分析异常: {e}")

    def _analyze_batch_failure(self, job: BatchJob, output: str):
        """分析批处理失败的原因"""
        logger.error(f"🔍 分析 {job.name} 失败原因:")

        # 分析输出中的错误信息
        if "quota" in output.lower():
            logger.error(f"   💰 可能原因: API配额不足")
            job.error_message = "API配额不足"
        elif "rate" in output.lower() and "limit" in output.lower():
            logger.error(f"   🚦 可能原因: 速率限制")
            job.error_message = "速率限制"
        elif "api" in output.lower() and "key" in output.lower():
            logger.error(f"   🔑 可能原因: API密钥问题")
            job.error_message = "API密钥问题"
        elif "timeout" in output.lower():
            logger.error(f"   ⏰ 可能原因: 请求超时")
            job.error_message = "请求超时"
        elif "validation" in output.lower():
            logger.error(f"   📝 可能原因: 输入验证失败")
            job.error_message = "输入验证失败"
        else:
            logger.error(f"   ❓ 未知错误，需要详细分析")
            job.error_message = f"未知错误: {output[:200]}"

        # 如果有batch ID，运行详细分析
        if hasattr(job, 'batch_id') and job.batch_id:
            self._run_batch_debug_analysis(job.batch_id)

    def _find_result_files(self) -> List[str]:
        """查找所有批处理结果文件"""
        result_pattern = os.path.join(self.output_dir, "batch_results_*.jsonl")
        result_files = glob.glob(result_pattern)
        return sorted(result_files)

    def _parse_result_file(self, file_path: str) -> List[Dict]:
        """解析单个结果文件"""
        results = []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        data = json.loads(line)

                        # 提取关键信息
                        custom_id = data.get('custom_id', '')
                        response = data.get('response', {})

                        if response and response.get('body'):
                            choices = response['body'].get('choices', [])
                            if choices:
                                content = choices[0].get('message', {}).get('content', '')

                                # 提取行号
                                row_num = None
                                if custom_id.startswith('request-'):
                                    try:
                                        row_num = int(custom_id.split('-')[1])
                                    except:
                                        pass

                                results.append({
                                    'row_number': row_num,
                                    'custom_id': custom_id,
                                    'response_content': content,
                                    'file_source': os.path.basename(file_path)
                                })

                    except json.JSONDecodeError as e:
                        logger.warning(f"⚠️  {os.path.basename(file_path)} 第{line_num}行JSON解析失败: {e}")
                        continue

        except Exception as e:
            logger.error(f"❌ 读取文件失败 {file_path}: {e}")
            return []

        logger.debug(f"📊 {os.path.basename(file_path)}: 解析出 {len(results)} 条结果")
        return results

def main():
    parser = argparse.ArgumentParser(description="健壮的批处理器 - 增强版")
    parser.add_argument("--input-csv", help="输入CSV文件路径（可选，支持交互式选择）")
    parser.add_argument("--start-row", type=int, default=0, help="起始行")
    parser.add_argument("--end-row", type=int, default=None, help="结束行，不指定则使用文件总行数")
    parser.add_argument("--batch-size", type=int, default=50, help="批次大小（推荐50-100以减少API调用频率）")
    parser.add_argument("--model", default="gpt-4o-mini", help="使用的模型（推荐gpt-4o-mini用于批处理）")
    parser.add_argument("--batch-interval", type=int, default=120, help="批次间等待时间（秒），默认120秒")
    parser.add_argument("--interactive", action="store_true", help="强制使用交互式选择")
    args = parser.parse_args()

    logger.info("🚀 健壮的批处理器 - 增强版")
    logger.info("="*80)

    # 选择CSV文件
    input_csv = None

    if args.interactive or not args.input_csv:
        # 交互式选择
        input_csv = interactive_csv_selection()
        if not input_csv:
            logger.error("❌ 未选择CSV文件，退出")
            return 1
    else:
        # 使用命令行参数
        input_csv = args.input_csv
        if not os.path.exists(input_csv):
            logger.error(f"❌ 输入文件不存在: {input_csv}")
            logger.info("💡 使用 --interactive 参数进行交互式选择")
            return 1

    # 显示选择的文件信息
    logger.info(f"📊 选择的CSV文件: {input_csv}")
    total_lines = get_csv_line_count(input_csv)
    file_size = os.path.getsize(input_csv) / 1024 / 1024  # MB
    logger.info(f"📈 文件统计: {total_lines} 行数据, {file_size:.2f} MB")

    # 如果未指定结束行，使用文件总行数
    if args.end_row is None:
        args.end_row = total_lines
        logger.info(f"📊 将处理全部 {args.end_row} 行数据")
    else:
        logger.info(f"📊 将处理第 {args.start_row+1} 到 {args.end_row} 行")

    # 显示处理计划
    total_batches = (args.end_row - args.start_row + args.batch_size - 1) // args.batch_size
    estimated_cost = total_batches * args.batch_size * 0.003  # 粗略估算

    logger.info("📋 处理计划:")
    logger.info(f"   - 批次大小: {args.batch_size}")
    logger.info(f"   - 预计批次数: {total_batches}")
    logger.info(f"   - 使用模型: {args.model}")
    logger.info(f"   - 估算成本: ${estimated_cost:.2f}")

    # 确认执行
    try:
        response = input("\n🤔 确认开始处理？(Y/n): ").strip().lower()
        if response not in ['', 'y', 'yes']:
            logger.info("🛑 用户取消处理")
            return 0
    except KeyboardInterrupt:
        logger.info("\n🛑 用户中断")
        return 0

    # 创建处理器
    processor = RobustBatchProcessor(input_csv, args.batch_size, args.model, args.batch_interval)

    # 创建任务（如果还没有）
    if not processor.jobs:
        processor.create_jobs(args.start_row, args.end_row)

    # 处理所有任务
    processor.process_all_jobs()

    # 询问是否重试失败的任务
    failed_jobs = [job for job in processor.jobs if job.status in ["failed", "timeout"] and job.attempts < job.max_attempts]
    if failed_jobs:
        try:
            response = input(f"\n🔄 发现 {len(failed_jobs)} 个失败任务，是否重试？(y/N): ")
            if response.lower() == 'y':
                processor.retry_failed_jobs()
        except KeyboardInterrupt:
            logger.info("\n🛑 跳过重试")

    logger.info("🎉 批处理器执行完成")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
