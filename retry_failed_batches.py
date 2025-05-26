#!/usr/bin/env python3
"""
重试失败批次和处理缺失行的专用脚本
"""

import os
import json
import logging
import argparse
import pandas as pd
from robust_batch_processor import RobustBatchProcessor, BatchJob
import time
from typing import List, Optional
import subprocess

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def show_failed_batches(processor: RobustBatchProcessor):
    """显示失败的批次"""
    failed_jobs = [job for job in processor.jobs if job.status in ["failed", "timeout"]]

    if not failed_jobs:
        logger.info("✅ 没有失败的批次！")
        return []

    logger.info("=" * 80)
    logger.info("失败的批次详情")
    logger.info("=" * 80)

    for i, job in enumerate(failed_jobs, 1):
        logger.info(f"{i}. 任务: {job.name}")
        logger.info(f"   行范围: {job.start_row+1}-{job.end_row}")
        logger.info(f"   状态: {job.status}")
        logger.info(f"   尝试次数: {job.attempts}/{job.max_attempts}")
        logger.info(f"   错误信息: {job.error_message}")
        logger.info(f"   创建时间: {job.created_at}")
        logger.info("-" * 40)

    return failed_jobs

def retry_specific_batches(processor: RobustBatchProcessor, batch_names: list):
    """重试指定的批次"""
    for batch_name in batch_names:
        job = next((job for job in processor.jobs if job.name == batch_name), None)
        if not job:
            logger.error(f"未找到批次: {batch_name}")
            continue

        if job.status == "completed":
            logger.info(f"批次 {batch_name} 已完成，跳过")
            continue

        if job.attempts >= job.max_attempts:
            logger.warning(f"批次 {batch_name} 已达到最大重试次数")
            continue

        logger.info(f"🔄 重试批次: {batch_name}")
        processor.process_single_job(job)

def read_missing_rows(missing_file: str) -> List[int]:
    """从文件中读取缺失行号列表"""
    if not os.path.exists(missing_file):
        logger.error(f"缺失行文件不存在: {missing_file}")
        return []

    missing_rows = []
    try:
        with open(missing_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and line.isdigit():
                    # 将1-indexed转换为0-indexed
                    missing_rows.append(int(line) - 1)
    except Exception as e:
        logger.error(f"读取缺失行文件时出错: {e}")
        return []

    return missing_rows

def create_missing_rows_batches(missing_rows: List[int], batch_size: int = 20) -> List[List[int]]:
    """将缺失行分组为连续的批次"""
    if not missing_rows:
        return []

    # 按行号排序
    missing_rows.sort()

    batches = []
    current_batch = []

    for row in missing_rows:
        if not current_batch:
            current_batch = [row]
        elif len(current_batch) < batch_size and (row - current_batch[-1]) <= 5:
            # 如果当前批次未满且行号相近（差距<=5），加入当前批次
            current_batch.append(row)
        else:
            # 否则开始新批次
            batches.append(current_batch)
            current_batch = [row]

    # 添加最后一个批次
    if current_batch:
        batches.append(current_batch)

    return batches

def create_missing_rows_input_file(processor: RobustBatchProcessor, missing_rows: List[int], output_file: str) -> bool:
    """为缺失行创建批处理输入文件"""
    try:
        import pandas as pd

        # 读取原始CSV
        df = pd.read_csv(processor.input_csv)

        # 创建批处理输入
        batch_requests = []

        for row_idx in missing_rows:
            if row_idx >= len(df):
                logger.warning(f"跳过超出范围的行: {row_idx+1}")
                continue

            row = df.iloc[row_idx]
            image_path = row['Image Path']
            prompt = row['Content of P*']

            # 检查图片文件是否存在
            if not os.path.exists(image_path):
                logger.warning(f"图片文件不存在，跳过行 {row_idx+1}: {image_path}")
                continue

            # 编码图片为base64
            import base64
            try:
                with open(image_path, "rb") as image_file:
                    base64_image = base64.b64encode(image_file.read()).decode('utf-8')
            except Exception as e:
                logger.warning(f"无法编码图片，跳过行 {row_idx+1}: {e}")
                continue

            # 创建请求
            request = {
                "custom_id": f"request-{row_idx+1}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": processor.model,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": prompt
                                },
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/jpeg;base64,{base64_image}"
                                    }
                                }
                            ]
                        }
                    ],
                    "max_tokens": 1000,
                    "temperature": 0.7
                }
            }

            batch_requests.append(request)

        # 写入JSONL文件
        import json
        with open(output_file, 'w', encoding='utf-8') as f:
            for request in batch_requests:
                f.write(json.dumps(request, ensure_ascii=False) + '\n')

        logger.info(f"成功创建批处理输入文件: {output_file}，包含 {len(batch_requests)} 个请求")
        return len(batch_requests) > 0

    except Exception as e:
        logger.error(f"创建批处理输入文件失败: {e}")
        return False

def process_missing_rows(processor: RobustBatchProcessor, missing_file: str, batch_size: int = 20, delay: int = 60):
    """处理缺失行"""
    missing_rows = read_missing_rows(missing_file)
    if not missing_rows:
        logger.error("没有找到有效的缺失行")
        return

    logger.info(f"读取到 {len(missing_rows)} 个缺失行")

    # 加载原始CSV数据
    if not os.path.exists(processor.input_csv):
        logger.error(f"输入CSV文件不存在: {processor.input_csv}")
        return

    try:
        df = pd.read_csv(processor.input_csv)
        total_rows = len(df)
        logger.info(f"成功加载CSV文件，共 {total_rows} 行")

        # 过滤掉超出CSV行数范围的行
        valid_missing_rows = [r for r in missing_rows if r < total_rows]
        if len(valid_missing_rows) < len(missing_rows):
            logger.warning(f"过滤掉 {len(missing_rows) - len(valid_missing_rows)} 个超出CSV范围的行")
            missing_rows = valid_missing_rows
    except Exception as e:
        logger.error(f"读取CSV文件时出错: {e}")
        return

    if not missing_rows:
        logger.error("没有有效的缺失行需要处理")
        return

    # 检查输入CSV文件路径
    logger.info(f"当前使用的输入CSV文件: {processor.input_csv}")

    # 确认一下输入CSV文件路径是否正确
    correct_csv = input(f"\n当前CSV文件是: {processor.input_csv}\n这是正确的文件路径吗? (y/N): ")
    if correct_csv.lower() != 'y':
        new_csv = input("请输入正确的CSV文件路径: ")
        if os.path.exists(new_csv):
            processor.input_csv = new_csv
            logger.info(f"已更新CSV文件路径为: {processor.input_csv}")
        else:
            logger.error(f"文件不存在: {new_csv}")
            return

    # 创建智能批次分组
    batches = create_missing_rows_batches(missing_rows, batch_size)
    logger.info(f"将缺失行分为 {len(batches)} 个智能批次进行处理")

    # 显示批次信息
    for i, batch in enumerate(batches, 1):
        logger.info(f"批次 {i}: 行号 {batch[0]+1}-{batch[-1]+1} (共{len(batch)}行)")

    # 询问是否继续
    response = input(f"\n准备处理 {len(batches)} 个批次，共 {len(missing_rows)} 行。是否继续？(y/N): ")
    if response.lower() != 'y':
        logger.info("用户取消处理")
        return

    successful_batches = 0
    failed_batches = 0

    for i, batch in enumerate(batches, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"处理批次 {i}/{len(batches)}")
        logger.info(f"行号范围: {batch[0]+1}-{batch[-1]+1} (共{len(batch)}行)")
        logger.info(f"{'='*60}")

        # 创建批处理任务名称
        job_name = f"retry_missing_batch_{i:03d}"

        # 创建输入文件
        jsonl_file = os.path.join(processor.output_dir, f"{job_name}.jsonl")

        if not create_missing_rows_input_file(processor, batch, jsonl_file):
            logger.error(f"批次 {i} 创建输入文件失败，跳过")
            failed_batches += 1
            continue

        # 创建批处理任务
        job = BatchJob(job_name, batch[0], batch[-1])

        # 手动处理批次（不使用 process_single_job，因为它会重新创建输入文件）
        job.attempts += 1
        job.status = "running"
        processor.jobs.append(job)
        processor.save_status()

        logger.info(f"📦 处理任务 {job.name} (第{job.attempts}次尝试)")

        # 提交批处理
        process_cmd = f"python batch_processor.py {jsonl_file} --output-dir {processor.output_dir} --check-interval 30"

        success, output = processor.run_command_with_timeout(process_cmd, 600)
        if success:
            job.status = "completed"
            job.completed_at = datetime.now()
            job.error_message = ""
            successful_batches += 1

            # 记录成本（估算）
            num_requests = len(batch)
            cost_estimate = processor.cost_tracker.estimate_batch_cost(num_requests)
            cost_estimate["actual_completed"] = num_requests
            processor.cost_tracker.record_batch_cost(job.name, job.batch_id, cost_estimate)

            processor.save_status()
            logger.info(f"✅ {job.name} 完成 (估算成本: ${cost_estimate['batch_cost']:.4f})")
        else:
            if "超时" in output:
                job.status = "timeout"
                job.error_message = f"处理超时: {output}"
            else:
                job.status = "failed"
                job.error_message = f"处理失败: {output}"

            failed_batches += 1
            processor.save_status()
            logger.error(f"❌ {job.name} 失败: {output}")

            # 询问是否继续
            if i < len(batches):
                continue_prompt = input(f"\n批次 {i} 处理失败，是否继续处理下一批次？(y/N): ")
                if continue_prompt.lower() != 'y':
                    logger.info("用户取消后续处理")
                    break

        # 在批次之间添加延迟
        if i < len(batches):
            wait_time = delay if success else 10
            logger.info(f"等待 {wait_time} 秒后处理下一批次...")
            time.sleep(wait_time)

    # 显示最终结果
    logger.info("\n" + "="*80)
    logger.info("缺失行处理完成总结")
    logger.info("="*80)
    logger.info(f"成功批次: {successful_batches}")
    logger.info(f"失败批次: {failed_batches}")
    logger.info(f"总批次: {len(batches)}")
    logger.info(f"成功率: {successful_batches/len(batches)*100:.1f}%")

    if successful_batches > 0:
        logger.info("\n✅ 部分或全部缺失行处理完成！")
        logger.info("建议运行合并脚本来更新最终输出文件")

    if failed_batches > 0:
        logger.warning(f"\n⚠️  仍有 {failed_batches} 个批次失败，可以稍后重试")

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="重试失败批次和处理缺失行的专用脚本")
    parser.add_argument("--batch-dir", required=False, help="批处理结果目录")
    parser.add_argument("--input-csv", required=False, help="输入CSV文件路径")
    parser.add_argument("--model", default="gpt-4o", help="使用的模型，默认gpt-4o")
    parser.add_argument("--missing-file", default="missing_rows.txt", help="缺失行文件路径")
    parser.add_argument("--process-missing", action="store_true", help="处理缺失行")
    parser.add_argument("--batch-size", type=int, default=20, help="处理缺失行的批次大小")
    parser.add_argument("--delay", type=int, default=60, help="批次间的延迟秒数")
    parser.add_argument("batches", nargs="*", help="要重试的特定批次名称")
    args = parser.parse_args()

    # 自动检测最新的批处理目录
    if not args.batch_dir:
        import glob
        batch_dirs = glob.glob("batch_results_*")
        if batch_dirs:
            # 按修改时间排序，获取最新的
            batch_dirs.sort(key=os.path.getmtime, reverse=True)
            args.batch_dir = batch_dirs[0]
            logger.info(f"自动检测到最新批处理目录: {args.batch_dir}")
        else:
            logger.error("未找到批处理目录，请使用--batch-dir指定")
            return

    # 检查状态文件是否存在
    status_file = os.path.join(args.batch_dir, "batch_status.json")
    if not os.path.exists(status_file):
        logger.error(f"状态文件不存在: {status_file}")
        logger.info(f"请先运行 robust_batch_processor.py --output-dir {args.batch_dir}")
        return

    # 创建处理器并加载状态
    processor = RobustBatchProcessor(args.batch_dir)

    # 如果指定了输入CSV和模型，更新处理器
    if args.input_csv:
        processor.input_csv = args.input_csv
    if args.model:
        processor.model = args.model

    # 处理缺失行
    if args.process_missing:
        process_missing_rows(processor, args.missing_file, args.batch_size, args.delay)
        return

    # 处理命令行参数
    if args.batches:
        # 直接重试指定的批次
        retry_specific_batches(processor, args.batches)
    else:
        # 交互模式
        failed_jobs = show_failed_batches(processor)

        if not failed_jobs:
            # 提示是否处理缺失行
            if os.path.exists(args.missing_file):
                print("\n选项:")
                print("1. 处理缺失行")
                print("2. 查看详细状态")
                print("3. 退出")

                choice = input("\n请选择 (1-3): ").strip()

                if choice == "1":
                    process_missing_rows(processor, args.missing_file, args.batch_size, args.delay)
                elif choice == "2":
                    processor.print_summary()
            return

        print("\n选项:")
        print("1. 重试所有失败的批次")
        print("2. 重试特定的批次")
        print("3. 处理缺失行")
        print("4. 查看详细状态")
        print("5. 退出")

        choice = input("\n请选择 (1-5): ").strip()

        if choice == "1":
            processor.retry_failed_jobs()
        elif choice == "2":
            print("\n可重试的批次:")
            retryable_jobs = [job for job in failed_jobs if job.attempts < job.max_attempts]
            for i, job in enumerate(retryable_jobs, 1):
                print(f"  {i}. {job.name} (第{job.start_row+1}-{job.end_row}行)")

            if retryable_jobs:
                indices = input("请输入要重试的批次编号 (用逗号分隔，如: 1,3,5): ").strip()
                try:
                    selected_indices = [int(x.strip()) - 1 for x in indices.split(",")]
                    selected_batches = [retryable_jobs[i].name for i in selected_indices if 0 <= i < len(retryable_jobs)]

                    if selected_batches:
                        retry_specific_batches(processor, selected_batches)
                    else:
                        logger.error("无效的选择")
                except ValueError:
                    logger.error("输入格式错误")
        elif choice == "3":
            process_missing_rows(processor, args.missing_file, args.batch_size, args.delay)
        elif choice == "4":
            processor.print_summary()

if __name__ == "__main__":
    main()
