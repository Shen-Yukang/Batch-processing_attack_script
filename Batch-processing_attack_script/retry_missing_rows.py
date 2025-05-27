#!/usr/bin/env python3
"""
专门处理缺失行的重试脚本
"""

import os
import sys
import json
import time
import base64
import logging
import argparse
import pandas as pd
from datetime import datetime
from typing import List
from robust_batch_processor import RobustBatchProcessor, BatchJob

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_missing_rows(missing_file: str) -> List[int]:
    """读取缺失行文件"""
    missing_rows = []
    try:
        with open(missing_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and line.isdigit():
                    # 转换为0-indexed
                    missing_rows.append(int(line) - 1)

        logger.info(f"从 {missing_file} 读取到 {len(missing_rows)} 个缺失行")
        return sorted(missing_rows)
    except Exception as e:
        logger.error(f"读取缺失行文件失败: {e}")
        return []

def find_correct_csv_file():
    """自动查找正确的CSV文件"""
    possible_files = [
        "_csvs/content_MMSafeBench_cleaned.csv",
        "_csvs/content_FigStep.csv",
        "_csvs/content_Jailbreak28k.csv",
        "content_MMSafeBench_cleaned.csv",
        "content_FigStep.csv",
        "content_Jailbreak28k.csv"
    ]

    for file_path in possible_files:
        if os.path.exists(file_path):
            logger.info(f"找到CSV文件: {file_path}")
            return file_path

    return None

def detect_csv_from_batch_input(batch_dir):
    """从批处理输入文件中检测使用的CSV文件"""
    try:
        # 查找第一个批处理输入文件
        for file in os.listdir(batch_dir):
            if file.endswith('.jsonl') and file.startswith('batch_'):
                jsonl_path = os.path.join(batch_dir, file)

                # 读取第一行来分析
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline()
                    if first_line:
                        data = json.loads(first_line)

                        # 从请求中提取信息
                        if 'body' in data and 'messages' in data['body']:
                            messages = data['body']['messages']
                            if messages and 'content' in messages[0]:
                                content = messages[0]['content']

                                # 检查是否包含图片
                                for item in content:
                                    if item.get('type') == 'image_url':
                                        logger.info("检测到这是图片处理任务")
                                        return find_correct_csv_file()
                break
    except Exception as e:
        logger.warning(f"无法从批处理输入检测CSV文件: {e}")

    return None

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
        logger.info(f"🔄 [步骤1/4] 开始创建批处理输入文件: {output_file}")
        logger.info(f"📊 需要处理 {len(missing_rows)} 行数据")

        # 读取原始CSV
        logger.info(f"📖 [步骤1.1] 读取CSV文件: {processor.input_csv}")
        df = pd.read_csv(processor.input_csv)
        logger.info(f"✅ CSV文件读取成功，总行数: {len(df)}")

        # 创建批处理输入
        logger.info(f"🏗️  [步骤1.2] 开始构建批处理请求...")
        batch_requests = []
        processed_count = 0
        skipped_count = 0

        for i, row_idx in enumerate(missing_rows, 1):
            if i % 5 == 0 or i == len(missing_rows):
                logger.info(f"📈 进度: {i}/{len(missing_rows)} ({i/len(missing_rows)*100:.1f}%)")

            if row_idx >= len(df):
                logger.warning(f"⚠️  跳过超出范围的行: {row_idx+1}")
                skipped_count += 1
                continue

            row = df.iloc[row_idx]
            image_path = row['Image Path']
            prompt = row['Content of P*']

            # 检查图片文件是否存在
            if not os.path.exists(image_path):
                logger.warning(f"⚠️  图片文件不存在，跳过行 {row_idx+1}: {image_path}")
                skipped_count += 1
                continue

            # 编码图片为base64
            try:
                logger.debug(f"🖼️  编码图片: {os.path.basename(image_path)} (行 {row_idx+1})")
                with open(image_path, "rb") as image_file:
                    base64_image = base64.b64encode(image_file.read()).decode('utf-8')
                logger.debug(f"✅ 图片编码完成，大小: {len(base64_image)} 字符")
            except Exception as e:
                logger.warning(f"❌ 无法编码图片，跳过行 {row_idx+1}: {e}")
                skipped_count += 1
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
            processed_count += 1

        logger.info(f"📊 处理统计: 成功 {processed_count} 个，跳过 {skipped_count} 个")

        # 写入JSONL文件
        logger.info(f"💾 [步骤1.3] 写入JSONL文件...")
        with open(output_file, 'w', encoding='utf-8') as f:
            for i, request in enumerate(batch_requests, 1):
                f.write(json.dumps(request, ensure_ascii=False) + '\n')
                if i % 10 == 0:
                    logger.debug(f"📝 已写入 {i}/{len(batch_requests)} 个请求")

        file_size = os.path.getsize(output_file) / 1024 / 1024  # MB
        logger.info(f"✅ 批处理输入文件创建成功!")
        logger.info(f"📄 文件: {output_file}")
        logger.info(f"📊 请求数: {len(batch_requests)}")
        logger.info(f"💾 文件大小: {file_size:.2f} MB")

        return len(batch_requests) > 0

    except Exception as e:
        logger.error(f"❌ 创建批处理输入文件失败: {e}")
        import traceback
        logger.error(f"🔍 错误详情: {traceback.format_exc()}")
        return False

def process_missing_rows(processor: RobustBatchProcessor, missing_file: str, batch_size: int = 20, delay: int = 60):
    """处理缺失行"""
    missing_rows = read_missing_rows(missing_file)
    if not missing_rows:
        logger.error("没有找到有效的缺失行")
        return

    logger.info(f"读取到 {len(missing_rows)} 个缺失行")

    # 确定正确的CSV文件
    if not os.path.exists(processor.input_csv):
        logger.warning(f"配置的CSV文件不存在: {processor.input_csv}")

        # 尝试自动检测
        detected_csv = detect_csv_from_batch_input(processor.output_dir)
        if detected_csv:
            processor.input_csv = detected_csv
            logger.info(f"自动检测到CSV文件: {detected_csv}")
        else:
            logger.error("无法确定CSV文件")
            return

    # 验证CSV文件
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
        logger.info(f"\n{'='*80}")
        logger.info(f"🚀 开始处理批次 {i}/{len(batches)}")
        logger.info(f"📊 行号范围: {batch[0]+1}-{batch[-1]+1} (共{len(batch)}行)")
        logger.info(f"⏰ 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*80}")

        # 创建批处理任务名称
        job_name = f"retry_missing_batch_{i:03d}"
        logger.info(f"🏷️  任务名称: {job_name}")

        # 创建输入文件
        jsonl_file = os.path.join(processor.output_dir, f"{job_name}.jsonl")
        logger.info(f"📁 输入文件路径: {jsonl_file}")

        logger.info(f"🔄 [阶段1/4] 创建批处理输入文件...")
        if not create_missing_rows_input_file(processor, batch, jsonl_file):
            logger.error(f"❌ 批次 {i} 创建输入文件失败，跳过")
            failed_batches += 1
            continue

        # 创建批处理任务
        logger.info(f"🔄 [阶段2/4] 创建批处理任务对象...")
        job = BatchJob(job_name, batch[0], batch[-1])

        # 手动处理批次
        job.attempts += 1
        job.status = "running"
        processor.jobs.append(job)
        processor.save_status()
        logger.info(f"✅ 任务对象创建完成，状态已保存")

        logger.info(f"🔄 [阶段3/4] 提交到OpenAI批处理API...")
        logger.info(f"📦 处理任务 {job.name} (第{job.attempts}次尝试)")

        # 提交批处理
        process_cmd = f"python batch_processor.py {jsonl_file} --output-dir {processor.output_dir} --check-interval 30"
        logger.info(f"🖥️  执行命令: {process_cmd}")
        logger.info(f"⏱️  超时设置: 600秒 (10分钟)")
        logger.info(f"🔄 正在提交批处理请求到OpenAI...")

        start_time = datetime.now()
        success, output = processor.run_command_with_timeout(process_cmd, 600)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"⏱️  批处理执行耗时: {duration:.1f}秒")

        if success:
            logger.info(f"🔄 [阶段4/4] 处理成功，更新状态...")
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
            logger.info(f"✅ {job.name} 完成!")
            logger.info(f"💰 估算成本: ${cost_estimate['batch_cost']:.4f}")
            logger.info(f"📊 累计成功批次: {successful_batches}/{i}")

            # 检查结果文件
            result_files = [f for f in os.listdir(processor.output_dir) if f.startswith(f"batch_results_") and job_name in f]
            if result_files:
                logger.info(f"📄 生成结果文件: {result_files[0]}")
            else:
                logger.warning(f"⚠️  未找到结果文件，但任务标记为成功")

        else:
            logger.error(f"❌ 批处理执行失败!")
            logger.error(f"🔍 错误输出: {output}")

            if "超时" in output:
                job.status = "timeout"
                job.error_message = f"处理超时: {output}"
                logger.error(f"⏰ 任务超时 (>600秒)")
            else:
                job.status = "failed"
                job.error_message = f"处理失败: {output}"
                logger.error(f"💥 任务执行失败")

            failed_batches += 1
            processor.save_status()
            logger.error(f"📊 累计失败批次: {failed_batches}/{i}")

            # 询问是否继续
            if i < len(batches):
                continue_prompt = input(f"\n❓ 批次 {i} 处理失败，是否继续处理下一批次？(y/N): ")
                if continue_prompt.lower() != 'y':
                    logger.info("🛑 用户取消后续处理")
                    break

        # 在批次之间添加延迟
        if i < len(batches):
            wait_time = delay if success else 10
            logger.info(f"⏸️  等待 {wait_time} 秒后处理下一批次...")
            logger.info(f"📈 总体进度: {i}/{len(batches)} ({i/len(batches)*100:.1f}%)")

            # 显示倒计时
            for remaining in range(wait_time, 0, -10):
                if remaining <= 10:
                    logger.info(f"⏰ 还有 {remaining} 秒...")
                    time.sleep(remaining)
                    break
                else:
                    logger.info(f"⏰ 还有 {remaining} 秒...")
                    time.sleep(10)

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
    parser = argparse.ArgumentParser(description="处理缺失行的重试脚本")
    parser.add_argument("--batch-dir", default="batch_results_20250525_182528", help="批处理结果目录")
    parser.add_argument("--missing-file", default="missing_rows.txt", help="缺失行文件")
    parser.add_argument("--csv-file", help="指定CSV文件路径（可选）")
    parser.add_argument("--batch-size", type=int, default=20, help="批次大小")
    parser.add_argument("--delay", type=int, default=60, help="批次间延迟（秒）")
    args = parser.parse_args()

    # 检查目录是否存在
    if not os.path.exists(args.batch_dir):
        logger.error(f"批处理目录不存在: {args.batch_dir}")
        return

    # 检查缺失行文件是否存在
    if not os.path.exists(args.missing_file):
        logger.error(f"缺失行文件不存在: {args.missing_file}")
        return

    # 创建处理器
    processor = RobustBatchProcessor(args.batch_dir)

    # 如果指定了CSV文件，使用指定的文件
    if args.csv_file and os.path.exists(args.csv_file):
        processor.input_csv = args.csv_file
        logger.info(f"使用指定的CSV文件: {args.csv_file}")

    # 处理缺失行
    process_missing_rows(processor, args.missing_file, args.batch_size, args.delay)

if __name__ == "__main__":
    main()
