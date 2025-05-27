# OpenAI Batch API 使用指南

## 概述

OpenAI Batch API 是处理大量请求的最佳方式，具有以下优势：

- **成本节省**: 比实时API便宜50%
- **无频率限制**: 不受API调用频率限制  
- **大批量处理**: 一次可处理数万个请求
- **自动重试**: 失败的请求会自动重试

## 快速开始

### 1. 一键批处理（推荐）

最简单的方式是使用 `batch_workflow.py`：

```bash
# 设置API密钥
export OPENAI_API_KEY='your-api-key-here'

# 处理整个CSV文件
python batch_workflow.py new_csv/content_CogAgent.csv output_batch.csv

# 仅处理前10行（测试用）
python batch_workflow.py new_csv/content_CogAgent.csv output_batch.csv --end-row 10

# 估算成本（不执行处理）
python batch_workflow.py new_csv/content_CogAgent.csv output_batch.csv --estimate-cost
```

### 2. 快速测试

使用测试脚本验证功能：

```bash
# 测试前3行（包含成本估算）
python test_batch.py

# 仅显示成本估算
python test_batch.py --estimate-only
```

## 详细工作流程

### 步骤1: 创建批处理输入文件

```bash
python create_batch_input.py new_csv/content_CogAgent.csv batch_input.jsonl --end-row 10
```

这会将CSV文件转换为OpenAI Batch API所需的JSONL格式。

### 步骤2: 提交批处理任务

```bash
python batch_processor.py batch_input.jsonl --output-dir results
```

这会：
1. 上传JSONL文件到OpenAI
2. 创建批处理任务
3. 监控处理状态
4. 下载完成的结果

### 步骤3: 合并结果

```bash
python process_batch_results.py merge results/batch_results_*.jsonl \
  --original-csv new_csv/content_CogAgent.csv \
  --output-csv final_output.csv
```

## 成本对比

以处理100个图片+文本请求为例：

| 模式 | 成本 | 时间 | 限制 |
|------|------|------|------|
| 实时API | ~$2.00 | 10-20分钟 | 频率限制 |
| 批处理API | ~$1.00 | 1-24小时 | 无限制 |

## 常用参数

### batch_workflow.py 参数

- `--model`: 模型名称（默认: gpt-4o）
- `--start-row`: 开始行号（默认: 0）
- `--end-row`: 结束行号（默认: 全部）
- `--completion-window`: 完成时间窗口（默认: 24h）
- `--check-interval`: 状态检查间隔秒数（默认: 60）
- `--keep-temp-files`: 保留临时文件
- `--estimate-cost`: 仅估算成本

### 示例命令

```bash
# 处理第100-200行
python batch_workflow.py input.csv output.csv --start-row 100 --end-row 200

# 使用不同模型
python batch_workflow.py input.csv output.csv --model gpt-4o-mini

# 更频繁的状态检查
python batch_workflow.py input.csv output.csv --check-interval 30

# 保留临时文件用于调试
python batch_workflow.py input.csv output.csv --keep-temp-files
```

## 故障排除

### 常见问题

1. **API密钥错误**
   ```bash
   export OPENAI_API_KEY='your-correct-api-key'
   ```

2. **图片文件不存在**
   - 检查CSV中的图片路径是否正确
   - 确保图片文件存在且可读

3. **批处理失败**
   - 检查JSONL文件格式是否正确
   - 验证API密钥权限
   - 查看错误日志

4. **结果合并失败**
   - 确保原始CSV文件存在
   - 检查批处理结果文件路径

### 调试技巧

1. **保留临时文件**
   ```bash
   python batch_workflow.py input.csv output.csv --keep-temp-files
   ```

2. **分析批处理结果**
   ```bash
   python process_batch_results.py analyze results/batch_results_*.jsonl
   ```

3. **小批量测试**
   ```bash
   python batch_workflow.py input.csv output.csv --end-row 5
   ```

## 最佳实践

1. **先测试小批量**: 使用 `--end-row 5` 测试前几行
2. **估算成本**: 使用 `--estimate-cost` 预估费用
3. **分批处理**: 对于超大文件，分批处理避免超时
4. **保留日志**: 使用 `--keep-temp-files` 保留调试信息
5. **监控状态**: 批处理可能需要几小时，耐心等待

## 文件说明

- `batch_workflow.py`: 一键批处理脚本（推荐使用）
- `create_batch_input.py`: 创建批处理输入文件
- `batch_processor.py`: 处理批处理任务
- `process_batch_results.py`: 处理批处理结果
- `test_batch.py`: 批处理功能测试

## 支持

如果遇到问题，请：

1. 检查日志输出中的错误信息
2. 使用 `--keep-temp-files` 保留调试文件
3. 尝试小批量测试
4. 验证API密钥和权限
