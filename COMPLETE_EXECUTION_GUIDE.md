# 完整批处理执行指南

## 概述

这是一个完整的批处理API自动执行方案，包含详细的日志记录、错误处理和重试机制。

## 当前项目状态

根据检查，您的项目包含：
- **CSV文件**: `_csvs/content_MMSafeBench_cleaned.csv` (1257行数据)
- **图片目录**: `image/` (包含大量图片文件)
- **现有批处理结果**: 多个 `batch_results_*` 目录
- **缺失行**: `missing_rows.txt` (301个缺失行)

## 核心脚本文件

### 1. 主要执行脚本
- `complete_batch_execution.py` - 全新的完整执行脚本
- `robust_batch_processor.py` - 健壮的批处理器
- `retry_missing_rows.py` - 专门处理缺失行的脚本

### 2. 支持脚本
- `create_safe_batch_input.py` - 创建批处理输入
- `batch_processor.py` - 批处理提交和监控
- `cost_tracker.py` - 成本跟踪
- `merge_all_results.py` - 结果合并

## 重新执行方案

### 方案1: 完全重新开始（推荐）

```bash
# 1. 清理所有旧结果并重新执行
python complete_batch_execution.py --cleanup --csv-file _csvs/content_MMSafeBench_cleaned.csv

# 2. 或者保留最新的批处理目录作为备份
python complete_batch_execution.py --cleanup --keep-latest --csv-file _csvs/content_MMSafeBench_cleaned.csv
```

### 方案2: 只处理缺失行

```bash
# 处理现有的缺失行
python retry_missing_rows.py --csv-file _csvs/content_MMSafeBench_cleaned.csv
```

### 方案3: 自定义范围执行

```bash
# 执行特定行范围
python complete_batch_execution.py \
  --csv-file _csvs/content_MMSafeBench_cleaned.csv \
  --start-row 0 \
  --end-row 500 \
  --batch-size 20 \
  --model gpt-4o-mini
```

## 日志记录特性

### 详细日志
- **文件日志**: 保存在 `logs/batch_execution_YYYYMMDD_HHMMSS.log`
- **控制台日志**: 实时显示执行进度
- **日志级别**: DEBUG级别记录所有细节

### 日志内容包括
- 环境验证结果
- CSV文件分析
- 每个批次的创建和执行状态
- API调用详情
- 错误和异常信息
- 成本统计
- 执行时间统计

## 执行步骤

### 1. 环境准备
```bash
# 确保在正确的目录
cd /Users/shenyukang/StudioSpace/test/task

# 检查Python环境
python --version

# 检查必要的包
pip list | grep openai
```

### 2. 清理旧数据（可选）
```bash
# 手动删除所有旧的批处理目录
rm -rf batch_results_*

# 或者使用脚本自动清理
python complete_batch_execution.py --cleanup
```

### 3. 执行批处理

#### 推荐执行命令：
```bash
python complete_batch_execution.py \
  --cleanup \
  --csv-file _csvs/content_MMSafeBench_cleaned.csv \
  --batch-size 20 \
  --model gpt-4o-mini \
  --log-dir logs
```

### 4. 监控执行
- 观察控制台输出
- 检查日志文件中的详细信息
- 监控API配额使用情况

## 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--csv-file` | CSV文件路径 | 自动检测 |
| `--start-row` | 起始行（0-indexed） | 0 |
| `--end-row` | 结束行 | CSV文件总行数 |
| `--batch-size` | 每批次行数 | 20 |
| `--model` | 使用的模型 | gpt-4o-mini |
| `--cleanup` | 清理旧结果 | False |
| `--keep-latest` | 保留最新目录 | False |
| `--log-dir` | 日志目录 | logs |

## 错误处理

### 自动重试机制
- 每个批次最多重试3次
- 失败后自动延长等待时间
- 支持手动选择重试失败的批次

### 常见错误处理
1. **API配额超限**: 自动等待并重试
2. **网络错误**: 自动重试
3. **图片文件缺失**: 跳过并记录
4. **JSON格式错误**: 重新生成输入文件

## 成本控制

### 成本估算
- 每个请求的预估成本
- 批次成本统计
- 累计成本跟踪

### 成本优化建议
- 使用 `gpt-4o-mini` 模型降低成本
- 适当调整批次大小
- 监控API使用情况

## 结果验证

### 执行完成后检查
```bash
# 查看最新的批处理目录
ls -la batch_results_*/

# 检查成本统计
python view_costs.py

# 合并所有结果
python merge_all_results.py batch_results_YYYYMMDD_HHMMSS _csvs/content_MMSafeBench_cleaned.csv final_output.csv
```

### 验证完整性
```bash
# 检查是否有缺失行
python -c "
import pandas as pd
df = pd.read_csv('final_output.csv')
print(f'最终输出包含 {len(df)} 行')
print(f'原始CSV包含 {len(pd.read_csv('_csvs/content_MMSafeBench_cleaned.csv'))} 行')
"
```

## 故障排除

### 1. 环境问题
- 检查Python版本和依赖包
- 验证OpenAI API密钥
- 确认网络连接

### 2. 文件问题
- 检查CSV文件格式
- 验证图片文件存在性
- 确认文件权限

### 3. API问题
- 检查API配额
- 验证模型可用性
- 监控请求频率

## 预期执行时间

- **总行数**: 1257行
- **批次数**: 约63个批次（每批20行）
- **预计时间**: 2-4小时（取决于API响应速度）
- **预计成本**: $10-20（使用gpt-4o-mini）

## 下一步操作

执行完成后：
1. 检查日志文件确认无错误
2. 验证输出文件完整性
3. 运行结果合并脚本
4. 备份重要结果文件
