# CSV ChatGPT 处理脚本

这个Python脚本可以读取CSV文件，将图片地址和prompt传递给ChatGPT API，并将响应结果添加到CSV的新列中。

## 功能特点

- 支持处理包含图片路径和文本prompt的CSV文件
- 使用OpenAI的GPT-4 Vision模型处理图片和文本
- 自动将ChatGPT响应添加到新的列中
- 支持断点续传（如果程序中断，可以从上次停止的地方继续）
- 支持批量处理和进度保存
- 包含错误处理和日志记录

## 安装依赖

```bash
pip install -r requirements.txt
```

## 设置API密钥

在运行脚本之前，需要设置OpenAI API密钥：

```bash
export OPENAI_API_KEY='your-api-key-here'
```
或者在运行时通过参数传递：

```bash
python process_csv_with_chatgpt.py input.csv output.csv --api-key your-api-key-here
```

## 使用方法

### 方法1：快速测试（推荐新手使用）

```bash
# 快速测试第1行
python quick_test.py

# 快速测试前5行
python quick_test.py 5

# 快速测试第1行（明确指定）
python quick_test.py first
```

### 方法2：交互式测试

```bash
# 运行交互式脚本，可以选择测试任意行或范围
python run_example.py
```

这个脚本会显示CSV预览，然后让您选择：
- 测试单行（指定行号）
- 测试前N行
- 测试指定范围
- 查看更多CSV预览

### 方法3：使用命令行参数

```bash
# 基本使用
python process_csv_with_chatgpt.py new_csv/content_CogAgent.csv output.csv

# 指定延迟和处理范围
python process_csv_with_chatgpt.py new_csv/content_CogAgent.csv output.csv --delay 2.0 --start-row 0 --end-row 10

# 查看所有参数
python process_csv_with_chatgpt.py --help
```

## 参数说明

- `input_file`: 输入CSV文件路径
- `output_file`: 输出CSV文件路径
- `--api-key`: OpenAI API密钥（可选，也可通过环境变量设置）
- `--delay`: API调用之间的延迟时间（秒），默认1.0秒
- `--start-row`: 开始处理的行号（从0开始），默认0
- `--end-row`: 结束处理的行号（不包含），默认处理到最后
- `--model`: 使用的模型，默认'gpt-4o'

## CSV文件格式要求

输入的CSV文件需要包含以下列：

- `Image Path`: 图片文件的路径
- `Content of P*`: 要发送给ChatGPT的prompt文本

脚本会自动添加一个新列 `ChatGPT Response` 来存储API响应。

## 示例CSV格式

```csv
Category,Image Path,Content of P*
Adversarial Suffix,/path/to/image1.png,"请分析这张图片..."
Health Consultation,/path/to/image2.jpg,"请描述图片中的内容..."
```

## 批处理模式（推荐）

对于大量数据处理，强烈推荐使用OpenAI的Batch API，可以节省50%的成本：

### 一键批处理（最简单）

```bash
# 完整的批处理工作流程
python batch_workflow.py new_csv/content_CogAgent.csv output_batch.csv

# 仅处理前10行
python batch_workflow.py new_csv/content_CogAgent.csv output_batch.csv --end-row 10

# 估算成本（不执行处理）
python batch_workflow.py new_csv/content_CogAgent.csv output_batch.csv --estimate-cost
```

### 分步批处理（高级用户）

```bash
# 1. 创建批处理输入文件
python create_batch_input.py new_csv/content_CogAgent.csv batch_input.jsonl

# 2. 处理批处理（会自动等待完成）
python batch_processor.py batch_input.jsonl --output-dir results

# 3. 合并结果到CSV
python process_batch_results.py merge results/batch_results_*.jsonl --original-csv new_csv/content_CogAgent.csv --output-csv output.csv
```

## 健壮批处理器（推荐大型项目使用）

对于需要处理大量数据的项目，我们提供了具有完整容错和重试机制的健壮批处理器：

### 健壮批处理器特点

- **任务管理**：自动将CSV文件分割成多个小批次任务
- **自动断点续传**：中断后可从上次停止的地方继续
- **错误处理**：自动重试失败的任务（最多3次）
- **状态跟踪**：记录每个任务的状态和错误信息
- **成本估算**：自动计算和记录API使用成本
- **自动检测行数**：自动检测CSV文件的总行数
- **可定制化**：支持自定义模型、批次大小等参数

### 使用健壮批处理器

```bash
# 处理CSV文件全部内容
python robust_batch_processor.py --input-csv _csvs/content_FigStep.csv

# 指定处理范围
python robust_batch_processor.py --input-csv _csvs/content_MMSafeBench_cleaned.csv --start-row 100 --end-row 500

# 指定批次大小和模型
python robust_batch_processor.py --input-csv _csvs/content_Jailbreak28k.csv --batch-size 50 --model gpt-4o

# 指定输出目录
python robust_batch_processor.py --input-csv _csvs/content_FigStep.csv --output-dir my_batch_results
```

### 健壮批处理器参数说明

- `--input-csv`：输入CSV文件路径（必需）
- `--start-row`：开始处理的行号，默认为0
- `--end-row`：结束处理的行号，不指定则处理到文件末尾
- `--batch-size`：每个批次的行数，默认为20
- `--output-dir`：输出目录，默认使用时间戳命名
- `--model`：使用的模型，默认为gpt-4o-mini

### 批处理优势

- **成本节省**: 比实时API便宜50%
- **无频率限制**: 不受API调用频率限制
- **大批量处理**: 一次可处理数万个请求
- **自动重试**: 失败的请求会自动重试

## 注意事项

1. **API费用**: 使用GPT-4o Vision模型会产生费用，批处理模式可节省50%成本
2. **API限制**: 实时模式有API调用频率限制，批处理模式无此限制
3. **图片格式**: 支持常见的图片格式（JPG, PNG等）
4. **文件路径**: 确保图片文件路径正确且文件存在
5. **断点续传**: 如果程序中断，重新运行时会跳过已处理的行
6. **批处理时间**: 批处理通常在24小时内完成，具体时间取决于队列情况

## 错误处理

脚本包含完善的错误处理机制：

- 图片文件不存在时会记录错误信息
- API调用失败时会记录错误并继续处理下一行
- 每10行自动保存进度，避免数据丢失

## 日志输出

脚本会输出详细的处理日志，包括：

- 当前处理的行号和文件名
- API调用状态
- 错误信息
- 进度保存信息

## 批处理建议

对于大量数据的处理：

1. 先用小范围测试（如前5行）
2. 设置合适的延迟时间避免API限制
3. 定期检查输出文件确保处理正常
4. 可以分批处理，使用 `--start-row` 和 `--end-row` 参数

## 示例运行

```bash
# 设置API密钥
export OPENAI_API_KEY='sk-your-api-key-here'

# 快速测试第1行（最简单的开始方式）
python quick_test.py

# 快速测试前5行
python quick_test.py 5

# 交互式选择测试范围
python run_example.py

# 使用命令行测试前5行
python process_csv_with_chatgpt.py new_csv/content_CogAgent.csv test_output.csv --delay 2.0 --end-row 5

# 处理全部数据
python process_csv_with_chatgpt.py new_csv/content_CogAgent.csv full_output.csv --delay 1.5

# 使用健壮批处理器处理大量数据
python robust_batch_processor.py --input-csv _csvs/content_FigStep.csv
```

## 推荐的测试流程

1. **首次使用**：运行 `python quick_test.py` 测试第1行
2. **确认正常**：运行 `python quick_test.py 5` 测试前5行
3. **小规模批处理**：使用 `python batch_workflow.py` 处理少量数据
4. **大规模处理**：使用 `python robust_batch_processor.py` 处理大量数据
