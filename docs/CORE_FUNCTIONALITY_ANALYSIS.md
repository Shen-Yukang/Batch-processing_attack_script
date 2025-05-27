# 核心功能分析报告

## 核心模块分类

### 批处理核心 (优先级: 1)

**描述**: 主要的批处理执行引擎

**文件**:
- ✅ batch_processor.py
  - 主要函数: main, __init__, upload_batch_file
  - 主要类: BatchProcessor
- ✅ robust_batch_processor.py
  - 主要函数: setup_enhanced_logging, get_csv_line_count, find_csv_files
  - 主要类: BatchJob, RobustBatchProcessor

### 输入处理 (优先级: 2)

**描述**: 创建批处理输入文件

**文件**:
- ✅ create_batch_input.py
  - 主要函数: encode_image, create_batch_request, create_batch_input_file
- ✅ create_safe_batch_input.py
  - 主要函数: is_image_valid, encode_image_safe, create_safe_batch_request

### 结果处理 (优先级: 2)

**描述**: 处理和合并批处理结果

**文件**:
- ✅ process_batch_results.py
  - 主要函数: parse_batch_results, merge_results_to_csv, analyze_batch_results

### 工作流管理 (优先级: 2)

**描述**: 完整的批处理工作流

**文件**:
- ✅ batch_workflow.py
  - 主要函数: run_batch_workflow, estimate_cost, main

### 成本跟踪 (优先级: 3)

**描述**: API成本计算和跟踪

**文件**:
- ✅ cost_tracker.py
  - 主要函数: update_costs_from_results, main, __init__
  - 主要类: CostTracker

### 恢复处理 (优先级: 3)

**描述**: 断点续传和恢复处理

**文件**:
- ✅ resume_batch_processing.py
  - 主要函数: main, __init__, analyze_completion_status
  - 主要类: BatchResumer

### 快速测试 (优先级: 4)

**描述**: 快速测试功能

**文件**:
- ✅ quick_test.py
  - 主要函数: main

### 实时处理 (优先级: 4)

**描述**: 实时API调用处理

**文件**:
- ✅ process_csv_with_chatgpt.py
  - 主要函数: process_csv_file, main, __init__
  - 主要类: ChatGPTProcessor

### 工具脚本 (优先级: 5)

**描述**: 查看成本等工具

**文件**:
- ✅ view_costs.py
  - 主要函数: main

## 模块依赖关系

### batch_workflow.py

**外部依赖**:
- argparse
- datetime
- logging
- os
- pandas
- shutil

**本地依赖**:
- batch_processor
- create_batch_input
- process_batch_results

### batch_processor.py

**外部依赖**:
- argparse
- json
- logging
- openai
- os
- sys
- time
- typing

### quick_test.py

**外部依赖**:
- os
- sys

**本地依赖**:
- process_csv_with_chatgpt

### view_costs.py

**外部依赖**:
- os
- sys

**本地依赖**:
- cost_tracker

### cost_tracker.py

**外部依赖**:
- datetime
- glob
- json
- logging
- openai
- os
- sys
- typing

### create_batch_input.py

**外部依赖**:
- argparse
- base64
- json
- logging
- os
- pandas
- typing

### process_csv_with_chatgpt.py

**外部依赖**:
- argparse
- base64
- json
- logging
- openai
- os
- pandas
- requests
- time
- typing

### robust_batch_processor.py

**外部依赖**:
- argparse
- base64
- csv
- datetime
- glob
- json
- logging
- os
- pandas
- re
- shlex
- subprocess
- sys
- time
- traceback
- typing

**本地依赖**:
- cost_tracker

### resume_batch_processing.py

**外部依赖**:
- argparse
- datetime
- json
- logging
- os
- pandas
- subprocess
- sys
- typing

### create_safe_batch_input.py

**外部依赖**:
- argparse
- base64
- json
- logging
- os
- pandas
- typing

### process_batch_results.py

**外部依赖**:
- argparse
- json
- logging
- os
- pandas
- typing

## 推荐的最小核心文件集合

基于功能重要性和依赖关系，推荐保留以下核心文件：

- ✅ **batch_processor.py**: 基础批处理功能
- ✅ **robust_batch_processor.py**: 健壮的批处理器，包含重试和错误处理
- ✅ **create_safe_batch_input.py**: 安全的批处理输入创建
- ✅ **process_batch_results.py**: 结果处理和合并
- ✅ **cost_tracker.py**: 成本跟踪
- ✅ **batch_workflow.py**: 完整工作流
- ✅ **quick_test.py**: 快速测试
- ✅ **requirements.txt**: 依赖管理
- ✅ **README.md**: 项目文档
- ✅ **BATCH_GUIDE.md**: 使用指南

## 可选文件

- ✅ **resume_batch_processing.py**: 断点续传功能
- ✅ **process_csv_with_chatgpt.py**: 实时处理功能
- ✅ **view_costs.py**: 成本查看工具
- ✅ **batch_config.conf**: 配置文件

## 重构建议

1. **合并重复功能**: `batch_processor.py` 和 `robust_batch_processor.py` 可以合并
2. **统一输入处理**: `create_batch_input.py` 和 `create_safe_batch_input.py` 功能重复
3. **模块化重构**: 将核心功能按照功能域重新组织
4. **配置统一**: 使用统一的配置文件管理所有参数
5. **接口标准化**: 为所有核心模块提供统一的接口
