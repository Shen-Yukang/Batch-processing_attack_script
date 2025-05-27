# 项目重构计划

## 当前问题分析

### 主要问题：
1. **脚本过多且分散**：根目录有40+个Python脚本，功能重复且难以维护
2. **文档冗余**：多个指南文档重复内容
3. **结果文件混乱**：批处理结果散布在多个目录中
4. **临时文件未清理**：大量__pycache__文件和临时输出文件
5. **图片资源庞大**：image目录包含大量测试图片

## 建议的新目录结构

```
chatgpt-batch-processor/
├── README.md                    # 主要文档
├── requirements.txt             # 依赖文件
├── config/                      # 配置文件
│   ├── batch_config.conf
│   └── models.yaml
├── src/                         # 核心源代码
│   ├── __init__.py
│   ├── core/                    # 核心模块
│   │   ├── __init__.py
│   │   ├── batch_processor.py   # 主要批处理器
│   │   ├── cost_tracker.py      # 成本跟踪
│   │   └── image_utils.py       # 图片处理工具
│   ├── input/                   # 输入处理
│   │   ├── __init__.py
│   │   ├── csv_reader.py        # CSV读取
│   │   └── batch_creator.py     # 批处理输入创建
│   ├── output/                  # 输出处理
│   │   ├── __init__.py
│   │   ├── result_merger.py     # 结果合并
│   │   └── csv_writer.py        # CSV写入
│   ├── workflow/                # 工作流管理
│   │   ├── __init__.py
│   │   ├── batch_workflow.py    # 批处理工作流
│   │   └── resume_handler.py    # 恢复处理
│   └── utils/                   # 工具函数
│       ├── __init__.py
│       ├── logger.py            # 日志配置
│       ├── validators.py        # 验证工具
│       └── file_utils.py        # 文件工具
├── scripts/                     # 可执行脚本
│   ├── batch_process.py         # 主要批处理脚本
│   ├── quick_test.py            # 快速测试
│   ├── resume_processing.py     # 恢复处理
│   └── view_costs.py            # 查看成本
├── data/                        # 数据文件
│   ├── input/                   # 输入CSV文件
│   │   ├── content_FigStep.csv
│   │   ├── content_Jailbreak28k.csv
│   │   └── content_MMSafeBench_cleaned.csv
│   └── samples/                 # 示例图片（精简版）
│       ├── sample1.jpg
│       ├── sample2.jpg
│       └── sample3.jpg
├── output/                      # 输出目录
│   ├── results/                 # 批处理结果
│   ├── logs/                    # 日志文件
│   └── temp/                    # 临时文件
├── tests/                       # 测试文件
│   ├── __init__.py
│   ├── test_batch_processor.py
│   ├── test_cost_tracker.py
│   └── test_utils.py
└── docs/                        # 文档
    ├── USER_GUIDE.md            # 用户指南
    ├── API_REFERENCE.md         # API参考
    └── EXAMPLES.md              # 使用示例
```

## 核心模块重构

### 1. 主要批处理器 (src/core/batch_processor.py)
- 合并 `batch_processor.py` 和 `robust_batch_processor.py`
- 统一接口，支持不同处理模式

### 2. 工作流管理 (src/workflow/)
- 整合 `batch_workflow.py` 和相关工作流脚本
- 提供统一的工作流接口

### 3. 输入输出处理 (src/input/, src/output/)
- 分离输入和输出处理逻辑
- 统一CSV处理和结果合并

### 4. 工具函数 (src/utils/)
- 整合所有工具函数
- 提供统一的日志和验证接口

## 重构步骤

### 第一阶段：清理和整理
1. 删除重复和过时的脚本
2. 清理临时文件和缓存
3. 整理图片资源

### 第二阶段：模块化重构
1. 创建新的目录结构
2. 重构核心模块
3. 统一接口和配置

### 第三阶段：文档和测试
1. 整合文档
2. 添加测试用例
3. 更新使用指南

## 保留的核心脚本

### 需要保留并重构的脚本：
1. `batch_processor.py` → `src/core/batch_processor.py`
2. `robust_batch_processor.py` → 合并到主处理器
3. `cost_tracker.py` → `src/core/cost_tracker.py`
4. `create_safe_batch_input.py` → `src/input/batch_creator.py`
5. `process_batch_results.py` → `src/output/result_merger.py`
6. `batch_workflow.py` → `src/workflow/batch_workflow.py`
7. `resume_batch_processing.py` → `src/workflow/resume_handler.py`

### 可以删除的脚本：
1. 各种调试脚本 (`debug_*.py`)
2. 重复的处理脚本 (`process_*.py`)
3. 临时修复脚本 (`fix_*.py`)
4. 过时的测试脚本

## 配置统一

### 统一配置文件 (config/batch_config.yaml)
```yaml
models:
  default: "gpt-4o-mini"
  available: ["gpt-4o", "gpt-4o-mini"]

batch:
  default_size: 20
  max_size: 100
  delay_between_batches: 60

api:
  timeout: 600
  max_retries: 3
  retry_delay: 30

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file_rotation: true
```

## 主要入口脚本

### scripts/batch_process.py (主要入口)
```python
#!/usr/bin/env python3
"""
ChatGPT批处理系统主入口
"""

import argparse
from src.workflow.batch_workflow import BatchWorkflow

def main():
    parser = argparse.ArgumentParser(description='ChatGPT批处理系统')
    parser.add_argument('input_csv', help='输入CSV文件')
    parser.add_argument('--output-dir', help='输出目录')
    parser.add_argument('--model', default='gpt-4o-mini', help='使用的模型')
    parser.add_argument('--batch-size', type=int, default=20, help='批次大小')
    # ... 其他参数
    
    args = parser.parse_args()
    
    workflow = BatchWorkflow(
        input_csv=args.input_csv,
        output_dir=args.output_dir,
        model=args.model,
        batch_size=args.batch_size
    )
    
    workflow.run()

if __name__ == "__main__":
    main()
```

## 迁移计划

1. **备份当前项目**
2. **创建新目录结构**
3. **逐步迁移核心模块**
4. **测试新结构**
5. **更新文档**
6. **清理旧文件**

这个重构将大大提高项目的可维护性和可扩展性，同时保持所有核心功能。
