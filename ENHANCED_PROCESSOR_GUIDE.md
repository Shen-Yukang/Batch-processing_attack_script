# 增强版批处理器使用指南

## 🎯 功能概述

增强版 `robust_batch_processor.py` 集成了智能合并功能，提供了完整的批处理解决方案：

### ✨ 新增功能
1. **交互式CSV选择** - 自动发现并选择CSV文件
2. **智能合并** - 自动合并批处理结果
3. **安全命令执行** - 防止shell注入攻击
4. **增强日志记录** - 详细的执行日志
5. **统一输出管理** - 所有结果保存到output目录

### 🔒 安全改进
- 使用列表参数而非字符串执行命令，防止shell注入
- 输入验证和错误处理
- 详细的日志记录用于审计

## 🚀 使用方法

### 1. 交互式模式（推荐）
```bash
python robust_batch_processor.py --interactive
```

这将：
- 自动扫描可用的CSV文件
- 显示文件信息（行数、大小）
- 让您选择要处理的文件
- 显示处理计划和成本估算
- 确认后开始处理

### 2. 命令行模式
```bash
python robust_batch_processor.py --input-csv _csvs/content_FigStep.csv
```

### 3. 自定义参数
```bash
python robust_batch_processor.py --interactive --batch-size 10 --model gpt-4o-mini
```

### 4. 处理特定行范围
```bash
python robust_batch_processor.py --input-csv _csvs/content_FigStep.csv --start-row 0 --end-row 100
```

## 📁 输出结构

```
output/
├── logs/                                    # 详细日志
│   └── batch_processor_20250526_094434.log
├── batch_results_content_FigStep_20250526_094500/  # 批处理结果
│   ├── batch_status.json                   # 任务状态
│   ├── batch_costs.json                    # 成本统计
│   ├── batch_001.jsonl                     # 输入文件
│   └── batch_results_*.jsonl               # 结果文件
└── final_output_content_FigStep.csv        # 最终合并结果
```

## 🔧 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--input-csv` | CSV文件路径 | 交互式选择 |
| `--interactive` | 强制交互式选择 | False |
| `--start-row` | 起始行 | 0 |
| `--end-row` | 结束行 | 文件总行数 |
| `--batch-size` | 批次大小 | 20 |
| `--model` | AI模型 | gpt-4o-mini |

## 📊 执行流程

### 阶段1: 文件选择
- 扫描 `_csvs/`, `data/`, 当前目录
- 显示文件列表和统计信息
- 用户选择目标文件

### 阶段2: 处理计划
- 计算批次数量
- 估算处理成本
- 显示详细计划
- 用户确认执行

### 阶段3: 批处理执行
- 创建输入文件
- 提交到OpenAI API
- 监控执行状态
- 记录成本和日志

### 阶段4: 智能合并
- 自动发现结果文件
- 解析和去重数据
- 生成最终输出文件
- 创建缺失行报告

### 阶段5: 结果报告
- 显示完成统计
- 成本总结
- 文件位置信息

## 🔍 发现的CSV文件

当前系统中发现的CSV文件：
- `_csvs/content_FigStep.csv` (500行, 0.13MB)
- `_csvs/content_Jailbreak28k.csv` (896行, 0.51MB)  
- `_csvs/content_MMSafeBench_cleaned.csv` (1256行, 0.23MB)

## 💡 最佳实践

### 1. 使用交互式模式
- 避免手动输入路径错误
- 获得详细的文件信息
- 确认处理计划

### 2. 监控日志
- 详细日志保存在 `output/logs/`
- 实时查看：`tail -f output/logs/batch_processor_*.log`

### 3. 检查输出
- 最终结果在 `output/final_output_*.csv`
- 缺失行列表在 `*_missing_rows.txt`
- 批处理详情在对应的结果目录

### 4. 成本控制
- 查看成本估算
- 监控API使用情况
- 检查 `batch_costs.json`

## 🛠️ 故障排除

### 常见问题

1. **API密钥未设置**
   ```bash
   export OPENAI_API_KEY="your-key-here"
   ```

2. **CSV文件未找到**
   - 确保文件在 `_csvs/` 目录
   - 使用 `--interactive` 查看可用文件

3. **权限问题**
   ```bash
   chmod +x robust_batch_processor.py
   ```

4. **依赖缺失**
   ```bash
   pip install pandas openai
   ```

### 日志分析
- 查看详细日志：`output/logs/batch_processor_*.log`
- 检查错误模式和执行时间
- 监控API调用状态

## 🔄 与之前版本的区别

| 功能 | 原版本 | 增强版 |
|------|--------|--------|
| CSV选择 | 手动输入 | 交互式选择 |
| 结果合并 | 手动执行 | 自动合并 |
| 输出位置 | 当前目录 | output目录 |
| 命令执行 | shell=True | 安全列表参数 |
| 日志记录 | 基础日志 | 详细分级日志 |
| 错误处理 | 基本处理 | 增强验证 |

## 📞 支持

如果遇到问题：
1. 查看详细日志文件
2. 检查API配额和余额
3. 验证输入文件格式
4. 确认网络连接状态

---

**注意**：增强版保持了与原版本的完全兼容性，同时提供了更好的用户体验和安全性。
