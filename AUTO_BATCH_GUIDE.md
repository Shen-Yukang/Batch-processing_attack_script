# 自动批处理脚本使用指南

## 🎯 功能概述

`auto_batch_processor.sh` 是一个智能的批处理自动化脚本，它能够：

1. **智能检测**：自动检查 `_csvs/` 目录下的所有CSV文件
2. **状态判断**：识别已完成处理的文件并跳过
3. **自动处理**：对未处理的文件执行完整的批处理流程
4. **结果合并**：自动合并每个文件的处理结果
5. **状态报告**：提供详细的处理进度和最终总结

## 📁 当前检测到的CSV文件

根据您的项目，脚本将处理以下文件：
- `_csvs/content_FigStep.csv`
- `_csvs/content_Jailbreak28k.csv`
- `_csvs/content_MMSafeBench_cleaned.csv`

## 🚀 快速开始

### 1. 设置API密钥
```bash
export OPENAI_API_KEY="your-api-key-here"
```

### 2. 运行脚本
```bash
./auto_batch_processor.sh
```

### 3. 查看结果
处理完成后，结果将保存在 `final_results/` 目录中：
- `final_output_content_FigStep.csv`
- `final_output_content_Jailbreak28k.csv`
- `final_output_content_MMSafeBench_cleaned.csv`

## 🔧 脚本工作流程

### 阶段1: 环境检查
- ✅ 检查API密钥是否设置
- ✅ 验证必要的Python脚本文件
- ✅ 确认CSV目录存在

### 阶段2: 状态分析
- 🔍 扫描所有CSV文件
- 📊 检查每个文件的处理状态
- ⏭️ 标记已完成的文件（跳过）
- 📝 列出需要处理的文件

### 阶段3: 批处理执行
对每个未完成的文件：
- 📁 创建专用的输出目录
- 🔄 执行批处理任务
- ⏱️ 监控处理进度
- 📊 记录成本和状态

### 阶段4: 结果合并
- 🔗 合并所有批次结果
- ✅ 验证输出完整性
- 📈 计算完成率
- 💾 保存最终文件

### 阶段5: 总结报告
- 📊 显示处理统计
- 💰 汇总成本信息
- 📄 列出所有输出文件
- 📝 生成详细日志

## 📊 输出示例

```bash
================================================================================
🚀 自动批处理脚本启动
================================================================================
[INFO] 2025-05-25 22:30:00 - 检查执行环境
[SUCCESS] 2025-05-25 22:30:00 - 环境检查通过
[INFO] 2025-05-25 22:30:01 - 找到 3 个CSV文件
[INFO] 2025-05-25 22:30:01 - 检查 content_MMSafeBench_cleaned 的处理状态...
[SUCCESS] 2025-05-25 22:30:01 - content_MMSafeBench_cleaned 已处理完成
[INFO] 2025-05-25 22:30:01 - 跳过已处理的文件: content_MMSafeBench_cleaned
[INFO] 2025-05-25 22:30:02 - 检查 content_FigStep 的处理状态...
[INFO] 2025-05-25 22:30:02 - content_FigStep 需要处理
[INFO] 2025-05-25 22:30:03 - 检查 content_Jailbreak28k 的处理状态...
[INFO] 2025-05-25 22:30:03 - content_Jailbreak28k 需要处理

[STEP] 2025-05-25 22:30:03 - 处理计划
[INFO] 2025-05-25 22:30:03 - 需要处理: 2 个文件
[INFO] 2025-05-25 22:30:03 - 跳过已完成: 1 个文件

是否开始处理 2 个文件？(y/N): y

[STEP] 2025-05-25 22:30:05 - 处理文件 1/2: content_FigStep
[STEP] 2025-05-25 22:30:05 - 开始处理 content_FigStep (856 行数据)
[INFO] 2025-05-25 22:30:05 - 创建输出目录: batch_results_content_FigStep_20250525_223005
[INFO] 2025-05-25 22:30:05 - 启动批处理器...
...
[SUCCESS] 2025-05-25 22:45:30 - content_FigStep 批处理完成
[INFO] 2025-05-25 22:45:30 - 合并 content_FigStep 的结果...
[SUCCESS] 2025-05-25 22:45:35 - 结果合并完成: final_results/final_output_content_FigStep.csv
[INFO] 2025-05-25 22:45:35 - 完成率: 856/856 (100%)
[SUCCESS] 2025-05-25 22:45:35 - content_FigStep 处理成功 (完成率: 100%)
```

## ⚙️ 配置选项

可以通过修改 `batch_config.conf` 文件来调整参数：

```bash
# 主要参数
BATCH_SIZE=20              # 每批次行数
MODEL="gpt-4o-mini"        # AI模型
DELAY=60                   # 批次间延迟
COMPLETION_THRESHOLD=90    # 完成率阈值
```

## 📁 目录结构

处理完成后的目录结构：
```
project/
├── _csvs/                          # 输入CSV文件
│   ├── content_FigStep.csv
│   ├── content_Jailbreak28k.csv
│   └── content_MMSafeBench_cleaned.csv
├── final_results/                  # 最终输出文件
│   ├── final_output_content_FigStep.csv
│   ├── final_output_content_Jailbreak28k.csv
│   └── final_output_content_MMSafeBench_cleaned.csv
├── logs/                           # 详细日志
│   └── auto_batch_20250525_223000.log
├── batch_results_*/                # 各文件的批处理目录
└── auto_batch_processor.sh         # 主脚本
```

## 🔍 状态检查逻辑

脚本通过以下方式判断文件是否已处理：

1. **检查最终输出文件**：
   - `final_output_[filename].csv`
   - `complete_final_output_[filename].csv`

2. **分析批处理状态**：
   - 读取 `batch_status.json`
   - 计算完成的任务数量
   - 对比预期的总任务数

3. **完成率评估**：
   - 完成率 ≥ 90% 认为已完成
   - 可通过配置文件调整阈值

## 🛠️ 故障排除

### 常见问题

1. **API密钥未设置**
   ```bash
   export OPENAI_API_KEY="sk-your-key-here"
   ```

2. **权限问题**
   ```bash
   chmod +x auto_batch_processor.sh
   ```

3. **Python依赖缺失**
   ```bash
   pip install openai pandas
   ```

4. **磁盘空间不足**
   - 检查可用空间
   - 清理旧的批处理目录

### 日志查看

详细日志保存在 `logs/auto_batch_*.log`：
```bash
tail -f logs/auto_batch_20250525_223000.log
```

### 手动重试

如果某个文件处理失败，可以手动重试：
```bash
python3 robust_batch_processor.py --input-csv _csvs/filename.csv
```

## 💰 成本估算

- **gpt-4o-mini 模型**：约 $0.15 per 1K tokens
- **每个图片+文本请求**：约 $0.003-0.005
- **1000行数据**：约 $3-5
- **总成本预估**：根据文件大小而定

## 🎯 最佳实践

1. **分批执行**：大文件建议分批处理
2. **监控成本**：定期检查API使用情况
3. **备份数据**：处理前备份重要文件
4. **网络稳定**：确保网络连接稳定
5. **资源监控**：监控磁盘空间和内存使用

## 📞 支持

如果遇到问题：
1. 查看详细日志文件
2. 检查API配额和余额
3. 验证输入文件格式
4. 确认网络连接状态

---

**注意**：此脚本会自动处理所有未完成的CSV文件，请确保有足够的API配额和时间来完成处理。
