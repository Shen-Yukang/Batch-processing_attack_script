# ChatGPT批处理系统

专业的ChatGPT Batch API处理系统，支持大规模图片和文本批处理。绝大部分由 Augment Code 辅助生成，完整干净的项目组成，用于学习和研究。

## 🚀 快速开始

### 安装依赖
```bash
pip install -r requirements.txt
```

### 设置API密钥
```bash
export OPENAI_API_KEY='your-api-key-here'
```

### 基本使用
```bash
# 批处理
python main.py batch data/input/your_file.csv --output-dir output/results

# 快速测试
python main.py test data/input/your_file.csv --rows 5

# 查看成本
python main.py costs --output-dir output/results
```

## 📁 项目结构

```
chatgpt-batch-processor/
├── main.py                     # 主入口脚本
├── requirements.txt            # 依赖文件
├── src/                        # 核心源代码
│   ├── core/                   # 核心模块
│   │   ├── batch_processor.py  # 批处理引擎
│   │   ├── robust_batch_processor.py  # 健壮批处理器
│   │   └── cost_tracker.py     # 成本跟踪
│   ├── input/                  # 输入处理
│   │   ├── create_batch_input.py
│   │   └── create_safe_batch_input.py
│   ├── output/                 # 输出处理
│   │   └── process_batch_results.py
│   ├── workflow/               # 工作流管理
│   │   ├── batch_workflow.py
│   │   └── resume_batch_processing.py
│   └── utils/                  # 工具函数
├── scripts/                    # 可执行脚本
│   ├── quick_test.py
│   ├── view_costs.py
│   └── process_csv_with_chatgpt.py
├── data/                       # 数据文件
│   └── input/                  # 输入CSV文件
├── output/                     # 输出目录
│   ├── results/                # 批处理结果
│   ├── logs/                   # 日志文件
│   └── temp/                   # 临时文件
├── config/                     # 配置文件
│   └── batch_config.conf
└── docs/                       # 文档
    ├── README.md
    └── BATCH_GUIDE.md
```

## 🎯 核心功能

- ✅ ChatGPT Batch API处理
- ✅ 批处理任务管理和监控  
- ✅ 断点续传和错误恢复
- ✅ 详细的日志系统
- ✅ 成本跟踪和计算
- ✅ 结果下载和合并
- ✅ 快速测试功能

## 📖 详细文档

查看 `docs/` 目录获取详细使用指南和API文档。

## 🔧 开发

项目采用模块化设计，核心功能位于 `src/` 目录，可执行脚本位于 `scripts/` 目录。

## 🤖参考

==LLM==: Claude 4 & claude 3.7
==Agents==: [Augment Code](https://www.augmentcode.com/) & [Cursor](https://www.cursor.com/students)


## 📄 许可证

MIT License
