#!/usr/bin/env python3
"""
实现完整的目录结构重组
将核心源代码模块化，分离可执行脚本，整理文档
"""

import os
import shutil
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectoryStructureImplementer:
    def __init__(self, project_root=".", dry_run=True):
        self.project_root = Path(project_root)
        self.dry_run = dry_run
        
        # 目录结构定义
        self.directories = {
            "src": self.project_root / "src",
            "src/core": self.project_root / "src" / "core",
            "src/input": self.project_root / "src" / "input", 
            "src/output": self.project_root / "src" / "output",
            "src/workflow": self.project_root / "src" / "workflow",
            "src/utils": self.project_root / "src" / "utils",
            "scripts": self.project_root / "scripts",
            "docs": self.project_root / "docs",
            "config": self.project_root / "config"
        }
        
        # 文件分类映射
        self.file_mappings = {
            # 核心模块
            "src/core": [
                "batch_processor.py",
                "robust_batch_processor.py", 
                "cost_tracker.py"
            ],
            # 输入处理
            "src/input": [
                "create_batch_input.py",
                "create_safe_batch_input.py"
            ],
            # 输出处理
            "src/output": [
                "process_batch_results.py"
            ],
            # 工作流管理
            "src/workflow": [
                "batch_workflow.py",
                "resume_batch_processing.py"
            ],
            # 可执行脚本
            "scripts": [
                "quick_test.py",
                "view_costs.py",
                "batch_process.py",
                "process_csv_with_chatgpt.py"
            ],
            # 文档
            "docs": [
                "README.md",
                "BATCH_GUIDE.md",
                "PROJECT_RESTRUCTURE_PLAN.md",
                "PROJECT_REVIEW_SUMMARY.md",
                "CORE_FUNCTIONALITY_ANALYSIS.md",
                "CLEANUP_SUMMARY.md"
            ],
            # 配置文件
            "config": [
                "batch_config.conf"
            ]
        }
    
    def create_directories(self):
        """创建目录结构"""
        logger.info("📁 创建目录结构...")
        
        for dir_name, dir_path in self.directories.items():
            if self.dry_run:
                logger.info(f"[DRY RUN] 将创建目录: {dir_path}")
            else:
                dir_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"✅ 创建目录: {dir_path}")
    
    def move_files(self):
        """移动文件到对应目录"""
        logger.info("📦 移动文件到对应目录...")
        
        for target_dir, files in self.file_mappings.items():
            target_path = self.directories.get(target_dir, self.project_root / target_dir)
            
            for file_name in files:
                src_file = self.project_root / file_name
                dst_file = target_path / file_name
                
                if src_file.exists():
                    if self.dry_run:
                        logger.info(f"[DRY RUN] 将移动: {src_file} -> {dst_file}")
                    else:
                        shutil.move(str(src_file), str(dst_file))
                        logger.info(f"📦 移动: {file_name} -> {target_dir}/")
                else:
                    logger.warning(f"⚠️  文件不存在: {src_file}")
    
    def create_init_files(self):
        """创建Python包的__init__.py文件"""
        logger.info("🐍 创建Python包初始化文件...")
        
        python_dirs = [
            "src",
            "src/core", 
            "src/input",
            "src/output",
            "src/workflow",
            "src/utils"
        ]
        
        for dir_name in python_dirs:
            init_file = self.directories[dir_name] / "__init__.py"
            
            if self.dry_run:
                logger.info(f"[DRY RUN] 将创建: {init_file}")
            else:
                with open(init_file, 'w') as f:
                    f.write(f'"""\\n{dir_name.replace("/", ".")} package\\n"""\\n')
                logger.info(f"✅ 创建: {init_file}")
    
    def create_main_entry_script(self):
        """创建主入口脚本"""
        logger.info("🚀 创建主入口脚本...")
        
        main_script_content = '''#!/usr/bin/env python3
"""
ChatGPT批处理系统主入口脚本
"""

import sys
import os
import argparse
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

def main():
    parser = argparse.ArgumentParser(
        description='ChatGPT批处理系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s batch input.csv --output-dir results    # 批处理
  %(prog)s test input.csv                          # 快速测试
  %(prog)s costs                                   # 查看成本
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 批处理命令
    batch_parser = subparsers.add_parser('batch', help='运行批处理')
    batch_parser.add_argument('input_csv', help='输入CSV文件')
    batch_parser.add_argument('--output-dir', help='输出目录')
    batch_parser.add_argument('--model', default='gpt-4o-mini', help='使用的模型')
    batch_parser.add_argument('--batch-size', type=int, default=20, help='批次大小')
    
    # 测试命令
    test_parser = subparsers.add_parser('test', help='快速测试')
    test_parser.add_argument('input_csv', help='输入CSV文件')
    test_parser.add_argument('--rows', type=int, default=1, help='测试行数')
    
    # 成本查看命令
    costs_parser = subparsers.add_parser('costs', help='查看成本')
    costs_parser.add_argument('--output-dir', help='批处理结果目录')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        if args.command == 'batch':
            from workflow.batch_workflow import BatchWorkflow
            workflow = BatchWorkflow(
                input_csv=args.input_csv,
                output_dir=args.output_dir,
                model=args.model,
                batch_size=args.batch_size
            )
            return workflow.run()
            
        elif args.command == 'test':
            from scripts.quick_test import run_quick_test
            return run_quick_test(args.input_csv, args.rows)
            
        elif args.command == 'costs':
            from scripts.view_costs import view_costs
            return view_costs(args.output_dir)
            
    except ImportError as e:
        print(f"错误: 无法导入模块 - {e}")
        print("请确保项目结构正确且所有依赖已安装")
        return 1
    except Exception as e:
        print(f"执行错误: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
'''
        
        main_script = self.project_root / "main.py"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] 将创建主入口脚本: {main_script}")
        else:
            with open(main_script, 'w', encoding='utf-8') as f:
                f.write(main_script_content)
            os.chmod(main_script, 0o755)  # 添加执行权限
            logger.info(f"✅ 创建主入口脚本: {main_script}")
    
    def create_updated_readme(self):
        """创建更新的README文件"""
        logger.info("📚 创建更新的README文件...")
        
        readme_content = '''# ChatGPT批处理系统

专业的ChatGPT Batch API处理系统，支持大规模图片和文本批处理。

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

## 📄 许可证

MIT License
'''
        
        new_readme = self.project_root / "README_NEW.md"
        
        if self.dry_run:
            logger.info(f"[DRY RUN] 将创建新README: {new_readme}")
        else:
            with open(new_readme, 'w', encoding='utf-8') as f:
                f.write(readme_content)
            logger.info(f"✅ 创建新README: {new_readme}")
    
    def implement_structure(self):
        """实现完整的目录结构"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        logger.info(f"🚀 开始实现目录结构 ({mode})...")
        
        if self.dry_run:
            logger.info("⚠️  这是预览模式，不会实际移动文件")
            logger.info("⚠️  要执行实际重组，请使用 --execute 参数")
        
        # 执行重组步骤
        self.create_directories()
        self.move_files()
        self.create_init_files()
        self.create_main_entry_script()
        self.create_updated_readme()
        
        # 统计信息
        total_files = sum(len(files) for files in self.file_mappings.values())
        logger.info("=" * 60)
        logger.info(f"📊 目录结构实现完成 ({mode})")
        logger.info("=" * 60)
        logger.info(f"创建目录: {len(self.directories)} 个")
        logger.info(f"移动文件: {total_files} 个")
        
        if self.dry_run:
            logger.info("🔧 要执行实际重组，请运行:")
            logger.info("   python implement_directory_structure.py --execute")
        else:
            logger.info("✅ 目录结构重组完成！")
            logger.info("🚀 现在可以使用: python main.py --help")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='实现完整的目录结构')
    parser.add_argument('--execute', action='store_true', 
                       help='执行实际重组（默认为预览模式）')
    
    args = parser.parse_args()
    
    implementer = DirectoryStructureImplementer(dry_run=not args.execute)
    implementer.implement_structure()

if __name__ == "__main__":
    main()
