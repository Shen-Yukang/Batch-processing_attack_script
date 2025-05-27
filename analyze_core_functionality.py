#!/usr/bin/env python3
"""
核心功能分析脚本
分析项目中的核心功能模块和依赖关系
"""

import os
import ast
import glob
from pathlib import Path
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoreFunctionalityAnalyzer:
    def __init__(self, project_root="."):
        self.project_root = Path(project_root)
        self.imports = defaultdict(set)
        self.functions = defaultdict(list)
        self.classes = defaultdict(list)
        
    def analyze_python_file(self, file_path):
        """分析Python文件的导入、函数和类"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            file_name = os.path.basename(file_path)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.imports[file_name].add(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        self.imports[file_name].add(node.module)
                elif isinstance(node, ast.FunctionDef):
                    self.functions[file_name].append(node.name)
                elif isinstance(node, ast.ClassDef):
                    self.classes[file_name].append(node.name)
                    
        except Exception as e:
            logger.warning(f"无法分析文件 {file_path}: {e}")
    
    def analyze_all_python_files(self):
        """分析所有Python文件"""
        logger.info("🔍 分析所有Python文件...")
        
        python_files = glob.glob(str(self.project_root / "*.py"))
        
        for file_path in python_files:
            self.analyze_python_file(file_path)
        
        logger.info(f"📊 分析了 {len(python_files)} 个Python文件")
    
    def identify_core_modules(self):
        """识别核心模块"""
        logger.info("🎯 识别核心模块...")
        
        # 核心功能模块定义
        core_modules = {
            "批处理核心": {
                "files": ["batch_processor.py", "robust_batch_processor.py"],
                "description": "主要的批处理执行引擎",
                "priority": 1
            },
            "输入处理": {
                "files": ["create_batch_input.py", "create_safe_batch_input.py"],
                "description": "创建批处理输入文件",
                "priority": 2
            },
            "结果处理": {
                "files": ["process_batch_results.py"],
                "description": "处理和合并批处理结果",
                "priority": 2
            },
            "成本跟踪": {
                "files": ["cost_tracker.py"],
                "description": "API成本计算和跟踪",
                "priority": 3
            },
            "工作流管理": {
                "files": ["batch_workflow.py"],
                "description": "完整的批处理工作流",
                "priority": 2
            },
            "恢复处理": {
                "files": ["resume_batch_processing.py"],
                "description": "断点续传和恢复处理",
                "priority": 3
            },
            "快速测试": {
                "files": ["quick_test.py"],
                "description": "快速测试功能",
                "priority": 4
            },
            "实时处理": {
                "files": ["process_csv_with_chatgpt.py"],
                "description": "实时API调用处理",
                "priority": 4
            },
            "工具脚本": {
                "files": ["view_costs.py"],
                "description": "查看成本等工具",
                "priority": 5
            }
        }
        
        return core_modules
    
    def analyze_dependencies(self):
        """分析模块依赖关系"""
        logger.info("🔗 分析模块依赖关系...")
        
        dependencies = {}
        
        for file_name, imports in self.imports.items():
            local_imports = []
            external_imports = []
            
            for imp in imports:
                # 检查是否是本地模块
                if any(imp.startswith(local_mod.replace('.py', '')) for local_mod in self.imports.keys()):
                    local_imports.append(imp)
                else:
                    external_imports.append(imp)
            
            dependencies[file_name] = {
                "local": local_imports,
                "external": external_imports
            }
        
        return dependencies
    
    def generate_core_analysis_report(self):
        """生成核心功能分析报告"""
        logger.info("📄 生成核心功能分析报告...")
        
        core_modules = self.identify_core_modules()
        dependencies = self.analyze_dependencies()
        
        report_file = self.project_root / "CORE_FUNCTIONALITY_ANALYSIS.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# 核心功能分析报告\n\n")
            
            # 核心模块分析
            f.write("## 核心模块分类\n\n")
            
            for category, info in sorted(core_modules.items(), key=lambda x: x[1]['priority']):
                f.write(f"### {category} (优先级: {info['priority']})\n\n")
                f.write(f"**描述**: {info['description']}\n\n")
                f.write("**文件**:\n")
                
                for file in info['files']:
                    file_path = self.project_root / file
                    if file_path.exists():
                        f.write(f"- ✅ {file}\n")
                        
                        # 显示主要功能
                        if file in self.functions:
                            main_functions = self.functions[file][:3]  # 显示前3个函数
                            if main_functions:
                                f.write(f"  - 主要函数: {', '.join(main_functions)}\n")
                        
                        if file in self.classes:
                            f.write(f"  - 主要类: {', '.join(self.classes[file])}\n")
                    else:
                        f.write(f"- ❌ {file} (文件不存在)\n")
                
                f.write("\n")
            
            # 依赖关系分析
            f.write("## 模块依赖关系\n\n")
            
            for file_name, deps in dependencies.items():
                if any(file_name in info['files'] for info in core_modules.values()):
                    f.write(f"### {file_name}\n\n")
                    
                    if deps['external']:
                        f.write("**外部依赖**:\n")
                        for dep in sorted(deps['external']):
                            f.write(f"- {dep}\n")
                        f.write("\n")
                    
                    if deps['local']:
                        f.write("**本地依赖**:\n")
                        for dep in sorted(deps['local']):
                            f.write(f"- {dep}\n")
                        f.write("\n")
            
            # 推荐的最小核心集合
            f.write("## 推荐的最小核心文件集合\n\n")
            f.write("基于功能重要性和依赖关系，推荐保留以下核心文件：\n\n")
            
            essential_files = [
                ("batch_processor.py", "基础批处理功能"),
                ("robust_batch_processor.py", "健壮的批处理器，包含重试和错误处理"),
                ("create_safe_batch_input.py", "安全的批处理输入创建"),
                ("process_batch_results.py", "结果处理和合并"),
                ("cost_tracker.py", "成本跟踪"),
                ("batch_workflow.py", "完整工作流"),
                ("quick_test.py", "快速测试"),
                ("requirements.txt", "依赖管理"),
                ("README.md", "项目文档"),
                ("BATCH_GUIDE.md", "使用指南")
            ]
            
            for file, description in essential_files:
                file_path = self.project_root / file
                status = "✅" if file_path.exists() else "❌"
                f.write(f"- {status} **{file}**: {description}\n")
            
            f.write("\n## 可选文件\n\n")
            optional_files = [
                ("resume_batch_processing.py", "断点续传功能"),
                ("process_csv_with_chatgpt.py", "实时处理功能"),
                ("view_costs.py", "成本查看工具"),
                ("batch_config.conf", "配置文件")
            ]
            
            for file, description in optional_files:
                file_path = self.project_root / file
                status = "✅" if file_path.exists() else "❌"
                f.write(f"- {status} **{file}**: {description}\n")
            
            # 重构建议
            f.write("\n## 重构建议\n\n")
            f.write("1. **合并重复功能**: `batch_processor.py` 和 `robust_batch_processor.py` 可以合并\n")
            f.write("2. **统一输入处理**: `create_batch_input.py` 和 `create_safe_batch_input.py` 功能重复\n")
            f.write("3. **模块化重构**: 将核心功能按照功能域重新组织\n")
            f.write("4. **配置统一**: 使用统一的配置文件管理所有参数\n")
            f.write("5. **接口标准化**: 为所有核心模块提供统一的接口\n")
        
        logger.info(f"📄 核心功能分析报告已保存: {report_file}")
    
    def run_analysis(self):
        """运行完整分析"""
        logger.info("🚀 开始核心功能分析...")
        
        self.analyze_all_python_files()
        self.generate_core_analysis_report()
        
        logger.info("✅ 核心功能分析完成")
        logger.info("📋 请查看 CORE_FUNCTIONALITY_ANALYSIS.md 了解详细分析结果")

def main():
    analyzer = CoreFunctionalityAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()
