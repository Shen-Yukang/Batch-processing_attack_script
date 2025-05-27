#!/usr/bin/env python3
"""
项目清理脚本
用于清理和重组ChatGPT批处理项目的目录结构
"""

import os
import shutil
import glob
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProjectCleaner:
    def __init__(self, project_root="."):
        self.project_root = Path(project_root)
        self.backup_dir = self.project_root / "backup_before_cleanup"
        
    def create_backup(self):
        """创建备份"""
        logger.info("🔄 创建项目备份...")
        
        if self.backup_dir.exists():
            shutil.rmtree(self.backup_dir)
        
        # 备份重要文件
        important_files = [
            "batch_processor.py",
            "robust_batch_processor.py", 
            "cost_tracker.py",
            "create_safe_batch_input.py",
            "process_batch_results.py",
            "batch_workflow.py",
            "resume_batch_processing.py",
            "requirements.txt",
            "README.md"
        ]
        
        self.backup_dir.mkdir(exist_ok=True)
        
        for file in important_files:
            src = self.project_root / file
            if src.exists():
                shutil.copy2(src, self.backup_dir / file)
                logger.info(f"✅ 备份: {file}")
        
        # 备份配置文件
        config_files = glob.glob(str(self.project_root / "*.conf"))
        for config_file in config_files:
            shutil.copy2(config_file, self.backup_dir)
            logger.info(f"✅ 备份配置: {os.path.basename(config_file)}")
        
        logger.info(f"📁 备份完成，保存在: {self.backup_dir}")
    
    def clean_cache_files(self):
        """清理缓存文件"""
        logger.info("🧹 清理缓存文件...")
        
        # 清理__pycache__目录
        pycache_dirs = glob.glob(str(self.project_root / "**/__pycache__"), recursive=True)
        for cache_dir in pycache_dirs:
            shutil.rmtree(cache_dir)
            logger.info(f"🗑️  删除缓存: {cache_dir}")
        
        # 清理.pyc文件
        pyc_files = glob.glob(str(self.project_root / "**/*.pyc"), recursive=True)
        for pyc_file in pyc_files:
            os.remove(pyc_file)
            logger.info(f"🗑️  删除: {pyc_file}")
        
        # 清理.DS_Store文件 (macOS)
        ds_store_files = glob.glob(str(self.project_root / "**/.DS_Store"), recursive=True)
        for ds_file in ds_store_files:
            os.remove(ds_file)
            logger.info(f"🗑️  删除: {ds_file}")
    
    def identify_redundant_scripts(self):
        """识别冗余脚本"""
        logger.info("🔍 识别冗余脚本...")
        
        # 可以删除的脚本模式
        delete_patterns = [
            "debug_*.py",
            "test_*.py",
            "fix_*.py", 
            "check_*.py",
            "analyze_*.py",
            "diagnose_*.py",
            "enhanced_*.py",
            "intelligent_*.py",
            "smart_*.py",
            "optimize_*.py",
            "monitor_*.py",
            "cleanup_*.py",
            "correct_*.py",
            "finish_*.py",
            "retry_*.py",
            "find_*.py"
        ]
        
        redundant_scripts = []
        for pattern in delete_patterns:
            matches = glob.glob(str(self.project_root / pattern))
            redundant_scripts.extend(matches)
        
        # 特定的冗余脚本
        specific_redundant = [
            "simple_test.py",
            "test_script.py", 
            "run_example.py",
            "complete_batch_execution.py",
            "process_all_batches.py",
            "process_remaining_batches.py",
            "merge_all_results.py",
            "auto_batch_processor.sh",
            "test_environment.sh"
        ]
        
        for script in specific_redundant:
            script_path = self.project_root / script
            if script_path.exists():
                redundant_scripts.append(str(script_path))
        
        logger.info(f"📋 发现 {len(redundant_scripts)} 个冗余脚本:")
        for script in redundant_scripts:
            logger.info(f"   - {os.path.basename(script)}")
        
        return redundant_scripts
    
    def identify_redundant_outputs(self):
        """识别冗余输出文件"""
        logger.info("🔍 识别冗余输出文件...")
        
        redundant_outputs = []
        
        # 旧的批处理结果目录
        old_batch_dirs = glob.glob(str(self.project_root / "batch_results_*"))
        redundant_outputs.extend(old_batch_dirs)
        
        # 临时CSV文件
        temp_csvs = [
            "current_status.csv",
            "new_output.csv", 
            "output.csv",
            "final_output*.csv",
            "complete_final_output.csv",
            "quick_test_result.csv",
            "resume_data_*.csv"
        ]
        
        for pattern in temp_csvs:
            matches = glob.glob(str(self.project_root / pattern))
            redundant_outputs.extend(matches)
        
        # 临时JSONL文件
        temp_jsonls = glob.glob(str(self.project_root / "*.jsonl"))
        redundant_outputs.extend(temp_jsonls)
        
        # 临时JSON文件
        temp_jsons = glob.glob(str(self.project_root / "batch_analysis_*.json"))
        redundant_outputs.extend(temp_jsons)
        
        # 临时文本文件
        temp_txts = glob.glob(str(self.project_root / "*missing_rows*.txt"))
        redundant_outputs.extend(temp_txts)
        
        logger.info(f"📋 发现 {len(redundant_outputs)} 个冗余输出文件:")
        for output in redundant_outputs[:10]:  # 只显示前10个
            logger.info(f"   - {os.path.basename(output)}")
        if len(redundant_outputs) > 10:
            logger.info(f"   ... 还有 {len(redundant_outputs) - 10} 个文件")
        
        return redundant_outputs
    
    def identify_redundant_docs(self):
        """识别冗余文档"""
        logger.info("🔍 识别冗余文档...")
        
        redundant_docs = []
        
        # 多余的指南文档
        guide_docs = [
            "AUTO_BATCH_GUIDE.md",
            "ENHANCED_PROCESSOR_GUIDE.md", 
            "COMPLETE_EXECUTION_GUIDE.md",
            "missing_rows_guide.md"
        ]
        
        for doc in guide_docs:
            doc_path = self.project_root / doc
            if doc_path.exists():
                redundant_docs.append(str(doc_path))
        
        logger.info(f"📋 发现 {len(redundant_docs)} 个冗余文档:")
        for doc in redundant_docs:
            logger.info(f"   - {os.path.basename(doc)}")
        
        return redundant_docs
    
    def create_cleanup_summary(self, redundant_scripts, redundant_outputs, redundant_docs):
        """创建清理摘要"""
        summary_file = self.project_root / "CLEANUP_SUMMARY.md"
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("# 项目清理摘要\n\n")
            f.write(f"清理时间: {logging.Formatter().formatTime(logging.LogRecord('', 0, '', 0, '', (), None))}\n\n")
            
            f.write("## 可删除的冗余脚本\n\n")
            for script in redundant_scripts:
                f.write(f"- {os.path.basename(script)}\n")
            
            f.write("\n## 可删除的冗余输出文件\n\n")
            for output in redundant_outputs:
                f.write(f"- {os.path.basename(output)}\n")
            
            f.write("\n## 可删除的冗余文档\n\n")
            for doc in redundant_docs:
                f.write(f"- {os.path.basename(doc)}\n")
            
            f.write("\n## 建议保留的核心文件\n\n")
            core_files = [
                "batch_processor.py",
                "robust_batch_processor.py",
                "cost_tracker.py", 
                "create_safe_batch_input.py",
                "process_batch_results.py",
                "batch_workflow.py",
                "resume_batch_processing.py",
                "quick_test.py",
                "view_costs.py",
                "requirements.txt",
                "README.md",
                "BATCH_GUIDE.md"
            ]
            
            for file in core_files:
                f.write(f"- {file}\n")
        
        logger.info(f"📄 清理摘要已保存: {summary_file}")
    
    def run_analysis(self):
        """运行分析，不执行删除"""
        logger.info("🚀 开始项目清理分析...")
        
        # 创建备份
        self.create_backup()
        
        # 清理缓存文件
        self.clean_cache_files()
        
        # 识别冗余文件
        redundant_scripts = self.identify_redundant_scripts()
        redundant_outputs = self.identify_redundant_outputs()
        redundant_docs = self.identify_redundant_docs()
        
        # 创建清理摘要
        self.create_cleanup_summary(redundant_scripts, redundant_outputs, redundant_docs)
        
        # 统计信息
        total_redundant = len(redundant_scripts) + len(redundant_outputs) + len(redundant_docs)
        logger.info("=" * 60)
        logger.info("📊 清理分析完成")
        logger.info("=" * 60)
        logger.info(f"冗余脚本: {len(redundant_scripts)} 个")
        logger.info(f"冗余输出: {len(redundant_outputs)} 个") 
        logger.info(f"冗余文档: {len(redundant_docs)} 个")
        logger.info(f"总计冗余文件: {total_redundant} 个")
        logger.info("=" * 60)
        logger.info("📋 请查看 CLEANUP_SUMMARY.md 了解详细信息")
        logger.info("🔧 请查看 PROJECT_RESTRUCTURE_PLAN.md 了解重构计划")

def main():
    cleaner = ProjectCleaner()
    cleaner.run_analysis()

if __name__ == "__main__":
    main()
