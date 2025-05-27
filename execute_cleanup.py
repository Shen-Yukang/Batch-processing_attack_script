#!/usr/bin/env python3
"""
执行项目清理脚本
安全地清理和重组项目目录结构
"""

import os
import shutil
import glob
from pathlib import Path
import logging
import json
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProjectCleanupExecutor:
    def __init__(self, project_root=".", dry_run=True):
        self.project_root = Path(project_root)
        self.dry_run = dry_run
        self.cleanup_log = []
        
        # 核心文件列表（绝对不能删除）
        self.core_files = {
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
            "BATCH_GUIDE.md",
            "batch_config.conf"
        }
        
        # 可选保留文件
        self.optional_files = {
            "create_batch_input.py",  # 功能与create_safe_batch_input.py重复
            "process_csv_with_chatgpt.py",  # 实时处理功能
            "batch_process.py"  # 简单批处理脚本
        }
        
        # 数据文件（移动到data目录）
        self.data_files = {
            "_csvs/content_FigStep.csv",
            "_csvs/content_Jailbreak28k.csv", 
            "_csvs/content_MMSafeBench_cleaned.csv"
        }
    
    def log_action(self, action, file_path, status="planned"):
        """记录清理动作"""
        self.cleanup_log.append({
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "file": str(file_path),
            "status": status
        })
    
    def safe_delete(self, file_path):
        """安全删除文件或目录"""
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] 将删除: {file_path}")
                self.log_action("delete", file_path, "dry_run")
                return True
            
            if os.path.isdir(file_path):
                shutil.rmtree(file_path)
                logger.info(f"🗑️  删除目录: {file_path}")
            else:
                os.remove(file_path)
                logger.info(f"🗑️  删除文件: {file_path}")
            
            self.log_action("delete", file_path, "completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ 删除失败 {file_path}: {e}")
            self.log_action("delete", file_path, f"failed: {e}")
            return False
    
    def safe_move(self, src, dst):
        """安全移动文件"""
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] 将移动: {src} -> {dst}")
                self.log_action("move", f"{src} -> {dst}", "dry_run")
                return True
            
            # 确保目标目录存在
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.move(src, dst)
            logger.info(f"📦 移动: {src} -> {dst}")
            self.log_action("move", f"{src} -> {dst}", "completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ 移动失败 {src} -> {dst}: {e}")
            self.log_action("move", f"{src} -> {dst}", f"failed: {e}")
            return False
    
    def clean_redundant_scripts(self):
        """清理冗余脚本"""
        logger.info("🧹 清理冗余脚本...")
        
        # 获取所有Python文件
        all_py_files = set(glob.glob(str(self.project_root / "*.py")))
        
        # 排除核心文件和可选文件
        core_paths = {str(self.project_root / f) for f in self.core_files if f.endswith('.py')}
        optional_paths = {str(self.project_root / f) for f in self.optional_files}
        
        # 要删除的文件
        files_to_delete = all_py_files - core_paths - optional_paths
        
        # 额外排除一些重要文件
        important_patterns = [
            "*cleanup*.py",  # 清理脚本本身
            "*analyze*.py"   # 分析脚本
        ]
        
        for pattern in important_patterns:
            important_files = set(glob.glob(str(self.project_root / pattern)))
            files_to_delete -= important_files
        
        deleted_count = 0
        for file_path in files_to_delete:
            if self.safe_delete(file_path):
                deleted_count += 1
        
        logger.info(f"✅ 清理了 {deleted_count} 个冗余脚本")
    
    def clean_output_files(self):
        """清理输出文件"""
        logger.info("🧹 清理输出文件...")
        
        # 旧的批处理结果目录
        old_batch_dirs = glob.glob(str(self.project_root / "batch_results_*"))
        
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
        
        # 临时文件
        temp_files = []
        for pattern in temp_csvs:
            temp_files.extend(glob.glob(str(self.project_root / pattern)))
        
        # JSONL文件
        temp_files.extend(glob.glob(str(self.project_root / "*.jsonl")))
        
        # JSON报告文件
        temp_files.extend(glob.glob(str(self.project_root / "batch_analysis_*.json")))
        
        # 文本文件
        temp_files.extend(glob.glob(str(self.project_root / "*missing_rows*.txt")))
        
        # 删除文件
        deleted_count = 0
        for file_path in old_batch_dirs + temp_files:
            if self.safe_delete(file_path):
                deleted_count += 1
        
        logger.info(f"✅ 清理了 {deleted_count} 个输出文件")
    
    def clean_redundant_docs(self):
        """清理冗余文档"""
        logger.info("🧹 清理冗余文档...")
        
        redundant_docs = [
            "AUTO_BATCH_GUIDE.md",
            "ENHANCED_PROCESSOR_GUIDE.md",
            "COMPLETE_EXECUTION_GUIDE.md", 
            "missing_rows_guide.md"
        ]
        
        deleted_count = 0
        for doc in redundant_docs:
            doc_path = self.project_root / doc
            if doc_path.exists() and self.safe_delete(doc_path):
                deleted_count += 1
        
        logger.info(f"✅ 清理了 {deleted_count} 个冗余文档")
    
    def organize_data_files(self):
        """整理数据文件"""
        logger.info("📁 整理数据文件...")
        
        # 创建data目录结构
        data_dir = self.project_root / "data"
        input_dir = data_dir / "input"
        
        if not self.dry_run:
            input_dir.mkdir(parents=True, exist_ok=True)
        
        # 移动CSV文件
        csv_dir = self.project_root / "_csvs"
        if csv_dir.exists():
            csv_files = glob.glob(str(csv_dir / "*.csv"))
            moved_count = 0
            
            for csv_file in csv_files:
                filename = os.path.basename(csv_file)
                dst = input_dir / filename
                if self.safe_move(csv_file, dst):
                    moved_count += 1
            
            # 删除空的_csvs目录
            if not self.dry_run and not os.listdir(csv_dir):
                self.safe_delete(csv_dir)
            
            logger.info(f"✅ 移动了 {moved_count} 个CSV文件到 data/input/")
    
    def organize_output_directory(self):
        """整理输出目录"""
        logger.info("📁 整理输出目录...")
        
        output_dir = self.project_root / "output"
        if output_dir.exists():
            # 创建子目录
            subdirs = ["results", "logs", "temp"]
            
            for subdir in subdirs:
                subdir_path = output_dir / subdir
                if not self.dry_run:
                    subdir_path.mkdir(exist_ok=True)
                logger.info(f"📁 确保目录存在: output/{subdir}/")
    
    def create_new_structure_summary(self):
        """创建新结构摘要"""
        summary = {
            "cleanup_date": datetime.now().isoformat(),
            "dry_run": self.dry_run,
            "actions_performed": self.cleanup_log,
            "recommended_structure": {
                "core_files": list(self.core_files),
                "optional_files": list(self.optional_files),
                "directories": {
                    "data/input/": "输入CSV文件",
                    "output/results/": "批处理结果",
                    "output/logs/": "日志文件", 
                    "output/temp/": "临时文件"
                }
            }
        }
        
        summary_file = self.project_root / "CLEANUP_EXECUTION_LOG.json"
        
        if not self.dry_run:
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📄 清理执行日志: {summary_file}")
    
    def execute_cleanup(self):
        """执行完整清理"""
        mode = "DRY RUN" if self.dry_run else "EXECUTION"
        logger.info(f"🚀 开始项目清理 ({mode})...")
        
        if self.dry_run:
            logger.info("⚠️  这是预览模式，不会实际删除文件")
            logger.info("⚠️  要执行实际清理，请使用 --execute 参数")
        
        # 执行清理步骤
        self.clean_redundant_scripts()
        self.clean_output_files() 
        self.clean_redundant_docs()
        self.organize_data_files()
        self.organize_output_directory()
        
        # 创建摘要
        self.create_new_structure_summary()
        
        # 统计信息
        total_actions = len(self.cleanup_log)
        logger.info("=" * 60)
        logger.info(f"📊 清理完成 ({mode})")
        logger.info("=" * 60)
        logger.info(f"总计操作: {total_actions} 个")
        
        if self.dry_run:
            logger.info("🔧 要执行实际清理，请运行:")
            logger.info("   python execute_cleanup.py --execute")
        else:
            logger.info("✅ 项目清理完成！")
            logger.info("📋 请查看 CLEANUP_EXECUTION_LOG.json 了解详细信息")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='执行项目清理')
    parser.add_argument('--execute', action='store_true', 
                       help='执行实际清理（默认为预览模式）')
    
    args = parser.parse_args()
    
    executor = ProjectCleanupExecutor(dry_run=not args.execute)
    executor.execute_cleanup()

if __name__ == "__main__":
    main()
