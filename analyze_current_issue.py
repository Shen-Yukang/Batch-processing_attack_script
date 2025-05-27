#!/usr/bin/env python3
"""
分析当前批处理问题的专用脚本
针对"返回码0但没有结果文件"的问题进行深度分析
"""

import os
import glob
import json
import re
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Optional

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_recent_batch_runs(output_base_dir: str = "output") -> Dict:
    """分析最近的批处理运行情况"""
    logger.info(f"🔍 分析最近的批处理运行: {output_base_dir}")
    
    analysis = {
        'timestamp': datetime.now().isoformat(),
        'output_directories': [],
        'problematic_batches': [],
        'successful_batches': [],
        'recommendations': []
    }
    
    # 查找所有批处理输出目录
    if os.path.exists(output_base_dir):
        pattern = os.path.join(output_base_dir, "batch_results_*")
        output_dirs = glob.glob(pattern)
        analysis['output_directories'] = output_dirs
        
        logger.info(f"📁 找到 {len(output_dirs)} 个输出目录")
        
        for output_dir in output_dirs:
            dir_analysis = analyze_single_output_directory(output_dir)
            
            if dir_analysis['has_issues']:
                analysis['problematic_batches'].append(dir_analysis)
            else:
                analysis['successful_batches'].append(dir_analysis)
    
    # 生成总体建议
    analysis['recommendations'] = generate_overall_recommendations(analysis)
    
    return analysis

def analyze_single_output_directory(output_dir: str) -> Dict:
    """分析单个输出目录"""
    logger.info(f"📂 分析目录: {os.path.basename(output_dir)}")
    
    dir_analysis = {
        'directory': output_dir,
        'directory_name': os.path.basename(output_dir),
        'has_issues': False,
        'issues': [],
        'batch_files': [],
        'result_files': [],
        'status_info': None,
        'log_files': [],
        'batch_ids_found': []
    }
    
    # 查找批处理输入文件
    batch_pattern = os.path.join(output_dir, "batch_*.jsonl")
    batch_files = glob.glob(batch_pattern)
    dir_analysis['batch_files'] = batch_files
    
    # 查找结果文件
    result_pattern = os.path.join(output_dir, "batch_results_*.jsonl")
    result_files = glob.glob(result_pattern)
    dir_analysis['result_files'] = result_files
    
    # 查找日志文件
    log_pattern = os.path.join(output_dir, "logs", "*.log")
    log_files = glob.glob(log_pattern)
    dir_analysis['log_files'] = log_files
    
    # 读取状态文件
    status_file = os.path.join(output_dir, "batch_status.json")
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                status_data = json.load(f)
                dir_analysis['status_info'] = status_data
        except Exception as e:
            logger.warning(f"⚠️  读取状态文件失败: {e}")
    
    # 分析问题
    issues = []
    
    # 检查是否有批处理文件但没有结果文件
    if batch_files and not result_files:
        issues.append("有批处理输入文件但没有结果文件")
        dir_analysis['has_issues'] = True
    
    # 检查批处理数量与结果数量的匹配
    if len(batch_files) > len(result_files):
        missing_count = len(batch_files) - len(result_files)
        issues.append(f"缺少 {missing_count} 个结果文件")
        dir_analysis['has_issues'] = True
    
    # 从日志文件中提取batch ID
    batch_ids = extract_batch_ids_from_logs(log_files)
    dir_analysis['batch_ids_found'] = batch_ids
    
    # 检查是否有batch ID但没有对应的结果文件
    if batch_ids and not result_files:
        issues.append(f"找到 {len(batch_ids)} 个batch ID但没有结果文件")
        dir_analysis['has_issues'] = True
    
    dir_analysis['issues'] = issues
    
    if issues:
        logger.warning(f"⚠️  发现问题: {', '.join(issues)}")
    else:
        logger.info(f"✅ 目录正常")
    
    return dir_analysis

def extract_batch_ids_from_logs(log_files: List[str]) -> List[str]:
    """从日志文件中提取batch ID"""
    batch_ids = set()
    
    for log_file in log_files:
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # 查找batch ID模式
                pattern = r'batch_[a-f0-9]{32}'
                found_ids = re.findall(pattern, content)
                batch_ids.update(found_ids)
                
        except Exception as e:
            logger.warning(f"⚠️  读取日志文件失败 {log_file}: {e}")
    
    return list(batch_ids)

def analyze_log_for_errors(log_file: str) -> Dict:
    """分析日志文件中的错误信息"""
    logger.info(f"📄 分析日志文件: {os.path.basename(log_file)}")
    
    analysis = {
        'file': log_file,
        'error_patterns': {},
        'success_indicators': 0,
        'failure_indicators': 0,
        'batch_ids': [],
        'timeline': []
    }
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        error_patterns = {
            'quota': r'quota|配额',
            'rate_limit': r'rate.?limit|速率限制',
            'api_key': r'api.?key|密钥',
            'timeout': r'timeout|超时',
            'validation': r'validation|验证',
            'network': r'network|网络|connection',
            'permission': r'permission|权限'
        }
        
        for line in lines:
            # 检查错误模式
            for pattern_name, pattern in error_patterns.items():
                if re.search(pattern, line, re.IGNORECASE):
                    if pattern_name not in analysis['error_patterns']:
                        analysis['error_patterns'][pattern_name] = []
                    analysis['error_patterns'][pattern_name].append(line.strip())
            
            # 检查成功/失败指示器
            if '成功' in line or 'success' in line.lower():
                analysis['success_indicators'] += 1
            if '失败' in line or 'failed' in line.lower() or 'error' in line.lower():
                analysis['failure_indicators'] += 1
            
            # 提取batch ID
            batch_ids = re.findall(r'batch_[a-f0-9]{32}', line)
            analysis['batch_ids'].extend(batch_ids)
        
        # 去重batch ID
        analysis['batch_ids'] = list(set(analysis['batch_ids']))
        
    except Exception as e:
        logger.error(f"❌ 分析日志文件失败: {e}")
    
    return analysis

def generate_overall_recommendations(analysis: Dict) -> List[str]:
    """生成总体修复建议"""
    recommendations = []
    
    problematic_count = len(analysis['problematic_batches'])
    successful_count = len(analysis['successful_batches'])
    
    if problematic_count > 0:
        recommendations.append(f"🚨 发现 {problematic_count} 个有问题的批处理目录")
        
        # 收集所有batch ID
        all_batch_ids = []
        for batch_info in analysis['problematic_batches']:
            all_batch_ids.extend(batch_info['batch_ids_found'])
        
        if all_batch_ids:
            recommendations.append(f"🔍 建议使用调试工具分析以下batch ID:")
            for batch_id in all_batch_ids[:5]:  # 只显示前5个
                recommendations.append(f"   python enhanced_batch_debugger.py {batch_id}")
            
            if len(all_batch_ids) > 5:
                recommendations.append(f"   ... 还有 {len(all_batch_ids) - 5} 个batch ID")
        
        recommendations.append(f"🔧 建议使用修复工具:")
        for batch_info in analysis['problematic_batches']:
            recommendations.append(f"   python fix_missing_batch_results.py {batch_info['directory']}")
    
    if successful_count > 0:
        recommendations.append(f"✅ {successful_count} 个批处理目录正常")
    
    return recommendations

def main():
    parser = argparse.ArgumentParser(description='分析当前批处理问题')
    parser.add_argument('--output-dir', default='output', help='输出基础目录，默认为output')
    parser.add_argument('--specific-dir', help='分析特定的输出目录')
    parser.add_argument('--save-report', action='store_true', help='保存分析报告到文件')
    
    args = parser.parse_args()
    
    if args.specific_dir:
        # 分析特定目录
        if not os.path.exists(args.specific_dir):
            logger.error(f"❌ 目录不存在: {args.specific_dir}")
            return 1
        
        dir_analysis = analyze_single_output_directory(args.specific_dir)
        
        print("\n" + "="*80)
        print(f"📋 目录分析报告: {dir_analysis['directory_name']}")
        print("="*80)
        
        print(f"📁 目录: {dir_analysis['directory']}")
        print(f"📄 批处理文件: {len(dir_analysis['batch_files'])}")
        print(f"📄 结果文件: {len(dir_analysis['result_files'])}")
        print(f"📄 日志文件: {len(dir_analysis['log_files'])}")
        print(f"🆔 找到的batch ID: {len(dir_analysis['batch_ids_found'])}")
        
        if dir_analysis['has_issues']:
            print(f"\n⚠️  发现问题:")
            for issue in dir_analysis['issues']:
                print(f"   - {issue}")
        else:
            print(f"\n✅ 目录状态正常")
        
        if dir_analysis['batch_ids_found']:
            print(f"\n🆔 Batch ID列表:")
            for batch_id in dir_analysis['batch_ids_found']:
                print(f"   - {batch_id}")
    
    else:
        # 分析所有目录
        analysis = analyze_recent_batch_runs(args.output_dir)
        
        print("\n" + "="*80)
        print("📋 批处理问题分析报告")
        print("="*80)
        
        print(f"📁 分析目录: {args.output_dir}")
        print(f"📂 找到输出目录: {len(analysis['output_directories'])}")
        print(f"⚠️  有问题的批处理: {len(analysis['problematic_batches'])}")
        print(f"✅ 正常的批处理: {len(analysis['successful_batches'])}")
        
        if analysis['problematic_batches']:
            print(f"\n🚨 有问题的批处理目录:")
            for batch_info in analysis['problematic_batches']:
                print(f"   📂 {batch_info['directory_name']}")
                for issue in batch_info['issues']:
                    print(f"      - {issue}")
                if batch_info['batch_ids_found']:
                    print(f"      🆔 Batch IDs: {', '.join(batch_info['batch_ids_found'][:3])}...")
        
        if analysis['recommendations']:
            print(f"\n💡 修复建议:")
            for rec in analysis['recommendations']:
                print(rec)
        
        # 保存报告
        if args.save_report:
            report_file = f"batch_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, indent=2, ensure_ascii=False, default=str)
            print(f"\n📄 分析报告已保存: {report_file}")
    
    print("\n" + "="*80)
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
