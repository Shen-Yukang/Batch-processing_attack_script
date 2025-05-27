#!/usr/bin/env python3
"""
查找大文件的脚本
"""

import os
import sys
from pathlib import Path

def get_file_size_mb(file_path):
    """获取文件大小（MB）"""
    try:
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 * 1024)
    except (OSError, IOError):
        return 0

def find_large_files(directory=".", min_size_mb=10):
    """查找大文件"""
    large_files = []
    
    print(f"扫描目录: {os.path.abspath(directory)}")
    print(f"查找大于 {min_size_mb}MB 的文件...")
    print("=" * 80)
    
    # 遍历所有文件
    for root, dirs, files in os.walk(directory):
        # 跳过 .git 目录
        if '.git' in dirs:
            dirs.remove('.git')
        
        for file in files:
            file_path = os.path.join(root, file)
            file_size_mb = get_file_size_mb(file_path)
            
            if file_size_mb >= min_size_mb:
                large_files.append((file_path, file_size_mb))
    
    # 按大小排序
    large_files.sort(key=lambda x: x[1], reverse=True)
    
    # 显示结果
    if large_files:
        print(f"找到 {len(large_files)} 个大文件:")
        print()
        for file_path, size_mb in large_files:
            rel_path = os.path.relpath(file_path, directory)
            if size_mb >= 100:
                status = "🔴 VERY LARGE (>100MB)"
            elif size_mb >= 50:
                status = "🟡 LARGE (>50MB)"
            else:
                status = "🟢 Medium"
            
            print(f"{status}")
            print(f"  文件: {rel_path}")
            print(f"  大小: {size_mb:.2f} MB")
            print()
    else:
        print(f"没有找到大于 {min_size_mb}MB 的文件")
    
    return large_files

def check_git_status():
    """检查Git状态中的大文件"""
    print("检查Git暂存区中的大文件...")
    print("=" * 80)
    
    try:
        import subprocess
        
        # 获取暂存的文件
        result = subprocess.run(['git', 'diff', '--cached', '--name-only'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            staged_files = result.stdout.strip().split('\n')
            staged_files = [f for f in staged_files if f]  # 过滤空行
            
            if staged_files:
                print("暂存区中的文件:")
                large_staged = []
                
                for file_path in staged_files:
                    if os.path.exists(file_path):
                        size_mb = get_file_size_mb(file_path)
                        print(f"  {file_path}: {size_mb:.2f} MB")
                        
                        if size_mb >= 10:
                            large_staged.append((file_path, size_mb))
                
                if large_staged:
                    print("\n⚠️  暂存区中的大文件:")
                    for file_path, size_mb in large_staged:
                        print(f"  🔴 {file_path}: {size_mb:.2f} MB")
                else:
                    print("\n✅ 暂存区中没有大文件")
            else:
                print("暂存区为空")
        else:
            print("无法获取Git状态（可能不在Git仓库中）")
            
    except FileNotFoundError:
        print("Git未安装或不在PATH中")
    except Exception as e:
        print(f"检查Git状态失败: {e}")

def suggest_solutions(large_files):
    """建议解决方案"""
    if not large_files:
        return
    
    print("=" * 80)
    print("🔧 解决方案建议:")
    print()
    
    very_large = [f for f in large_files if f[1] >= 100]
    if very_large:
        print("对于超大文件 (>100MB):")
        print("1. 使用 Git LFS (Large File Storage):")
        print("   git lfs install")
        print("   git lfs track '*.jpg' '*.png' '*.jsonl' '*.csv'")
        print("   git add .gitattributes")
        print()
        print("2. 或者从Git中移除:")
        for file_path, size_mb in very_large[:3]:  # 只显示前3个
            rel_path = os.path.relpath(file_path)
            print(f"   git rm --cached '{rel_path}'")
        print()
    
    print("3. 添加到 .gitignore:")
    print("   echo '*.jsonl' >> .gitignore")
    print("   echo 'batch_*' >> .gitignore")
    print("   echo '*.csv' >> .gitignore")
    print()
    
    print("4. 清理已提交的大文件历史:")
    print("   git filter-branch --force --index-filter \\")
    print("     'git rm --cached --ignore-unmatch LARGE_FILE' \\")
    print("     --prune-empty --tag-name-filter cat -- --all")

def main():
    # 设置最小文件大小阈值
    min_size = 10  # MB
    if len(sys.argv) > 1:
        try:
            min_size = float(sys.argv[1])
        except ValueError:
            print("用法: python find_large_files.py [最小大小MB]")
            print("例如: python find_large_files.py 50")
            return
    
    # 查找大文件
    large_files = find_large_files(".", min_size)
    
    # 检查Git状态
    check_git_status()
    
    # 建议解决方案
    suggest_solutions(large_files)

if __name__ == "__main__":
    main()
