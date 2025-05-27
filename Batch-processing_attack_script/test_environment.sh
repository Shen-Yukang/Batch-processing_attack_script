#!/bin/bash

# 环境测试脚本
echo "🔍 测试自动批处理环境"
echo "================================"

# 检查文件权限
echo "📁 检查脚本文件..."
if [[ -x "auto_batch_processor.sh" ]]; then
    echo "✅ auto_batch_processor.sh 可执行"
else
    echo "❌ auto_batch_processor.sh 不可执行"
    chmod +x auto_batch_processor.sh
    echo "🔧 已修复权限"
fi

# 检查CSV文件
echo ""
echo "📊 检查CSV文件..."
if [[ -d "_csvs" ]]; then
    csv_count=$(find _csvs -name "*.csv" | wc -l)
    echo "✅ 找到 $csv_count 个CSV文件:"
    find _csvs -name "*.csv" | while read file; do
        lines=$(wc -l < "$file" 2>/dev/null || echo "0")
        echo "   - $(basename "$file"): $lines 行"
    done
else
    echo "❌ _csvs 目录不存在"
fi

# 检查Python脚本
echo ""
echo "🐍 检查Python脚本..."
required_scripts=(
    "robust_batch_processor.py"
    "batch_processor.py"
    "create_safe_batch_input.py"
    "merge_all_results.py"
    "cost_tracker.py"
)

for script in "${required_scripts[@]}"; do
    if [[ -f "$script" ]]; then
        echo "✅ $script"
    else
        echo "❌ $script 缺失"
    fi
done

# 检查API密钥
echo ""
echo "🔑 检查API密钥..."
if [[ -n "$OPENAI_API_KEY" ]]; then
    if [[ "$OPENAI_API_KEY" == sk-* ]]; then
        echo "✅ API密钥已设置且格式正确"
    else
        echo "⚠️  API密钥格式可能不正确"
    fi
else
    echo "❌ OPENAI_API_KEY 未设置"
    echo "💡 请运行: export OPENAI_API_KEY='your-key-here'"
fi

# 检查Python依赖
echo ""
echo "📦 检查Python依赖..."
python3 -c "
import sys
modules = ['openai', 'pandas', 'json', 'os', 'subprocess']
missing = []
for module in modules:
    try:
        __import__(module)
        print(f'✅ {module}')
    except ImportError:
        print(f'❌ {module} 缺失')
        missing.append(module)

if missing:
    print(f'💡 安装缺失模块: pip install {\" \".join(missing)}')
" 2>/dev/null || echo "❌ Python3 不可用"

# 检查磁盘空间
echo ""
echo "💾 检查磁盘空间..."
available=$(df . | tail -1 | awk '{print $4}')
if [[ $available -gt 1000000 ]]; then  # 1GB
    echo "✅ 磁盘空间充足 ($(($available/1024/1024))GB 可用)"
else
    echo "⚠️  磁盘空间可能不足 ($(($available/1024))MB 可用)"
fi

echo ""
echo "================================"
echo "🎯 环境测试完成"

# 显示下一步
echo ""
echo "📋 下一步操作:"
echo "1. 确保设置了 OPENAI_API_KEY"
echo "2. 运行: ./auto_batch_processor.sh"
echo "3. 查看处理进度和结果"
