#!/bin/bash

# 自动批处理脚本 - 检查、处理和合并所有CSV文件
# 作者: AI Assistant
# 日期: $(date +%Y-%m-%d)

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_step() {
    echo -e "${PURPLE}[STEP]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 配置变量
CSV_DIR="_csvs"
BATCH_SIZE=20
MODEL="gpt-4o-mini"
DELAY=60
LOG_DIR="logs"
RESULTS_DIR="final_results"

# 创建必要目录
mkdir -p "$LOG_DIR" "$RESULTS_DIR"

# 主日志文件
MAIN_LOG="$LOG_DIR/auto_batch_$(date +%Y%m%d_%H%M%S).log"

# 记录到文件的函数
log_to_file() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$MAIN_LOG"
}

# 检查环境
check_environment() {
    log_step "检查执行环境"
    
    # 检查API密钥
    if [[ -z "$OPENAI_API_KEY" ]]; then
        log_error "OPENAI_API_KEY 环境变量未设置"
        return 1
    fi
    
    # 检查必要文件
    local required_files=(
        "robust_batch_processor.py"
        "create_safe_batch_input.py" 
        "batch_processor.py"
        "merge_all_results.py"
        "cost_tracker.py"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "缺少必要文件: $file"
            return 1
        fi
    done
    
    # 检查CSV目录
    if [[ ! -d "$CSV_DIR" ]]; then
        log_error "CSV目录不存在: $CSV_DIR"
        return 1
    fi
    
    log_success "环境检查通过"
    return 0
}

# 获取CSV文件行数
get_csv_line_count() {
    local csv_file="$1"
    if [[ -f "$csv_file" ]]; then
        # 减去标题行
        echo $(($(wc -l < "$csv_file") - 1))
    else
        echo 0
    fi
}

# 检查CSV文件是否已处理完成
check_csv_processed() {
    local csv_file="$1"
    local csv_basename=$(basename "$csv_file" .csv)
    
    log_info "检查 $csv_basename 的处理状态..."
    
    # 查找对应的批处理目录
    local batch_dirs=($(find . -maxdepth 1 -name "batch_results_*" -type d | sort -r))
    
    if [[ ${#batch_dirs[@]} -eq 0 ]]; then
        log_warning "未找到任何批处理目录"
        return 1
    fi
    
    # 检查最新的批处理目录
    for batch_dir in "${batch_dirs[@]}"; do
        local status_file="$batch_dir/batch_status.json"
        
        if [[ -f "$status_file" ]]; then
            # 检查状态文件中是否包含此CSV的处理记录
            if python3 -c "
import json, sys
try:
    with open('$status_file', 'r') as f:
        data = json.load(f)
    
    # 检查是否有对应的最终输出文件
    import os
    final_output = 'final_output_${csv_basename}.csv'
    complete_output = 'complete_final_output_${csv_basename}.csv'
    
    if os.path.exists(final_output) or os.path.exists(complete_output):
        print('COMPLETED')
        sys.exit(0)
    
    # 检查批处理任务完成情况
    jobs = data.get('jobs', [])
    completed_jobs = [j for j in jobs if j.get('status') == 'completed']
    total_expected = $(get_csv_line_count "$csv_file") // $BATCH_SIZE + 1
    
    if len(completed_jobs) >= total_expected * 0.9:  # 90%完成率认为已完成
        print('MOSTLY_COMPLETED')
        sys.exit(0)
    else:
        print('INCOMPLETE')
        sys.exit(1)
        
except Exception as e:
    print('ERROR')
    sys.exit(1)
" 2>/dev/null; then
                local result=$?
                if [[ $result -eq 0 ]]; then
                    log_success "$csv_basename 已处理完成"
                    return 0
                fi
            fi
        fi
    done
    
    log_info "$csv_basename 需要处理"
    return 1
}

# 处理单个CSV文件
process_csv_file() {
    local csv_file="$1"
    local csv_basename=$(basename "$csv_file" .csv)
    local csv_lines=$(get_csv_line_count "$csv_file")
    
    log_step "开始处理 $csv_basename ($csv_lines 行数据)"
    log_to_file "开始处理 $csv_file"
    
    # 创建带时间戳的输出目录
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local output_dir="batch_results_${csv_basename}_${timestamp}"
    
    log_info "创建输出目录: $output_dir"
    mkdir -p "$output_dir"
    
    # 执行批处理
    log_info "启动批处理器..."
    if python3 robust_batch_processor.py \
        --input-csv "$csv_file" \
        --start-row 0 \
        --end-row "$csv_lines" \
        --batch-size "$BATCH_SIZE" \
        --output-dir "$output_dir" \
        --model "$MODEL" 2>&1 | tee -a "$MAIN_LOG"; then
        
        log_success "$csv_basename 批处理完成"
        
        # 合并结果
        log_info "合并 $csv_basename 的结果..."
        local final_output="$RESULTS_DIR/final_output_${csv_basename}.csv"
        
        if python3 merge_all_results.py "$output_dir" "$csv_file" "$final_output" 2>&1 | tee -a "$MAIN_LOG"; then
            log_success "结果合并完成: $final_output"
            
            # 验证输出文件
            local output_lines=$(get_csv_line_count "$final_output")
            local completion_rate=$((output_lines * 100 / csv_lines))
            
            log_info "完成率: $output_lines/$csv_lines ($completion_rate%)"
            
            if [[ $completion_rate -ge 90 ]]; then
                log_success "$csv_basename 处理成功 (完成率: $completion_rate%)"
                return 0
            else
                log_warning "$csv_basename 完成率较低 ($completion_rate%)"
                return 1
            fi
        else
            log_error "$csv_basename 结果合并失败"
            return 1
        fi
    else
        log_error "$csv_basename 批处理失败"
        return 1
    fi
}

# 显示处理总结
show_summary() {
    local processed_files=("$@")
    
    echo ""
    echo "================================================================================"
    log_step "处理总结"
    echo "================================================================================"
    
    log_info "处理的文件数量: ${#processed_files[@]}"
    
    for file in "${processed_files[@]}"; do
        local basename=$(basename "$file" .csv)
        local final_output="$RESULTS_DIR/final_output_${basename}.csv"
        
        if [[ -f "$final_output" ]]; then
            local lines=$(get_csv_line_count "$final_output")
            log_success "$basename: $lines 行数据 -> $final_output"
        else
            log_error "$basename: 处理失败"
        fi
    done
    
    echo ""
    log_info "所有结果文件保存在: $RESULTS_DIR/"
    log_info "详细日志保存在: $MAIN_LOG"
    
    # 显示成本统计
    if command -v python3 >/dev/null 2>&1; then
        log_info "成本统计:"
        find . -name "batch_costs.json" -exec python3 -c "
import json, sys
try:
    with open(sys.argv[1], 'r') as f:
        data = json.load(f)
    total_cost = sum(batch.get('batch_cost', 0) for batch in data.get('batches', {}).values())
    print(f'  总估算成本: \${total_cost:.4f}')
except:
    pass
" {} \; 2>/dev/null || true
    fi
}

# 主函数
main() {
    echo "================================================================================"
    echo "🚀 自动批处理脚本启动"
    echo "================================================================================"
    
    log_to_file "自动批处理脚本启动"
    
    # 检查环境
    if ! check_environment; then
        log_error "环境检查失败，退出"
        exit 1
    fi
    
    # 获取所有CSV文件
    local csv_files=($(find "$CSV_DIR" -name "*.csv" -type f | sort))
    
    if [[ ${#csv_files[@]} -eq 0 ]]; then
        log_error "在 $CSV_DIR 中未找到CSV文件"
        exit 1
    fi
    
    log_info "找到 ${#csv_files[@]} 个CSV文件"
    
    # 检查每个文件的处理状态
    local files_to_process=()
    local skipped_files=()
    
    for csv_file in "${csv_files[@]}"; do
        local basename=$(basename "$csv_file" .csv)
        
        if check_csv_processed "$csv_file"; then
            log_info "跳过已处理的文件: $basename"
            skipped_files+=("$csv_file")
        else
            log_info "需要处理的文件: $basename"
            files_to_process+=("$csv_file")
        fi
    done
    
    # 显示处理计划
    echo ""
    log_step "处理计划"
    log_info "需要处理: ${#files_to_process[@]} 个文件"
    log_info "跳过已完成: ${#skipped_files[@]} 个文件"
    
    if [[ ${#files_to_process[@]} -eq 0 ]]; then
        log_success "所有文件都已处理完成！"
        show_summary "${csv_files[@]}"
        exit 0
    fi
    
    # 询问是否继续
    echo ""
    read -p "是否开始处理 ${#files_to_process[@]} 个文件？(y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "用户取消处理"
        exit 0
    fi
    
    # 处理文件
    local processed_files=()
    local failed_files=()
    
    for csv_file in "${files_to_process[@]}"; do
        local basename=$(basename "$csv_file" .csv)
        
        echo ""
        log_step "处理文件 $((${#processed_files[@]} + ${#failed_files[@]} + 1))/${#files_to_process[@]}: $basename"
        
        if process_csv_file "$csv_file"; then
            processed_files+=("$csv_file")
            log_success "$basename 处理完成"
        else
            failed_files+=("$csv_file")
            log_error "$basename 处理失败"
            
            # 询问是否继续
            echo ""
            read -p "是否继续处理下一个文件？(y/N): " -n 1 -r
            echo
            
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                log_info "用户选择停止处理"
                break
            fi
        fi
        
        # 文件间延迟
        if [[ $((${#processed_files[@]} + ${#failed_files[@]})) -lt ${#files_to_process[@]} ]]; then
            log_info "等待 $DELAY 秒后处理下一个文件..."
            sleep "$DELAY"
        fi
    done
    
    # 显示最终总结
    echo ""
    echo "================================================================================"
    log_step "最终总结"
    echo "================================================================================"
    
    log_info "成功处理: ${#processed_files[@]} 个文件"
    log_info "处理失败: ${#failed_files[@]} 个文件"
    log_info "跳过已完成: ${#skipped_files[@]} 个文件"
    
    if [[ ${#failed_files[@]} -gt 0 ]]; then
        log_warning "失败的文件:"
        for file in "${failed_files[@]}"; do
            log_warning "  - $(basename "$file")"
        done
    fi
    
    # 显示所有文件的总结
    local all_processed=("${processed_files[@]}" "${skipped_files[@]}")
    show_summary "${all_processed[@]}"
    
    log_success "自动批处理脚本执行完成"
    log_to_file "自动批处理脚本执行完成"
}

# 信号处理
trap 'log_warning "脚本被中断"; exit 1' INT TERM

# 执行主函数
main "$@"
