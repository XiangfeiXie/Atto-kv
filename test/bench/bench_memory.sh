#!/bin/bash
# =============================================================================
# bench_memory.sh — 内存分配器对比测试
#
# 对比 glibc malloc 与 jemalloc 在不同阶段的内存占用:
#   - 启动空闲态
#   - 数据写入峰值态
#   - FLUSHALL 后稳定态
#
# 用法:
#   sudo ./test/bench/bench_memory.sh              # 全部测试
#   sudo ./test/bench/bench_memory.sh 500000       # 指定 key 数量
#
# 前置条件:
#   - 需要分别编译两个版本: jemalloc / glibc
#   - 本脚本检查两个二进制是否存在，不存在则提示编译
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

NKEYS=${1:-500000}
VALUE_SIZE=64

KVS_JEMALLOC="${PROJECT_DIR}/build/kvstore"
KVS_GLIBC="${PROJECT_DIR}/build-glibc/kvstore"

init_results
record_raw "=== 内存分配器对比测试 (${NKEYS} keys × ${VALUE_SIZE}B value) ==="
record_raw ""

# ---- 编译检查 ----
ensure_build() {
    local label=$1 bin=$2 use_jemalloc=$3
    if [ -x "$bin" ]; then
        log "${label} 二进制已存在: $bin"
        return
    fi
    warn "${label} 二进制不存在，开始编译..."
    local build_dir=$(dirname "$bin")
    mkdir -p "$build_dir"
    cd "$build_dir"
    if [ "$use_jemalloc" = "yes" ]; then
        cmake "${PROJECT_DIR}" -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
    else
        cmake "${PROJECT_DIR}" -DCMAKE_BUILD_TYPE=Release \
            -DUSE_JEMALLOC=OFF > /dev/null 2>&1
    fi
    make -j$(nproc) kvstore > /dev/null 2>&1
    cd "$PROJECT_DIR"
    if [ -x "$bin" ]; then
        log "${label} 编译完成: $bin"
    else
        err "${label} 编译失败"
        exit 1
    fi
}

# 检查 jemalloc 版本
ensure_build "jemalloc" "$KVS_JEMALLOC" "yes"

# 检查 glibc 版本（需要 CMakeLists.txt 支持 USE_JEMALLOC option）
ensure_build "glibc" "$KVS_GLIBC" "no"

# ---- 测试函数 ----
test_allocator() {
    local label=$1 bin=$2

    step "=== 测试: ${label} ==="
    stop_kvs
    stop_redis
    rm -f "${PROJECT_DIR}/build/dump.rdb" "${PROJECT_DIR}/build/appendonly.aof"

    # 使用不持久化、不复制的最小配置
    local conf="${LOG_DIR}/mem_test_${label}.conf"
    cat > "$conf" << 'CONFEOF'
port 2000
network proactor
maxclients 10000
loglevel error
persistence none
rdma_sync off
rdma_role master
CONFEOF

    # 1. 启动空闲态
    nohup "$bin" "$conf" > "${LOG_DIR}/mem_${label}.log" 2>&1 &
    local pid=$!
    sleep 2

    if ! kill -0 "$pid" 2>/dev/null; then
        err "${label} 启动失败"
        tail -10 "${LOG_DIR}/mem_${label}.log"
        return 1
    fi

    wait_for_port 127.0.0.1 "$KVS_PORT" 10

    local idle=$(sample_memory "$pid")
    local idle_vmsize=$(echo "$idle" | awk '{print $1}')
    local idle_vmrss=$(echo "$idle" | awk '{print $2}')
    record_raw "  ${label} 空闲态: VmSize=${idle_vmsize} kB, VmRSS=${idle_vmrss} kB"

    # 2. 写入数据（峰值态）
    log "写入 ${NKEYS} 条数据..."
    local batch_size=5000
    local sent=0
    while [ $sent -lt $NKEYS ]; do
        local batch=$batch_size
        [ $((sent + batch)) -gt $NKEYS ] && batch=$((NKEYS - sent))
        for i in $(seq $((sent+1)) $((sent+batch))); do
            printf '*3\r\n$3\r\nSET\r\n$9\r\nkey%06d\r\n$%d\r\n%0*d\r\n' \
                $i $VALUE_SIZE $VALUE_SIZE 0
        done | timeout 300 nc -w 5 127.0.0.1 "$KVS_PORT" > /dev/null 2>&1
        sent=$((sent + batch))
    done
    log "写入完成"

    local peak=$(sample_memory "$pid")
    local peak_vmsize=$(echo "$peak" | awk '{print $1}')
    local peak_vmrss=$(echo "$peak" | awk '{print $2}')
    record_raw "  ${label} 峰值态: VmSize=${peak_vmsize} kB, VmRSS=${peak_vmrss} kB"

    # 3. FLUSHALL 后稳定态
    printf '*1\r\n$8\r\nFLUSHALL\r\n' | nc -w 2 127.0.0.1 "$KVS_PORT" > /dev/null 2>&1
    log "等待内存归还（30 秒）..."
    sleep 30

    local stable=$(sample_memory "$pid")
    local stable_vmsize=$(echo "$stable" | awk '{print $1}')
    local stable_vmrss=$(echo "$stable" | awk '{print $2}')
    record_raw "  ${label} 稳定态: VmSize=${stable_vmsize} kB, VmRSS=${stable_vmrss} kB"

    # 计算指标
    local peak_growth=$(echo "scale=1; ($peak_vmrss - $idle_vmrss) / 1024" | bc -l 2>/dev/null || echo "N/A")
    local reclaim=$(echo "scale=1; ($peak_vmrss - $stable_vmrss) / 1024" | bc -l 2>/dev/null || echo "N/A")
    local leak=$(echo "scale=1; ($stable_vmrss - $idle_vmrss) / 1024" | bc -l 2>/dev/null || echo "N/A")
    record_raw "  内存增长: ${peak_growth} MB, 归还: ${reclaim} MB, 残留: ${leak} MB"
    record_raw ""

    # 记录到 CSV
    echo "memory,${label}-idle-VmSize,${idle_vmsize}" >> "$RESULT_CSV"
    echo "memory,${label}-idle-VmRSS,${idle_vmrss}" >> "$RESULT_CSV"
    echo "memory,${label}-peak-VmSize,${peak_vmsize}" >> "$RESULT_CSV"
    echo "memory,${label}-peak-VmRSS,${peak_vmrss}" >> "$RESULT_CSV"
    echo "memory,${label}-stable-VmSize,${stable_vmsize}" >> "$RESULT_CSV"
    echo "memory,${label}-stable-VmRSS,${stable_vmrss}" >> "$RESULT_CSV"

    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    rm -f "$conf"
}

# ---- 执行 ----
test_allocator "jemalloc" "$KVS_JEMALLOC"
test_allocator "glibc"   "$KVS_GLIBC"

stop_kvs

# ---- 汇总表格 ----
record_raw ""
record_raw "=== 内存方案对比汇总 ==="
record_raw ""
record_raw "  分配器   | 阶段     | VmSize (kB) | VmRSS (kB)"
record_raw "  ---------|----------|-------------|------------"
for alloc in jemalloc glibc; do
    for stage in idle peak stable; do
        vs=$(grep "memory,${alloc}-${stage}-VmSize," "$RESULT_CSV" | cut -d',' -f3)
        rs=$(grep "memory,${alloc}-${stage}-VmRSS," "$RESULT_CSV" | cut -d',' -f3)
        printf "  %-8s | %-8s | %11s | %10s\n" "$alloc" "$stage" "$vs" "$rs" >> "$RESULT_FILE"
    done
done

echo ""
cat "$RESULT_FILE"
echo ""
log "结果保存至: $RESULT_FILE / $RESULT_CSV"
