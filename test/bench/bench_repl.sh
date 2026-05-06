#!/bin/bash
# =============================================================================
# bench_repl.sh — 主从同步性能测试
#
# 分两部分:
#   Part A: 实时增量同步（eBPF 对 master QPS 的影响）
#   Part B: 全量同步传输速度（RDMA RDB 二进制）
#
# 用法:
#   sudo ./test/bench/bench_repl.sh              # 全部测试
#   sudo ./test/bench/bench_repl.sh sync         # 只测实时同步
#   sudo ./test/bench/bench_repl.sh full         # 只测全量同步
#
# 前置条件:
#   - Soft-RoCE 已配置: rdma link add rxe0 type rxe netdev <网卡>
#   - eBPF BTF 可用
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

require_root
require_tools

BENCH_N=500000
BENCH_C=50
BENCH_P=20
FULLSYNC_KEY_COUNTS=(10000 100000 500000)

init_results
record_raw "=== 主从同步性能测试 ==="
record_raw ""

# ===========================================================================
# Part A: 实时增量同步
# ===========================================================================
test_realtime_sync() {
    step "=== Part A: 实时增量同步性能 ==="
    stop_redis
    stop_kvs
    fuser -k ${KVS_SLAVE_PORT}/tcp 2>/dev/null || true
    fuser -k ${MASTER_RDMA_PORT}/tcp 2>/dev/null || true

    # A1. 关闭主从同步（基线）
    step "A1. 关闭主从同步（基线）"
    start_kvs "kvstore-none.conf"
    populate 127.0.0.1 "$KVS_PORT" 5000
    local base_qps=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C $BENCH_P)
    record_result "实时同步" "A1-关闭主从同步" "$base_qps"
    stop_kvs

    # A2. 开启 eBPF 增量复制
    step "A2. 开启 eBPF 增量复制（slave 连接）"
    start_kvs_master
    populate 127.0.0.1 "$KVS_PORT" 5000

    # 启动 slave（触发全量同步 + eBPF 增量注册）
    start_kvs_slave
    sleep 3  # 等待全量同步完成

    local ebpf_qps=$(run_bench 127.0.0.1 "$KVS_PORT" set $BENCH_N $BENCH_C $BENCH_P)
    local ebpf_drop=$(echo "scale=1; (1 - $ebpf_qps / $base_qps) * 100" | bc -l 2>/dev/null || echo "N/A")
    record_result "实时同步" "A2-eBPF增量复制" "$ebpf_qps"
    record_raw "  eBPF 性能损耗: ${ebpf_drop}%"
    record_raw ""

    stop_kvs
    fuser -k ${KVS_SLAVE_PORT}/tcp 2>/dev/null || true
}

# ===========================================================================
# Part B: 全量同步
# ===========================================================================
test_full_sync() {
    step "=== Part B: 全量同步（RDMA RDB 二进制传输） ==="

    for nkeys in "${FULLSYNC_KEY_COUNTS[@]}"; do
        step "B. 全量同步: ${nkeys} keys"

        stop_kvs
        fuser -k ${KVS_SLAVE_PORT}/tcp 2>/dev/null || true
        fuser -k ${MASTER_RDMA_PORT}/tcp 2>/dev/null || true

        # 启动 master 并填充数据
        start_kvs_master
        populate 127.0.0.1 "$KVS_PORT" "$nkeys"

        # 记录 RDB 数据大小（通过 SAVE 生成后查看）
        printf '*1\r\n$4\r\nSAVE\r\n' | nc -w 5 127.0.0.1 "$KVS_PORT" > /dev/null 2>&1
        sleep 2
        local rdb_file="${PROJECT_DIR}/build/dump.rdb"
        local rdb_size=0
        [ -f "$rdb_file" ] && rdb_size=$(stat -c%s "$rdb_file" 2>/dev/null || echo 0)

        # 启动 slave 并计时
        local start_ts=$(date +%s%N)
        start_kvs_slave
        # 等待全量同步完成: 检查 slave 上有数据
        local timeout=120
        local synced=0
        for ((i=0; i<timeout; i++)); do
            local exist=$(printf '*2\r\n$5\r\nEXIST\r\n$9\r\nkey000001\r\n' | \
                          timeout 2 nc 127.0.0.1 "$KVS_SLAVE_PORT" 2>/dev/null | head -c 4)
            if [ "$exist" = ":1" ]; then
                synced=1
                break
            fi
            sleep 1
        done
        local end_ts=$(date +%s%N)
        local elapsed=$(echo "scale=3; ($end_ts - $start_ts) / 1000000000" | bc -l)

        if [ "$synced" -eq 1 ]; then
            local speed_mbps=$(echo "scale=2; $rdb_size * 8 / 1000000 / $elapsed" | bc -l)
            record_result "全量同步-${nkeys}" "RDMA-耗时${elapsed}s" "$speed_mbps"
            record_raw "  keys: ${nkeys}, RDB: $(echo "scale=1; $rdb_size/1024/1024" | bc -l) MB"
            record_raw "  同步耗时: ${elapsed}s, 传输速度: ${speed_mbps} Mbps"
        else
            record_result "全量同步-${nkeys}" "RDMA-失败" "0"
            record_raw "  同步超时 (>${timeout}s)"
        fi
        record_raw ""

        stop_kvs
        fuser -k ${KVS_SLAVE_PORT}/tcp 2>/dev/null || true
        rm -f "$rdb_file"
    done
}

# ---- 主流程 ----
mode="${1:-all}"
case "$mode" in
    sync) test_realtime_sync ;;
    full) test_full_sync ;;
    all)
        test_realtime_sync
        test_full_sync
        ;;
    *)
        err "未知模式: $mode (支持: sync | full | all)"
        exit 1
        ;;
esac

echo ""
cat "$RESULT_FILE"
echo ""
log "结果保存至: $RESULT_FILE / $RESULT_CSV"
