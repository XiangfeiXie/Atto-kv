#!/bin/bash
# =============================================================================
# bench_sync_qps.sh — 实时同步 QPS 损耗测试
#
# 不管理服务生命周期。由用户手动启动目标服务后，运行本脚本压测。
# 每次运行追加结果到 CSV，跑完 4 个场景后汇总对比。
#
# 场景:
#   kvs-off    kvstore 单机，无复制 (port 2000)
#   kvs-on     kvstore master + slave eBPF (port 2000)
#   redis-off  Redis 单机，无 slave (port 6379)
#   redis-on   Redis master + slave (port 6379)
#
# 用法:
#   # 1. kvstore 关闭复制
#   sudo ./build/kvstore conf/bench/kvstore-none.conf
#   ./test/bench/bench_sync_qps.sh kvs-off
#
#   # 2. kvstore 开启 eBPF 复制
#   sudo ./build/kvstore conf/bench/kvstore-master-repl.conf   # master
#   sudo ./build/kvstore conf/bench/kvstore-slave-repl.conf    # slave
#   # (等待全量同步完成)
#   ./test/bench/bench_sync_qps.sh kvs-on
#
#   # 3. Redis 单机
#   sudo systemctl start redis-server
#   redis-cli -p 6379 FLUSHALL
#   ./test/bench/bench_sync_qps.sh redis-off
#
#   # 4. Redis 主从复制
#   # master (默认 6379) 已启动
#   redis-server --port 6380 --replicaof 127.0.0.1 6379
#   ./test/bench/bench_sync_qps.sh redis-on
#
# 环境变量:
#   HOST=127.0.0.1    目标地址
#   PORT=2000         目标端口
#   N=500000          请求数/轮
#   C=50              并发连接数
#   P_VAL="1 20 80"   Pipeline 深度 (空格分隔)
#   D_VAL="16 256"    Value 大小 (空格分隔)
#
# 输出: test/bench/results_sync_qps.csv
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULT_CSV="${SCRIPT_DIR}/results_sync_qps.csv"

N=${N:-500000}
C=${C:-50}
P_VALUES=(${P_VAL:-1 20 80})
D_VALUES=(${D_VAL:-16 256 1024})

GREEN='\033[0;32m'; CYAN='\033[0;36m'; RED='\033[0;31m'; NC='\033[0m'

usage() {
    echo "用法: $0 <scenario>"
    echo ""
    echo "  kvs-off    kvstore 关闭 eBPF 复制 (port 2000)"
    echo "  kvs-on     kvstore 开启 eBPF 复制 (port 2000)"
    echo "  redis-off  Redis 单机基线 (port 6379)"
    echo "  redis-on   Redis 开启主从复制 (port 6379)"
    echo ""
    echo "环境变量:"
    echo "  HOST=IP        目标地址 (默认 127.0.0.1)"
    echo "  PORT=N         目标端口 (默认 2000/6379)"
    echo "  N=500000       请求数/轮"
    echo "  C=50           并发连接数"
    echo "  P_VAL=\"1 20\"   Pipeline 深度"
    echo "  D_VAL=\"16 256\" Value 大小"
    echo ""
    echo "示例:"
    echo "  $0 kvs-off"
    echo "  PORT=6380 $0 redis-on      # 压测非标准端口的 slave"
    exit 1
}

[ $# -lt 1 ] && usage

SCENARIO="$1"

case "$SCENARIO" in
    kvs-off|off)            # off 保留向后兼容
        HOST="${HOST:-127.0.0.1}"
        PORT="${PORT:-2000}"
        LABEL="kvs-off"
        LABEL_CN="kvstore-关闭复制"
        ;;
    kvs-on|on)              # on 保留向后兼容
        HOST="${HOST:-127.0.0.1}"
        PORT="${PORT:-2000}"
        LABEL="kvs-on"
        LABEL_CN="kvstore-eBPF复制"
        ;;
    redis-off|redis)        # redis 保留向后兼容
        HOST="${HOST:-127.0.0.1}"
        PORT="${PORT:-6379}"
        LABEL="redis-off"
        LABEL_CN="Redis-关闭复制"
        ;;
    redis-on)
        HOST="${HOST:-127.0.0.1}"
        PORT="${PORT:-6379}"
        LABEL="redis-on"
        LABEL_CN="Redis-主从复制"
        ;;
    *)  usage ;;
esac

# ---- 预检 ----
for tool in redis-benchmark redis-cli bc; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} 缺少工具: $tool"
        exit 1
    fi
done

if ! redis-cli -h "$HOST" -p "$PORT" PING >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} 无法连接 $HOST:$PORT，请确认服务已启动"
    exit 1
fi

echo ""
echo "============================================"
echo "  实时同步 QPS 测试"
echo "============================================"
echo "  场景: $LABEL_CN"
echo "  目标: $HOST:$PORT"
echo "  请求: $N/轮, 连接: $C"
echo "  P 值: ${P_VALUES[*]}"
echo "  D 值: ${D_VALUES[*]} (bytes)"
echo "============================================"
echo ""

# 首次运行时写 CSV 表头
if [ ! -f "$RESULT_CSV" ]; then
    echo "scenario,cmd,P,data_size,qps" > "$RESULT_CSV"
fi

for d in "${D_VALUES[@]}"; do
    echo -e "${CYAN}── data_size=${d}B ──${NC}"
    for P in "${P_VALUES[@]}"; do
        for test in set get; do
            csv_line=$(redis-benchmark -h "$HOST" -p "$PORT" -t "$test" \
                -n "$N" -c "$C" -P "$P" -d "$d" --csv 2>/dev/null | tail -1)
            qps=$(echo "$csv_line" | cut -d',' -f2 | tr -d '"')
            qps="${qps:-0}"
            echo "$LABEL,$test,$P,$d,$qps" >> "$RESULT_CSV"
            printf "  %-4s P=%-3d d=%-5d → ${GREEN}%10s${NC} req/s\n" \
                "$test" "$P" "$d" "$qps"
        done
    done
    echo ""
done

echo "============================================"
echo "  本场景结果已追加到: $RESULT_CSV"
echo "============================================"

# 检查全部 4 个场景是否跑完
ALL_LABELS=("kvs-off" "kvs-on" "redis-off" "redis-on")
ALL_NAMES=("kvstore-关闭复制" "kvstore-eBPF复制" "Redis-关闭复制" "Redis-主从复制")
MISSING=""
for i in "${!ALL_LABELS[@]}"; do
    grep -q "^${ALL_LABELS[$i]}," "$RESULT_CSV" 2>/dev/null || MISSING="$MISSING ${ALL_LABELS[$i]}"
done

if [ -n "$MISSING" ]; then
    echo ""
    echo "  尚未完成的场景:$MISSING"
    echo "  全部跑完后再次运行任意场景即可看到汇总对比。"
else
    echo ""
    echo "============================================"
    echo "  四场景汇总对比"
    echo "============================================"
    echo ""

    show_table() {
        local ref_P=$1
        echo "── P=${ref_P} ──"
        printf "%-20s" "场景"
        for test in set get; do
            for d in "${D_VALUES[@]}"; do
                printf "%-12s" "${test}/${d}B"
            done
        done
        echo ""
        printf "%-20s" "--------------------"
        for test in set get; do
            for d in "${D_VALUES[@]}"; do
                printf "%-12s" "----------"
            done
        done
        echo ""

        for i in "${!ALL_LABELS[@]}"; do
            local lbl="${ALL_LABELS[$i]}"
            local name="${ALL_NAMES[$i]}"
            printf "%-20s" "$name"
            for test in set get; do
                for d in "${D_VALUES[@]}"; do
                    local val
                    val=$(grep "^${lbl},${test},${ref_P},${d}," "$RESULT_CSV" 2>/dev/null | cut -d',' -f5)
                    printf "%-12s" "${val:--}"
                done
            done
            echo ""
        done
        echo ""
    }

    show_table 20
    show_table 80

    # 计算 QPS 损耗百分比 (SET, P=20, d=16 作为代表)
    echo "── SET QPS 损耗 (P=20, d=16B) ──"
    echo ""
    local kvs_off_qps kvs_on_qps redis_off_qps redis_on_qps
    kvs_off_qps=$(grep "^kvs-off,set,20,16," "$RESULT_CSV" 2>/dev/null | cut -d',' -f5)
    kvs_on_qps=$(grep "^kvs-on,set,20,16," "$RESULT_CSV" 2>/dev/null | cut -d',' -f5)
    redis_off_qps=$(grep "^redis-off,set,20,16," "$RESULT_CSV" 2>/dev/null | cut -d',' -f5)
    redis_on_qps=$(grep "^redis-on,set,20,16," "$RESULT_CSV" 2>/dev/null | cut -d',' -f5)

    if [ -n "$kvs_off_qps" ] && [ -n "$kvs_on_qps" ] && [ "$kvs_off_qps" != "0" ]; then
        local kvs_drop=$(echo "scale=1; (1 - $kvs_on_qps / $kvs_off_qps) * 100" | bc -l 2>/dev/null || echo "N/A")
        echo "  kvstore:  ${kvs_off_qps} → ${kvs_on_qps} req/s (损耗 ${kvs_drop}%)"
    fi
    if [ -n "$redis_off_qps" ] && [ -n "$redis_on_qps" ] && [ "$redis_off_qps" != "0" ]; then
        local redis_drop=$(echo "scale=1; (1 - $redis_on_qps / $redis_off_qps) * 100" | bc -l 2>/dev/null || echo "N/A")
        echo "  Redis:    ${redis_off_qps} → ${redis_on_qps} req/s (损耗 ${redis_drop}%)"
    fi
    echo ""
    echo "  完整数据: $RESULT_CSV"
fi
