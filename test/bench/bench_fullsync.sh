#!/bin/bash
# =============================================================================
# bench_fullsync.sh — 全量同步传输速率测试 (slave 侧执行)
#
# 对比 kvstore RDMA vs Redis sendfile vs Redis diskless 的端到端同步吞吐量。
# 脚本运行在 slave 机器上，负责启动 slave 并监控同步完成。
#
# 用法:
#   # === 准备阶段 (master 机器) ===
#   # 1. 启动 master，填充数据
#   sudo ./build/kvstore conf/bench/kvstore-master-repl.conf &
#   sudo ./test/bench/bench_fullsync.sh populate 50000
#   #    ↑ 最后一行输出 "PAYLOAD=<bytes>"，记下这个值
#
#   # === 测试阶段 (slave 机器) ===
#   sudo ./test/bench/bench_fullsync.sh kvs --payload <bytes>
#   sudo ./test/bench/bench_fullsync.sh redis --payload <bytes>
#   sudo ./test/bench/bench_fullsync.sh redis-diskless --payload <bytes>
#
# 选项:
#   --master-host IP     master 地址 (默认 192.168.30.135)
#   --master-port N      master 端口 (默认 2000/6379)
#   --payload N          RDB 数据大小 (bytes)
#   --kvs-slave-port N   kvstore slave 端口 (默认 2002)
#   --redis-slave-port N  Redis slave 端口 (默认 6380)
#
# 输出: test/bench/results_fullsync.csv
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KVS_BIN="${PROJECT_DIR}/build/kvstore"
KVS_CONF_DIR="${PROJECT_DIR}/conf/bench"
RESULT_CSV="${SCRIPT_DIR}/results_fullsync.csv"

# 默认值
MASTER_HOST="${MASTER_HOST:-192.168.30.135}"
MASTER_PORT="${MASTER_PORT:-2000}"
PAYLOAD="${PAYLOAD:-0}"
SLAVE_PORT="${SLAVE_PORT:-2002}"
REDIS_SLAVE_PORT="${REDIS_SLAVE_PORT:-6380}"
REDIS_MASTER_PORT="${REDIS_MASTER_PORT:-6379}"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; RED='\033[0;31m'; NC='\033[0m'

usage() {
    echo "用法: $0 <command> [options]"
    echo ""
    echo "命令:"
    echo "  kvs              测试 kvstore RDMA 全量同步速率"
    echo "  redis            测试 Redis sendfile 全量同步速率"
    echo "  redis-diskless   测试 Redis diskless 全量同步速率"
    echo "  populate <nkeys> 向 master 填充测试数据 (在 master 机器执行)"
    echo ""
    echo "选项:"
    echo "  --master-host IP    master 地址"
    echo "  --master-port N     master 端口"
    echo "  --payload <bytes>   RDB 数据大小 (populate 最后一行会给出)"
    echo "  --kvs-slave-port N   kvstore slave 端口 (默认 2002)"
echo "  --redis-slave-port N  Redis slave 端口 (默认 6380)"
    echo ""
    echo "完整流程:"
    echo "  # master 机器: 启动 master + 填充数据"
    echo "  sudo ./build/kvstore conf/bench/kvstore-master-repl.conf &"
    echo "  sudo $0 populate 50000"
    echo ""
    echo "  # slave 机器: 分别测试三个场景"
    echo "  sudo $0 kvs --payload 1250000"
    echo "  sudo $0 redis --payload 1250000"
    echo "  sudo $0 redis-diskless --payload 1250000"
    exit 1
}

# 需要 root — 启动 slave 用 proactor
require_root() {
    if [ "${EUID:-0}" -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC} 需要 root 权限 (启动 slave 需要 proactor)"
        exit 1
    fi
}

# ---- populate (master 机器执行) ----

do_populate() {
    local nkeys=$1 host port
    host="${MASTER_HOST}"
    port="${MASTER_PORT}"

    if ! redis-cli -h "$host" -p "$port" PING >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} master 未运行 ($host:$port)"
        exit 1
    fi

    echo "填充 $nkeys 条数据到 $host:$port ..."
    local batch=10000 sent=0
    while [ $sent -lt $nkeys ]; do
        local batch_n=$batch
        [ $((sent + batch_n)) -gt $nkeys ] && batch_n=$((nkeys - sent))
        for i in $(seq $((sent + 1)) $((sent + batch_n))); do
            printf '*3\r\n$3\r\nSET\r\n$9\r\nkey%06d\r\n$16\r\nbenchmark_value_\r\n' $i
        done | timeout 300 nc -w 5 "$host" "$port" > /dev/null 2>&1 || true
        sent=$((sent + batch_n))
        echo -n "."
    done
    echo ""
    echo "填充完成: $nkeys 条"

    # 触发 SAVE 获取 RDB 大小
    echo -ne '*1\r\n$4\r\nSAVE\r\n' | nc -w 10 "$host" "$port" > /dev/null 2>&1 || true
    sleep 2
    local rdb_file="${PROJECT_DIR}/build/dump.rdb"
    local rdb_size=0
    if [ -f "$rdb_file" ]; then
        rdb_size=$(stat -c%s "$rdb_file" 2>/dev/null || echo 0)
    fi

    if [ "$rdb_size" -gt 0 ]; then
        local rdb_mb=$(echo "scale=2; $rdb_size / 1048576" | bc -l)
        echo ""
        echo -e "  ${GREEN}PAYLOAD=$rdb_size${NC}  (${rdb_mb} MB)"
        echo "  ← 在 slave 机器执行 bench 命令时传入此值:"
        echo "    sudo $0 kvs --payload $rdb_size --master-host $host"
    else
        echo ""
        echo -e "  ${RED}[WARN]${NC} 无法获取 RDB 大小，请手动计算"
    fi
}

# ---- kvs 全量同步测试 ----

do_test_kvs() {
    require_root

    local host="${MASTER_HOST}" port="${MASTER_PORT}"

    # 检查 master 是否可达
    if ! redis-cli -h "$host" -p "$port" PING >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} kvstore master 未运行 ($host:$port)"
        exit 1
    fi

    # 检查 payload
    if [ "$PAYLOAD" -le 0 ]; then
        echo -e "${RED}[ERROR]${NC} 缺少 --payload，请从 populate 输出获取"
        exit 1
    fi
    local payload_mb=$(echo "scale=2; $PAYLOAD / 1048576" | bc -l)

    # 确保 slave 配置中的 rdma_master_ip 与 --master-host 一致
    local conf_template="${KVS_CONF_DIR}/kvstore-slave-repl.conf"
    local conf_effective="${conf_template}"
    local conf_ip
    conf_ip=$(grep '^rdma_master_ip ' "$conf_template" | awk '{print $2}')
    if [ "$conf_ip" != "$host" ]; then
        conf_effective="/tmp/kvstore_slave_$$.conf"
        sed "s/^rdma_master_ip .*/rdma_master_ip $host/" \
            "$conf_template" > "$conf_effective"
        echo "  rdma_master_ip 已从配置中的 $conf_ip 更新为 $host"
    fi

    # 清理旧 slave
    echo ""; echo "清理旧 slave 进程..."
    pkill -f "kvstore.*slave" 2>/dev/null || true
    sleep 0.5
    fuser -k "${SLAVE_PORT}/tcp" 2>/dev/null || true
    sleep 0.3

    local slave_log="/tmp/kvstore_fullsync_slave.log"
    rm -f "$slave_log"

    echo ""
    echo "============================================"
    echo "  全量同步测试 — kvstore RDMA"
    echo "============================================"
    echo "  Master:    $host:$port"
    echo "  Payload:   ${payload_mb} MB"
    echo "  Slave log: $slave_log"
    echo "============================================"
    echo ""

    # 启动 slave 并计时
    local t0_ns=$(date +%s%N)

    nohup "$KVS_BIN" "$conf_effective" > "$slave_log" 2>&1 &
    local slave_pid=$!
    echo "  slave pid=$slave_pid"

    # 等待全量同步完成
    local timeout=120 synced=0
    for ((i = 1; i <= timeout; i++)); do
        if grep -q 'loaded.*keys from fullsync' "$slave_log" 2>/dev/null; then
            synced=1
            break
        fi
        if ! kill -0 "$slave_pid" 2>/dev/null; then
            echo ""
            echo -e "${RED}[ERROR]${NC} slave 进程意外退出"
            tail -10 "$slave_log"
            exit 1
        fi
        [ $((i % 10)) -eq 0 ] && echo "    ...等待同步完成 (${i}s)"
        sleep 1
    done

    local t1_ns=$(date +%s%N)

    if [ "$synced" -eq 0 ]; then
        echo -e "${RED}[ERROR]${NC} 全量同步超时 (${timeout}s)"
        kill "$slave_pid" 2>/dev/null || true
        exit 1
    fi

    local elapsed=$(echo "scale=3; ($t1_ns - $t0_ns) / 1000000000" | bc -l)
    if [ "$(echo "$elapsed < 0.001" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
        elapsed="0.001"
    fi
    local speed_mbps=$(echo "scale=2; $PAYLOAD / $elapsed / 1048576" | bc -l)

    local loaded_info
    loaded_info=$(grep 'loaded.*keys from fullsync' "$slave_log" | tail -1)
    loaded_info="${loaded_info:-N/A}"

    echo ""
    echo "  ═══════════════════════════════════"
    echo "  kvstore RDMA 全量同步结果"
    echo "  ═══════════════════════════════════"
    echo "  数据量:     ${payload_mb} MB"
    echo "  耗时:       ${elapsed} s"
    echo -e "  吞吐量:     ${GREEN}${speed_mbps} MB/s${NC}"
    echo "  加载信息:   $loaded_info"
    echo ""

    # 数据验证 (等待 slave 端口绑定)
    local verify
    sleep 0.5
    verify=$(printf '*2\r\n$5\r\nEXIST\r\n$9\r\nkey000001\r\n' | nc -w 2 127.0.0.1 "$SLAVE_PORT" 2>/dev/null | head -c 4 || true)
    if [ "$verify" != ":1" ]; then
        sleep 1.0
        verify=$(printf '*2\r\n$5\r\nEXIST\r\n$9\r\nkey000001\r\n' | nc -w 2 127.0.0.1 "$SLAVE_PORT" 2>/dev/null | head -c 4 || true)
    fi
    if [ "$verify" = ":1" ]; then
        echo -e "  数据验证:   ${GREEN}通过${NC}"
    else
        echo -e "  数据验证:   ${RED}失败${NC} (EXIST=$verify)"
    fi

    # 写入 CSV
    if [ ! -f "$RESULT_CSV" ]; then
        echo "scenario,payload_bytes,payload_mb,elapsed_s,speed_mbps" > "$RESULT_CSV"
    fi
    echo "kvstore,$PAYLOAD,${payload_mb},${elapsed},${speed_mbps}" >> "$RESULT_CSV"

    # 显示对比
    _show_comparison "kvstore" "$speed_mbps"

    # 清理
    kill "$slave_pid" 2>/dev/null || true
    sleep 0.5
    fuser -k "${SLAVE_PORT}/tcp" 2>/dev/null || true
    [ "$conf_effective" != "$conf_template" ] && rm -f "$conf_effective"
}

# ---- Redis 全量同步测试 ----

do_test_redis_diskless() {
    local diskless="no"
    [ "$1" = "yes" ] && diskless="yes"

    local label scenario
    if [ "$diskless" = "yes" ]; then
        label="Redis diskless"
        scenario="redis-diskless"
    else
        label="Redis sendfile"
        scenario="redis"
    fi

    local master_host="${MASTER_HOST}"
    local redis_master_port="${REDIS_MASTER_PORT}"
    local slave_port="${REDIS_SLAVE_PORT}"

    # 检查本地 Redis slave 是否可操作
    if ! redis-cli -p "$slave_port" PING >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} 本地 Redis slave 未运行 (port $slave_port)"
        echo "  请先启动 slave: redis-server --port $slave_port"
        exit 1
    fi

    # 检查 master 是否可达
    if ! redis-cli -h "$master_host" -p "$redis_master_port" PING >/dev/null 2>&1; then
        echo -e "${RED}[ERROR]${NC} Redis master 未运行 ($master_host:$redis_master_port)"
        exit 1
    fi

    # 获取 payload 大小
    local payload rdb_size
    if [ "$PAYLOAD" -gt 0 ]; then
        payload="$PAYLOAD"
    else
        # 尝试从 master INFO 获取
        redis-cli -h "$master_host" -p "$redis_master_port" SAVE >/dev/null 2>&1
        sleep 2
        rdb_size=$(redis-cli -h "$master_host" -p "$redis_master_port" INFO persistence 2>/dev/null | \
                   grep 'rdb_last_save_size' | cut -d: -f2 | tr -d '\r')
        payload="${rdb_size:-0}"
    fi

    if [ "$payload" -le 0 ]; then
        echo -e "${RED}[ERROR]${NC} 无法获取 payload，请用 --payload 指定"
        exit 1
    fi
    local payload_mb=$(echo "scale=2; $payload / 1048576" | bc -l)

    # 断开已有复制关系
    redis-cli -p "$slave_port" SLAVEOF NO ONE >/dev/null 2>&1 || true
    sleep 0.5

    # diskless 模式: 在 master 上开启
    if [ "$diskless" = "yes" ]; then
        redis-cli -h "$master_host" -p "$redis_master_port" CONFIG SET repl-diskless-sync yes >/dev/null 2>&1 || true
        redis-cli -h "$master_host" -p "$redis_master_port" CONFIG SET repl-diskless-sync-delay 0 >/dev/null 2>&1 || true
    else
        redis-cli -h "$master_host" -p "$redis_master_port" CONFIG SET repl-diskless-sync no >/dev/null 2>&1 || true
    fi

    echo ""
    echo "============================================"
    echo "  全量同步测试 — $label"
    echo "============================================"
    echo "  Master:    $master_host:$redis_master_port"
    echo "  Slave:     127.0.0.1:$slave_port"
    echo "  Payload:   ${payload_mb} MB"
    echo "  Diskless:  $diskless"
    echo "============================================"
    echo ""

    # 断掉旧复制关系，开始同步并计时
    redis-cli -p "$slave_port" SLAVEOF NO ONE >/dev/null 2>&1 || true
    sleep 1

    local t0_ns=$(date +%s%N)
    redis-cli -p "$slave_port" SLAVEOF "$master_host" "$redis_master_port" >/dev/null 2>&1

    # 等待同步完成
    local timeout=120 synced=0
    for ((i = 1; i <= timeout; i++)); do
        local link_status
        link_status=$(redis-cli -p "$slave_port" INFO replication 2>/dev/null | \
                      grep 'master_link_status' | cut -d: -f2 | tr -d '\r')
        if [ "$link_status" = "up" ]; then
            synced=1
            break
        fi
        # 检查是否报错
        if redis-cli -p "$slave_port" INFO replication 2>/dev/null | grep -q 'master_link_down_since_seconds:'; then
            [ $((i % 10)) -eq 0 ] && echo "    ...等待同步完成 (${i}s, 可能仍在传输中)"
        fi
        sleep 1
    done

    local t1_ns=$(date +%s%N)

    if [ "$synced" -eq 0 ]; then
        echo -e "${RED}[ERROR]${NC} Redis 全量同步超时 (${timeout}s)"
        redis-cli -p "$slave_port" SLAVEOF NO ONE >/dev/null 2>&1 || true
        exit 1
    fi

    local elapsed=$(echo "scale=3; ($t1_ns - $t0_ns) / 1000000000" | bc -l)
    if [ "$(echo "$elapsed < 0.001" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
        elapsed="0.001"
    fi
    local speed_mbps=$(echo "scale=2; $payload / $elapsed / 1048576" | bc -l)

    echo ""
    echo "  ═══════════════════════════════════"
    echo "  $label 全量同步结果"
    echo "  ═══════════════════════════════════"
    echo "  数据量:     ${payload_mb} MB"
    echo "  耗时:       ${elapsed} s"
    echo -e "  吞吐量:     ${GREEN}${speed_mbps} MB/s${NC}"
    echo ""

    # 数据验证
    local verify
    verify=$(redis-cli -p "$slave_port" GET key000001 2>/dev/null)
    if [ "$verify" = "benchmark_value_" ]; then
        echo -e "  数据验证:   ${GREEN}通过${NC} (key000001=$verify)"
    else
        echo -e "  数据验证:   ${RED}失败${NC} (key000001=$verify)"
    fi

    # 写入 CSV
    if [ ! -f "$RESULT_CSV" ]; then
        echo "scenario,payload_bytes,payload_mb,elapsed_s,speed_mbps" > "$RESULT_CSV"
    fi
    echo "${scenario},${payload},${payload_mb},${elapsed},${speed_mbps}" >> "$RESULT_CSV"

    # 显示对比
    _show_comparison "$scenario" "$speed_mbps"

    # 清理: 断开复制关系 (保留 slave 进程)
    redis-cli -p "$slave_port" SLAVEOF NO ONE >/dev/null 2>&1 || true
}

# ---- 对比显示 ----

_show_comparison() {
    local current="$1" speed="$2"
    local scenarios=("kvstore" "redis" "redis-diskless")
    local names=("kvstore RDMA" "Redis sendfile" "Redis diskless")
    local found=()

    for s in "${scenarios[@]}"; do
        if grep -q "^${s}," "$RESULT_CSV" 2>/dev/null; then
            found+=("$s")
        fi
    done

    [ "${#found[@]}" -lt 2 ] && return

    echo ""
    echo "  ═══════════════════════════════════"
    echo "  已有结果对比"

    local max_speed=0 max_name=""
    for s in "${found[@]}"; do
        local s_speed name
        s_speed=$(grep "^${s}," "$RESULT_CSV" | tail -1 | cut -d',' -f5)
        case "$s" in
            kvstore) name="kvstore RDMA" ;;
            redis) name="Redis sendfile" ;;
            redis-diskless) name="Redis diskless" ;;
        esac
        printf "  %-20s %10s MB/s\n" "$name" "$s_speed"
        if [ "$(echo "$s_speed > $max_speed" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
            max_speed="$s_speed"
            max_name="$name"
        fi
    done

    if [ "${#found[@]}" -eq 3 ]; then
        echo "  ──────────────────────────────────"
        # kvstore / redis sendfile
        local kvs_speed redis_speed
        kvs_speed=$(grep "^kvstore," "$RESULT_CSV" | tail -1 | cut -d',' -f5)
        redis_speed=$(grep "^redis," "$RESULT_CSV" | tail -1 | cut -d',' -f5)
        local ratio1=$(echo "scale=2; $kvs_speed / $redis_speed" | bc -l 2>/dev/null || echo "N/A")
        echo -e "  kvstore/Redis-sendfile:  ${GREEN}${ratio1}x${NC}"

        local diskless_speed
        diskless_speed=$(grep "^redis-diskless," "$RESULT_CSV" | tail -1 | cut -d',' -f5)
        local ratio2=$(echo "scale=2; $kvs_speed / $diskless_speed" | bc -l 2>/dev/null || echo "N/A")
        echo -e "  kvstore/Redis-diskless:  ${GREEN}${ratio2}x${NC}"
    fi
    echo ""
}

# ---- main ----

[ $# -lt 1 ] && usage

# 解析选项
CMD=""
while [ $# -gt 0 ]; do
    case "$1" in
        kvs|redis|redis-diskless|populate)
            CMD="$1"; shift
            ;;
        --master-host) MASTER_HOST="$2"; shift 2 ;;
        --master-port) MASTER_PORT="$2"; shift 2 ;;
        --payload)     PAYLOAD="$2"; shift 2 ;;
        --kvs-slave-port)   SLAVE_PORT="$2"; shift 2 ;;
        --redis-slave-port) REDIS_SLAVE_PORT="$2"; shift 2 ;;
        -*) echo "未知选项: $1"; exit 1 ;;
        *)  break ;;
    esac
done

case "$CMD" in
    kvs)
        do_test_kvs
        ;;
    redis)
        do_test_redis_diskless "no"
        ;;
    redis-diskless)
        do_test_redis_diskless "yes"
        ;;
    populate)
        [ $# -lt 1 ] && { echo "用法: $0 populate <nkeys>"; exit 1; }
        do_populate "$1"
        ;;
    *)
        usage
        ;;
esac
