#!/bin/bash
# =============================================================================
# redis_fullsync_bench.sh — Redis 全量同步基准测试
#
# 测试 Redis 两种全量同步传输模式的速率:
#   disk     — 有盘传输 (先 BGSAVE 写 RDB 到磁盘, 再发送文件给 replica)
#   diskless — 无盘传输 (BGSAVE 直接流式写入 replica socket, 不落盘)
#
# 统一速率标准: Throughput (MB/s) = RDB_file_size_bytes / time_sec / 1048576
#
# RDB 格式是 kvstore 和 Redis 的最大公约数。有盘模式将 RDB 文件写入磁盘后传输,
# 无盘模式直接流式发送 RDB 数据到 replica socket — 内容相同, 路径不同。
# 使用 RDB 文件大小作为统一分母, 确保跨模式、跨系统的可比性。
#
# 对比关系:
#   kvstore sendfile  ↔  Redis disk     (都经过磁盘: SAVE → 文件 → 传输)
#   kvstore RDMA      ↔  Redis diskless  (都跳过磁盘: 内存 → 网络)
#
# 用法:
#   ./test/bench/redis_fullsync_bench.sh [--keys N] [--vsize B] [--iterations N]
#                                         [--port P] [--mode MODE]
# =============================================================================

set -u
shopt -s nullglob

# ── 默认参数 ────────────────────────────────────────────────────────────────

KEYS=60000
VSIZE=256
ITERATIONS=3
PORT=8400
MODE=both
TIMEOUT=300

# ── 派生常量 ────────────────────────────────────────────────────────────────

SLAVE_PORT=$((PORT + 1))
LOG_DIR="/tmp/redis_fsync_bench_$$"
mkdir -p "$LOG_DIR"

# ── 统一速率计算辅助函数 ──
# 使用 bc 而非 awk, 避免 gawk 常量折叠导致未执行分支的除零错误
_fmt() { local v=$1; [ "${v:0:1}" = "." ] && echo "0$v" || echo "$v"; }
calc_mbps() {
    if [ "$(echo "$2 > 0" | bc 2>/dev/null)" = 1 ]; then
        _fmt "$(echo "scale=2; $1 / $2 / 1048576" | bc)"
    else
        echo "0.00"
    fi
}
calc_entps() {
    if [ "$(echo "$2 > 0" | bc 2>/dev/null)" = 1 ]; then
        echo "scale=0; $1 / $2" | bc
    else
        echo "0"
    fi
}
export -f _fmt calc_mbps calc_entps

C_BLD=$'\033[1m';   C_DIM=$'\033[2m'
C_RED=$'\033[31m';  C_GRN=$'\033[32m';  C_YEL=$'\033[33m'
C_CYN=$'\033[36m';  C_RST=$'\033[0m'

# ── 辅助函数 ────────────────────────────────────────────────────────────────

die()  { echo "${C_RED}[FATAL]${C_RST} $*" >&2; exit 1; }
info() { echo "${C_GRN}[INFO]${C_RST}  $*" >&2; }
warn() { echo "${C_YEL}[WARN]${C_RST}  $*" >&2; }
step() { echo "${C_CYN}[STEP]${C_RST}  $*" >&2; }

usage() {
    cat <<EOF
用法: $0 [选项]

Redis 全量同步基准测试 — 有盘 (disk) 和无盘 (diskless) 两种传输模式

选项:
  --keys N         测试 key 数量 (默认: $KEYS)
  --vsize B        每个 value 字节数 (默认: $VSIZE)
  --iterations N   每种模式的测试轮数 (默认: $ITERATIONS)
  --port P         master 基准端口 (默认: $PORT, slave = port+1)
  --mode MODE      测试模式: disk | diskless | both (默认: $MODE)
  -h, --help       显示此帮助

统一速率标准:
  Throughput = RDB文件大小(字节) / 同步耗时(秒) / 1048576  →  MB/s
  有盘和无盘传输的是相同内容的 RDB 数据 (仅路径不同),
  使用相同的 RDB 文件大小作为分母, 确保公平对比.

对比关系:
  kvstore sendfile  ↔  Redis disk     (磁盘 → 文件 → 网络)
  kvstore RDMA      ↔  Redis diskless (内存 → 网络)
EOF
    exit 0
}

# ── 参数解析 ────────────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keys)       KEYS="$2";       shift 2 ;;
        --vsize)      VSIZE="$2";      shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --port)       PORT="$2";       SLAVE_PORT=$((PORT + 1)); shift 2 ;;
        --mode)       MODE="$2";       shift 2 ;;
        -h|--help)    usage ;;
        *) die "未知参数: $1 (使用 -h 查看帮助)" ;;
    esac
done

[[ "$MODE" =~ ^(disk|diskless|both)$ ]] || die "无效 mode: $MODE (应为 disk|diskless|both)"
[[ "$KEYS" -gt 0 ]] 2>/dev/null || die "无效 keys: $KEYS"
[[ "$VSIZE" -gt 0 ]] 2>/dev/null || die "无效 vsize: $VSIZE"
[[ "$ITERATIONS" -gt 0 ]] 2>/dev/null || die "无效 iterations: $ITERATIONS"

# ── 工具检查 ────────────────────────────────────────────────────────────────

for tool in redis-server redis-cli python3 bc; do
    command -v "$tool" >/dev/null 2>&1 || die "缺少工具: $tool"
done

# ── 进程/端口管理 ──────────────────────────────────────────────────────────

redkill() {
    redis-cli -p "$PORT" SHUTDOWN NOSAVE 2>/dev/null || true
    redis-cli -p "$SLAVE_PORT" SHUTDOWN NOSAVE 2>/dev/null || true
    pkill -9 -x redis-server 2>/dev/null || true
    # 等待端口释放
    local i
    for ((i=0; i<30; i++)); do
        if ! ss -tlnp 2>/dev/null | grep -qE ":($PORT|$SLAVE_PORT)\b"; then
            break
        fi
        sleep 0.2
    done
    sleep 0.5
}

cleanup() {
    redkill
    rm -rf "${LOG_DIR}/disk_m" "${LOG_DIR}/disk_s" \
           "${LOG_DIR}/diskless_m" "${LOG_DIR}/diskless_s" 2>/dev/null
    rm -f "${LOG_DIR}"/*.log 2>/dev/null
}

# 等待 Redis 退出 LOADING 状态并返回 DBSIZE (RDB 加载可能滞后于同步完成)
await_loaded() {
    local port=$1 timeout=${2:-60} i
    for ((i=0; i<timeout*2; i++)); do
        local reply; reply=$(redis-cli -p "$port" DBSIZE 2>/dev/null || echo "")
        if [[ "$reply" =~ ^[0-9]+$ ]]; then
            echo "$reply"; return 0
        fi
        sleep 0.5
    done
    echo "0"; return 1
}

# 等待日志中出现指定模式 (返回匹配时的 Unix 时间戳)
await_log() {
    local log=$1 pattern=$2 i=0 max=$((TIMEOUT * 10))
    while [ $i -lt $max ]; do
        if [ -f "$log" ] && grep -qE "$pattern" "$log" 2>/dev/null; then
            date +%s.%N
            return 0
        fi
        i=$((i + 1))
        sleep 0.1
    done
    return 1
}

# ── Phase 1: 数据填充 ──────────────────────────────────────────────────────

populate_redis() {
    step "Phase 1: 填充测试数据 ($KEYS 条 × ${VSIZE} B)"

    redkill
    local mdir="${LOG_DIR}/populate_m"
    rm -rf "$mdir"; mkdir -p "$mdir"

    redis-server --port "$PORT" --daemonize yes \
        --dir "$mdir" --dbfilename dump.rdb \
        --logfile "${LOG_DIR}/populate.log" \
        --save "" --appendonly no >/dev/null 2>&1
    sleep 1
    redis-cli -p "$PORT" PING >/dev/null 2>&1 || die "Redis 填充实例启动失败"

    # 分批 pipe 写入, 避免单次超大 pipe 超时丢失数据
    # 使用与 kvstore 脚本完全相同的数据生成方式 (确定性二进制 value)
    local batch=10000 start=1 end keys=0
    while [ "$start" -le "$KEYS" ]; do
        end=$((start + batch - 1))
        [ "$end" -gt "$KEYS" ] && end=$KEYS

        python3 -c "
import sys, struct

start = $start
end = $end
vsize = $VSIZE
buf = bytearray()

for i in range(start, end + 1):
    k = f'key:{i:012d}'.encode()
    unit = struct.pack('>Q', i)
    rep = (vsize + 7) // 8
    v = (unit * rep)[:vsize]
    buf.extend(f'*3\r\n\$3\r\nSET\r\n\${len(k)}\r\n'.encode())
    buf.extend(k)
    buf.extend(f'\r\n\${len(v)}\r\n'.encode())
    buf.extend(v)
    buf.extend(b'\r\n')

sys.stdout.buffer.write(bytes(buf))
sys.stdout.buffer.flush()
" 2>/dev/null | timeout 120 redis-cli -p "$PORT" --pipe >/dev/null 2>&1

        keys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
        printf "  ${C_DIM}批 %d-%d/%d: %d keys${C_RST}\r" "$start" "$end" "$KEYS" "$keys" >&2
        start=$((end + 1))
    done
    echo "" >&2

    if [ "$keys" -ne "$KEYS" ]; then
        warn "填充 key 数量不匹配: 期望 $KEYS, 实际 $keys"
        redkill; return 1
    fi

    # SAVE 获取 RDB 文件大小 — 这是统一的基准数据量
    redis-cli -p "$PORT" SAVE >/dev/null 2>&1
    sleep 1
    REDIS_RDB_PATH="${mdir}/dump.rdb"
    REDIS_RDB_SIZE=$(stat -c %s "$REDIS_RDB_PATH" 2>/dev/null || echo 0)
    if [ "$REDIS_RDB_SIZE" -eq 0 ]; then
        die "Redis RDB 文件为空"
    fi

    REDIS_RDB_MB=$(awk "BEGIN { printf \"%.2f\", ${REDIS_RDB_SIZE}/1048576 }")
    info "填充完成: $keys keys, RDB = $REDIS_RDB_MB MB ($REDIS_RDB_SIZE bytes)"

    redkill
    return 0
}

# ── 单轮测试: 有盘 (disk) ──────────────────────────────────────────────────

run_disk() {
    local iteration=$1
    local mdir="${LOG_DIR}/disk_m" sdir="${LOG_DIR}/disk_s"
    local mlog="${LOG_DIR}/disk_master_${iteration}.log"
    local slog="${LOG_DIR}/disk_slave_${iteration}.log"
    rm -rf "$mdir" "$sdir"; mkdir -p "$mdir" "$sdir"

    # 复制填充好的 RDB 到 master dir, master 启动时自动加载
    cp "$REDIS_RDB_PATH" "$mdir/dump.rdb"

    # Master: 有盘同步模式
    # --save "" 关闭自动 SAVE (避免干扰)
    # --repl-diskless-sync no 强制有盘传输
    redis-server --port "$PORT" --daemonize yes \
        --dir "$mdir" --dbfilename dump.rdb \
        --logfile "$mlog" --save "" --appendonly no \
        --repl-diskless-sync no >/dev/null 2>&1
    sleep 1

    local mkeys; mkeys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
    if [ "$mkeys" -ne "$KEYS" ]; then
        warn "[disk #$iteration] master RDB 加载失败: 期望 $KEYS, 实际 $mkeys"
        redkill; return 1
    fi

    # Slave: 空实例, 等待 SLAVEOF
    redis-server --port "$SLAVE_PORT" --daemonize yes \
        --dir "$sdir" --dbfilename dump.rdb \
        --logfile "$slog" --save "" --appendonly no >/dev/null 2>&1
    sleep 1

    # 触发全量同步
    redis-cli -p "$SLAVE_PORT" SLAVEOF 127.0.0.1 "$PORT" >/dev/null 2>&1

    # ── 计时: "Starting BGSAVE for SYNC" → "Synchronization succeeded" ──
    local t0; t0=$(await_log "$mlog" "Starting BGSAVE for SYNC")
    if [ -z "$t0" ]; then
        warn "[disk #$iteration] 超时: 未检测到 BGSAVE 开始"
        redkill; return 1
    fi

    local t1; t1=$(await_log "$mlog" "Synchronization with replica .* succeeded")
    if [ -z "$t1" ]; then
        warn "[disk #$iteration] 超时: 同步未完成"
        redkill; return 1
    fi

    local total_sec; total_sec=$(echo "$t1 - $t0" | bc)

    # 等待 slave 退出 LOADING 状态 (有盘模式下 RDB 加载可能滞后于同步完成)
    local skeys; skeys=$(await_loaded "$SLAVE_PORT" 60)
    if [ "$skeys" -ne "$KEYS" ]; then
        warn "[disk #$iteration] slave 数据不完整: 期望 $KEYS, 实际 $skeys"
    fi

    local total_mbps; total_mbps=$(calc_mbps "$REDIS_RDB_SIZE" "$total_sec")
    local ent_sec; ent_sec=$(calc_entps "$KEYS" "$total_sec")

    redkill
    echo "disk $iteration $skeys $REDIS_RDB_SIZE $REDIS_RDB_MB $total_sec $total_mbps $ent_sec"
}

# ── 单轮测试: 无盘 (diskless) ──────────────────────────────────────────────

run_diskless() {
    local iteration=$1
    local mdir="${LOG_DIR}/diskless_m" sdir="${LOG_DIR}/diskless_s"
    local mlog="${LOG_DIR}/diskless_master_${iteration}.log"
    local slog="${LOG_DIR}/diskless_slave_${iteration}.log"
    rm -rf "$mdir" "$sdir"; mkdir -p "$mdir" "$sdir"

    # 复用同一个 RDB 文件 — 确保有盘/无盘传输完全等量的数据
    cp "$REDIS_RDB_PATH" "$mdir/dump.rdb"

    # Master: 无盘同步模式
    # --repl-diskless-sync yes 强制无盘传输
    # --repl-diskless-sync-delay 0 立即开始, 不等待更多 replica
    redis-server --port "$PORT" --daemonize yes \
        --dir "$mdir" --dbfilename dump.rdb \
        --logfile "$mlog" --save "" --appendonly no \
        --repl-diskless-sync yes \
        --repl-diskless-sync-delay 0 >/dev/null 2>&1
    sleep 1

    local mkeys; mkeys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
    if [ "$mkeys" -ne "$KEYS" ]; then
        warn "[diskless #$iteration] master RDB 加载失败: 期望 $KEYS, 实际 $mkeys"
        redkill; return 1
    fi

    # Slave
    redis-server --port "$SLAVE_PORT" --daemonize yes \
        --dir "$sdir" --dbfilename dump.rdb \
        --logfile "$slog" --save "" --appendonly no >/dev/null 2>&1
    sleep 1

    # 触发全量同步
    redis-cli -p "$SLAVE_PORT" SLAVEOF 127.0.0.1 "$PORT" >/dev/null 2>&1

    # ── 计时: 同 disk 模式的计时方式, 从日志提取 ──
    local t0; t0=$(await_log "$mlog" "Starting BGSAVE for SYNC")
    if [ -z "$t0" ]; then
        warn "[diskless #$iteration] 超时: 未检测到 BGSAVE 开始"
        redkill; return 1
    fi

    local t1; t1=$(await_log "$mlog" "Synchronization with replica .* succeeded")
    if [ -z "$t1" ]; then
        warn "[diskless #$iteration] 超时: 同步未完成"
        redkill; return 1
    fi

    local total_sec; total_sec=$(echo "$t1 - $t0" | bc)

    # 验证 (diskless 模式也可能有短暂的 LOADING 窗口)
    local skeys; skeys=$(await_loaded "$SLAVE_PORT" 60)
    if [ "$skeys" -ne "$KEYS" ]; then
        warn "[diskless #$iteration] slave 数据不完整: 期望 $KEYS, 实际 $skeys"
    fi

    local total_mbps; total_mbps=$(calc_mbps "$REDIS_RDB_SIZE" "$total_sec")
    local ent_sec; ent_sec=$(calc_entps "$KEYS" "$total_sec")

    redkill
    echo "diskless $iteration $skeys $REDIS_RDB_SIZE $REDIS_RDB_MB $total_sec $total_mbps $ent_sec"
}

# ── 统计计算 ────────────────────────────────────────────────────────────────

calc_stats() {
    local mode=$1
    local total_sec_sum=0 total_mbps_sum=0 ent_sec_sum=0 count=0
    local min_total_mbps="" max_total_mbps=""
    local keys="" rdb_mb=""

    while IFS=' ' read -r _mode _iter _skeys _rdb_sz _rdb_mb _total_sec _total_mbps _ent_sec; do
        [ "$_mode" != "$mode" ] && continue
        count=$((count + 1))
        keys=$_skeys; rdb_mb=$_rdb_mb

        total_sec_sum=$(echo "$total_sec_sum + $_total_sec" | bc)
        total_mbps_sum=$(echo "$total_mbps_sum + $_total_mbps" | bc)
        ent_sec_sum=$(echo "$ent_sec_sum + $_ent_sec" | bc)

        if [ -z "$min_total_mbps" ] || [ "$(echo "$_total_mbps < $min_total_mbps" | bc)" = 1 ]; then
            min_total_mbps=$_total_mbps
        fi
        if [ -z "$max_total_mbps" ] || [ "$(echo "$_total_mbps > $max_total_mbps" | bc)" = 1 ]; then
            max_total_mbps=$_total_mbps
        fi
    done < "${LOG_DIR}/results.txt"

    if [ "$count" -eq 0 ]; then
        echo "0 0 0 0 0 0 0 0"
        return
    fi

    local avg_total_sec avg_total_mbps avg_ent_sec
    avg_total_sec=$(_fmt "$(echo "scale=3; $total_sec_sum / $count" | bc)")
    avg_total_mbps=$(_fmt "$(echo "scale=2; $total_mbps_sum / $count" | bc)")
    avg_ent_sec=$(echo "scale=0; $ent_sec_sum / $count" | bc)

    echo "$keys $rdb_mb $count $avg_total_sec $min_total_mbps $avg_total_mbps $max_total_mbps $avg_ent_sec"
}

# ── 报告 ────────────────────────────────────────────────────────────────────

print_sep() {
    printf "  %s\n" "$(printf '─%.0s' $(seq 1 60))"
}

print_report() {
    echo ""
    echo "══════════════════════════════════════════════════════════════════════"
    echo "  ${C_BLD}Redis Fullsync Benchmark${C_RST} — ${C_YEL}KEYS=$KEYS  VSIZE=$VSIZE B${C_RST}"
    echo "  RDB Size: $REDIS_RDB_MB MB ($REDIS_RDB_SIZE bytes)  Port: $PORT"
    echo "══════════════════════════════════════════════════════════════════════"
    echo ""
    printf "  %-10s %6s %8s %10s %11s %8s\n" \
        "MODE" "KEYS" "RDB_MB" "TOTAL_SEC" "TOTAL_MB/s" "ENT/s"
    print_sep

    for mode in disk diskless; do
        if [ "$MODE" != "both" ] && [ "$MODE" != "$mode" ]; then
            continue
        fi

        while IFS=' ' read -r _mode _iter _skeys _rdb_sz _rdb_mb _total_sec _total_mbps _ent_sec; do
            [ "$_mode" != "$mode" ] && continue
            printf "  %-10s %6s %8s %10.3f %11s %8s\n" \
                "$_mode" "$_skeys" "$_rdb_mb" "$_total_sec" "$_total_mbps" "$_ent_sec"
        done < "${LOG_DIR}/results.txt"

        local stats; stats=$(calc_stats "$mode")
        read keys rdb_mb cnt avg_ts min_tmbps avg_tmbps max_tmbps avg_es <<<"$stats"

        if [ "$cnt" -gt 0 ]; then
            print_sep
            printf "  ${C_CYN}%-10s${C_RST} ${C_DIM}(avg)${C_RST} %4s %8s ${C_DIM}%10.3f${C_RST} ${C_BLD}%11s${C_RST} ${C_DIM}%8s${C_RST}\n" \
                "$mode" "" "$rdb_mb" "$avg_ts" "$avg_tmbps" "$avg_es"
            printf "  ${C_DIM}%-10s (min) %55s %11s${C_RST}\n" \
                "" "$min_tmbps"
            printf "  ${C_DIM}%-10s (max) %55s %11s${C_RST}\n" \
                "" "$max_tmbps"
            [ "$cnt" -gt 1 ] && printf "  ${C_DIM}%-10s (n=%d)${C_RST}\n" "" "$cnt"
        fi
    done

    echo ""

    # 跨模式对比
    if [ "$MODE" = "both" ]; then
        echo "══════════════════════════════════════════════════════════════════════"
        echo "  ${C_BLD}Cross-mode Comparison${C_RST} (avg of $ITERATIONS runs each)"
        echo "══════════════════════════════════════════════════════════════════════"
        printf "  %-10s %11s %11s %8s\n" \
            "MODE" "TOTAL_MB/s" "TOTAL_SEC" "ENT/s"
        print_sep
        for mode in disk diskless; do
            local stats; stats=$(calc_stats "$mode")
            read keys rdb_mb cnt avg_ts min_tmbps avg_tmbps max_tmbps avg_es <<<"$stats"
            if [ "$cnt" -gt 0 ]; then
                printf "  ${C_BLD}%-10s${C_RST} %11s %11s %8s\n" \
                    "$mode" "$avg_tmbps" "$avg_ts" "$avg_es"
            fi
        done
        echo "══════════════════════════════════════════════════════════════════════"
    fi

    echo ""
    echo "  ${C_DIM}统一速率标准: Throughput = RDB文件大小 / (BGSAVE开始→同步完成)${C_RST}"
    echo "  ${C_DIM}对比: kvstore sendfile ↔ Redis disk | kvstore RDMA ↔ Redis diskless${C_RST}"
    echo ""
}

# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

echo "${C_BLD}Redis Fullsync Benchmark${C_RST}" >&2
echo "  keys=$KEYS  vsize=$VSIZE B  iterations=$ITERATIONS  mode=$MODE" >&2
echo "  port=$PORT  slave_port=$SLAVE_PORT" >&2
echo "" >&2

cleanup

# Phase 1: 填充数据
populate_redis || die "数据填充失败"

# Phase 2 + 3: 预热 + 多轮测试
> "${LOG_DIR}/results.txt"

for mode in disk diskless; do
    if [ "$MODE" != "both" ] && [ "$MODE" != "$mode" ]; then
        continue
    fi

    step "Phase: $mode ($ITERATIONS iterations + warmup)"

    # 预热 (不计入结果)
    info "  warmup ..."
    if [ "$mode" = "disk" ]; then
        run_disk 0 >/dev/null 2>&1 || warn "  warmup 失败, 继续"
    else
        run_diskless 0 >/dev/null 2>&1 || warn "  warmup 失败, 继续"
    fi
    cleanup

    # 正式测试
    ok=0; fail=0
    for ((i=1; i<=ITERATIONS; i++)); do
        info "  run $i/$ITERATIONS ..."
        result=""
        if [ "$mode" = "disk" ]; then
            result=$(run_disk "$i" 2>/dev/null) || { fail=$((fail+1)); warn "  run $i/$ITERATIONS FAILED"; cleanup; continue; }
        else
            result=$(run_diskless "$i" 2>/dev/null) || { fail=$((fail+1)); warn "  run $i/$ITERATIONS FAILED"; cleanup; continue; }
        fi

        echo "$result" >> "${LOG_DIR}/results.txt"
        ok=$((ok + 1))

        read _m _i skeys rdb_sz rdb_mb total_sec tmbps es <<<"$result"
        printf "    ${C_DIM}total=%.3fs  mb/s=%s  ent/s=%s  keys=%s${C_RST}\n" \
            "$total_sec" "$tmbps" "$es" "$skeys"
        cleanup
    done
    info "  $mode: $ok passed, $fail failed"
done

# Phase 4: 报告
print_report

# 清理
rm -f "${LOG_DIR}"/*.log 2>/dev/null
rmdir "${LOG_DIR}" 2>/dev/null || true

info "完成. 详细结果: ${LOG_DIR}/results.txt"
