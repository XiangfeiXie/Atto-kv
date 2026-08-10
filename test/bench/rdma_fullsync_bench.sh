#!/bin/bash
# =============================================================================
# rdma_fullsync_bench.sh — kvstore 全量同步基准测试
#
# 测试 kvstore 两种全量同步传输模式的速率:
#   sendfile — TCP sendfile 零拷贝 (对应 Redis 有盘传输)
#   rdma     — RDMA verbs RDMA_WRITE  (对应 Redis 无盘传输)
#
# 统一速率标准: Throughput (MB/s) = RDB_file_size_bytes / time_sec / 1048576
#
# RDB 格式是 kvstore 和 Redis 的最大公约数。无论是通过 sendfile 传输 RDB
# 文件，还是通过 RDMA 直接传输序列化数据，传输的内容都等效于该 RDB 文件。
# 使用 RDB 文件大小作为统一分母，确保跨模式、跨系统的可比性。
#
# 用法:

#   echo "1234" | sudo -S prlimit --pid $$ --memlock=unlimited && bash test/bench/rdma_fullsync_bench.sh
#   ./test/bench/rdma_fullsync_bench.sh [--keys N] [--vsize B] [--iterations N]
#                                        [--port P] [--mode MODE] [--rdma-ip IP]
# =============================================================================

set -u
shopt -s nullglob

# ── 默认参数 ────────────────────────────────────────────────────────────────

KEYS=500000
VSIZE=256
ITERATIONS=3
PORT=2000
MODE=both
RDMA_IP=""
TIMEOUT=300

# ── 派生常量 ────────────────────────────────────────────────────────────────

SLAVE_PORT=$((PORT + 2))
PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
KVS="${PROJECT_DIR}/build/kvstore"
RDB_PATH="${PROJECT_DIR}/build/dump.rdb"
POP_RDB_PATH="${PROJECT_DIR}/build/populate.rdb"
LOG_DIR="/tmp/kvs_fsync_bench_$$"
mkdir -p "$LOG_DIR"

# ── 统一速率计算辅助函数 ──
# 使用 bc 而非 awk, 避免 gawk 常量折叠导致未执行分支的除零错误
# bc 输出 .06 时自动补前导零 → 0.06
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
C_CYN=$'\033[36m';  C_WHT=$'\033[37m';  C_RST=$'\033[0m'

# ── 辅助函数 ────────────────────────────────────────────────────────────────

die()  { echo "${C_RED}[FATAL]${C_RST} $*" >&2; exit 1; }
info() { echo "${C_GRN}[INFO]${C_RST}  $*" >&2; }
warn() { echo "${C_YEL}[WARN]${C_RST}  $*" >&2; }
step() { echo "${C_CYN}[STEP]${C_RST}  $*" >&2; }

usage() {
    cat <<EOF
用法: $0 [选项]

kvstore 全量同步基准测试 — sendfile (TCP) 和 RDMA verbs 两种传输模式

选项:
  --keys N         测试 key 数量 (默认: $KEYS, ~140MB RDB)
  --vsize B        每个 value 字节数 (默认: $VSIZE)
  --iterations N   每种模式的测试轮数 (默认: $ITERATIONS)
  --port P         master 基准端口 (默认: $PORT)
  --mode MODE      测试模式: sendfile | rdma | both (默认: $MODE)
  --rdma-ip IP     RDMA 设备 IP, 默认自动检测 rxe0 设备
  -h, --help       显示此帮助

提示:
  --keys >= 1000000 时 RDB >256MB，RDMA 流水线 (多 batch) 激活
  RDMA 需要 memlock=unlimited: sudo prlimit --pid \$\$ --memlock=unlimited

统一速率标准:
  NET_MS = Pure Network Transfer (对齐 inazuma-kv)
  prep   = 序列化时间 (rdb_serialize_next), net = 纯网络传输
  NET_MB/s = RDB文件大小 / NET_MS
EOF
    exit 0
}

# ── 参数解析 ────────────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keys)       KEYS="$2";       shift 2 ;;
        --vsize)      VSIZE="$2";      shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --port)       PORT="$2";       SLAVE_PORT=$((PORT + 2)); shift 2 ;;
        --mode)       MODE="$2";       shift 2 ;;
        --rdma-ip)    RDMA_IP="$2";    shift 2 ;;
        -h|--help)    usage ;;
        *) die "未知参数: $1 (使用 -h 查看帮助)" ;;
    esac
done

# 验证参数
[[ "$MODE" =~ ^(sendfile|rdma|both)$ ]] || die "无效 mode: $MODE (应为 sendfile|rdma|both)"
[[ "$KEYS" -gt 0 ]] 2>/dev/null || die "无效 keys: $KEYS"
[[ "$VSIZE" -gt 0 ]] 2>/dev/null || die "无效 vsize: $VSIZE"
[[ "$ITERATIONS" -gt 0 ]] 2>/dev/null || die "无效 iterations: $ITERATIONS"

# ── 工具检查 ────────────────────────────────────────────────────────────────

for tool in redis-cli python3 bc; do
    command -v "$tool" >/dev/null 2>&1 || die "缺少工具: $tool"
done
[ -x "$KVS" ] || die "kvstore 未编译: $KVS (请先运行 cmake --build build)"

# ── RDMA 设备检测 ───────────────────────────────────────────────────────────

detect_rdma_ip() {
    if [ -n "$RDMA_IP" ]; then
        info "RDMA IP (user-supplied): $RDMA_IP"
        return 0
    fi
    # 尝试从 rxe0 设备获取 IP
    local ip
    ip=$(ip -4 addr show rxe0 2>/dev/null | grep -oP 'inet \K[\d.]+' | head -1)
    if [ -n "$ip" ]; then
        RDMA_IP="$ip"
        info "RDMA IP (rxe0): $RDMA_IP"
        return 0
    fi
    # 回退到本机 IP 或 loopback
    ip=$(hostname -I 2>/dev/null | awk '{print $1}')
    if [ -n "$ip" ]; then
        RDMA_IP="$ip"
    else
        RDMA_IP="127.0.0.1"
        warn "无法检测 RDMA 设备 IP, 回退到 $RDMA_IP"
    fi
    info "RDMA IP (fallback): $RDMA_IP"
}

detect_rdma_ip

# ── RDMA 需要 memlock=unlimited (8×256MB MR registration) ──
# 默认 rlimit 约 742MB，不足以注册全部 RDMA buffer
_raise_memlock() {
    if command -v sudo &>/dev/null && sudo -n true 2>/dev/null; then
        sudo prlimit --pid $$ --memlock=unlimited 2>/dev/null && return 0
    fi
    # sudo 不可用或需要密码 — 尝试直接 prlimit (需要 CAP_SYS_RESOURCE)
    prlimit --pid $$ --memlock=unlimited 2>/dev/null && return 0
    return 1
}
if ! _raise_memlock; then
    warn "无法提升 memlock (RDMA 模式可能因 MR 注册失败)"
    warn "  手动执行: sudo prlimit --pid \$\$ --memlock=unlimited"
fi

# ── 进程/端口管理 ──────────────────────────────────────────────────────────

kvskill() {
    local i pid
    { pkill -9 -x kvstore; wait; } 2>/dev/null || true
    for pid in $(pgrep -x kvstore 2>/dev/null || true); do
        { kill -9 "$pid"; wait; } 2>/dev/null || true
    done
    # 等待端口释放
    for ((i=0; i<20; i++)); do
        if ! ss -tlnp 2>/dev/null | grep -qE ":($PORT|$SLAVE_PORT|$((PORT+1)))\b"; then
            break
        fi
        sleep 0.1
    done
    sleep 0.5
}

cleanup() {
    # wait for any tracked background jobs first to suppress "Killed" messages
    wait 2>/dev/null || true
    kvskill
    rm -f "$RDB_PATH" "${RDB_PATH}.tmp" /tmp/kvstore_fs_*.rdb 2>/dev/null
    rm -f "${LOG_DIR}"/*.conf "${LOG_DIR}"/*.log 2>/dev/null
}

wait_port() {
    local p=$1 timeout=${2:-30} i
    for ((i=0; i<timeout*2; i++)); do
        timeout 0.5 bash -c "exec 3<>/dev/tcp/127.0.0.1/$p" 2>/dev/null && return 0
        sleep 0.5
    done
    return 1
}

# ── 配置生成 ────────────────────────────────────────────────────────────────

gen_conf() {
    local role=$1 transport=$2 conf_path=$3
    local listen_port=$PORT

    cat > "$conf_path" <<KVCONF
port ${listen_port}
network proactor
maxclients 100
persistence rdb
rdma_sync on
rdma_role ${role}
rdma_listen_ip ${RDMA_IP}
rdma_listen_port ${listen_port}
rdma_master_ip ${RDMA_IP}
rdma_master_port ${PORT}
fullsync_transport ${transport}
expire off
KVCONF
}

# ── Phase 1: 数据填充 ──────────────────────────────────────────────────────

populate_data() {
    step "Phase 1: 填充测试数据 ($KEYS 条 × ${VSIZE} B)"

    kvskill
    rm -f "$RDB_PATH" "${RDB_PATH}.tmp"

    # 生成临时配置 (仅用于填充数据，rdma_sync off)
    cat > "${LOG_DIR}/populate.conf" <<KVCONF
port $PORT
network reactor
maxclients 1000
persistence rdb
rdma_sync off
expire off
KVCONF

    "$KVS" "${LOG_DIR}/populate.conf" > "${LOG_DIR}/populate.log" 2>&1 &
    local kvs_pid=$!; disown "$kvs_pid" 2>/dev/null

    if ! wait_port "$PORT" 30; then
        die "kvstore 填充实例启动失败"
    fi

    # 使用 Python 直连 socket 批量写入, 替代 redis-cli --pipe
    # 响应显式验证不会丢数据; 流式生成避免 150MB+ 内存占用
    # 进度输出到 stderr → 终端可见; stdout = 实际插入数
    local POPULATE_PY="${PROJECT_DIR}/test/bench/populate_kv.py"
    local keys sent retries=0
    while true; do
        sent=$(python3 "$POPULATE_PY" "127.0.0.1" "$PORT" 1 "$KEYS" "$VSIZE")
        keys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
        if [ "$keys" -ge "$KEYS" ]; then
            break
        fi
        retries=$((retries + 1))
        if [ "$retries" -ge 3 ]; then
            warn "填充 key 数量不匹配: 期望 $KEYS, 实际 $keys (已重试 $retries 次)"
            kvskill; return 1
        fi
        warn "  填充不完整 ($keys/$KEYS), 重试第 $retries 次..."
    done

    # SAVE 获取 RDB 文件大小
    redis-cli -p "$PORT" SAVE >/dev/null 2>&1
    local i
    for ((i=0; i<120; i++)); do
        if [ -f "$RDB_PATH" ] && [ -s "$RDB_PATH" ]; then break; fi
        sleep 0.5
    done

    POP_RDB_SIZE=$(stat -c %s "$RDB_PATH" 2>/dev/null || echo 0)
    if [ "$POP_RDB_SIZE" -eq 0 ]; then
        die "RDB 文件为空, SAVE 可能失败"
    fi

    # 保存填充好的 RDB 以便后续每轮复用
    cp "$RDB_PATH" "$POP_RDB_PATH"

    POP_RDB_MB=$(awk "BEGIN { printf \"%.2f\", ${POP_RDB_SIZE}/1048576 }")
    info "填充完成: $keys keys, RDB = $POP_RDB_MB MB ($POP_RDB_SIZE bytes)"

    kvskill
    rm -f "$RDB_PATH"
    return 0
}

# ── Phase 2 & 3: 单轮测试 ──────────────────────────────────────────────────

run_one() {
    local mode=$1 iteration=$2
    local mlog="${LOG_DIR}/${mode}_m_${iteration}.log"
    local slog="${LOG_DIR}/${mode}_s_${iteration}.log"
    local mconf="${LOG_DIR}/${mode}_master.conf"
    local sconf="${LOG_DIR}/${mode}_slave.conf"

    # 恢复填充好的 RDB 文件，master 启动时自动加载
    rm -f "$RDB_PATH"
    cp "$POP_RDB_PATH" "$RDB_PATH"

    # 生成配置
    # Master: port=PORT (FULLSYNC 命令在此端口接收, 也是 rdma_listen_port)
    #   - TCP 子进程监听 rdma_listen_port+1 (数据端口)
    #   - RDMA 子进程监听 rdma_listen_port
    # Slave: port=SLAVE_PORT (PORT+2, 避免与数据端口冲突)
    #   - FULLSYNC 发送到 rdma_master_port=PORT
    #   - TCP 数据连接到 rdma_master_port+1
    #   - RDMA 数据连接到 rdma_master_port
    gen_conf master "$mode" "$mconf"

    # 为 slave 单独写 port
    cat > "$sconf" <<KVCONF
port ${SLAVE_PORT}
network proactor
maxclients 100
persistence none
rdma_sync on
rdma_role slave
rdma_master_ip ${RDMA_IP}
rdma_master_port ${PORT}
fullsync_transport ${mode}
expire off
KVCONF

    # 启动 Master
    "$KVS" "$mconf" > "$mlog" 2>&1 &
    local mpid=$!; disown "$mpid" 2>/dev/null
    if ! wait_port "$PORT" 20; then
        warn "[$mode #$iteration] master 启动失败"
        kvskill; return 1
    fi
    sleep 1  # 等待 proactor 事件循环完全就绪

    # 验证 Master 已加载数据
    local mkeys; mkeys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
    if [ "$mkeys" -ne "$KEYS" ]; then
        warn "[$mode #$iteration] master 数据不完整: 期望 $KEYS, 实际 $mkeys"
        kvskill; return 1
    fi

    # ── 墙钟计时: slave 启动到端口就绪 ──
    local t0 t1 total_sec
    t0=$(date +%s.%N)

    timeout "$TIMEOUT" "$KVS" "$sconf" > "$slog" 2>&1 &
    local spid=$!; disown "$spid" 2>/dev/null

    # 等待 slave 完成 inline fullsync 并开始监听
    if ! wait_port "$SLAVE_PORT" "$TIMEOUT"; then
        warn "[$mode #$iteration] slave 启动超时 ($TIMEOUT s)"
        # 打印 slave 日志帮助诊断
        tail -10 "$slog" 2>/dev/null | while read line; do warn "  slave: $line"; done
        kvskill; return 1
    fi

    t1=$(date +%s.%N)
    total_sec=$(echo "$t1 - $t0" | bc)

    # 验证 slave 数据 (重试几次, 避免 redis-cli 瞬间超时)
    local skeys=0 i
    for ((i=0; i<5; i++)); do
        skeys=$(redis-cli -p "$SLAVE_PORT" DBSIZE 2>/dev/null || echo 0)
        [[ "$skeys" =~ ^[0-9]+$ ]] && [ "$skeys" -gt 0 ] && break
        sleep 0.2
    done
    if [ "$skeys" -ne "$KEYS" ]; then
        warn "[$mode #$iteration] slave 数据不完整: 期望 $KEYS, 实际 $skeys"
    fi

    # ── 解析 Master log (fallback: slave log) ──
    local xfer_bytes xfer_ms xfer_mbps
    if [ "$mode" = "sendfile" ]; then
        # [tcp-master] xfer-end: bytes=%zd ms=%.1f mbps=%.2f
        # ⚠️ 仅含 sendfile() syscall 时间, 不含 SAVE
        # 主备双源: master log → slave log, 确保日志落盘后被读取
        local xfer_line
        xfer_line=$(grep '\[tcp-master\] xfer-end:' "$mlog" 2>/dev/null | tail -1)
        [ -z "$xfer_line" ] && xfer_line=$(grep '\[tcp-slave\] xfer-end:' "$slog" 2>/dev/null | tail -1)
        xfer_bytes=$(echo "$xfer_line" | grep -oP 'bytes=\K[0-9]+' || echo "0")
        xfer_ms=$(echo "$xfer_line" | grep -oP 'ms=\K[0-9.]+' || echo "0")
        xfer_mbps=$(echo "$xfer_line" | grep -oP 'mbps=\K[0-9.]+' || echo "0")

        # 墙钟速率 (统一标准: RDB文件大小 / 总时间)
        TOTAL_MBPS=$(calc_mbps "$POP_RDB_SIZE" "$total_sec")
        # 纯传输速率 (sendfile syscall 时间 → 秒)
        XFER_MBPS_CALC=$(calc_mbps "$POP_RDB_SIZE" "$(echo "scale=6; $xfer_ms / 1000" | bc)")
        ENT_SEC=$(calc_entps "$KEYS" "$total_sec")

        echo "$mode $iteration $KEYS $POP_RDB_SIZE $POP_RDB_MB $total_sec $TOTAL_MBPS $xfer_ms 0 $XFER_MBPS_CALC $ENT_SEC $skeys"
    else
        # RDMA: [master] xfer-end: bytes=%zu wall=%.1f prep=%.1f xfer=%.1f mbps=%.2f batches=%d
        # xfer_ms = Pure Network Transfer (对应 inazuma-kv 的 "Pure Network Sync")
        # prep_ms = Data Prep 序列化时间
        local xfer_line
        xfer_line=$(grep '\[master\] xfer-end:' "$mlog" 2>/dev/null | tail -1)
        xfer_bytes=$(echo "$xfer_line" | grep -oP 'bytes=\K[0-9]+' || echo "0")
        xfer_ms=$(echo "$xfer_line" | grep -oP 'xfer=\K[0-9.]+' || echo "0")
        local prep_ms
        prep_ms=$(echo "$xfer_line" | grep -oP 'prep=\K[0-9.]+' || echo "0")
        local batches=$(echo "$xfer_line" | grep -oP 'batches=\K[0-9]+' || echo "0")

        TOTAL_MBPS=$(calc_mbps "$POP_RDB_SIZE" "$total_sec")
        XFER_MBPS_CALC=$(calc_mbps "$POP_RDB_SIZE" "$(echo "scale=6; $xfer_ms / 1000" | bc)")
        ENT_SEC=$(calc_entps "$KEYS" "$total_sec")

        # 也解析 slave log 获取 entries 数
        local s_xfer_line
        s_xfer_line=$(grep '\[slave\] xfer-end:' "$slog" 2>/dev/null | tail -1)
        local s_entries
        s_entries=$(echo "$s_xfer_line" | grep -oP 'entries=\K[0-9]+' || echo "$skeys")

        echo "$mode $iteration $s_entries $POP_RDB_SIZE $POP_RDB_MB $total_sec $TOTAL_MBPS $xfer_ms $prep_ms $XFER_MBPS_CALC $ENT_SEC $batches"
    fi
}

# ── 统计计算 ────────────────────────────────────────────────────────────────

calc_stats() {
    # 读入所有结果行, 按 mode 分组, 计算 min/max/avg
    local mode=$1
    local values total_sec_sum=0 xfer_ms_sum=0 total_mbps_sum=0 xfer_mbps_sum=0 ent_sec_sum=0
    local count=0
    local min_total_mbps="" max_total_mbps=""
    local min_xfer_ms="" max_xfer_ms=""
    local keys=""

    while IFS=' ' read -r _mode _iter _keys _rdb_sz _rdb_mb _total_sec _total_mbps _xfer_ms _prep_ms _xfer_mbps _ent_sec _extra; do
        [ "$_mode" != "$mode" ] && continue
        count=$((count + 1))
        keys=$_keys
        rdb_mb=$_rdb_mb

        total_sec_sum=$(echo "$total_sec_sum + $_total_sec" | bc)
        xfer_ms_sum=$(echo "$xfer_ms_sum + $_xfer_ms" | bc)
        total_mbps_sum=$(echo "$total_mbps_sum + $_total_mbps" | bc)
        xfer_mbps_sum=$(echo "$xfer_mbps_sum + $_xfer_mbps" | bc)
        ent_sec_sum=$(echo "$ent_sec_sum + $_ent_sec" | bc)

        # min/max
        if [ -z "$min_total_mbps" ] || [ "$(echo "$_total_mbps < $min_total_mbps" | bc)" = 1 ]; then
            min_total_mbps=$_total_mbps
        fi
        if [ -z "$max_total_mbps" ] || [ "$(echo "$_total_mbps > $max_total_mbps" | bc)" = 1 ]; then
            max_total_mbps=$_total_mbps
        fi
        if [ -z "$min_xfer_ms" ] || [ "$(echo "$_xfer_ms < $min_xfer_ms" | bc)" = 1 ]; then
            min_xfer_ms=$_xfer_ms
        fi
        if [ -z "$max_xfer_ms" ] || [ "$(echo "$_xfer_ms > $max_xfer_ms" | bc)" = 1 ]; then
            max_xfer_ms=$_xfer_ms
        fi
    done < "${LOG_DIR}/results.txt"

    if [ "$count" -eq 0 ]; then
        echo "0 0 0 0 0 0 0 0 0 0"
        return
    fi

    local avg_total_sec avg_xfer_ms avg_total_mbps avg_xfer_mbps avg_ent_sec
    avg_total_sec=$(_fmt "$(echo "scale=3; $total_sec_sum / $count" | bc)")
    avg_xfer_ms=$(_fmt "$(echo "scale=1; $xfer_ms_sum / $count" | bc)")
    avg_total_mbps=$(_fmt "$(echo "scale=2; $total_mbps_sum / $count" | bc)")
    avg_xfer_mbps=$(_fmt "$(echo "scale=2; $xfer_mbps_sum / $count" | bc)")
    avg_ent_sec=$(echo "scale=0; $ent_sec_sum / $count" | bc)

    echo "$keys $rdb_mb $count $avg_total_sec $min_total_mbps $avg_total_mbps $max_total_mbps $avg_xfer_ms $min_xfer_ms $avg_xfer_mbps $max_xfer_ms $avg_ent_sec"
}

# ── Phase 4: 报告 ───────────────────────────────────────────────────────────

print_sep() {
    printf "  %s\n" "$(printf '─%.0s' $(seq 1 70))"
}

print_report() {
    echo ""
    echo "══════════════════════════════════════════════════════════════════════"
    echo "  ${C_BLD}kvstore Fullsync Benchmark${C_RST} — ${C_YEL}KEYS=$KEYS  VSIZE=$VSIZE B${C_RST}"
    echo "  RDMA IP: $RDMA_IP   RDB Size: $POP_RDB_MB MB"
    echo "══════════════════════════════════════════════════════════════════════"
    echo ""
    echo "  ${C_DIM}Timing methodology (inazuma-kv aligned):${C_RST}"
    echo "  ${C_DIM}  NET_MS = Pure Network Transfer — excludes serialization,${C_RST}"
    echo "  ${C_DIM}           MR registration, handshake, and deserialization.${C_RST}"
    echo "  ${C_DIM}  RDMA:   wall = prep(rdb_serialize_next) + xfer(RDMA_WRITE+poll_CQ)${C_RST}"
    echo "  ${C_DIM}  TCP:    wall = sendfile() syscall time${C_RST}"
    echo ""
    printf "  %-10s %6s %8s %10s %11s %10s %11s %8s\n" \
        "MODE" "KEYS" "RDB_MB" "TOTAL_SEC" "TOTAL_MB/s" "NET_MS" "NET_MB/s" "ENT/s"
    print_sep

    for mode in sendfile rdma; do
        # 跳过未测试的模式
        if [ "$MODE" != "both" ] && [ "$MODE" != "$mode" ]; then
            continue
        fi

        while IFS=' ' read -r _mode _iter _keys _rdb_sz _rdb_mb _total_sec _total_mbps _xfer_ms _prep_ms _xfer_mbps _ent_sec _extra; do
            [ "$_mode" != "$mode" ] && continue
            printf "  %-10s %6s %8s %10.3f %11s %10s %11s %8s\n" \
                "$_mode" "$_keys" "$_rdb_mb" "$_total_sec" "$_total_mbps" "$_xfer_ms" "$_xfer_mbps" "$_ent_sec"
        done < "${LOG_DIR}/results.txt"

        # 统计行
        local stats; stats=$(calc_stats "$mode")
        read keys rdb_mb cnt avg_ts min_tmbps avg_tmbps max_tmbps avg_xms min_xms avg_xmbps max_xms avg_es <<<"$stats"

        if [ "$cnt" -gt 0 ]; then
            print_sep
            printf "  ${C_CYN}%-10s${C_RST} ${C_DIM}(avg)${C_RST} %4s %8s ${C_DIM}%10.3f${C_RST} ${C_BLD}%11s${C_RST} ${C_DIM}%10s${C_RST} ${C_BLD}%11s${C_RST} ${C_DIM}%8s${C_RST}\n" \
                "$mode" "" "$rdb_mb" "$avg_ts" "$avg_tmbps" "$avg_xms" "$avg_xmbps" "$avg_es"
            printf "  ${C_DIM}%-10s (min) %55s %11s %22s %11s${C_RST}\n" \
                "" "$min_tmbps" "" "$min_xms" ""
            printf "  ${C_DIM}%-10s (max) %55s %11s %22s %11s${C_RST}\n" \
                "" "$max_tmbps" "" "$max_xms" ""
            [ "$cnt" -gt 1 ] && printf "  ${C_DIM}%-10s (n=%d)${C_RST}\n" "" "$cnt"
        fi
    done

    echo ""

    # 跨模式对比
    if [ "$MODE" = "both" ]; then
        echo "══════════════════════════════════════════════════════════════════════"
        echo "  ${C_BLD}Cross-mode Comparison${C_RST} (avg of $ITERATIONS runs each)"
        echo "══════════════════════════════════════════════════════════════════════"
        printf "  %-10s %11s %11s %11s %8s\n" \
            "MODE" "TOTAL_MB/s" "XFER_MB/s" "XFER_MS" "ENT/s"
        print_sep
        for mode in sendfile rdma; do
            local stats; stats=$(calc_stats "$mode")
            read keys rdb_mb cnt avg_ts min_tmbps avg_tmbps max_tmbps avg_xms min_xms avg_xmbps max_xms avg_es <<<"$stats"
            if [ "$cnt" -gt 0 ]; then
                printf "  ${C_BLD}%-10s${C_RST} %11s %11s %11s %8s\n" \
                    "$mode" "$avg_tmbps" "$avg_xmbps" "$avg_xms" "$avg_es"
            fi
        done
        echo "══════════════════════════════════════════════════════════════════════"
    fi

    echo ""
    echo "  ${C_DIM}统一速率标准: Throughput = RDB文件大小 / 时间${C_RST}"
    echo "  ${C_DIM}TOTAL = 墙钟时间 (slave启动→就绪), XFER = 纯传输时间 (master log)${C_RST}"
    echo ""
}

# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

echo "${C_BLD}kvstore Fullsync Benchmark${C_RST}" >&2
echo "  keys=$KEYS  vsize=$VSIZE B  iterations=$ITERATIONS  mode=$MODE" >&2
echo "  port=$PORT  slave_port=$SLAVE_PORT  rdma_ip=$RDMA_IP" >&2
echo "" >&2

# ── 工具依赖额外检查 ──
if [ "$MODE" = "rdma" ] || [ "$MODE" = "both" ]; then
    if ! command -v ibv_devinfo >/dev/null 2>&1; then
        warn "未找到 ibv_devinfo, RDMA 模式可能失败 (需 libibverbs-utils)"
    fi
    if [ ! -d /sys/class/infiniband ] || [ -z "$(ls -A /sys/class/infiniband 2>/dev/null)" ]; then
        warn "未检测到 RDMA 设备 (/sys/class/infiniband 为空), RDMA 模式可能失败"
    fi
fi

cleanup

# Phase 1: 填充数据
populate_data || die "数据填充失败"

# Phase 2 + 3: 预热 + 多轮测试
> "${LOG_DIR}/results.txt"

for mode in sendfile rdma; do
    # 跳过不需要的模式
    if [ "$MODE" != "both" ] && [ "$MODE" != "$mode" ]; then
        continue
    fi

    step "Phase: $mode ($ITERATIONS iterations + warmup)"

    # 预热 (不计入结果)
    info "  warmup ..."
    run_one "$mode" 0 >/dev/null 2>&1 || warn "  warmup 失败, 继续"
    cleanup

    # 正式测试
    ok=0; fail=0
    for ((i=1; i<=ITERATIONS; i++)); do
        info "  run $i/$ITERATIONS ..."
        if result=$(run_one "$mode" "$i"); then
            echo "$result" >> "${LOG_DIR}/results.txt"
            ok=$((ok + 1))

            # 内联打印结果 (格式: mode iter keys rdb_sz rdb_mb total_sec total_mbps net_ms prep_ms net_mbps ent_s extra)
            read _m _i keys rdb_sz rdb_mb total_sec tmbps xms pms xmbps es _x <<<"$result"
            printf "    ${C_DIM}total=%.3fs  prep=%sms  net=%sms  net_mb/s=%s  ent/s=%s${C_RST}\n" \
                "$total_sec" "$pms" "$xms" "$xmbps" "$es"
        else
            fail=$((fail + 1))
            warn "  run $i/$ITERations FAILED"
        fi
        cleanup
    done
    info "  $mode: $ok passed, $fail failed"
done

# Phase 4: 报告
print_report

# 清理
rm -f "${LOG_DIR}"/*.conf "${LOG_DIR}"/*.log "$POP_RDB_PATH" 2>/dev/null
[ "$(ls -A "$LOG_DIR" 2>/dev/null)" ] || rmdir "$LOG_DIR" 2>/dev/null

info "完成. 详细结果: ${LOG_DIR}/results.txt"
