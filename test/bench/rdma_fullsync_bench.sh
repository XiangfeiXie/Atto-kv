#!/bin/bash
# =============================================================================
# rdma_fullsync_bench.sh — kvstore fullsync 端到端传输速率对比 (sendfile vs RDMA)
#   sendfile xfer= 含落盘+传输 (落盘是 sendfile 固有耗时)
#   rdma    xfer= 含序列化+直写, 不含连接建立 (softRoCE 模拟里连接最耗时, 不计入速率)
#
# 一次运行分别测 sendfile 和 rdma 各一次:
#   启动 master(该传输配置) → 填充数据 → SAVE 生成 RDB
#   → 启动 slave 触发全量同步 → 等传输完成
#   → 从 kvstore 日志解析 [sendfile|rdma] xfer-end 的 bytes/耗时/MB/s
#
# 指标只有传输速率; 数据来自 kvstore 自己打印的传输信息, 不做其他计算。
#
# 用法（项目根目录执行）:
#   sudo ./test/bench/rdma_fullsync_bench.sh
#
# 输出: 终端 + test/bench/results_fullsync.txt
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# 固定参数
KEYS=400000                # 填充 key 数
VSIZE=256                  # value 字节
PORT=2000
SLAVE_PORT=$((PORT + 2))
FPORT=$((PORT + 1))
TIMEOUT=120
TRANSPORTS=(sendfile rdma)
RDB_PATH="build/dump.rdb"
RESULT_FILE="test/bench/results_fullsync.txt"

WORK="$(mktemp -d /tmp/fullsyncbench.XXXXXX)"

cleanup() {
    stop_all
    rm -f "$RDB_PATH" "${RDB_PATH}.tmp"
    rm -rf "$WORK"
}
trap cleanup EXIT

require_tools

# ── RDMA 准备: 设备 IP + memlock ─────────────────────────────────────────
# 注意 pipefail: 每条管道结尾加 || true 防 set -e 误杀
# IP 优先级: 环境变量 RDMA_IP > rxe 链接绑定 netdev 的 IPv4 > hostname
if [ -z "${RDMA_IP:-}" ]; then
    dev=$(rdma link show 2>/dev/null | grep -oP 'netdev \K\S+' | head -1 || true)
    RDMA_IP=$(ip -4 addr show "${dev:-rxe0}" 2>/dev/null | grep -oP 'inet \K[\d.]+' | head -1 || true)
    [ -z "$RDMA_IP" ] && RDMA_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || true)
    RDMA_IP="${RDMA_IP:-127.0.0.1}"
fi
log "RDMA IP: $RDMA_IP"

ensure_memlock() {
    local cur
    cur=$(ulimit -l 2>/dev/null || echo 0)
    [ "$cur" = "unlimited" ] && return 0
    [ "$cur" -ge 2097152 ] && return 0
    prlimit --pid $$ --memlock=unlimited 2>/dev/null && { log "memlock 已提升到 unlimited"; return 0; }
    warn "memlock=$cur KB, RDMA 需要 ≥2GB; 可先执行: prlimit --pid \$\$ --memlock=unlimited"
    return 1
}
ensure_memlock || warn "memlock 不足, rdma 传输可能失败"

# ── 配置生成 (per transport) ─────────────────────────────────────────────
write_configs() { # $1=transport  $2=master.conf  $3=slave.conf
    cat > "$2" <<EOF
port ${PORT}
network proactor
maxclients 100
persistence rdb
repl_role master
incr_sync off
fullsync_listen_ip ${RDMA_IP}
fullsync_port ${FPORT}
fullsync_transport $1
expire off
EOF
    cat > "$3" <<EOF
port ${SLAVE_PORT}
network proactor
maxclients 100
persistence none
repl_role slave
incr_sync off
master_port ${PORT}
fullsync_master_ip ${RDMA_IP}
fullsync_port ${FPORT}
fullsync_transport $1
expire off
EOF
}

# ── 单传输测试 → "transport bytes 耗时(ms) MB/s" ─────────────────────────
run_one() { # $1=transport
    local transport=$1
    local mconf="${WORK}/${transport}_master.conf"
    local sconf="${WORK}/${transport}_slave.conf"
    local mlog="${WORK}/${transport}_master.log"
    local slog="${WORK}/${transport}_slave.log"

    stop_all
    rm -f "$RDB_PATH" "${RDB_PATH}.tmp"
    write_configs "$transport" "$mconf" "$sconf"

    # 启动 master
    taskset -c "$SERVER_CORES" "$KVS_BIN" "$mconf" >"$mlog" 2>&1 &
    if ! wait_for_port 127.0.0.1 "$PORT" 20; then
        err "$transport master 启动失败"; tail -20 "$mlog"; return 1
    fi

    # 填充数据 → SAVE 生成 RDB (sendfile 传输读 build/dump.rdb)
    pipe_populate 127.0.0.1 "$PORT" "$KEYS" "$VSIZE" || true
    local mkeys
    mkeys=$(redis-cli -p "$PORT" DBSIZE 2>/dev/null || echo 0)
    [ "$mkeys" -ge "$KEYS" ] || { err "$transport 填充不完整 ($mkeys/$KEYS)"; return 1; }

    redis-cli -p "$PORT" SAVE >/dev/null 2>&1 || true
    local rdb_size=0 i
    for ((i=0; i<60; i++)); do
        [ -f "$RDB_PATH" ] && [ -s "$RDB_PATH" ] && break
        sleep 0.5
    done
    rdb_size=$(stat -c %s "$RDB_PATH" 2>/dev/null || echo 0)
    [ "$rdb_size" -gt 0 ] || { err "$transport SAVE 失败 (无 RDB)"; return 1; }
    log "  $transport: $mkeys keys, RDB=$(awk "BEGIN{printf \"%.1f\", $rdb_size/1048576}") MB" >&2   # log→stderr, 避免污染 run_one 返回值

    # 启动 slave → 触发全量同步; 轮询日志里的 xfer-end 即传输完成
    timeout "$TIMEOUT" "$KVS_BIN" "$sconf" >"$slog" 2>&1 &
    local spid=$!
    local xfer_line=""
    for ((i=0; i<TIMEOUT*2; i++)); do
        xfer_line=$(grep -h -E '\[sendfile\] xfer-end:|\[rdma\] xfer-end:' "$mlog" "$slog" 2>/dev/null || true)
        [ -n "$xfer_line" ] && break
        if ! kill -0 "$spid" 2>/dev/null; then
            xfer_line=$(grep -h -E '\[sendfile\] xfer-end:|\[rdma\] xfer-end:' "$mlog" "$slog" 2>/dev/null || true)
            [ -n "$xfer_line" ] && break
            err "$transport slave 异常退出且无传输日志"
            grep -iE 'error|cannot allocate|rdma_reg' "$slog" 2>/dev/null | head -5 >&2 || true
            return 1
        fi
        sleep 0.5
    done
    [ -n "$xfer_line" ] || { err "$transport 传输超时 (${TIMEOUT}s)"; return 1; }

    # 数据校验 (best-effort; slave 可能已退出, 以日志为准)
    local skeys
    skeys=$(redis-cli -p "$SLAVE_PORT" DBSIZE 2>/dev/null || echo 0)
    [ "$skeys" -eq "$KEYS" ] || warn "$transport slave 数据 $skeys/$KEYS (以传输日志为准)"

    # 解析传输日志
    local line bytes t_ms mbps
    if [ "$transport" = sendfile ]; then
        line=$(grep '\[sendfile\] xfer-end:' "$mlog" 2>/dev/null | tail -1)
        [ -z "$line" ] && line=$(grep '\[sendfile\] xfer-end:' "$slog" 2>/dev/null | tail -1)
        bytes=$(echo "$line" | grep -oP 'bytes=\K[0-9]+' || echo "0")
        t_ms=$(echo "$line" | grep -oP 'xfer=\K[0-9.]+' || echo "0")   # sendfile 含落盘; rdma 含序列化, 均不含连接
        mbps=$(echo "$line" | grep -oP 'mbps=\K[0-9.]+' || echo "0")
    else
        line=$(grep '\[rdma\] xfer-end:' "$mlog" 2>/dev/null | tail -1)
        [ -z "$line" ] && line=$(grep '\[rdma\] xfer-end:' "$slog" 2>/dev/null | tail -1)
        bytes=$(echo "$line" | grep -oP 'bytes=\K[0-9]+' || echo "0")
        t_ms=$(echo "$line" | grep -oP 'xfer=\K[0-9.]+' || echo "0")
        mbps=$(echo "$line" | grep -oP 'mbps=\K[0-9.]+' || echo "0")
    fi
    echo "$transport $bytes $t_ms $mbps"
}

# ── 主流程: sendfile 与 rdma 各测一次 ────────────────────────────────────
> "$RESULT_FILE"
{
    echo "rdma_fullsync_bench — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "keys=$KEYS  vsize=$VSIZE  rdma_ip=$RDMA_IP  指标: xfer 端到端速率 (sendfile 含落盘; rdma 含序列化+直写, 均不含连接建立)"
    echo ""
    printf "  %-9s | %12s | %10s | %10s\n" "transport" "bytes" "耗时(ms)" "MB/s"
} >> "$RESULT_FILE"

for transport in "${TRANSPORTS[@]}"; do
    res=$(run_one "$transport") || { warn "$transport 测量失败, 跳过"; continue; }
    read -r t bytes t_ms mbps <<< "$res"
    line="$(printf "  %-9s | %12s | %10s | %10s" "$t" "$bytes" "$t_ms" "$mbps")"
    echo "$line"
    echo "$line" >> "$RESULT_FILE"
done
stop_all

echo ""
echo "结果已写入: $RESULT_FILE"
