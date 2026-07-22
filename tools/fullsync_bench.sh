#!/bin/bash
# =============================================================================
# fullsync_bench.sh — kvstore 全量同步端到端速率测试 (支持跨机器)
#
# 用法:
#   主机端: ./fullsync_bench.sh --role master -k 50000 -v 512 -p tcp
#           → 启动 master，填充数据，等待从机连接
#
#   从机端: ./fullsync_bench.sh --role slave -k 50000 -v 512 -p tcp --master-ip <IP>
#           → 连接 master，触发全量同步，输出端到端速率
#
# 流程:
#   1. 主机: 运行本脚本，完成数据填充后提示 "ready"
#   2. 从机: 运行本脚本，自动连接 + 全量同步 + 显示结果
#   3. 结果仅在从机端显示；主机端 Ctrl+C 退出
# =============================================================================
set -euo pipefail

# ── 默认参数 ────────────────────────────────────────────────────────────────
ROLE=""
NKEYS=50000
VSIZE=512
PROVIDER="tcp"
MASTER_IP="127.0.0.1"
MASTER_PORT=2000
SLAVE_PORT=2001
SUDO_PASS="${SUDO_PASS:-1234}"
TIMEOUT=120
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

usage() {
    cat <<EOF
Usage:
  主机端: $0 --role master   [-k N] [-v B] [-p tcp|verbs]
  从机端: $0 --role slave    [-k N] [-v B] [-p tcp|verbs] --master-ip <IP>

Options:
  --role master|slave   角色: master 填充数据并监听, slave 连接并同步
  -k, --keys N          Key 数量 (默认: 50000)
  -v, --vsize B         Value 大小, 字节 (默认: 512)
  -p, --provider P      libfabric provider: tcp / verbs (默认: tcp)
  --master-ip IP        Master IP 地址 (slave 模式必需)
  --master-port N       Master 服务端口 (默认: 2000)
  --timeout N           同步超时秒数 (默认: 120)
  -h, --help            显示此帮助

Examples:
  # 主机端 (192.168.1.10):
  ./fullsync_bench.sh --role master -k 200000 -v 1024 -p tcp

  # 从机端 (连接到 192.168.1.10):
  ./fullsync_bench.sh --role slave -k 200000 -v 1024 -p tcp --master-ip 192.168.1.10
EOF
    exit 1
}

# ── 参数解析 ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --role)        ROLE="$2";       shift 2 ;;
        -k|--keys)     NKEYS="$2";      shift 2 ;;
        -v|--vsize)    VSIZE="$2";      shift 2 ;;
        -p|--provider) PROVIDER="$2";   shift 2 ;;
        --master-ip)   MASTER_IP="$2";  shift 2 ;;
        --master-port) MASTER_PORT="$2"; shift 2 ;;
        --timeout)     TIMEOUT="$2";    shift 2 ;;
        -h|--help)     usage ;;
        *) echo "Unknown: $1"; usage ;;
    esac
done

[[ -z "$ROLE" ]] && { echo "ERROR: --role is required"; usage; }
[[ "$ROLE" != "master" && "$ROLE" != "slave" ]] && { echo "ERROR: role must be 'master' or 'slave'"; usage; }

# Slave: master_ip defaults to 127.0.0.1 (local test)

# ── 公共函数 ────────────────────────────────────────────────────────────────
BYTES=$(( NKEYS * (VSIZE + 30) ))  # 30B: key 开销
MB=$(( BYTES / 1024 / 1024 ))

bar() { echo "============================================"; }

cleanup_master() {
    echo "$SUDO_PASS" | sudo -S pkill -9 kvstore 2>/dev/null || true
}

cleanup_slave() {
    kill ${SLAVE_PID:-0} 2>/dev/null || true
    echo "$SUDO_PASS" | sudo -S fuser -k ${SLAVE_PORT}/tcp 2>/dev/null || true
}

# ── 生成配置 ────────────────────────────────────────────────────────────────
write_master_conf() {
    cat > /tmp/fs_master.conf << EOF
port $MASTER_PORT
network proactor
maxclients 10000
loglevel info
persistence none
rdma_sync on
rdma_role master
rdma_provider $PROVIDER
rdma_listen_port 2100
incr_repl on
incr_repl_port 6483
EOF
}

write_slave_conf() {
    cat > /tmp/fs_slave.conf << EOF
port $SLAVE_PORT
network proactor
maxclients 10000
loglevel info
persistence none
rdma_sync on
rdma_role slave
rdma_provider $PROVIDER
rdma_master_ip $MASTER_IP
rdma_master_port 2100
incr_repl on
incr_repl_master_port 6483
EOF
}

# ── 填充数据（分块 pipe，避免 redis-cli --pipe 30s 超时）──────────────────
populate() {
    local chunk=50000
    local done=0
    while [[ $done -lt $NKEYS ]]; do
        local end=$((done + chunk))
        [[ $end -gt $NKEYS ]] && end=$NKEYS
        printf "  chunk %d/%d ... " $((done/chunk + 1)) $(( (NKEYS+chunk-1)/chunk ))
        python3 -c "
import sys
v = 'x' * $VSIZE
for i in range($done, $end):
    k = f'key:{i:07d}'
    sys.stdout.write(f'*3\r\n\$3\r\nSET\r\n\${len(k)}\r\n{k}\r\n\${len(v)}\r\n{v}\r\n')
" | redis-cli -p "$MASTER_PORT" --pipe 2>&1 | grep -oP 'replies: \K\d+' | tr '\n' ' '
        echo "ok"
        done=$end
    done
}

# ── Master 模式 ─────────────────────────────────────────────────────────────
run_master() {
    trap cleanup_master EXIT

    bar
    echo "  kvstore Fullsync — MASTER"
    bar
    printf "  %-14s %s\n" "Provider:"   "$PROVIDER"
    printf "  %-14s %s x %s B\n" "Data:" "$NKEYS" "$VSIZE"
    printf "  %-14s ~%s MB\n" "Est.size:" "$MB"
    printf "  %-14s 0.0.0.0:%s\n" "Listen:"    "$MASTER_PORT"
    bar

    cd "$ROOT"

    # Build
    echo ""
    echo "── Build ──"
    make -j$(nproc) -C build 2>&1 | tail -1
    echo "$SUDO_PASS" | sudo -S setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep build/kvstore 2>/dev/null

    # Config
    write_master_conf

    # Kill old
    echo "$SUDO_PASS" | sudo -S pkill -9 kvstore 2>/dev/null || true
    sleep 1

    # Start master
    echo ""
    echo "── Starting master ──"
    echo "$SUDO_PASS" | sudo -S ./build/kvstore /tmp/fs_master.conf > /tmp/fs_master.log 2>&1 &
    sleep 3

    if ! redis-cli -p "$MASTER_PORT" PING >/dev/null 2>&1; then
        echo "ERROR: Master failed to start"
        cat /tmp/fs_master.log
        exit 1
    fi
    echo "  Master OK (pid=$!)"

    # Populate（分块 pipe，不会超时）
    echo ""
    echo "── Populating $NKEYS keys in chunks ──"
    T0=$(date +%s%3N)
    populate
    T1=$(date +%s%3N)
    MC=$(redis-cli -p "$MASTER_PORT" DBSIZE | tr -d '\r')
    echo "  $MC keys  (pipe in $(( T1 - T0 )) ms)"

    # Done
    bar
    echo ""
    echo "  >>> Master ready, waiting for slave... <<<"
    echo ""
    echo "  On slave machine, run:"
    echo "    ./tools/fullsync_bench.sh --role slave \\"
    echo "        -k $NKEYS -v $VSIZE -p $PROVIDER \\"
    echo "        --master-ip $(hostname -I 2>/dev/null | awk '{print $1}' || echo '<IP>')"
    echo ""
    echo "  Press Ctrl+C to stop"
    bar

    # Wait forever (or until slave disconnects)
    while true; do
        sleep 5
    done
}

# ── Slave 模式 ─────────────────────────────────────────────────────────────
run_slave() {
    trap cleanup_slave EXIT

    bar
    echo "  kvstore Fullsync — SLAVE"
    bar
    printf "  %-14s %s\n" "Provider:"   "$PROVIDER"
    printf "  %-14s %s x %s B\n" "Data:" "$NKEYS" "$VSIZE"
    printf "  %-14s ~%s MB\n" "Est.size:" "$MB"
    printf "  %-14s %s\n" "Master:"    "$MASTER_IP"
    printf "  %-14s %s\n" "Timeout:"   "${TIMEOUT}s"
    bar

    cd "$ROOT"

    # Build
    echo ""
    echo "── Build ──"
    make -j$(nproc) -C build 2>&1 | tail -1
    echo "$SUDO_PASS" | sudo -S setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep build/kvstore 2>/dev/null

    # Config
    write_slave_conf

    # Kill old slave-only processes
    echo "$SUDO_PASS" | sudo -S fuser -k ${SLAVE_PORT}/tcp 2>/dev/null || true
    sleep 1
    echo ""
    echo "── Connecting to $MASTER_IP, waiting for fullsync ──"
    T0=$(date +%s%3N)

    echo "$SUDO_PASS" | sudo -S ./build/kvstore /tmp/fs_slave.conf > /tmp/fs_slave.log 2>&1 &
    SLAVE_PID=$!

    # Wait for completion
    OK=0
    for ((i=1; i<=TIMEOUT; i++)); do
        if ! kill -0 "$SLAVE_PID" 2>/dev/null; then
            echo "  ERROR: Slave process died"
            tail -30 /tmp/fs_slave.log
            exit 1
        fi
        if grep -q "End-to-end" /tmp/fs_slave.log 2>/dev/null; then
            OK=1
            break
        fi
        sleep 1
    done

    T1=$(date +%s%3N)
    WALL_MS=$(( T1 - T0 ))

    if [[ "$OK" -ne 1 ]]; then
        echo "ERROR: Fullsync timeout after ${TIMEOUT}s"
        tail -30 /tmp/fs_slave.log
        exit 1
    fi

    # Verify
    SC=$(redis-cli -p "$SLAVE_PORT" DBSIZE 2>/dev/null | tail -1 | tr -d '\r' || echo "0")

    # Extract metrics
    E2E_LINE=$(grep 'End-to-end' /tmp/fs_slave.log | tail -1)
    RECV_LINE=$(grep 'Recv throughput' /tmp/fs_slave.log | tail -1)

    E2E_MBPS=$(  echo "$E2E_LINE"  | grep -oP '[\d.]+(?= MB/s)'   || echo "-")
    E2E_MS=$(    echo "$E2E_LINE"  | grep -oP '[\d.]+(?= ms)'     || echo "-")
    E2E_BYTES=$( echo "$E2E_LINE"  | grep -oP '\d+(?= bytes)'     || echo "0")
    E2E_ENTRIES=$(echo "$E2E_LINE" | grep -oP '\d+(?= entries)'   || echo "0")
    RECV_MBPS=$( echo "$RECV_LINE" | grep -oP '[\d.]+(?= MB/s)'   || echo "-")
    RECV_EPS=$(  echo "$RECV_LINE" | grep -oP '[\d.]+(?= entries/s)' || echo "-")
    ARENA=$(     grep 'Arena:'     /tmp/fs_slave.log | grep -oP '[\d.]+(?= MB)' | head -1 || echo "-")

    # Results
    echo ""
    bar
    echo "  Fullsync Complete"
    bar
    printf "  %-22s %s\n" "Provider:"           "$PROVIDER"
    printf "  %-22s %s keys\n" "Keys synced:"    "$E2E_ENTRIES"
    printf "  %-22s %s MB\n"  "Data transferred:" "$(( E2E_BYTES / 1024 / 1024 ))"
    printf "  %-22s %s MB/s"  "End-to-end rate:"  "$E2E_MBPS"
    printf "  (%s ms)\n"       "$E2E_MS"
    printf "  %-22s %s MB/s"  "Recv throughput:" "$RECV_MBPS"
    printf "  (%s entries/s)\n" "$RECV_EPS"
    printf "  %-22s %s\n"     "Slave entries:"   "$SC"
    if [[ "$ARENA" != "-" ]]; then
        printf "  %-22s %s MB\n"  "Arena:"       "$ARENA"
    fi
    printf "  %-22s %s ms\n" "Wall clock:"      "$WALL_MS"
    bar

    # Cleanup slave process gracefully
    kill ${SLAVE_PID:-0} 2>/dev/null || true
    wait ${SLAVE_PID:-0} 2>/dev/null || true

}

# ── Main ────────────────────────────────────────────────────────────────────
case "$ROLE" in
    master)  run_master ;;
    slave)   run_slave  ;;
esac
