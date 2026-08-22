#!/usr/bin/env bash
# ============================================================================
# repl-incr-xdpkcp 网关增量同步 —— 大流量压力测试 (本机, veth pair)
# ============================================================================
# 目标: 在大流量突发写入下, 验证 AF_XDP + KCP 内核旁路增量同步【不丢数据】,
#       即修复后 (A 背压 + B 批量 flush + C 大命令分片) 的回归测试。
#
#   拓扑同 test_xdpkcp.sh (单机 veth 对 vxdp0↔vxdp1):
#     master kvstore ──SHM──▶ master 网关 ──AF_XDP/UDP──▶ slave 网关 ──SHM──▶ slave kvstore
#
# 与功能 smoke 的区别:
#   - 用 kv_resp.py fill 一次性 (单 socket, RESP 突发) 灌入 STRESS_N 条 SET,
#     制造远超网关实时排空能力的突发, 触发 SHM ring 背压 (sched_yield) 而非丢弃;
#   - 另写 BIG_N 条 >8KB 的超长 value, 覆盖「大命令分片」路径;
#   - INCR × INCR_N 非幂等写入, 校验 slave 恰好重放一次 (无重复/无丢失);
#   - 最终用 kv_resp.py verify 校验 slave 逐 key 一致 + DBSIZE 精确相等,
#     并从引擎/网关日志对账 enqueued/replayed/rx_left/drops。
#
# 注意: 只支持 String 类型。写入务必走 RESP (redis-cli 普通模式 / kv_resp.py),
#       inline 命令 (redis-cli --pipe) 不会被 slave 重放 (重放依赖 resp_parse_slices)。
#
# 用法:
#   sudo ./test/smoke/test_xdpkcp_stress.sh
#   STRESS_N=50000 BIG_VAL_SIZE=32768 INCR_N=5000 sudo ./test/smoke/test_xdpkcp_stress.sh
#
# 可调环境变量:
#   STRESS_N      大流量批量 SET 条数 (默认 20000)
#   BIG_N         超长 value (>8KB) key 数 (默认 40)
#   BIG_VAL_SIZE  超长 value 字节数, 须 > 8192 才触发分片 (默认 16384)
#   INCR_N        INCR 次数 (非幂等 exactly-once 校验, 默认 1000)
#   WAIT          同步收敛等待上限秒 (默认 120)
#   以及 MASTER_PORT/SLAVE_PORT/SYNC_PORT/VETH0/VETH1/SHM_*/SUDO_PASSWORD/BUILD_DIR
#
# 前置条件: kvstore + kvstore-xdp-gateway + xdp_kcp.bpf.o 已编译;
#           redis-cli, python3, sudo, iproute2 可用
# ============================================================================
set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── 可调参数（环境变量覆盖）─────────────────────────────────────────────────

STRESS_N="${STRESS_N:-20000}"          # 大流量批量 SET 条数
BIG_N="${BIG_N:-40}"                   # 超长 value 的 key 数
BIG_VAL_SIZE="${BIG_VAL_SIZE:-16384}"  # 超长 value 字节数 (> SHM_MAX_PAYLOAD=8192)
INCR_N="${INCR_N:-1000}"               # INCR 次数
WAIT="${WAIT:-120}"                    # 收敛等待上限（秒）

MASTER_PORT="${MASTER_PORT:-6666}"
SLAVE_PORT="${SLAVE_PORT:-6667}"
SYNC_PORT="${SYNC_PORT:-8888}"

BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
KVSTORE_BIN="${KVSTORE_BIN:-${BUILD_DIR}/kvstore}"
GATEWAY_BIN="${GATEWAY_BIN:-${BUILD_DIR}/kvstore-xdp-gateway}"
BPF_OBJ="${BPF_OBJ:-${BUILD_DIR}/xdp_kcp.bpf.o}"
KV_RESP="${KV_RESP:-${SCRIPT_DIR}/kv_resp.py}"

VETH0="${VETH0:-vxdp0}"
VETH1="${VETH1:-vxdp1}"
VETH_IP0="10.200.0.1"
VETH_IP1="10.200.0.2"
SHM_MASTER="${SHM_MASTER:-/kvxdp_master}"
SHM_SLAVE="${SHM_SLAVE:-/kvxdp_slave}"

SUDO_PASSWORD="${SUDO_PASSWORD:-1234}"
HOST="${HOST:-127.0.0.1}"
TIMEOUT="${TIMEOUT:-3s}"
WRITE_TIMEOUT="${WRITE_TIMEOUT:-30s}"   # 写入阶段放宽 (背压下仍可能短暂阻塞)

RCLI_M="timeout ${TIMEOUT} redis-cli -h ${HOST} -p ${MASTER_PORT}"
RCLI_S="timeout ${TIMEOUT} redis-cli -h ${HOST} -p ${SLAVE_PORT}"
RCLI_M_W="timeout ${WRITE_TIMEOUT} redis-cli -h ${HOST} -p ${MASTER_PORT}"

# 预期总 key 数 = 突发 N + 超长 BIG_N + probe(1) + counter(1)
EXPECTED_DBSIZE=$((STRESS_N + BIG_N + 2))
# 预期写命令总数 = 突发 N + 超长 BIG_N + INCR_N + probe(1) + counter(1)
TOTAL_WRITES=$((STRESS_N + BIG_N + INCR_N + 2))

MASTER_CONF=$(mktemp)
SLAVE_CONF=$(mktemp)
MASTER_LOG=$(mktemp)
SLAVE_LOG=$(mktemp)
GW_M_LOG=$(mktemp)
GW_S_LOG=$(mktemp)

MASTER_PID=""
SLAVE_PID=""
GW_M_PID=""
GW_S_PID=""

# ── 测试框架（与 test_xdpkcp.sh 一致）───────────────────────────────────────

PASS=0; FAIL=0
FAILED_NAMES=()
RED=$'\033[31m' GRN=$'\033[32m' YEL=$'\033[33m' RST=$'\033[0m'

ok()  { PASS=$((PASS+1)); printf "  ${GRN}PASS${RST}  %s\n" "$1"; }
bad() { FAIL=$((FAIL+1)); FAILED_NAMES+=("$1");
        printf "  ${RED}FAIL${RST}  %s\n        expected: %s\n        actual:   %s\n" "$1" "$2" "$3"; }
section() { printf "\n${YEL}── %s ──${RST}\n" "$1"; }
chk() { [ "$3" = "$2" ] && ok "$1" || bad "$1" "$2" "$3"; }

sudo_cmd() { echo "$SUDO_PASSWORD" | sudo -S "$@" 2>/dev/null; }

# wait until `pattern` appears in `file` (default 10s)
wait_for_log() {
    local file="$1" pattern="$2" timeout="${3:-10}" i=0
    while [ "$i" -lt "$((timeout * 10))" ]; do
        grep -q "$pattern" "$file" 2>/dev/null && return 0
        sleep 0.1; i=$((i+1))
    done
    return 1
}

# wait until slave GET `key` == `expected` (default 30s)
wait_slave_get() {
    local key="$1" expected="$2" timeout="${3:-30}" i=0
    while [ "$i" -lt "$((timeout * 10))" ]; do
        [ "$($RCLI_S GET "$key" 2>/dev/null)" = "$expected" ] && return 0
        sleep 0.1; i=$((i+1))
    done
    return 1
}

summary() {
    local total=$((PASS + FAIL))
    printf "\n────────────────────────────────────────────────────────\n"
    printf "XDP+KCP stress: %d/%d passed" "$PASS" "$total"
    if [ "$FAIL" -gt 0 ]; then
        printf "  (${RED}%d failed${RST})\n" "$FAIL"
        for n in "${FAILED_NAMES[@]}"; do printf "    • %s\n" "$n"; done
        return 1
    fi
    printf "  ${GRN}ALL PASS${RST}\n"
    return 0
}

# ── Cleanup ─────────────────────────────────────────────────────────────────

cleanup() {
    for pid in "$GW_M_PID" "$GW_S_PID"; do
        [ -n "$pid" ] && sudo_cmd kill "$pid" 2>/dev/null
    done
    sudo_cmd pkill -f kvstore-xdp-gateway 2>/dev/null
    for pid in "$MASTER_PID" "$SLAVE_PID"; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null

    # Delete the veth pair (this also detaches any XDP program).
    sudo_cmd ip link del "$VETH0" 2>/dev/null

    rm -f /dev/shm/"${SHM_MASTER#/}" /dev/shm/"${SHM_SLAVE#/}" 2>/dev/null
    rm -f "$MASTER_CONF" "$SLAVE_CONF" "$MASTER_LOG" "$SLAVE_LOG" "$GW_M_LOG" "$GW_S_LOG"
}
trap cleanup EXIT INT TERM

# ── Prerequisites ────────────────────────────────────────────────────────────

section "Prerequisites"

if [ ! -x "$KVSTORE_BIN" ] || [ ! -x "$GATEWAY_BIN" ] || [ ! -f "$BPF_OBJ" ]; then
    printf "${RED}FATAL${RST}: binaries not found under %s\n" "$BUILD_DIR" >&2
    printf "  build with: cmake -S . -B %s -DCMAKE_BUILD_TYPE=Release\n" "$BUILD_DIR" >&2
    printf "              cmake --build %s --target kvstore kvstore-xdp-gateway\n" "$BUILD_DIR" >&2
    exit 2
fi
ok "kvstore + gateway + BPF object present"

command -v redis-cli >/dev/null 2>&1 || { echo "FATAL: redis-cli not found" >&2; exit 2; }
ok "redis-cli available"

command -v python3 >/dev/null 2>&1 || { echo "FATAL: python3 not found (kv_resp.py 需要)" >&2; exit 2; }
[ -f "$KV_RESP" ] || { echo "FATAL: kv_resp.py not found at $KV_RESP" >&2; exit 2; }
ok "python3 + kv_resp.py available"

if ! echo "$SUDO_PASSWORD" | sudo -S true 2>/dev/null; then
    printf "${RED}FATAL${RST}: sudo unavailable (needed for veth + XDP attach)\n" >&2
    exit 2
fi
ok "sudo available"

printf "  load: STRESS_N=%d BIG_N=%d BIG_VAL_SIZE=%d INCR_N=%d\n" \
    "$STRESS_N" "$BIG_N" "$BIG_VAL_SIZE" "$INCR_N"

# ── Network setup (veth pair) ────────────────────────────────────────────────

section "veth pair setup"

sudo_cmd ip link del "$VETH0" 2>/dev/null   # clear stale
sudo_cmd ip link add "$VETH0" type veth peer name "$VETH1" || \
    { echo "FATAL: cannot create veth pair" >&2; exit 2; }
sudo_cmd ip link set "$VETH0" up
sudo_cmd ip link set "$VETH1" up
sudo_cmd ip addr add "$VETH_IP0/24" dev "$VETH0" 2>/dev/null
sudo_cmd ip addr add "$VETH_IP1/24" dev "$VETH1" 2>/dev/null

MAC0=$(sudo_cmd ip link show "$VETH0" | awk '/link\/ether/{print $2; exit}')
MAC1=$(sudo_cmd ip link show "$VETH1" | awk '/link\/ether/{print $2; exit}')
if [ -z "$MAC0" ] || [ -z "$MAC1" ]; then
    echo "FATAL: cannot resolve veth MACs" >&2; exit 2
fi
ok "veth pair up ($VETH0=$MAC0 ↔ $VETH1=$MAC1)"

# ── Start kvstore master & slave ─────────────────────────────────────────────

section "Start kvstore master & slave"

cat > "$MASTER_CONF" <<EOF
port ${MASTER_PORT}
network proactor
maxclients 200
loglevel info
persistence none
expire off
rdma_sync off
incr_sync off
xdp_sync on
rdma_role master
EOF

cat > "$SLAVE_CONF" <<EOF
port ${SLAVE_PORT}
network proactor
maxclients 200
loglevel info
persistence none
expire off
rdma_sync off
incr_sync off
xdp_sync on
rdma_role slave
EOF

XDPKCP_SHM_NAME="$SHM_MASTER" "$KVSTORE_BIN" "$MASTER_CONF" &>"$MASTER_LOG" &
MASTER_PID=$!
XDPKCP_SHM_NAME="$SHM_SLAVE"  "$KVSTORE_BIN" "$SLAVE_CONF"  &>"$SLAVE_LOG" &
SLAVE_PID=$!

sleep 1
kill -0 "$MASTER_PID" 2>/dev/null || { echo "FATAL: master kvstore failed"; cat "$MASTER_LOG" >&2; exit 2; }
kill -0 "$SLAVE_PID"  2>/dev/null || { echo "FATAL: slave kvstore failed";  cat "$SLAVE_LOG"  >&2; exit 2; }
$RCLI_M PING >/dev/null 2>&1 || { echo "FATAL: master not responding"; exit 2; }
$RCLI_S PING >/dev/null 2>&1 || { echo "FATAL: slave not responding";  exit 2; }
ok "kvstore master + slave up (PING ok)"

# ── Start gateways ───────────────────────────────────────────────────────────

section "Start gateways"

echo "$SUDO_PASSWORD" | sudo -S env XDPKCP_SHM_NAME="$SHM_MASTER" "$GATEWAY_BIN" \
    --master --iface "$VETH0" --bpf "$BPF_OBJ" \
    --local-ip "$VETH_IP0" --peer-ip "$VETH_IP1" --peer-mac "$MAC1" \
    --port "$SYNC_PORT" &>"$GW_M_LOG" &
GW_M_PID=$!

echo "$SUDO_PASSWORD" | sudo -S env XDPKCP_SHM_NAME="$SHM_SLAVE" "$GATEWAY_BIN" \
    --slave --iface "$VETH1" --bpf "$BPF_OBJ" \
    --local-ip "$VETH_IP1" --peer-ip "$VETH_IP0" --peer-mac "$MAC0" \
    --port "$SYNC_PORT" &>"$GW_S_LOG" &
GW_S_PID=$!

if ! wait_for_log "$GW_M_LOG" "transport up" 15; then
    printf "${RED}FATAL${RST}: master gateway failed to init\n" >&2
    cat "$GW_M_LOG" >&2; exit 2
fi
if ! wait_for_log "$GW_S_LOG" "transport up" 15; then
    printf "${RED}FATAL${RST}: slave gateway failed to init\n" >&2
    cat "$GW_S_LOG" >&2; exit 2
fi
ok "master + slave gateways up (AF_XDP + KCP ready)"
sleep 1   # let KCP exchange an initial round-trip

# ── 写入阶段 ─────────────────────────────────────────────────────────────────

section "Write phase (burst + big values + INCR)"

# 探测命令: 确认全链路 master → SHM → 网关 → KCP/AF_XDP → 网关 → SHM → slave 已通
$RCLI_M SET stress:probe 1 >/dev/null
if wait_slave_get stress:probe 1 10; then
    ok "probe command propagated end-to-end"
else
    bad "probe command propagated" "1" "$($RCLI_S GET stress:probe)"
fi

# 大流量突发: 单 socket 一次性灌入 STRESS_N 条 RESP SET (kv_resp.py fill)
T0=$(date +%s%N)
FILL_OUT=$(python3 "$KV_RESP" fill --n "$STRESS_N" --prefix stress \
    --host "$HOST" --port "$MASTER_PORT" 2>&1)
FILL_RC=$?
T1=$(date +%s%N)
if [ "$FILL_RC" -eq 0 ]; then
    ELAPSED_MS=$(( (T1 - T0) / 1000000 ))
    ok "burst: master accepted ${STRESS_N} SETs (${ELAPSED_MS} ms)"
else
    bad "burst: master accepted ${STRESS_N} SETs" "OK ${STRESS_N}" "$(echo "$FILL_OUT" | tail -1)"
fi

# 超长 value (>8KB): 覆盖「大命令分片」路径 (master 引擎按 ≤8KB 切片, slave 重组)
BIG_VAL=$(head -c "$BIG_VAL_SIZE" /dev/zero | tr '\0' 'A')
for i in $(seq 1 "$BIG_N"); do
    $RCLI_M_W SET "stress:big:$i" "$BIG_VAL" >/dev/null
done
ok "wrote ${BIG_N} big values (${BIG_VAL_SIZE} B each)"

# INCR 非幂等写入: 校验 slave 恰好重放一次
$RCLI_M_W SET stress:counter 0 >/dev/null
python3 - "$HOST" "$MASTER_PORT" "$INCR_N" <<'PYEOF'
import socket, sys
host = sys.argv[1]; port = int(sys.argv[2]); n = int(sys.argv[3])
key = b"stress:counter"
cmd = b"*2\r\n$4\r\nINCR\r\n$" + str(len(key)).encode() + b"\r\n" + key + b"\r\n"
s = socket.create_connection((host, port), timeout=60)
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
s.sendall(cmd * n)
buf = b""
replies = 0
while replies < n:
    chunk = s.recv(65536)
    if not chunk:
        sys.exit(1)
    buf += chunk
    replies = buf.count(b"\r\n")
s.close()
print(f"OK {n} INCRs")
PYEOF
INCR_RC=$?
if [ "$INCR_RC" -eq 0 ]; then
    ok "sent ${INCR_N} INCRs to master"
else
    bad "sent ${INCR_N} INCRs" "OK ${INCR_N} INCRs" "INCR send failed (rc=$INCR_RC)"
fi

# ── 校验阶段 ─────────────────────────────────────────────────────────────────

section "Verify (no loss / no duplicate under load)"

# kv_resp.py verify: 轮询直到 slave 上 STRESS_N 个 key 全部命中 且 DBSIZE 精确等于
# EXPECTED_DBSIZE (覆盖 burst + big + probe + counter, 从根上杜绝丢 key/多 key)
VERIFY_OUT=$(python3 "$KV_RESP" verify --n "$STRESS_N" --prefix stress \
    --host "$HOST" --port "$SLAVE_PORT" --wait "$WAIT" \
    --expect-dbsize "$EXPECTED_DBSIZE" 2>&1)
VERIFY_RC=$?
if [ "$VERIFY_RC" -eq 0 ]; then
    ok "all ${STRESS_N} burst keys replicated + DBSIZE==${EXPECTED_DBSIZE}"
else
    bad "burst keys replicated + DBSIZE==${EXPECTED_DBSIZE}" \
        "verified=${STRESS_N}" "$(echo "$VERIFY_OUT" | tail -1)"
fi

# 超长 value 完整性: STRLEN 必须精确等于 BIG_VAL_SIZE (分片重组不得截断)
BIG_BAD=0
for i in $(seq 1 "$BIG_N"); do
    LEN=$($RCLI_S STRLEN "stress:big:$i")
    if [ "$LEN" != "$BIG_VAL_SIZE" ]; then BIG_BAD=$((BIG_BAD+1)); fi
done
chk "all ${BIG_N} big values intact (STRLEN)" "0" "$BIG_BAD"

# 超长 value 内容逐字节校验 (取第一个)
GOT=$(timeout "${TIMEOUT}" redis-cli --raw -h "$HOST" -p "$SLAVE_PORT" GET "stress:big:1")
chk "big value content byte-identical (stress:big:1)" "$BIG_VAL" "$GOT"

# INCR exactly-once: slave 上的 counter 必须精确等于 INCR_N (不多不少)
if wait_slave_get stress:counter "$INCR_N" "$WAIT"; then
    ok "INCR exactly-once (counter==${INCR_N})"
else
    bad "INCR exactly-once" "$INCR_N" "$($RCLI_S GET stress:counter)"
fi

# ── 停机对账 (诊断计数器) ────────────────────────────────────────────────────

section "Shutdown diagnostics"

# 优雅停机: 先网关 (SIGINT → flush + 打印统计), 再引擎 (SIGTERM → 事件循环返回 → shutdown 对账)
sudo_cmd pkill -INT -f kvstore-xdp-gateway 2>/dev/null
sleep 1
[ -n "$MASTER_PID" ] && kill -TERM "$MASTER_PID" 2>/dev/null
[ -n "$SLAVE_PID"  ] && kill -TERM "$SLAVE_PID"  2>/dev/null
sleep 1
wait 2>/dev/null

M_ENQ=$(grep -oE 'enqueued=[0-9]+' "$MASTER_LOG" | tail -1 | cut -d= -f2)
S_REPLAYED=$(grep -oE 'replayed=[0-9]+' "$SLAVE_LOG" | tail -1 | cut -d= -f2)
S_RX_LEFT=$(grep -oE 'rx_left=[0-9]+' "$SLAVE_LOG" | tail -1 | cut -d= -f2)
# 只从 "shutting down" 行取 drops (避免误匹配 af_xdp 行的 tx_drops=)
GW_M_DROP=$(grep -E '\[GW\] shutting down' "$GW_M_LOG" | grep -oE 'drops=[0-9]+' | cut -d= -f2)
GW_S_DROP=$(grep -E '\[GW\] shutting down' "$GW_S_LOG" | grep -oE 'drops=[0-9]+' | cut -d= -f2)

printf "  master engine: %s\n" "$(grep -E '\[XDP-ENGINE\] shutdown' "$MASTER_LOG" | tail -1)"
printf "  slave  engine: %s\n" "$(grep -E '\[XDP-ENGINE\] shutdown' "$SLAVE_LOG"  | tail -1)"
printf "  master  gw:    %s\n" "$(grep -E '\[GW\] shutting down' "$GW_M_LOG" | tail -1)"
printf "  slave   gw:    %s\n" "$(grep -E '\[GW\] shutting down' "$GW_S_LOG" | tail -1)"
if grep -q 'SHM ring full' "$MASTER_LOG"; then
    printf "  backpressure:  exercised (SHM ring 满, 引擎 sched_yield 等待网关)\n"
else
    printf "  backpressure:  not triggered (网关跟得上; 可调大 STRESS_N 触发)\n"
fi

# 关键对账: 引擎 enqueue 次数 == 写命令总数; slave 重放次数 == 写命令总数; rx_left==0
chk "master enqueued == TOTAL_WRITES ($TOTAL_WRITES)" "$TOTAL_WRITES" "${M_ENQ:-0}"
chk "slave replayed == TOTAL_WRITES ($TOTAL_WRITES)" "$TOTAL_WRITES" "${S_REPLAYED:-0}"
chk "slave rx_left == 0 (无卡死半包)"                   "0"            "${S_RX_LEFT:-1}"
chk "master gateway drops == 0"                         "0"            "${GW_M_DROP:-1}"
chk "slave gateway drops == 0"                          "0"            "${GW_S_DROP:-1}"

# ── Summary ──────────────────────────────────────────────────────────────────

summary
