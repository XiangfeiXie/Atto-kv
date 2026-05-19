#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
KVS_BIN="${PROJECT_DIR}/build/kvstore"
KVS_CONF_TEMPLATE="${PROJECT_DIR}/conf/bench/kvstore-slave-repl.conf"
MASTER_HOST="192.168.30.135"
MASTER_PORT="2000"
SLAVE_PORT="2002"
VALUE_SIZE="16"
PAYLOAD="0"
NKEYS=""

RED='\033[0;31m'; GREEN='\033[0;32m'; NC='\033[0m'

usage() {
  cat <<EOF
Usage:
  $0 master <nkeys> [--host IP] [--port PORT] [--value-size N]
  sudo $0 slave --payload BYTES [--master-host IP] [--master-port PORT] [--slave-port PORT]

master  Populate the existing master and print PAYLOAD=<bytes> for slave mode.
slave   Start a kvstore slave and measure RDMA fullsync transfer+load time.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} $1 not installed" >&2
    exit 1
  fi
}

require_root() {
  if [ "${EUID:-0}" -ne 0 ]; then
    echo -e "${RED}[ERROR]${NC} slave mode requires root" >&2
    exit 1
  fi
}

mode="${1:-}"
[ -z "$mode" ] && { usage; exit 1; }
shift || true

run_master() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --host) MASTER_HOST="$2"; shift 2 ;;
      --port) MASTER_PORT="$2"; shift 2 ;;
      --value-size) VALUE_SIZE="$2"; shift 2 ;;
      *)
        if [ -z "$NKEYS" ] && [[ "$1" =~ ^[0-9]+$ ]]; then
          NKEYS="$1"; shift
        else
          echo "unknown option: $1" >&2; usage; exit 1
        fi
        ;;
    esac
  done
  [ -n "$NKEYS" ] || { echo -e "${RED}[ERROR]${NC} missing nkeys" >&2; usage; exit 1; }
  require_cmd redis-cli
  require_cmd redis-benchmark
  require_cmd bc
  if ! redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" PING >/dev/null 2>&1; then
    echo -e "${RED}[ERROR]${NC} kvstore master not reachable ($MASTER_HOST:$MASTER_PORT)" >&2
    exit 1
  fi
  echo -e "${GREEN}master reachable${NC}: $MASTER_HOST:$MASTER_PORT"
  redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" FLUSHALL >/dev/null 2>&1 || true
  redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" SAVE >/dev/null 2>&1 || true
  sleep 0.5
  redis-benchmark -h "$MASTER_HOST" -p "$MASTER_PORT" \
      -n "$NKEYS" -r "$NKEYS" -t set -d "$VALUE_SIZE" -c 1 -P 16 -q
  redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" SET __bench_marker__ check >/dev/null 2>&1 || true
  redis-cli -h "$MASTER_HOST" -p "$MASTER_PORT" SAVE >/dev/null 2>&1 || true
  RDB_FILE="${PROJECT_DIR}/build/dump.rdb"
  RDB_SIZE=0
  for i in $(seq 1 60); do
    sleep 0.5
    if [ -f "$RDB_FILE" ]; then
      RDB_SIZE=$(stat -c%s "$RDB_FILE" 2>/dev/null || echo 0)
      [ "$RDB_SIZE" -gt 0 ] && break
    fi
  done
  [ "$RDB_SIZE" -gt 0 ] || { echo -e "${RED}[ERROR]${NC} SAVE timeout" >&2; exit 1; }
  rdb_mb=$(echo "scale=2; $RDB_SIZE / 1048576" | bc -l 2>/dev/null || echo "N/A")
  echo
  echo -e "${GREEN}PAYLOAD=$RDB_SIZE${NC} KEY_COUNT=$NKEYS (${rdb_mb} MB)"
  echo "Next: sudo ./test/bench/bench_fullsync.sh slave --payload $RDB_SIZE"
}

run_slave() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --payload) PAYLOAD="$2"; shift 2 ;;
      --master-host) MASTER_HOST="$2"; shift 2 ;;
      --master-port) MASTER_PORT="$2"; shift 2 ;;
      --slave-port) SLAVE_PORT="$2"; shift 2 ;;
      *) echo "unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done
  [ "$PAYLOAD" -gt 0 ] || { echo -e "${RED}[ERROR]${NC} missing --payload" >&2; usage; exit 1; }
  require_root
  require_cmd bc
  require_cmd nc
  [ -x "$KVS_BIN" ] || { echo -e "${RED}[ERROR]${NC} missing $KVS_BIN" >&2; exit 1; }
  resp=$(printf '*1\r\n$4\r\nPING\r\n' | nc -w 3 "$MASTER_HOST" "$MASTER_PORT" 2>/dev/null | tr -d '\r\n' || true)
  [ "$resp" = "+PONG" ] || { echo -e "${RED}[ERROR]${NC} master not reachable" >&2; exit 1; }
  conf_tmp="/tmp/kvstore_slave_fullsync_$$.conf"
  sed -e "s/^rdma_provider .*/rdma_provider verbs/" \
      -e "s/^rdma_master_ip .*/rdma_master_ip $MASTER_HOST/" \
      -e "s/^port .*/port $SLAVE_PORT/" \
      "$KVS_CONF_TEMPLATE" > "$conf_tmp"
  fuser -k "${SLAVE_PORT}/tcp" 2>/dev/null || true
  rm -f "${PROJECT_DIR}/build/dump.rdb"
  slave_log="/tmp/kvstore_fullsync_slave.log"
  rm -f "$slave_log"
  payload_mb=$(echo "scale=2; $PAYLOAD / 1048576" | bc -l)
  echo "RDMA fullsync slave: master=$MASTER_HOST:$MASTER_PORT payload=${payload_mb}MB"
  nohup "$KVS_BIN" "$conf_tmp" > "$slave_log" 2>&1 &
  slave_pid=$!
  timeout=120 synced=0
  for ((i = 1; i <= timeout; i++)); do
    if grep -q 'loaded.*keys from fullsync' "$slave_log" 2>/dev/null; then synced=1; break; fi
    if ss -tln 2>/dev/null | grep -q ":${SLAVE_PORT} "; then synced=1; break; fi
    if ! kill -0 "$slave_pid" 2>/dev/null; then tail -15 "$slave_log" 2>/dev/null || true; rm -f "$conf_tmp"; exit 1; fi
    sleep 1
  done
  [ "$synced" -eq 1 ] || { tail -15 "$slave_log" 2>/dev/null || true; kill "$slave_pid" 2>/dev/null || true; rm -f "$conf_tmp"; exit 1; }
  fullsync_ms=$(grep 'fullsync_time_ms=' "$slave_log" 2>/dev/null | tail -1 | sed 's/.*fullsync_time_ms=//' | tr -d ' ' || true)
  [ -n "$fullsync_ms" ] || { echo -e "${RED}[ERROR]${NC} fullsync_time_ms missing" >&2; kill "$slave_pid" 2>/dev/null || true; rm -f "$conf_tmp"; exit 1; }
  speed_mbps=$(echo "scale=2; $PAYLOAD / $fullsync_ms * 1000 / 1048576" | bc -l)
  echo -e "${GREEN}fullsync_time_ms=$fullsync_ms throughput=${speed_mbps} MB/s${NC}"
  kill "$slave_pid" 2>/dev/null || true
  fuser -k "${SLAVE_PORT}/tcp" 2>/dev/null || true
  rm -f "$conf_tmp"
}

case "$mode" in
  master) run_master "$@" ;;
  slave) run_slave "$@" ;;
  -h|--help) usage ;;
  *) echo "unknown mode: $mode" >&2; usage; exit 1 ;;
esac
