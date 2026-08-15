#!/bin/bash
# =============================================================================
# bench_expire_active.sh — 主动过期（active expiration）清理速率测试
#
# 目标：测量 kvstore 主动过期机制在「全量突发过期」下的清理吞吐与延迟，
#       分两种口径各跑一遍：
#         · idle — 空闲口径：写完 key 后不碰服务器，事件循环每 100ms 才醒一次，
#                  反映生产里"key 过期时无流量"的真实行为
#         · busy — 轻负载口径：后台持续 PING 保持事件循环活跃，测纯机制上限
#
# 方法：每个 (mode,N) 独立重启空服务 → FLUSHALL → populate_expire.py 批量写入
#       N 个带 EX ttl 的 key → 记录 t_last_expiry（最后写入 key 的到期时刻）→
#       轮询 DBSIZE 直到清空 → 计算清理延迟与速率。
#
# 已知行为（仅文档说明，不做断言）：
#   本实现是「纯主动过期」——dispatch 热路径无惰性删除，GET/EXISTS 在主动
#   回收前仍会读到已过期 key，TTL 返回 -2 但不删除。所有过期清理都靠
#   active_expire_cycle()（见 src/core/kvstore_expire.c）。
#
# 测量口径说明：
#   · idle 轮询间隔 1s：每次 DBSIZE 会额外唤醒事件循环一次（相对自然 100ms
#     唤醒，偏置 ≈10%），量级仍正确。
#   · 突发并非严格同时：相对 TTL 按写顺序 stagger 写耗时 W 秒；cleanup_ms
#     从「最后写入 key」到期起算，stagger 窗口内的提前清理已计入。
#   · TTL 必须大于写耗时，否则写入期间就有 key 过期（脚本会 warn）。
#
# 用法：
#   ./test/bench/bench_expire_active.sh                         # 默认全量
#   ./test/bench/bench_expire_active.sh --n "10000 100000" --ttl 5 --mode idle
#   ./test/bench/bench_expire_active.sh --n 100000 --mode busy --ttl 10
#
# 关键参数：
#   --n      key 数量列表         (默认 "100000 1000000")
#   --ttl    相对过期秒数         (默认 10，须大于写耗时)
#   --vsize  值大小(字节)         (默认 16)
#   --mode   测试口径 idle|busy   (默认 "idle busy")
#   --conf   配置文件             (默认 kvstore-expire.conf)
#   --out    结果 CSV 输出路径    (默认 results_expire_active.csv)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

# ---- 默认参数 ----
CONF="${CONF:-kvstore-expire.conf}"
N_LIST="100000 1000000"
TTL=10
VSIZE=16
MODE_LIST="idle busy"
POLL_IDLE=1          # 空闲口径轮询间隔（秒）
POLL_BUSY=0.05       # 轻负载口径轮询间隔（秒）
TIMEOUT_SEC=900      # 单次清理超时（秒）
RESULT_CSV="${PROJECT_DIR}/test/bench/results_expire_active.csv"
CURVE_CSV="${PROJECT_DIR}/test/bench/results_expire_active_curve.csv"

while [ $# -gt 0 ]; do
    case "$1" in
        --n)     N_LIST="$2";     shift 2 ;;
        --ttl)   TTL="$2";        shift 2 ;;
        --vsize) VSIZE="$2";      shift 2 ;;
        --mode)  MODE_LIST="$2";  shift 2 ;;
        --conf)  CONF="$2";       shift 2 ;;
        --out)   RESULT_CSV="$2"; shift 2 ;;
        *) err "未知参数: $1"; exit 1 ;;
    esac
done

# ---- 预检 ----
for tool in redis-cli python3 awk date; do
    command -v "$tool" >/dev/null 2>&1 || { err "缺少工具: $tool"; exit 1; }
done
[ -x "$KVS_BIN" ] || { err "kvstore 未编译: $KVS_BIN"; exit 1; }

CONF_PATH="${CONF_DIR}/${CONF}"
[ -f "$CONF_PATH" ] || { err "配置文件不存在: $CONF_PATH"; exit 1; }
HOST="127.0.0.1"
PORT=$(grep '^port ' "$CONF_PATH" | awk '{print $2}')
PORT=${PORT:-2000}

# ---- 工具函数 ----
now_ms() { date +%s%3N; }

db_size() {
    local v
    v=$(redis-cli -h "$HOST" -p "$PORT" DBSIZE 2>/dev/null | tr -d '\r')
    echo "${v:-0}"
}

expired_cnt() {
    local v
    v=$(redis-cli -h "$HOST" -p "$PORT" INFO stats 2>/dev/null | tr -d '\r' \
        | awk -F: '$1=="expired_keys"{print $2}')
    echo "${v:-0}"
}

# ---- 单次 (mode, N) 测量 ----
measure_one() {
    local mode=$1 n=$2 poll=$3

    step "=== mode=$mode N=$n ttl=${TTL}s ==="
    start_kvs "$CONF"
    redis-cli -h "$HOST" -p "$PORT" FLUSHALL >/dev/null 2>&1 || true

    step "写入 $n 个带 TTL 的 key (EX ${TTL})"
    local t_w0 t_w1 sent
    t_w0=$(now_ms)
    sent=$(python3 "${SCRIPT_DIR}/populate_expire.py" "$HOST" "$PORT" 1 "$n" "$VSIZE" "$TTL" 2>/dev/null)
    t_w1=$(now_ms)
    sent=${sent:-0}

    local dbsize
    dbsize=$(db_size)
    if [ "$dbsize" -ne "$n" ]; then
        warn "写入后 DBSIZE=$dbsize != $n（TTL 可能过短，写入期间已开始过期）"
    fi

    # 最后写入的 key 到期时刻（写结束 + TTL）
    local t_expiry=$(( t_w1 + TTL * 1000 ))

    # busy 口径：后台持续 PING 保持事件循环活跃
    local keeper_pid=""
    if [ "$mode" = "busy" ]; then
        python3 "${SCRIPT_DIR}/ping_keepalive.py" "$HOST" "$PORT" 3600 &
        keeper_pid=$!
        log "轻负载 PING 已启动 (pid=$keeper_pid)"
    fi

    step "轮询 DBSIZE 直到清空 (interval=${poll}s)"
    local t0 t_cleared=0 expired_final=0
    t0=$(now_ms)
    while [ $(( $(now_ms) - t0 )) -lt $(( TIMEOUT_SEC * 1000 )) ]; do
        local d tnow rel
        d=$(db_size)
        tnow=$(now_ms)
        rel=$(( tnow - t_expiry ))
        echo "$mode,$n,$TTL,$rel,$d" >> "$CURVE_CSV"
        if [ "$d" -le 0 ]; then
            t_cleared=$tnow
            break
        fi
        sleep "$poll"
    done

    if [ -n "$keeper_pid" ]; then
        kill "$keeper_pid" 2>/dev/null || true
        wait "$keeper_pid" 2>/dev/null || true
    fi

    expired_final=$(expired_cnt)

    if [ "$t_cleared" -eq 0 ]; then
        warn "mode=$mode N=$n 在 ${TIMEOUT_SEC}s 内未清空"
        echo "$mode,$n,$TTL,$VSIZE,$((t_w1 - t_w0)),TIMEOUT,-1,$expired_final" >> "$RESULT_CSV"
        stop_kvs
        return
    fi

    local cleanup_ms=$(( t_cleared - t_expiry ))
    local write_ms=$(( t_w1 - t_w0 ))
    local rate
    rate=$(awk -v n="$n" -v ms="$cleanup_ms" \
        'BEGIN{ if(ms>0) printf "%.0f", n/(ms/1000.0); else printf "-1" }')

    local row="$mode,$n,$TTL,$VSIZE,$write_ms,$cleanup_ms,$rate,$expired_final"
    echo "$row" >> "$RESULT_CSV"
    log "mode=$mode N=$n write=${write_ms}ms cleanup=${cleanup_ms}ms rate=${rate}/s expired=${expired_final}"

    stop_kvs
}

main() {
    echo "mode,N,ttl_sec,vsize,write_ms,cleanup_ms_after_expiry,rate_per_s,expired_keys" > "$RESULT_CSV"
    echo "mode,N,ttl_sec,rel_ms,dbsize" > "$CURVE_CSV"
    log "主动过期测试开始: conf=$CONF N={$N_LIST} ttl=${TTL}s modes={$MODE_LIST}"

    for mode in $MODE_LIST; do
        local poll
        if [ "$mode" = "busy" ]; then poll="$POLL_BUSY"; else poll="$POLL_IDLE"; fi
        for n in $N_LIST; do
            measure_one "$mode" "$n" "$poll"
        done
    done

    stop_kvs
    log "完成。汇总: $RESULT_CSV / 曲线: $CURVE_CSV"
}

main "$@"
