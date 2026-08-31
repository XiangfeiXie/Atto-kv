#!/bin/bash
# =============================================================================
# redis_fullsync_bench.sh — Redis 全量同步速率对比 (有盘 disk vs 无盘 diskless)
#
# 一次运行分别测 disk 和 diskless 各一次:
#   启动 master(该模式配置) → 填充数据 → SAVE 取 RDB 大小参考
#   → 启动 slave 触发全量同步 → 等 slave 日志 "Finished with success"
#   → 解析 master/slave 日志毫秒时间戳 → 耗时 → 速率 = RDB 大小 / 耗时
#
# 计时端点 (两种模式同一对端点, 只测 master 端, 等价于 kvstore 的 xfer= 口径:
#   序列化+传输, 不含连接建立、不含 slave 加载):
#   起始: master 日志
#     disk    : "Background saving started by pid"       (BGSAVE 子进程落盘)
#     diskless: "Background RDB transfer started by pid" (子进程内存序列化直传)
#   结束: master 日志 "Synchronization with replica ... succeeded" (发送完成)
#   有盘/无盘都 fork 子进程, 起始点都是 fork 时刻; 结束点是 master 自己打的日志,
#   同机毫秒时间戳无时钟漂移。不用 slave "Finished" 做结束点: 实测 slave RDB 加载
#   耗时在两种模式下不一致 (disk 2.35s vs diskless 3.94s), 会污染传输速率对比
#
# RDB 大小参考: 填充后对 master 执行 SAVE 取 dump.rdb 字节数 (有盘/无盘 key 相同
# → RDB 字节数相同; 已实测与 disk 模式 slave 日志 "receiving N bytes" 一致)
#
# 注意: 日志级别保持默认 notice (warning 会屏蔽上述关键日志行)
#
# 用法（项目根目录执行）:
#   ./test/bench/redis_fullsync_bench.sh
#
# 输出: 终端 + test/bench/results_redis_fullsync.txt
# =============================================================================

script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

# 固定参数
KEYS=400000                # 填充 key 数
VSIZE=256                  # value 字节
MPORT=4000                 # redis master 端口
SPORT=4002                 # redis slave 端口
TIMEOUT=120
MODES=(disk diskless)
RESULT_FILE="test/bench/results_redis_fullsync.txt"
WORK="$(mktemp -d /tmp/redisbench.XXXXXX)"

cleanup() {
    stop_all
    rm -rf "$WORK"
}
trap cleanup EXIT

# redis 工具预检（redis bench 不需要 kvstore）
for tool in redis-server redis-cli bc date; do
    command -v "$tool" >/dev/null 2>&1 || { err "缺少工具: $tool"; exit 1; }
done

# ── 日志时间戳 → epoch 秒(毫秒精度) ───────────────────────────────────────
# 行格式: "66319:M 30 Aug 2026 09:46:02.069 * Background saving started by pid N"
log_ts() { # $1=日志行; 输出 epoch 秒.毫秒
    local ts
    ts=$(echo "$1" | grep -oP '\d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2}\.\d{3}' | head -1 || true)
    if [ -n "$ts" ]; then
        date -d "$ts" +%s.%N 2>/dev/null || echo 0
    else
        echo 0
    fi
}

# ── 起始/结束时间戳 (都在 master 日志) ─────────────────────────────────────
start_ts() { # $1=mlog $2=mode
    local pat="Background saving started by pid"
    [ "$2" = diskless ] && pat="Background RDB transfer started by pid"
    local line
    line=$(grep -F "$pat" "$1" | tail -1 || true)
    log_ts "$line"
}
done_ts() { # $1=mlog — master 发送完成
    local line
    line=$(grep -F "Synchronization with replica" "$1" | grep -F "succeeded" | tail -1 || true)
    log_ts "$line"
}

# ── 单模式测试 → "mode bytes 耗时(ms) MB/s" ────────────────────────────────
run_one() { # $1=mode
    local mode=$1
    local mconf="$WORK/${mode}_master.conf"
    local sconf="$WORK/${mode}_slave.conf"
    local mlog="$WORK/${mode}_master.log"
    local slog="$WORK/${mode}_slave.log"
    local rdbdir="$WORK/${mode}_data"

    stop_all
    mkdir -p "$rdbdir"

    # master 配置 (repl-diskless-sync 区分模式; rdbcompression no 让 RDB 大小
    # 与 kvstore 同量级~110MB, 传输占主导; 日志级别保持默认 notice)
    cat > "$mconf" <<EOF
port ${MPORT}
bind 127.0.0.1
protected-mode no
save ""
appendonly no
rdbcompression no
dir ${rdbdir}
dbfilename dump.rdb
repl-diskless-sync $( [ "$mode" = diskless ] && echo yes || echo no )
repl-diskless-sync-delay 0
logfile ${mlog}
daemonize yes
pidfile ${rdbdir}/master.pid
EOF
    cat > "$sconf" <<EOF
port ${SPORT}
bind 127.0.0.1
protected-mode no
save ""
appendonly no
dir ${rdbdir}
dbfilename slave.rdb
replicaof 127.0.0.1 ${MPORT}
logfile ${slog}
daemonize yes
pidfile ${rdbdir}/slave.pid
EOF

    redis-server "$mconf"
    if ! wait_for_port 127.0.0.1 "$MPORT" 10; then
        err "$mode master 启动失败"; tail -5 "$mlog" >&2 || true; return 1
    fi

    pipe_populate 127.0.0.1 "$MPORT" "$KEYS" "$VSIZE" || true
    local mkeys
    mkeys=$(redis-cli -p "$MPORT" DBSIZE 2>/dev/null || echo 0)
    [ "$mkeys" -ge "$KEYS" ] || { err "$mode 填充不完整 ($mkeys/$KEYS)"; return 1; }

    # SAVE 取 RDB 大小参考 (两种模式 key 相同 → RDB 字节数相同)
    redis-cli -p "$MPORT" SAVE >/dev/null 2>&1 || true
    local ref_size=0 i
    for ((i=0; i<20; i++)); do
        ref_size=$(stat -c %s "$rdbdir/dump.rdb" 2>/dev/null || echo 0)
        [ "$ref_size" -gt 0 ] && break
        sleep 0.5
    done
    [ "$ref_size" -gt 0 ] || { err "$mode SAVE 失败 (无 RDB)"; return 1; }
    log "  $mode: $mkeys keys, RDB=$(awk "BEGIN{printf \"%.1f\", $ref_size/1048576}") MB" >&2   # log→stderr, 避免污染返回值

    # 启动 slave → 触发全量同步; 等 master 起始行 + 完成行都出现
    redis-server "$sconf"
    local s=0 e=0 tms done=no
    for ((i=0; i<TIMEOUT*2; i++)); do
        [ "$s" = 0 ] && s=$(start_ts "$mlog" "$mode")
        [ "$e" = 0 ] && e=$(done_ts "$mlog")
        if [ "$s" != 0 ] && [ "$e" != 0 ]; then done=yes; break; fi
        sleep 0.5
    done
    [ "$done" = yes ] || { err "$mode 同步超时 (${TIMEOUT}s)"; return 1; }

    # 数据校验 (best-effort; master 发送完成时 slave 可能还在加载, 重试到返回整数)
    local skeys=0 i2
    for ((i2=0; i2<20; i2++)); do
        skeys=$(redis-cli -p "$SPORT" DBSIZE 2>/dev/null || true)
        skeys=$(echo "$skeys" | grep -oE '^[0-9]+' || echo 0)
        [ "$skeys" -gt 0 ] 2>/dev/null && break
        sleep 0.5
    done
    [ "$skeys" = "$KEYS" ] || warn "$mode slave 数据 $skeys/$KEYS"

    # 速率 = RDB 大小 / 耗时(ms) (awk 取整, 避免 bc 乘法不带舍入)
    tms=$(awk -v a="$s" -v b="$e" 'BEGIN { printf "%.1f", (b - a) * 1000 }')
    if [ "$(echo "$tms > 0" | bc 2>/dev/null)" = 1 ]; then
        echo "$mode $ref_size $tms $(awk -v sz="$ref_size" -v t="$tms" \
            'BEGIN { printf "%.2f", sz * 1000 / t / 1048576 }')"
    else
        err "$mode 耗时无效 (start=$s end=$e)"
        return 1
    fi
}

# ── 主流程: disk 与 diskless 各测一次 ─────────────────────────────────────
> "$RESULT_FILE"
{
    echo "redis_fullsync_bench — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "keys=$KEYS  vsize=$VSIZE  指标: master 端传输耗时 (序列化+发送, 不含连接/不含 slave 加载)"
    echo "  起始: fork (disk=BGSAVE / diskless=RDB transfer); 结束: master Synchronization with replica succeeded"
    echo ""
    printf "  %-9s | %12s | %10s | %10s\n" "mode" "bytes" "耗时(ms)" "MB/s"
} >> "$RESULT_FILE"

for mode in "${MODES[@]}"; do
    res=$(run_one "$mode") || { warn "$mode 测量失败, 跳过"; continue; }
    read -r m bytes tms mbps <<< "$res"
    line="$(printf "  %-9s | %12s | %10s | %10s" "$m" "$bytes" "$tms" "$mbps")"
    echo "$line"
    echo "$line" >> "$RESULT_FILE"
done
stop_all

echo ""
echo "结果已写入: $RESULT_FILE"
