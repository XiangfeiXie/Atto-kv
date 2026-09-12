#!/bin/bash
#=============================================================================
#  bench_pool_mem.sh — 纯插入下 4 形态的内存"占用 + 回收"
#
#  4 形态 = 2 个二进制(底座) × 2 条 conf(运行期 kv_pool on|off):
#    glibc / glibc+池 / jemalloc / jemalloc+池
#
#  报告分两个口径块, 各回答一个问题:
#
#  A. used 口径 — "池省多少内存" (单位 B/key)
#     启动  = [MEM] startup              (刚建好引擎、尚未起 reactor 的空库值)
#     峰值  = INFO memory 的 used_memory (灌满 N×V、数据仍在)
#     回收  = [MEM] shutdown/post-release(优雅退出路径里 FLUSHALL 全清归还之后)
#     三者同源 get_used_mem(), 口径一致。但 jemalloc = Allocated 汇总行、
#     glibc = mallinfo2 的 uordblks+hblkhd —— 两底座定义不同, 空库基线差两个
#     数量级, 故 **绝对值只在同底座内相减**, 跨底座比大小无效。表里只出
#     峰值/key 与同底座内的 Δ; 绝对值打到 stderr 的 raw 行备查。
#
#  B. RSS 口径 — "实际物理占用多少、回收后回到哪" (单位 KiB)
#     启动/回收 = [MEM] 行里的 rss(VmRSS), 峰值 = /proc VmHWM(水位, 只增不减)。
#     进程退出后 /proc 消失, 故"关闭时内存"只能由进程打印 —— kvstore.c 已加。
#     回收−启动 即"回落量": 理想贴近 0, 正值 = 没还干净。
#
#  两底座均跑默认配置, 不注入 MALLOC_CONF —— glibc 无 trim、jemalloc 默认
#  dirty_decay_ms=10s, 回收 RSS 居高都是各自默认行为, 不做单边调优。
#
#  为什么测三档 value(klen 固定 16 → entry total = 48+V)? 池档表与底座档表
#  取整粒度不同, "省不省"取决于值落在哪段:
#    V=16  不省档: 池64 = jemalloc64, 对 glibc 省 16B(glibc 每块多 16B 头)
#    V=240 省  档: 池288 < jemalloc320, 对 glibc 也省 16B
#    V=465 亏损档: 池576 < jemalloc640(仍省), 对 glibc 反而亏 48B
#  第三档是关键: 同一测量里池"赢 jemalloc、输 glibc", 符号相反。
#  N 按 N×total ≈ 固定 payload 缩放 —— 池每档最后一个不满超块最多浪费 64KB,
#  档越大尾耗占比越高, N 太小会把节省量吃掉甚至反超。
#
#  用法(项目根): bash test/bench/bench_pool_mem.sh
#  单轮: 4 形态 × 3 档 各跑一次, 结果直接入表(不做重复取中位数)
#  输出: 终端表 + test/bench/results_pool_mem.txt
#=============================================================================
script_name=$(basename "$0")
source "$(dirname "$0")/common.sh"

PORT=2000
SERVER_CORES=5              # server 钉单核(bench_qps 同款隔离核), 减调度噪声
SETTLE=1                    # 启动后静置(等 reactor 就绪)
RESULT_FILE="test/bench/results_pool_mem.txt"

BINS=(  "build/kvstore"      "build/kvstore"
        "build-glibc/kvstore" "build-glibc/kvstore" )
POOLV=( "off" "on" "off" "on" )
LABELS=("jemalloc" "jemalloc+池" "glibc" "glibc+池" )

# "V:N:说明"    entry total = 48+V
ARMS=(
  "16:1000000:不省档 — 池64=je64(打平), 对 glibc 省16"
  "240:400000:省档   — 池288<je320(省32), 对 glibc 省16"
  "465:200000:亏损档 — 池576<je640(省64), 对 glibc 亏48"
)

require_tools
# 两底座一律跑默认配置: 清掉外部可能已注入的 MALLOC_CONF, 保证对比公平
unset MALLOC_CONF
for b in "${BINS[@]}"; do
    [ -x "$b" ] || { echo "缺二进制: $b" >&2; exit 1; }
done

WORK="$(mktemp -d /tmp/poolmem.XXXXXX)"
CONF="$WORK/mem.conf"
LOG="${LOG_DIR}/kvstore.log"
trap 'stop_all; rm -rf "$WORK"' EXIT

# 预生成 RESP 流(顺序唯一 key + 定长 value), 同档 4 形态共用同一文件
gen_stream() { # $1=V  $2=N  $3=out
    awk -v n="$2" -v vs="$1" 'BEGIN{
        for (j = 0; j < vs; j++) v = v sprintf("%c", 65 + (j % 26));
        for (i = 0; i < n; i++) {
            k = sprintf("key:%012d", i);
            printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, vs, v;
        }
    }' > "$3"
}

# 抓进程日志里某标签的 [MEM] 行 → "used rss" (进程自报, 见 kvstore.c mem_report)
log_mem() { # $1=标签
    grep -F "[MEM] $1 " "$LOG" 2>/dev/null | tail -1 | awk '{
        for (i = 1; i <= NF; i++) {
            if ($i ~ /^used=/) u = substr($i, 6);
            if ($i ~ /^rss=/)  r = substr($i, 5);
        }
        print u, r
    }'
}

# 读 /proc VmHWM (kB)
read_hwm() { awk '/^VmHWM:/{print $2; exit}' "/proc/$1/status" 2>/dev/null; }

# 左对齐补空格到指定"显示列宽"。printf 的 %-Ns 按字节算, 而 "池" 这类汉字 3 字节
# 只占 2 显示列 —— 中文标签行会比英文行少占列, 表行错位。故按
# 显示宽度 = 字符数 + 汉字数(每个汉字字节数−字符数=2) 手算。
# 注: 仅适用于 BMP 汉字; 若标签引入 4 字节字符(如 emoji)需另行处理。
pad_to() { # $1=串 $2=目标列宽
    local c=${#1} b n
    b=$(printf '%s' "$1" | wc -c)
    n=$(( $2 - (c + (b - c) / 2) ))
    [ "$n" -gt 0 ] || n=0
    printf '%s%*s' "$1" "$n" ''
}

# 等进程退出(优雅关闭走 dest_kvengine 打印 [MEM], 最长等 5s)
wait_gone() { # $1=pid
    for _ in $(seq 1 50); do
        kill -0 "$1" 2>/dev/null || return 0
        sleep 0.1
    done
    return 1
}

# ── 跑一轮: 启动→灌入→读峰值→SIGTERM→读回收 ────────────────────────────
# 输出 "启动used 启动rss 峰值used 峰值HWM 回收used 回收rss dbsize" (失败则不输出)
measure_round() { # $1=bin  $2=kv_pool值  $3=N  $4=stream
    local bin=$1 poolv=$2 n=$3 stream=$4 pid dbsize su sr pu ph qu qr

    write_kvs_conf "$CONF" "$PORT" proactor none
    echo "kv_pool $poolv" >> "$CONF"

    stop_all
    KVS_BIN="$bin"
    start_kvs "$CONF" "$PORT" >&2
    pid=$(pgrep -x kvstore | head -1)
    [ -n "$pid" ] || { echo "  进程未找到: $bin" >&2; return 1; }

    sleep "$SETTLE"
    read -r su sr <<< "$(log_mem startup)"          # 启动列
    [ -n "$su" ] || { echo "  [warn] 缺 [MEM] startup 行" >&2; return 1; }

    redis-cli -h 127.0.0.1 -p "$PORT" --pipe < "$stream" >/dev/null 2>&1 || true
    dbsize=$(redis-cli -h 127.0.0.1 -p "$PORT" DBSIZE 2>/dev/null | tr -d '\r')
    if [ "${dbsize:-0}" -ne "$n" ]; then
        echo "  [warn] DBSIZE=${dbsize:-0} != $n, 该轮弃用" >&2
        return 1
    fi

    pu=$(redis-cli -h 127.0.0.1 -p "$PORT" INFO memory 2>/dev/null \
        | tr -d '\r' | awk -F: '$1=="used_memory"{print $2; exit}')   # 峰值列(used)
    ph=$(read_hwm "$pid")                                              # 峰值列(RSS 水位)

    kill -TERM "$pid" 2>/dev/null || true
    wait_gone "$pid" || { echo "  [warn] 进程未按时退出" >&2; return 1; }
    read -r qu qr <<< "$(log_mem 'shutdown/post-release')"             # 回收列

    echo "$su $sr $pu $ph $qu $qr $dbsize"
}

> "$RESULT_FILE"
{
    echo "bench_pool_mem — $(date '+%Y-%m-%d %H:%M:%S')"
    echo "纯插入 N×V (无覆盖写/删除), 每形态 1 轮"
    echo "A. used 口径 — 池省多少: 启动/峰值/回收同源 get_used_mem()(jemalloc=Allocated, glibc=mallinfo2),"
    echo "   两底座定义不同, 故只出 峰值/key=(峰值−启动)/dbsize 与同底座内 Δ, 不列跨底座绝对值"
    echo "B. RSS 口径 — 物理占用: 启动/回收 = [MEM] 行 rss(VmRSS), 峰值 = /proc VmHWM(水位, 只增不减), 单位 KiB"
    echo "两底座均跑默认配置, 不注入 MALLOC_CONF"
    echo ""
} >> "$RESULT_FILE"

for arm in "${ARMS[@]}"; do
    V=${arm%%:*}; rest=${arm#*:}; N=${rest%%:*}; NOTE=${rest#*:}
    STREAM="$WORK/arm_v${V}.resp"
    gen_stream "$V" "$N" "$STREAM"

    # 每档重置数组: 防上一档的值在失败轮里被当成"陈旧数据"打印
    SR=(); PH=(); QR=(); PK=(); DEL=()
    for i in "${!BINS[@]}"; do
        row=$(measure_round "${BINS[$i]}" "${POOLV[$i]}" "$N" "$STREAM") \
            || { echo "  ${LABELS[$i]}: 该轮无效, 跳过" >&2; continue; }

        read -r s_u s_r p_u p_h q_u q_r _ <<< "$row"
        SR[$i]=$s_r; PH[$i]=$p_h; QR[$i]=$q_r
        PK[$i]=$(awk -v p="$p_u" -v s="$s_u" -v d="$N" \
            'BEGIN{ printf "%.1f", (p-s)/d }')
        # used 绝对值只进 raw 行(跨底座不可比, 不上表)
        echo "     raw: 启动used=$s_u 启动rss=$s_r 峰值used=$p_u 峰值HWM=$p_h 回收used=$q_u 回收rss=$q_r" >&2
    done

    # Δ 只在同底座内相减(0→1 = jemalloc, 2→3 = glibc); 任一侧缺测则留 "-"
    DEL[0]="-"; DEL[2]="-"
    for pair in "0:1" "2:3"; do
        no=${pair%%:*}; on=${pair#*:}
        [ -n "${PK[$no]:-}" ] && [ -n "${PK[$on]:-}" ] || continue
        DEL[$on]=$(awk -v a="${PK[$no]}" -v b="${PK[$on]}" 'BEGIN{ printf "%+.1f", b-a }')
    done

    {
        echo ""
        echo "── V=$V (N=$N, entry total=$((48+V)); $NOTE)"
        echo ""
        echo "  A. used 口径 — 池省多少"
        printf "  %s %12s %13s\n" "$(pad_to "形态" 14)" "峰值/key(B)" "Δ池−无池(B)"
        for i in "${!BINS[@]}"; do
            printf "  %s %12s %13s\n" \
                "$(pad_to "${LABELS[$i]}" 14)" "${PK[$i]:--}" "${DEL[$i]:--}"
        done
        echo ""
        echo "  B. RSS 口径 — 物理占用与回收 (KiB)"
        printf "  %s %10s %10s %10s %11s\n" "$(pad_to "形态" 14)" \
            "启动RSS" "峰值RSS" "回收RSS" "回收−启动"
        for i in "${!BINS[@]}"; do
            fb="-"
            if [ -n "${QR[$i]:-}" ] && [ -n "${SR[$i]:-}" ]; then
                fb=$(awk -v q="${QR[$i]}" -v s="${SR[$i]}" 'BEGIN{ printf "%+d", q-s }')
            fi
            printf "  %s %10s %10s %10s %11s\n" \
                "$(pad_to "${LABELS[$i]}" 14)" "${SR[$i]:--}" "${PH[$i]:--}" "${QR[$i]:--}" "$fb"
        done
    } | tee -a "$RESULT_FILE"
done

stop_all
echo ""
echo "结果已写入: $RESULT_FILE"
