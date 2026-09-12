#!/bin/bash
#=============================================================================
#  bench_pool_mem_manual.sh — 单一形态的内存"占用 + 回收"测量（人工驱动版）
#
#  与 bench_pool_mem.sh 的区别：本脚本**不启动、不停止 kvstore**，也不生成
#  配置。kvstore 由使用者自行以目标形态（底座 × kv_pool）启动，本脚本只对
#  "已经在跑的那个实例"做一轮测量，打出一行数据。跑 4 次（4 个形态）后由
#  使用者自行拼表。
#
#  因此脚本内不存在：四形态矩阵、conf 生成、进程生命周期、二进制路径假设。
#  一次调用 = 一个档位 × 一个形态。
#
#  用法（项目根）:
#    bash test/bench/bench_pool_mem_manual.sh --pid <pid> --arm <档位>
#
#    --pid   必填。kvstore 进程号（峰值 RSS 要读 /proc/<pid>/status）
#    --arm   必填。档位 1|2|3，或别名 不省|省|亏损（V/N 成对写死，不可单独改）
#
#  启动（两个必须：stdout 重定向到文件；脚本与 kvstore 同权限）:
#    sudo ./build/kvstore conf/kvstore-none.conf > /tmp/kvs.log 2>&1 &
#    PID=$(pgrep -x kvstore)
#    sudo bash test/bench/bench_pool_mem_manual.sh --pid "$PID" --arm 1
#  不要前台启动 —— 前台 stdout 是 tty，脚本反查不到日志；
#  也不要"sudo 起服务、普通用户跑脚本" —— 脚本读不到 /proc/<pid>。
#
#  端口固定 2000；日志路径由 /proc/<pid>/fd/1 反查（stdout 重定向到哪就读哪），
#  两者都不提供切换参数。
#
#  环境变量: WAIT_STOP 等停服超时秒数，默认 60
#
#  两个 [MEM] 数据来源（见 kvstore.c mem_report，stdout 且 setlinebuf 行缓冲）:
#    · 启动值  [MEM] startup            —— 进程启动时就已落日志，脚本前段即可读
#    · 回收值  [MEM] shutdown/post-release
#              —— 只在**优雅退出**（SIGTERM/SIGINT）后才打印，故脚本打完峰值后
#                 提示使用者停服，轮询 pid 消失再读。kill -9 不会有这一行。
#                 （stdin 在此不可用：与 redis-cli --pipe 冲突，故用轮询而非回车）
#
#  口径（与 bench_pool_mem.sh 一致，两份数据可直接拼在一张表里）:
#    used = get_used_mem()（jemalloc=Allocated 汇总行 / glibc=mallinfo2），
#           启动/峰值/回收同源；绝对值跨底座不可比，只看 峰值/key 与同底座 Δ
#    RSS  = 启动/回收取 [MEM] 行的 rss(VmRSS)，峰值取 /proc VmHWM(水位,只增不减)
#
#  输出: stdout 只有数据（供 | tee 留档）；提示/进度/raw 绝对值一律走 stderr
#
#  档位表（V/N 成对，不要单独改 —— 见 bench_pool_mem.sh 头注释的取整粒度说明）:
#    1 不省档  V=16  N=1000000  entry total=64   池64=je64(打平), 对 glibc 省16
#    2 省档    V=240 N=400000   entry total=288  池288<je320(省32), 对 glibc 省16
#    3 亏损档  V=465 N=200000   entry total=513  池576<je640(省64), 对 glibc 亏48
#=============================================================================

set -uo pipefail

PORT=2000           # 固定端口，不提供切换
PID=""
ARM=""
WAIT_STOP=${WAIT_STOP:-60}

err() { echo "[ERROR] $*" >&2; }
die() { err "$@"; exit 1; }
note() { echo "$*" >&2; }

usage() {
    sed -n '/^#  用法（项目根）:/,/^#  端口固定/p' "$0" | sed 's/^# \{0,1\}//'
}

#---------------------------------------------------------------------------
#  参数解析
#---------------------------------------------------------------------------
while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help) usage; exit 0;;
    esac
    [ $# -ge 2 ] || die "参数 $1 缺少取值"
    case "$1" in
        --pid) PID=$2;;
        --arm) ARM=$2;;
        *) die "未知参数: $1（-h 看用法）";;
    esac
    shift 2
done

[ -n "$PID" ] || die "必须指定 --pid"
[ -n "$ARM" ] || die "必须指定 --arm（1|2|3 或 不省|省|亏损）"

# 档位 → (V,N,说明)。V/N 成对锁死，不提供自由传入。
case "$ARM" in
    1|不省|不省档) V=16;  N=1000000; NOTE="不省档 — 池64=je64(打平), 对 glibc 省16";;
    2|省|省档)     V=240; N=400000;  NOTE="省档   — 池288<je320(省32), 对 glibc 省16";;
    3|亏损|亏损档) V=465; N=200000;  NOTE="亏损档 — 池576<je640(省64), 对 glibc 亏48";;
    *) die "未知档位: $ARM（1|2|3 或 不省|省|亏损）";;
esac

for t in redis-cli awk; do
    command -v "$t" >/dev/null 2>&1 || die "缺少工具: $t"
done

#---------------------------------------------------------------------------
#  进程与日志定位
#---------------------------------------------------------------------------
# 注意：不要用 kill -0 探活 —— 进程属主不是自己时它返回 EPERM，
# 会被误报成"不存在"。先看 /proc 在不在，再单独比对属主。
[ -d "/proc/$PID" ] || die "pid=$PID 不存在（kvstore 已退出？）"
OWNER=$(stat -c %u "/proc/$PID" 2>/dev/null)
if [ "${OWNER:-0}" != "$(id -u)" ]; then
    die "pid=$PID 属主 uid=${OWNER:-?}，当前 uid=$(id -u) —— 两者必须一致
      kvstore 若用 sudo 起的，本脚本也要 sudo 起"
fi
[ -r "/proc/$PID/status" ] || die "读不到 /proc/$PID/status"

LOG=$(readlink "/proc/$PID/fd/1" 2>/dev/null || true)
case "$LOG" in
    ""|/dev/pts/*|pipe:*|socket:*)
        die "反查到 stdout=${LOG:-（读不到）}，不是文件
      启动时须把 stdout 重定向到文件，例如:
        sudo ./build/kvstore <conf> > /tmp/kvs.log 2>&1 &";;
esac
[ -r "$LOG" ] || die "日志不可读: $LOG"
note "[bench] pid=$PID arm=$ARM V=$V N=$N  ($NOTE)"
note "[bench] log=$LOG"

WORK=$(mktemp -d /tmp/poolmem_manual.XXXXXX)
trap 'rm -rf "$WORK"' EXIT

# 抓日志里某标签的 [MEM] 行 → "used rss"（进程自报，见 kvstore.c mem_report）
# $2 非空时只在该字节偏移之后的追加内容里找（防读到上一轮的残留行）
log_mem() {
    local tag=$1 from=${2:-0}
    tail -c "+$(( from + 1 ))" "$LOG" 2>/dev/null \
        | grep -F "[MEM] $tag " | tail -1 | awk '{
            for (i = 1; i <= NF; i++) {
                if ($i ~ /^used=/) u = substr($i, 6);
                if ($i ~ /^rss=/)  r = substr($i, 5);
            }
            print u, r
        }'
}

#---------------------------------------------------------------------------
#  1. 启动值（进程启动时已落日志）
#---------------------------------------------------------------------------
read -r su sr <<< "$(log_mem startup)"
[ -n "${su:-}" ] || die "日志里没有 [MEM] startup 行（--pid 指对了吗？）"

# 记下当前日志长度：回收值只认此后追加的内容，避免读到上一轮 shutdown 行
LOG_MARK=$(stat -c %s "$LOG" 2>/dev/null || echo 0)

#---------------------------------------------------------------------------
#  2. 灌入（顺序唯一 key + 定长 value 的 RESP 流）
#---------------------------------------------------------------------------
STREAM="$WORK/arm_v${V}.resp"
awk -v n="$N" -v vs="$V" 'BEGIN{
    for (j = 0; j < vs; j++) v = v sprintf("%c", 65 + (j % 26));
    for (i = 0; i < n; i++) {
        k = sprintf("key:%012d", i);
        printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, vs, v;
    }
}' > "$STREAM"

note "[bench] 灌入 $N × ${V}B ..."
redis-cli -h 127.0.0.1 -p "$PORT" --pipe < "$STREAM" >/dev/null 2>&1 || true
dbsize=$(redis-cli -h 127.0.0.1 -p "$PORT" DBSIZE 2>/dev/null | tr -d '\r')
[ -n "$dbsize" ] || die "127.0.0.1:$PORT 无响应（--pid 与被测实例对得上吗？）"
[ "$dbsize" = "$N" ] || die "DBSIZE=$dbsize != $N，本轮数据无意义"

#---------------------------------------------------------------------------
#  3. 峰值
#---------------------------------------------------------------------------
pu=$(redis-cli -h 127.0.0.1 -p "$PORT" INFO memory 2>/dev/null \
    | tr -d '\r' | awk -F: '$1=="used_memory"{print $2; exit}')
ph=$(awk '/^VmHWM:/{print $2; exit}' "/proc/$PID/status" 2>/dev/null)
[ -n "${pu:-}" ] || die "INFO memory 未返回 used_memory"
[ -n "${ph:-}" ] || die "读不到 VmHWM"

PK=$(awk -v p="$pu" -v s="$su" -v d="$N" 'BEGIN{ printf "%.1f", (p-s)/d }')
note "[bench] raw: 启动used=$su 启动rss=$sr 峰值used=$pu 峰值HWM=$ph"

#---------------------------------------------------------------------------
#  4. 回收值 —— 等使用者优雅停服
#---------------------------------------------------------------------------
note "[bench] 峰值已取。请 kill -TERM $PID 停止 kvstore（-9 不会打印 shutdown 行）"
note "[bench] 等待进程退出，最长 ${WAIT_STOP}s ..."

stopped=0
for _ in $(seq 1 "$WAIT_STOP"); do
    kill -0 "$PID" 2>/dev/null || { stopped=1; break; }
    sleep 1
done

qu=""; qr=""
if [ "$stopped" = 1 ]; then
    sleep 0.3   # 等日志落盘（setlinebuf 行缓冲，留一点余量）
    read -r qu qr <<< "$(log_mem 'shutdown/post-release' "$LOG_MARK")"
    [ -n "${qu:-}" ] || note "[bench] 警告: 没有读到 shutdown/post-release 行"
else
    note "[bench] 警告: 超时未停服，回收列留空"
fi
[ -n "${qu:-}" ] || note "[bench] raw: 回收used=${qu:--} 回收rss=${qr:--}"

#---------------------------------------------------------------------------
#  5. 输出（stdout 只有数据）
#---------------------------------------------------------------------------
# 回收−启动：理想贴近 0，正值 = 没还干净；缺测留 "-"
FB="-"
if [ -n "${qr:-}" ] && [ -n "${sr:-}" ]; then
    FB=$(awk -v q="$qr" -v s="$sr" 'BEGIN{ printf "%+d", q-s }')
fi

echo "  A. used 口径 — 池省多少"
echo "  峰值/key(B) = $PK"
echo ""
echo "  B. RSS 口径 — 物理占用与回收 (KiB)"
echo "  启动RSS = ${sr:--}   峰值RSS = ${ph:--}   回收RSS = ${qr:--}   回收−启动 = $FB"
