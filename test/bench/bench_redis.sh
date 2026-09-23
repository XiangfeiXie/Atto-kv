#!/bin/bash
# =============================================================================
# bench_redis.sh — 单个 pipeline 深度 P 下的 SET QPS（Redis 对照）
#
# 在项目根目录执行：
#   ./test/bench/bench_redis.sh 40      # 测 P=40
#
# 脚本只做四件事：
#   1. 杀掉占着 2000 端口的旧实例（只认端口，不动你其它 redis 实例）
#   2. 清掉 appendonlydir
#   3. 起一个 redis-server（服务端绑核 1）
#   4. 跑 redis-benchmark（客户端绑核 3），跑完再把它杀掉
#
# 命令数 n = 100000 × P，一次只测一个 P。
# 数据落在项目根目录（--dir .），开 AOF 时会看到 appendonlydir。
#
# 测 AOF 开关的差别：手动改下面 START 那行的 --appendonly yes / no。
# --save "" 是关 RDB。
# =============================================================================

P=$1
n=$((100000 * P))
PORT=2000

fuser -k -n tcp "$PORT" 2>/dev/null
rm -rf appendonlydir
sleep 0.5

# ↓↓↓ START：改这一行的 --appendonly 切 AOF
taskset -c 1 redis-server --port "$PORT" --save "" --appendonly no --dir . &
sleep 1

taskset -c 3 redis-benchmark -p "$PORT" -c 1 -n "$n" -P "$P" -t set

fuser -k -n tcp "$PORT" 2>/dev/null
