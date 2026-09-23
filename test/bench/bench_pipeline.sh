#!/bin/bash
# =============================================================================
# bench_pipeline.sh — 单个 pipeline 深度 P 下的 SET QPS
#
# 在项目根目录执行：
#   ./test/bench/bench_pipeline.sh 40      # 测 P=40
#
# 脚本只做三件事：
#   1. 杀掉残留的 kvstore
#   2. 用 conf/kvstore-none.conf 起一个（服务端绑核 1）
#   3. 跑 redis-benchmark（客户端绑核 3），跑完再把它杀掉
#
# 命令数 n = 100000 × P，一次只测一个 P。
# =============================================================================

P=$1
n=$((100000 * P))

pkill -x kvstore
sleep 0.5

taskset -c 1 build/kvstore conf/kvstore-none.conf &
sleep 1

taskset -c 3 redis-benchmark -p 2000 -c 1 -n "$n" -P "$P" -t set

pkill -x kvstore
