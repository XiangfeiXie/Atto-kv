# KVStore Test Script Guide

本文档说明 `test/` 目录下保留脚本的作用、适用场景、依赖和推荐运行顺序。

---

## 推荐入口

| 脚本 | 作用 | 运行方式 |
|---|---|---|
| `test/run_regression.sh` | 功能回归统一入口，按模式组合多个 smoke 脚本 | `bash test/run_regression.sh quick` |
| `test/reset.sh` | 重置 KVStore/Redis、清理端口和持久化文件；benchmark 前使用 | `sudo bash test/reset.sh` |
| `test/setup_affinity.sh` | 调整 IRQ/RPS affinity，减少 benchmark 抢核噪声 | `sudo bash test/setup_affinity.sh --cores 0` |
| `test/smoke/run_all.sh` | 数据类型 smoke 聚合入口，要求 kvstore 已经运行 | `bash test/smoke/run_all.sh 127.0.0.1 2000` |
| `test/bench/baseline_capture.sh` | 本机 SET/GET baseline 统一入口，替代旧的 `baseline_8c_pcore.sh` 和 `bench_phase4.sh` | `bash test/bench/baseline_capture.sh --baseline` |
| `test/bench/bench_fastpath.sh` | fastpath 端到端 benchmark 统一入口，支持 uniform/skewed/all | `bash test/bench/bench_fastpath.sh all` |
| `test/bench/bench_fullsync.sh` | RDMA fullsync benchmark 统一入口，支持 master/slave 子命令 | `bash test/bench/bench_fullsync.sh master 100000` |

---

## 1. 功能正确性脚本

| 脚本 | 说明 | 依赖 | 是否启动 kvstore |
|---|---|---|---|
| `test/run_regression.sh` | 统一回归入口：`quick` / `full` / `tsan` / `fastpath` | `redis-cli`，部分模式需要 `redis-benchmark` | 视模式而定 |
| `test/smoke/run_all.sh` | 依次运行 string/list/hash/set/zset smoke | `redis-cli` | 否 |
| `test/smoke/smoke_string.sh` | STRING 命令基础兼容性 | `redis-cli` | 否 |
| `test/smoke/smoke_list.sh` | LIST 命令基础兼容性 | `redis-cli` | 否 |
| `test/smoke/smoke_hash.sh` | HASH 命令基础兼容性 | `redis-cli` | 否 |
| `test/smoke/smoke_set.sh` | SET 命令基础兼容性 | `redis-cli` | 否 |
| `test/smoke/smoke_zset.sh` | ZSET 命令基础兼容性 | `redis-cli` | 否 |
| `test/smoke/_lib.sh` | smoke 公共函数库 | - | 否 |
| `test/bench/config_smoke.sh` | `CONFIG GET/SET/RESETSTAT/REWRITE` 行为验证 | `redis-cli` / `redis-benchmark` | 是 |
| `test/bench/multi_thread_smoke.sh` | 多线程、BLPOP、AOF reload、BGSAVE 回归 | `redis-cli` / `redis-benchmark` | 是 |
| `test/bench/multi_key_smoke.sh` | SMOVE/LMOVE/RPOPLPUSH/BLPOP 跨 shard 不变量 | `redis-cli` | 是 |
| `test/test_wal_aof.sh` | WAL/AOF 统一路径、增量握手、WAL replay smoke | `redis-cli` / `nc` / `redis-benchmark` | 是 |

推荐顺序：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
bash test/run_regression.sh quick
bash test/run_regression.sh full
```

---

## 2. Sanitizer / focused correctness

| 脚本 | 说明 | 运行方式 |
|---|---|---|
| `test/bench/tsan_lock_smoke.sh` | TSAN 下验证 rdlock/wrlock、lazy-expire、BLPOP wake、MGET/MSET 交错 | `KVS=$(pwd)/build-tsan/kvstore bash test/bench/tsan_lock_smoke.sh` |
| `test/bench/fastpath_tsan_smoke.sh` | 单独用 TSAN 编译并运行 fastpath cache stress | `bash test/bench/fastpath_tsan_smoke.sh 10` |
| `test/fp_cache_test.c` | fastpath cache 单元测试和并发 stress 源码 | `cmake --build build --target fp_cache_test && ./build/fp_cache_test` |

TSAN 构建示例：

```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_FLAGS="-fsanitize=thread -g -O1 -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread" \
  -DUSE_JEMALLOC=OFF
cmake --build build-tsan -j"$(nproc)"
KVS=$(pwd)/build-tsan/kvstore bash test/bench/tsan_lock_smoke.sh
```

---

## 3. 性能 benchmark 脚本

| 脚本 | 说明 | 输出 |
|---|---|---|
| `test/bench/baseline_capture.sh` | SET/GET baseline sweep；支持 `--baseline`、`--pcore MASK`、`--phase4` | `baseline_*.txt` 或 `snapshot_*.txt` |
| `test/bench/dev_quick.sh` | 开发阶段快速性能冒烟：SET/GET/MSET/LRANGE | stdout，可选 `dev_quick_history.tsv` |
| `test/bench/bench_pipeline.sh` | KVStore vs Redis pipeline 对比 | `results.txt` / `results.csv` |
| `test/bench/bench_aof.sh` | AOF/WAL 持久化开销对比 | `results_aof.txt` / `results_aof.csv` |
| `test/bench/bench_save.sh` | SAVE/BGSAVE 对写入吞吐影响 | stdout / results 文件 |
| `test/bench/bench_memory.sh` | jemalloc vs glibc 内存占用对比 | stdout |
| `test/bench/bench_sync_qps.sh` | 复制开启/关闭对 QPS 的影响 | `results_sync_qps.csv` |
| `test/bench/bench_xdp.sh` | XDP passive monitor 开销对比 | `results_xdp.csv` |
| `test/bench/bench_fastpath.sh` | fastpath `uniform` / `skewed` / `all` benchmark | `fastpath_results.txt` |
| `test/bench/bench_fullsync.sh` | RDMA fullsync master/slave benchmark | stdout |
| `test/bench/vs_redis.sh` | KVStore vs Redis/KeyDB head-to-head | `vs_redis_*.txt` / `.csv` |
| `test/bench/profile_capture.sh` | perf top / perf record / snapshot 采集 | `profile_*.txt` |
| `test/bench/soak.sh` | 长时间稳定性 soak | `/tmp/kvstore-soak/` |
| `test/bench/common.sh` | benchmark 公共函数库 | - |
| `test/reset.sh` | 重置 KVStore/Redis 状态和持久化文件 | 需要 root；会停止服务并删除测试数据 |
| `test/setup_affinity.sh` | 设置/恢复 IRQ 与 RPS CPU affinity | 需要 root；只在性能基准前按需使用 |

常用命令：

```bash
bash test/bench/baseline_capture.sh --baseline
bash test/bench/baseline_capture.sh --pcore 0-5 --workers "1 2 3 4 5"
bash test/bench/bench_fastpath.sh all
sudo bash test/setup_affinity.sh --cores 0
sudo bash test/bench/bench_pipeline.sh
sudo bash test/bench/bench_aof.sh
```

---

## 4. RDMA fullsync benchmark

`bench_fullsync.sh` 合并了原来的 master/slave 两个脚本。

Master 侧填充数据：

```bash
bash test/bench/bench_fullsync.sh master 100000 --host 192.168.30.135 --port 2000 --value-size 64
```

Slave 侧启动并计时：

```bash
sudo bash test/bench/bench_fullsync.sh slave --payload <PAYLOAD_FROM_MASTER> --master-host 192.168.30.135
```

注意：

- 需要 Soft-RoCE/libfabric 环境。
- 默认使用 verbs provider。
- 同 VM 同时运行 master/slave/benchmark 时结果会有明显调度噪声。

---

## 5. 非脚本测试源码

| 文件 | 作用 |
|---|---|
| `test/fp_cache_test.c` | fastpath cache 单元测试 + stress |
| `test/bench_fp_inproc.c` | fastpath vs dict path 的 in-process CPU microbenchmark |
| `test/testcase.c` | 早期 C 测试样例，当前不作为主回归入口 |

---

## 6. 已合并或移除的脚本

| 原文件 | 处理方式 | 替代入口 |
|---|---|---|
| `test/bench/baseline_8c_pcore.sh` | 合并 | `test/bench/baseline_capture.sh --pcore 0-5` |
| `test/bench/bench_phase4.sh` | 合并 | `test/bench/baseline_capture.sh --phase4` |
| `test/bench/bench_fastpath_skewed.sh` | 合并 | `test/bench/bench_fastpath.sh skewed` |
| `test/bench/bench_fullsync_master.sh` | 合并 | `test/bench/bench_fullsync.sh master` |
| `test/bench/bench_fullsync_kvs_slave.sh` | 合并 | `test/bench/bench_fullsync.sh slave` |
| `test/a.txt` / `test/b.txt` | 非 KVStore 测试材料，移除 | 无 |
| `test/pipe.py` | 早期 RESP 生成临时脚本，移除 | `redis-benchmark` / smoke 脚本 |

---

## 7. 注意事项

- 多数 benchmark 会杀掉现有 `kvstore` 或占用固定端口，请不要在生产环境运行。
- 需要 root 的脚本通常是为了启动 Redis、绑定 BPF/XDP、设置 affinity 或处理端口占用。
- 性能数据默认是本地 VM / loopback 口径，适合作为趋势和回归基线，不代表物理机极限。
- smoke 脚本和 benchmark 脚本目标不同：smoke 以 correctness 为主，benchmark 以可复现性能趋势为主。
