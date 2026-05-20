# KVStore 测试执行计划（test-log）

## 1. 测试拓扑与资源安排

| 角色 | 建议机器 | 说明 |
| --- | --- | --- |
| 主节点 (Master) | VM-A | 运行 `kvstore` 主进程；负责 fullsync snapshot 与 AOF/WAL 输出 |
| 从节点 (Slave) | VM-B | 运行 `kvstore` 从进程；用于 fullsync + 增量复制验证、性能观测对比 |
| 压测 / 工具 | 视场景而定 | - 单机场景：主节点同时跑压测（最简单）<br>- 主从场景：建议第三终端或主机跑 `redis-benchmark` 与脚本 |

> 若当前仅一台机器可用，可先完成「单机基础性能 + 观测影响 + Sanitizer」部分；主从复制与跨机延迟需等双机部署。

---

## 2. 基础功能与可靠性测试

| 模块 | 脚本 / 命令 | 目标 |
| --- | --- | --- |
| 回归冒烟 | `bash test/run_regression.sh quick` | 基础命令正确性（string/list/hash/set/zset） |
| Fastpath 单元 | `cmake --build build --target fp_cache_test`<br>`./build/fp_cache_test` | seqlock/缓存一致性验证 |
| Fastpath 压测 | `cmake --build build --target bench_fp_inproc`<br>`./build/bench_fp_inproc` | CPU 内部压力测试，确认无崩溃 |
| AOF replay | `test/test_wal_aof.sh` | 写入/重放流程正确，检查 WAL replay 输出 |
| reset 工具 | `sudo bash test/reset.sh` | 清理环境，确保脚本执行无误 |

记录：执行日志、PASS/FAIL、异常截图。

---

## 3. 单机性能验证

### 3.1 基线吞吐
- 推荐分两档记录，避免把“开发期 sanity”和“正式性能基准”混在一起比较。

#### 3.1.1 开发期 sanity 基线
- 配置：`conf/kvstore-dev.conf`（`persistence none`、`network proactor`、`threads -2`、`cpu_pin auto`；8c 机器通常解析为 6 workers）。
- 用途：快速确认当前构建没有明显退化；不用于和历史最高吞吐直接比较。
- 压测：
  ```bash
  redis-benchmark -h 127.0.0.1 -p 2000 -t get,set -n 200000 -c 64 -P 16 --csv > logs/perf_baseline.csv
  ```
- 指标：OPS、平均延迟、p95/p99、CPU 利用率（`mpstat -P ALL 1`）、系统负载。
- 参考口径：当前本机实测约 SET/GET 22~24 万 rps；该口径主要受同机 client/server 调度和 benchmark 客户端线程数影响。

#### 3.1.2 正式推荐基准（与 8c 本机高吞吐对齐）
- 配置：`persistence none`、`rdma_sync off`、`xdp no`、`sockmap no`、`network proactor`。
- worker 配置：8c 本机优先 sweep `threads=1 2 4 6 8`；当前单轮 sweep 中 SET 最优为 `threads=2,cpu_pin=auto`，GET 最优为 `threads=4,cpu_pin=off`。
- `cpu_pin`：至少比较 `auto` 与 `off`；若要稳定复现，可加 `taskset` 固定 server/client CPU 集合。
- 压测：
  ```bash
  redis-benchmark -h 127.0.0.1 -p 2000 -t get,set -n 2000000 -c 400 -P 10 --threads 8 --csv > logs/perf_baseline_formal.csv
  ```
- 每个配置至少跑 3 轮，记录 best QPS 与 median QPS；正式展示用 best，波动判断用 median。
- 每轮隔离：
  1. 停止旧 `kvstore`。
  2. 启动目标配置。
  3. `redis-cli -p 2000 FLUSHALL`。
  4. 单独运行一轮 benchmark。
  5. 采集 `INFO stats`、`/proc/loadavg`、`mpstat -P ALL 1`。
  6. 停止 `kvstore`，再进入下一轮。
- 参考口径：当前 8c VM 单轮高吞吐 sweep 最高约 SET 39 万 rps、GET 43 万 rps；若只跑 dev sanity 口径，不能直接判断项目相对正式高基准退化。

当前 8c 单轮 sweep 记录（`logs/perf/20260519_133334_8c_formal_sweep/`）：

| KV worker threads | cpu_pin | SET rps | GET rps | 备注 |
|---:|---|---:|---:|---|
| 2 | auto | **391,389** | 349,040 | SET 最优 |
| 2 | off | 355,366 | 431,593 | GET 接近最优 |
| 4 | auto | 295,596 | 389,257 | SET 明显下降 |
| 4 | off | 387,898 | **432,339** | GET 最优，综合较好 |
| 6 | auto | 322,893 | 351,989 | worker 增多后同机抢核加重 |
| 6 | off | 323,625 | 349,162 | 无收益 |
| 8 | auto | 323,520 | 319,387 | worker 过多，client/softirq 空间不足 |
| 8 | off | 354,359 | 358,166 | 低于 2/4 workers |

结论：当前 8c 本地 loopback 测试中，最高 SET 为 `391,389 rps`（`threads=2,cpu_pin=auto`），最高 GET 为 `432,339 rps`（`threads=4,cpu_pin=off`）。`threads=6/8` 没有继续提升，主要受同机 client/server/softirq 抢核和 VM 调度影响。

### 3.2 fastpath 开关对照
- 使用 90% GET + 10% SET workload。
- 分别运行：
  1. fastpath on（默认）。
  2. `fastpath_cache no`。
- 对比命中率（`INFO fastpath`）、QPS。
- 注意：不要使用 Python 单连接/逐 RESP 解析脚本作为高 QPS 压测器；本次自定义 90/10 Python 压测只跑到 `2.5k~4k qps`，确认是 client 端瓶颈，结果作废。
- 当前 `threads=2,cpu_pin=auto` 的有效快速对照采用 `redis-benchmark` 分段近似：
  1. 先 SET 预热热 key。
  2. 再跑热 GET，采集 fastpath hit rate。
  3. 单独跑 SET，按 `90% GET + 10% SET` 估算混合吞吐。

| 场景 | fastpath | GET rps | SET rps | 估算 90/10 rps | hit rate | 备注 |
|---|---|---:|---:|---:|---:|---|
| 4096 hot keys | on | 358,295 | 394,322 | 361,598 | 24.66% | direct-mapped cache 冲突较多 |
| 4096 hot keys | off | 392,773 | 394,322 | 392,927 | 0.00% | off 在该端到端场景更高 |
| 64 hot keys | on | 394,166 | — | — | 95.27% | 高命中热 key GET |
| 64 hot keys | off | 396,354 | — | — | 0.00% | 与 on 基本持平 |

结论：`threads=2` 下 fastpath 没有“执行很慢”的问题；有效 `redis-benchmark` 口径仍在约 36~39 万 qps。端到端 fastpath 收益被 TCP/proactor/client 开销淹没，且 4096 key direct-mapped 冲突导致 hit rate 只有 `24.66%`，所以该场景 on 比 off 低约 `8%`；64 hot-key 命中率 `95.27%` 时 on/off 只差约 `0.55%`。

#### 3.2.1 fastpath 改进验证（hash mixing + 2048 buckets）

- 改动：
  1. `fp_index()` 改为 MurmurHash3 finalizer 风格二次混合，减少低位 hash 分布风险。
  2. `FP_BUCKETS` 从 `1024` 调整为 `2048`，cache table 从约 `512 KB` 增至约 `1 MB`，缓解 direct-mapped 容量冲突。
- 正确性：`cmake --build build --target kvstore fp_cache_test bench_fp_inproc` 通过；`./build/fp_cache_test` 通过，stress 无 torn-read。
- 原始日志：
  - `logs/perf/20260519_142219_fastpath_hashmix_threads2/`
  - `logs/perf/20260519_142715_fastpath_2048_threads2/`

| 版本 | 场景 | fastpath | GET rps | SET rps | 估算 90/10 rps | hit rate | 结论 |
|---|---|---|---:|---:|---:|---:|---|
| 原始 1024 buckets | 4096 hot keys | on | 358,295 | 394,322 | 361,598 | 24.66% | 冲突明显，on 慢于 off |
| hashmix only | 4096 hot keys | on | 390,778 | 391,083 | 390,808 | 24.38% | hit rate 未改善，说明不是低位 hash 主因 |
| hashmix + 2048 buckets | 4096 hot keys | on | 355,619 | 391,850 | 358,938 | 42.42% | hit rate 明显提升，端到端混合约持平 |
| hashmix + 2048 buckets | 4096 hot keys | off | 355,872 | 384,320 | 358,526 | 0.00% | 作为同轮 baseline |
| hashmix + 2048 buckets | 64 hot keys | on | 429,185 | — | — | 99.99% | 高命中 GET 明显收益 |
| hashmix + 2048 buckets | 64 hot keys | off | 388,048 | — | — | 0.00% | on 比 off 高约 10.6% |

结论：`2048 buckets` 能把 4096 hot keys 的 hit rate 从约 `24%` 提升到 `42%`，说明主要瓶颈确实是 direct-mapped 容量冲突。高命中 64-key GET 场景 fastpath on 达到 `429,185 rps`，比 off 高约 `10.6%`。4096-key 端到端混合仍基本持平，说明在中等命中率下收益仍被 network/proactor/client 固定成本抵消；如果要进一步提升 4096-key workload，需要考虑 2-way/4-way set-associative，而不只是继续增大 direct-mapped table。

### 3.3 持久化影响
- 配置矩阵：`persistence none` / `aof` / `rdb`。
- 记录：吞吐、延迟、WAL/AOF 尺寸（`ls -lh`）、CPU IOwait。

### 3.4 Observability 开销
- 场景：release 构建 + 基线 workload。
- 对比：
  1. 默认（未 attach）。
  2. `sudo ./build/kvs-top --attach-xdp --xdp-iface <iface> --attach-sockmap --sockmap-pid $(pidof kvstore)`。
- 输出：吞吐下降比例、`kvs_top` 仪表盘截图。

### 3.5 网络模型切换
- 配置切换 `network reactor` / `proactor`，同 workload 对比。
- 关注：线程扩展性、系统调用占比。

> 所有原始 CSV/日志置于 `logs/perf/YYYYMMDD_*`，方便归档。

---

## 4. 主从同步场景

### 4.1 部署步骤
1. **Master（VM-A）：** `./build/kvstore conf/kvstore-master.conf`
2. **Slave（VM-B）：** `./build/kvstore conf/kvstore-slave.conf`
3. 确认：
   - Slave 日志出现 `fullsync done`。
   - Slave `redis-cli -p <slave_port> INFO replication` 中 `role:slave`、`master_link_status:up`。

### 4.2 Fullsync + Catchup 压测
- 在 master 上使用 `test/bench/bench_fullsync.sh` 或手动流程：
  1. 预热：批量 SET (100k)；
  2. 启动 slave，观察 fullsync 时间；
  3. Fullsync 完成后继续写入增量，确认 slave catchup。
- 指标记录：
  - fullsync 时间、数据量；
  - backlog 队列指标（日志或 INFO）；
  - master/slave `dbsize`、热点 key 一致性；
  - 网络指标（`sar -n DEV` 或 `ifstat`）。

### 4.3 RDMA vs TCP
- 若环境支持 Soft-RoCE，比较 `rdma_provider verbs` vs `tcp`。
- 关注：fullsync 耗时、catchup 延迟、CPU 消耗。

### 4.4 失败/恢复流程
- 手动终止 slave，再次启动，验证增量 catchup 无数据丢失。
- 记录 WAL backlog 在 fullsync 前后的处理。

---

## 5. Pipeline / 批处理测试

### 5.1 redis-benchmark pipeline
- 命令：`redis-benchmark -p 2000 -c 200 -P 32 -n 1000000 -t set,get --csv`。
- 同时记录 OVS/系统统计（`pidstat -p $(pidof kvstore) 1`）。

### 5.2 自定义脚本
- `test/bench/baseline_capture.sh` 支持多阶段 pipeline，参数参照 `test/test.md`。
- 输出：
  - 每阶段 TPS、延迟；
  - kvstore 日志是否有 `command backlog` 警告。

### 5.3 Pipeline + 持久化
- AOF + pipeline 高压场景，确认 consumer 写磁盘无堆积（观察日志和 AOF 文件增长）。

---

## 6. 内存与资源测试

| 项目 | 方法 | 记录内容 |
| --- | --- | --- |
| RSS / 内存占用 | `smem -p` 或 `pmap -x $(pidof kvstore)` | 峰值 RSS、共享页占比 |
| GC / 释放 | 执行 `FLUSHALL` 后观测 RSS 回落、AOF 文件截断 |
| FD 使用 | `ls /proc/$(pidof kvstore)/fd | wc -l` | 连接负载情况下的 FD 数 |
| CPU pin 效果 | `taskset`/`numactl` | 是否存在 NUMA 跨节点迁移 |

如有必要，使用 `valgrind --tool=massif` 在小 workload 下分析堆结构（注意性能非常慢）。

---

## 7. Sanitizer / 静态分析

### 7.1 AddressSanitizer (ASAN)
- 构建：`cmake -S . -B build-asan -DWITH_ASAN=ON`
- 测试：`cmake --build build-asan`，`ctest` 或关键脚本。
- 记录：是否有 invalid read/write、内存泄露报告。

### 7.2 ThreadSanitizer (TSAN)
- 构建：`cmake -S . -B build-tsan -DWITH_TSAN=ON`
- 测试：
  ```bash
  cmake --build build-tsan
  ASAN_OPTIONS= halt_on_error=1 ./build-tsan/kvstore conf/kvstore-tsan.conf
  redis-benchmark -p <tsan_port> -n 10000 -c 4
  ```
- 注意：TSAN 性能极低，只验证 data race；将日志与报告保存到 `logs/tsan/`。

### 7.3 UndefinedBehaviorSanitizer (UBSan)（可选）
- 构建：`cmake -S . -B build-ubsan -DWITH_UBSAN=ON`
- 关注算术溢出、未定义行为。

---

## 8. 工具与度量

- **日志目录结构建议**
  - `logs/perf/20260519_baseline.csv`
  - `logs/perf/20260519_fastpath_off.csv`
  - `logs/replication/20260519_fullsync_master.log`
  - `logs/sanitizer/asan_20260519.txt`
- **系统监控**：`vmstat 1`、`iostat -x 1`、`perf top`、`sar -n DEV 1`。
- **BPF 观测**：`kvs-top` dashboard 截图或数据导出。

---

## 9. 报告输出模板

1. **环境说明**：硬件、内核、libfabric 版本、编译参数。
2. **测试矩阵**：表格列出各场景开关（fastpath / persistence / rdma / observability）。
3. **结果摘要**：
   - 基线 vs fastpath off；
   - 持久化对吞吐影响；
   - 观测层 attach 开销；
   - 主从 fullsync/catchup 指标。
4. **可靠性结论**：ASAN/TSAN 状态、长时间压测是否稳定。
5. **改进建议**：依据瓶颈（CPU、IO、锁）提出下一步优化方向。

---

## 10. 执行顺序建议

1. 单机基础正确性 & fastpath 校验。
2. Release 构建下完成单机性能矩阵。
3. 部署主从，进行 fullsync/catchup & RDMA 对照。
4. 扩展 pipeline、持久化组合测试。
5. 运行 ASAN/TSAN/UBSan 等 Sanitizer。
6. 汇总日志与数据，撰写报告。

> 若资源紧张，可优先完成 1~4，之后在空闲时跑 Sanitizer 和长压测试。
