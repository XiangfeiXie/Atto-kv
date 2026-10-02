cmake_minimum_required(VERSION 3.16)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
project(kvstore C)

set(CMAKE_C_STANDARD 11)

# Phase 8.4 — default to Release build.  Before this fix the build used
# default cmake flags (no -O, no -DNDEBUG), which silently shaved 20-50%
# off every benchmark we ever ran.  Override at configure time with
#   cmake -DCMAKE_BUILD_TYPE=Debug   for asserts + symbols
#   cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo  for perf + symbols
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING
        "Build type: Release/RelWithDebInfo/Debug" FORCE)
endif()
# Keep frame pointers in Release so perf top is still useful.
# Enable AVX2 for SIMD protocol parsing (CPU supports it, verified via /proc/cpuinfo)
add_compile_options(-fno-omit-frame-pointer -msse4.2 -mavx2)

# ── Dependencies ──────────────────────────────────────────────────────
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

option(USE_JEMALLOC "Use jemalloc allocator" ON)

# 数据域 kv entry 是否走自研内存池 kv_pool，改由运行期配置项控制：
#   conf 里 `kv_pool on|off`（默认 on），kvstore.c 的 init_kvengine 按此注入。
# 池代码恒编译进来 —— 一个二进制即可覆盖 有池/无池 两形态，不再需要编译开关。

pkg_check_modules(URING    REQUIRED liburing)
if(USE_JEMALLOC)
    pkg_check_modules(JEMALLOC REQUIRED jemalloc)
    add_compile_definitions(USE_JEMALLOC)
endif()
pkg_check_modules(ZLIB     REQUIRED zlib)

# RDMA: libibverbs (pkg-config) + librdmacm (find_library — dev pkg not installed)
pkg_check_modules(IBVERBS  REQUIRED libibverbs)
find_path(RDMACM_INCLUDE_DIR rdma/rdma_cma.h)
find_library(RDMACM_LIBRARY rdmacm)
if(NOT RDMACM_INCLUDE_DIR OR NOT RDMACM_LIBRARY)
    message(FATAL_ERROR "librdmacm not found. Install librdmacm-dev or set RDMACM_INCLUDE_DIR/RDMACM_LIBRARY")
endif()
message(STATUS "RDMACM include: ${RDMACM_INCLUDE_DIR}")
message(STATUS "RDMACM library: ${RDMACM_LIBRARY}")

# ── NtyCo (third-party coroutine library) ─────────────────────────────
set(NTYCO_DIR  ${CMAKE_SOURCE_DIR}/NtyCo)
set(NTYCO_LIB  ${NTYCO_DIR}/libntyco.a)

add_custom_command(
    OUTPUT ${NTYCO_LIB}
    COMMAND make -C ${NTYCO_DIR}
    COMMENT "Building NtyCo"
)

add_custom_target(ntyco DEPENDS ${NTYCO_LIB})

# ── Source files ──────────────────────────────────────────────────────
set(UTIL_SRCS
    src/util/dynbuf.c
    src/util/kv_pool.c
)

set(CONFIG_SRCS
    src/config/config.c
)

set(PROTOCOL_SRCS
    src/protocol/protocol.c
)

set(CORE_SRCS
    src/core/kvstore.c
    src/core/kvstore_expire.c
    src/core/kvstore_config.c
    src/core/kvstore_dispatch.c
    src/core/kvstore_info.c
)

# 存储引擎：kv_engine.c 只是路由层（选哪个引擎）；其余四个文件各是一个独立
# 引擎 —— 自带节点类型、自己的增删改查、自己管自己节点的内存，彼此不可见。
# （新增引擎 = 加一个 .c/.h 实现 14 个操作 + 6 个访问器，再挂进这里）
set(ENGINE_SRCS
    src/engine/kv_engine.c
    src/engine/kv_hash.c
    src/engine/kv_array.c
    src/engine/kv_rbtree.c
    src/engine/kv_skiplist.c
)

set(NET_SRCS
    src/net/reactor.c
    src/net/proactor.c
    src/net/ntyco.c
    src/net/rbuf.c
)

set(PERSIST_SRCS
    src/persist/persistence.c
    src/persist/persist_rdb.c
    src/persist/persist_aof.c
)

set(REPL_SRCS
    src/transport/sendfile.c
    src/transport/rdma.c
    src/repl/repl_fullsync.c
    src/repl/repl_incr.c
    src/repl/repl_incr_master.c
    src/repl/repl_incr_slave.c
)

set(KVSTORE_SRCS
    ${UTIL_SRCS}
    ${CORE_SRCS}
    ${CONFIG_SRCS}
    ${PROTOCOL_SRCS}
    ${ENGINE_SRCS}
    ${NET_SRCS}
    ${PERSIST_SRCS}
    ${REPL_SRCS}
)

# ── 旁路镜像组件共用的编译工具 ─────────────────────────────────────────
# BPF 侧一律用 clang 编成 .bpf.o，用户态 loader 链 libbpf
set(BPF_CLANG clang)
set(UTIL_DIR    ${CMAKE_SOURCE_DIR}/src/util)   # 公共 vmlinux.h 单份存放于此

# ── repl-xdp（XDP 旁路镜像同步）──────────────────────────────────────────
# 独立进程，与 kvstore 零耦合：XDP 程序挂 kvstore master 客户端流量所经网卡
# （veth0）入方向，把客户端 → master(MASTER_PORT) 的 TCP 负载采样进 ring
# buffer，原包一律 XDP_PASS；用户态进程用内核 TCP socket 把采到的字节写往
# 从机（从机内核 TCP 栈负责接收可靠），主/从机的增量同步由镜像完成。
# 默认 MASTER_PORT/SLAVE_PORT 对齐 kvstore 主从端口（2000/2001），
# 可通过 -DMASTER_PORT=.. -DSLAVE_PORT=.. 覆盖。
set(REPL_XDP_DIR ${CMAKE_SOURCE_DIR}/src/repl-xdp)
set(REPL_XDP_DEFINES "" CACHE STRING
    "repl-xdp 端口/网卡覆盖，例: -DMASTER_PORT=6380 -DSLAVE_PORT=6381")

add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/repl-xdp.bpf.o
    COMMAND ${BPF_CLANG} -g -O2 -target bpf -D__TARGET_ARCH_x86 ${REPL_XDP_DEFINES}
            -I${REPL_XDP_DIR} -I${UTIL_DIR} -I/usr/include
            -c ${REPL_XDP_DIR}/repl-xdp-sock.bpf.c
            -o ${CMAKE_BINARY_DIR}/repl-xdp.bpf.o
    DEPENDS ${REPL_XDP_DIR}/repl-xdp-sock.bpf.c ${UTIL_DIR}/vmlinux.h
    COMMENT "Compiling repl-xdp.bpf.o (eBPF)"
)
add_custom_target(repl-xdp-bpf DEPENDS ${CMAKE_BINARY_DIR}/repl-xdp.bpf.o)

add_executable(repl-xdp ${REPL_XDP_DIR}/repl-xdp-sock.c)
target_include_directories(repl-xdp PRIVATE ${REPL_XDP_DIR})
target_compile_options(repl-xdp PRIVATE ${REPL_XDP_DEFINES})
target_link_libraries(repl-xdp PRIVATE bpf elf ${ZLIB_LIBRARIES})
add_dependencies(repl-xdp repl-xdp-bpf)

# ── repl-tcp_recvmsg（kprobe 读侧镜像同步）────────────────────────────────
# 独立进程，与 kvstore 零耦合：kprobe/kretprobe 挂 tcp_recvmsg_locked（TCP 读
# 路径统一收口，同时覆盖 reactor 的 recv 与 proactor 的 io_uring_prep_recv）。
# 三引擎读路径已统一为单段 recv（msg_iter 为 ITER_UBUF，一次 recv = 一条记录），
# 本组件入口即按此放行。
# 按 tgid 过滤目标 master 进程，把其每次读到的字节镜像进 ring；用户态用内核
# TCP socket 转发从机，从机当普通客户端命令应用。目标进程由固定端口 MASTER_PORT
# 反查（监听该端口的 socket inode → 属主进程），命令行零参数；.o 取可执行文件
# 同目录。见 src/repl-tcp_recvmsg/README.md。
set(REPL_TCP_DIR ${CMAKE_SOURCE_DIR}/src/repl-tcp_recvmsg)
set(REPL_TCP_DEFINES "" CACHE STRING
    "repl-tcp_recvmsg 覆盖，例: -DMAX_REC=4096")

add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/repl-tcp_recvmsg.bpf.o
    COMMAND ${BPF_CLANG} -g -O2 -target bpf -D__TARGET_ARCH_x86 ${REPL_TCP_DEFINES}
            -I${REPL_TCP_DIR} -I${UTIL_DIR} -I/usr/include
            -c ${REPL_TCP_DIR}/repl-tcp_recvmsg.bpf.c
            -o ${CMAKE_BINARY_DIR}/repl-tcp_recvmsg.bpf.o
    DEPENDS ${REPL_TCP_DIR}/repl-tcp_recvmsg.bpf.c ${UTIL_DIR}/vmlinux.h
    COMMENT "Compiling repl-tcp_recvmsg.bpf.o (eBPF)"
)
add_custom_target(repl-tcp_recvmsg-bpf DEPENDS ${CMAKE_BINARY_DIR}/repl-tcp_recvmsg.bpf.o)

add_executable(repl-tcp_recvmsg ${REPL_TCP_DIR}/repl-tcp_recvmsg.c)
target_include_directories(repl-tcp_recvmsg PRIVATE ${REPL_TCP_DIR})
target_compile_options(repl-tcp_recvmsg PRIVATE ${REPL_TCP_DEFINES})
target_link_libraries(repl-tcp_recvmsg PRIVATE bpf elf ${ZLIB_LIBRARIES})
add_dependencies(repl-tcp_recvmsg repl-tcp_recvmsg-bpf)

# ── kvstore target ────────────────────────────────────────────────────
add_executable(kvstore ${KVSTORE_SRCS})

target_include_directories(kvstore PRIVATE
    src/util
    src/core
    src/config
    src/protocol
    src/engine
    src/net
    src/persist
    src/repl
    src/transport
    ${NTYCO_DIR}/core
    ${URING_INCLUDE_DIRS}
    ${JEMALLOC_INCLUDE_DIRS}
    ${IBVERBS_INCLUDE_DIRS}
    ${RDMACM_INCLUDE_DIR}
)

target_link_libraries(kvstore PRIVATE
    pthread          # ① NtyCo 协程库传递依赖（pthread TLS）；② AOF 专用落盘线程（persist_aof.c）
    ${NTYCO_LIB}
    ${URING_LIBRARIES}
    ${JEMALLOC_LIBRARIES}
    ${IBVERBS_LIBRARIES}
    ${RDMACM_LIBRARY}
    ${ZLIB_LIBRARIES}
    dl
    nl-3
)

add_dependencies(kvstore ntyco)

# ── testcase（功能测试客户端）───────────────────────────────────────────────
# 纯 RESP 客户端，fill/verify/basic/pipeline 等测试用（与 kvstore 零耦合）：
# 编译同原 `gcc -O2 -o build/testcase test/testcase.c`，随默认构建一起产出。
add_executable(testcase ${CMAKE_SOURCE_DIR}/test/testcase.c)
target_compile_options(testcase PRIVATE -Wall -Wextra)

# ── engtest（引擎内存账本自检）─────────────────────────────────────────────
# 不连网络，直接把四个引擎各驱动一遍增删改查 + 变长重建，以 kv_pool 的账本
# 收尾必须回 0（live_objects / slab_count）当尺寸契约的哨兵。
# 挂进默认构建：它是唯一直接盯着「每个引擎自己管内存」是否守约的测试，
# 之前靠手工 gcc 编译，接口一变就悄悄失修（编译不过却没人发现）。
add_executable(engtest ${CMAKE_SOURCE_DIR}/test/engtest.c
               ${ENGINE_SRCS} src/util/kv_pool.c)
target_include_directories(engtest PRIVATE src/util src/engine)
target_compile_options(engtest PRIVATE -Wall -Wextra)

# ── xfer-*（传输速率测试组件）───────────────────────────────────────────────
# sendfile / rdma 两条路各一个自包含的 .c：喂同一个文件，只测"传输能跑多快"。
# 与 kvstore 零耦合（不链 jemalloc / io_uring / 引擎 / ntyco，不 include 项目
# 任何头文件），在这里量出来的结论再往 kvstore 上搬。见 src/xfer/README.md。
set(XFER_DIR ${CMAKE_SOURCE_DIR}/src/xfer)

add_executable(xfer-sendfile ${XFER_DIR}/xfer-sendfile.c)
target_compile_options(xfer-sendfile PRIVATE -Wall -Wextra)

add_executable(xfer-rdma ${XFER_DIR}/xfer-rdma.c)
target_include_directories(xfer-rdma PRIVATE
    ${IBVERBS_INCLUDE_DIRS}
    ${RDMACM_INCLUDE_DIR}
)
target_link_libraries(xfer-rdma PRIVATE
    ${IBVERBS_LIBRARIES}
    ${RDMACM_LIBRARY}
)
target_compile_options(xfer-rdma PRIVATE -Wall -Wextra)


