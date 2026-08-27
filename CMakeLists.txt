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
    src/core/kvstore_info.c
    src/core/kvstore_dispatch.c
)

set(ENGINE_SRCS
    src/engine/kv_dict.c
)

set(NET_SRCS
    src/net/reactor.c
    src/net/proactor.c
    src/net/ntyco.c
)

set(PERSIST_SRCS
    src/persist/persistence.c
    src/persist/persist_rdb.c
    src/persist/persist_aof.c
)

set(REPL_SRCS
    src/transport/sendfile.c
    src/transport/rdma.c
    src/transport/rdma_transport.c
    src/repl/repl_fullsync.c
    src/repl/repl_backlog.c
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

# ── repl-recv（旁路 eBPF 增量同步）──────────────────────────────────────
# 独立进程，与 kvstore 零耦合：uprobe/uretprobe 挂 master 进程 libc `recv`
# （recv 挂载点，仅 reactor 引擎适用——proactor 的 io_uring recv 不经过 libc），
# 把镜像到的请求字节 TCP 原样转发从机，从机当普通客户端命令应用。BPF 侧
# 用 clang 编成 repl-recv.bpf.o，用户态 loader 链 libbpf。
set(BPF_CLANG clang)
set(REPL_RECV_DIR ${CMAKE_SOURCE_DIR}/src/repl-recv)

add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/repl-recv.bpf.o
    COMMAND ${BPF_CLANG} -g -O2 -target bpf -D__TARGET_ARCH_x86
            -I${REPL_RECV_DIR} -I/usr/include
            -c ${REPL_RECV_DIR}/repl-recv.bpf.c
            -o ${CMAKE_BINARY_DIR}/repl-recv.bpf.o
    DEPENDS ${REPL_RECV_DIR}/repl-recv.bpf.c ${REPL_RECV_DIR}/repl-recv.h
    COMMENT "Compiling repl-recv.bpf.o (eBPF)"
)
add_custom_target(repl-recv-bpf DEPENDS ${CMAKE_BINARY_DIR}/repl-recv.bpf.o)

add_executable(repl-recv ${REPL_RECV_DIR}/repl-recv.c)
target_include_directories(repl-recv PRIVATE ${REPL_RECV_DIR})
target_link_libraries(repl-recv PRIVATE Threads::Threads bpf elf ${ZLIB_LIBRARIES})
add_dependencies(repl-recv repl-recv-bpf)

# ── repl-xdp（XDP 旁路镜像同步）──────────────────────────────────────────
# 独立进程，与 kvstore 零耦合：XDP 程序挂客户端流量所经网卡（veth0）入方向，
# 把客户端 TCP 负载采样进 ring buffer（镜像数据），从机方向 ACK/应答拦进
# ring 并 DROP；用户态进程用 raw socket 重建镜像 TCP 段发往从机，从机内核
# TCP 栈负责接收可靠，本进程负责发送可靠（RTO 重传）。
set(REPL_XDP_DIR ${CMAKE_SOURCE_DIR}/src/repl-xdp)

add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/repl-xdp.bpf.o
    COMMAND ${BPF_CLANG} -g -O2 -target bpf -D__TARGET_ARCH_x86
            -I${REPL_XDP_DIR} -I/usr/include
            -c ${REPL_XDP_DIR}/repl-xdp.bpf.c
            -o ${CMAKE_BINARY_DIR}/repl-xdp.bpf.o
    DEPENDS ${REPL_XDP_DIR}/repl-xdp.bpf.c ${REPL_XDP_DIR}/repl-xdp.h
    COMMENT "Compiling repl-xdp.bpf.o (eBPF)"
)
add_custom_target(repl-xdp-bpf DEPENDS ${CMAKE_BINARY_DIR}/repl-xdp.bpf.o)

add_executable(repl-xdp ${REPL_XDP_DIR}/repl-xdp.c)
target_include_directories(repl-xdp PRIVATE ${REPL_XDP_DIR})
target_link_libraries(repl-xdp PRIVATE bpf elf ${ZLIB_LIBRARIES})
add_dependencies(repl-xdp repl-xdp-bpf)

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
    pthread          # NtyCo 协程库的传递依赖（libntyco.a 用 pthread TLS）；kvstore 本身单线程
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


