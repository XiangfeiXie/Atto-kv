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
pkg_check_modules(LIBBPF   REQUIRED libbpf)
pkg_check_modules(LIBXDP   REQUIRED libxdp)

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

set(TYPE_SRCS
    src/type/kv_type_string.c
)

set(NET_SRCS
    src/net/reactor.c
    src/net/proactor.c
    src/net/ntyco.c
)

set(PERSIST_SRCS
    src/persist/persistence.c
    src/persist/persist_rdb.c
    src/persist/persist_wal.c
)

set(REPL_SRCS
    src/repl/rdma_repl.c
    src/repl/rdma_repl_master.c
    src/repl/rdma_repl_slave.c
    src/repl/rdma_transport.c
    src/repl/tcp_repl_master.c
    src/repl/tcp_repl_slave.c
    src/repl-incr/repl_incr.c
    src/repl-incr/repl_incr_master.c
    src/repl-incr/repl_incr_slave.c
)

# Kernel-bypass (AF_XDP + KCP) engine-side sources.  shm_ipc.c is shared with
# the gateway binary; engine_xdp.c is the engine↔SHM glue.
set(XDPKCP_ENGINE_SRCS
    src/repl-incr-xdpkcp/engine_xdp.c
    src/repl-incr-xdpkcp/shm_ipc.c
)

set(KVSTORE_SRCS
    ${UTIL_SRCS}
    ${CORE_SRCS}
    ${CONFIG_SRCS}
    ${PROTOCOL_SRCS}
    ${ENGINE_SRCS}
    ${TYPE_SRCS}
    ${NET_SRCS}
    ${PERSIST_SRCS}
    ${REPL_SRCS}
    ${XDPKCP_ENGINE_SRCS}
)

# ── kvstore target ────────────────────────────────────────────────────
add_executable(kvstore ${KVSTORE_SRCS})

target_include_directories(kvstore PRIVATE
    src/util
    src/core
    src/config
    src/protocol
    src/engine
    src/type
    src/net
    src/persist
    src/repl
    src/repl-incr
    src/repl-incr-xdpkcp
    ${NTYCO_DIR}/core
    ${URING_INCLUDE_DIRS}
    ${JEMALLOC_INCLUDE_DIRS}
    ${IBVERBS_INCLUDE_DIRS}
    ${RDMACM_INCLUDE_DIR}
)

target_link_libraries(kvstore PRIVATE
    Threads::Threads
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

# ── AF_XDP + KCP gateway ──────────────────────────────────────────────
# Two binaries per machine: the engine (kvstore, above) and this gateway,
# which owns the busy-poll AF_XDP + KCP datapath and talks to the engine via
# /dev/shm.  The BPF program redirects UDP:sync_port into the XSK.

set(XDPKCP_BPF_SRC ${CMAKE_SOURCE_DIR}/src/repl-incr-xdpkcp/xdp_kcp.bpf.c)
set(XDPKCP_BPF_OBJ ${CMAKE_BINARY_DIR}/xdp_kcp.bpf.o)

add_custom_command(
    OUTPUT ${XDPKCP_BPF_OBJ}
    COMMAND clang -O2 -g -target bpf
            -I${CMAKE_SOURCE_DIR}/src/repl-incr-xdpkcp
            -I/usr/include/x86_64-linux-gnu
            -c ${XDPKCP_BPF_SRC}
            -o ${XDPKCP_BPF_OBJ}
    DEPENDS ${XDPKCP_BPF_SRC}
    COMMENT "Compiling xdp_kcp.bpf.c (XDP BPF)"
)
add_custom_target(xdp_kcp_bpf DEPENDS ${XDPKCP_BPF_OBJ})

set(XDPKCP_GW_SRCS
    src/repl-incr-xdpkcp/gateway.c
    src/repl-incr-xdpkcp/af_xdp.c
    src/repl-incr-xdpkcp/kcp_transport.c
    src/repl-incr-xdpkcp/shm_ipc.c
    src/repl-incr-xdpkcp/ikcp.c
)
add_executable(kvstore-xdp-gateway ${XDPKCP_GW_SRCS})
target_include_directories(kvstore-xdp-gateway PRIVATE
    src/repl-incr-xdpkcp
    ${LIBBPF_INCLUDE_DIRS}
    ${LIBXDP_INCLUDE_DIRS}
)
target_link_libraries(kvstore-xdp-gateway PRIVATE
    ${LIBXDP_LIBRARIES}
    ${LIBBPF_LIBRARIES}
    elf
)
add_dependencies(kvstore-xdp-gateway xdp_kcp_bpf)

# ── Set capabilities for eBPF (requires libcap) ───────────────────────
# This allows kvstore to load eBPF programs without running as root
find_program(SETCAP_EXECUTABLE setcap)
if(SETCAP_EXECUTABLE)
    # Note: Automatic capability setting requires passwordless sudo or manual intervention
    # To enable automatic setting, configure sudoers:
    #   echo "$USER ALL=(ALL) NOPASSWD: /usr/sbin/setcap" | sudo tee /etc/sudoers.d/setcap-kvstore
    # Or run manually after build:
    #   echo "1234" | sudo -S setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep build/kvstore
    message(STATUS "setcap found - you can set capabilities manually after build:")
    message(STATUS "  echo \"1234\" | sudo -S setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep build/kvstore")
else()
    message(WARNING "setcap not found - eBPF programs will require root privileges")
    message(WARNING "Install libcap2-bin: sudo apt-get install libcap2-bin")
endif()

# Add a custom target for setting capabilities
add_custom_target(set-caps
    COMMAND ${CMAKE_COMMAND} -E echo "Setting capabilities (requires sudo password)..."
    COMMAND bash -c "echo 1234 | sudo -S setcap cap_bpf,cap_perfmon,cap_net_admin,cap_sys_admin+ep ${CMAKE_BINARY_DIR}/kvstore"
    DEPENDS kvstore
    COMMENT "Setting eBPF capabilities on kvstore binary"
    VERBATIM
)


# ── Phase 8.1 — smoke / sanitizer-smoke targets ──────────────────────
# Run as:
#   cmake --build build --target smoke        (Release smoke 12 checks)
#   cmake --build build --target asan-smoke   (rebuilds ASAN if needed)
#   cmake --build build --target tsan-smoke   (TSAN focused lock smoke)
#   cmake --build build --target smoke-all    (the matrix, gates merges)
#
# Each target depends on the right binary so cmake rebuilds before
# running.  Exit non-zero on any FAIL; CI / hooks can simply run
# `cmake --build build --target smoke-all`.
add_custom_target(smoke
    COMMAND ${CMAKE_COMMAND} -E echo "-- Phase 4B smoke [Release] --"
    COMMAND ${CMAKE_SOURCE_DIR}/test/bench/multi_thread_smoke.sh
    DEPENDS kvstore
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    USES_TERMINAL
    COMMENT "Running Release multi_thread_smoke.sh"
)

# ASAN/TSAN builds live in separate build dirs so the targets here
# only invoke the smoke scripts; the sanitizer build is the user's
# responsibility (see test/bench/tsan.suppressions header for the
# canonical configure flags).  Both targets fail loudly if the
# expected binary is missing.
add_custom_target(asan-smoke
    COMMAND ${CMAKE_COMMAND} -E echo "-- Phase 4B smoke [ASAN] --"
    COMMAND ${CMAKE_COMMAND} -E env
            KVS=${CMAKE_SOURCE_DIR}/build-asan/kvstore
            ASAN_OPTIONS=halt_on_error=0:detect_leaks=1:abort_on_error=0
            ${CMAKE_SOURCE_DIR}/test/bench/multi_thread_smoke.sh
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    USES_TERMINAL
    COMMENT "Running ASAN multi_thread_smoke.sh"
)

add_custom_target(tsan-smoke
    COMMAND ${CMAKE_COMMAND} -E echo "-- Phase 8.2 TSAN focused lock smoke --"
    COMMAND ${CMAKE_COMMAND} -E env
            KVS=${CMAKE_SOURCE_DIR}/build-tsan/kvstore
            ${CMAKE_SOURCE_DIR}/test/bench/tsan_lock_smoke.sh
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    USES_TERMINAL
    COMMENT "Running TSAN lock smoke"
)

# Roll-up target — the merge gate.  Runs Release first (fastest, most
# coverage), then ASAN, then TSAN.  Each script exits non-zero on FAIL
# so cmake will halt the chain at the first failure.
add_custom_target(smoke-all
    DEPENDS smoke
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target asan-smoke
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target tsan-smoke
    COMMAND ${CMAKE_COMMAND} -E echo "OK smoke + asan-smoke + tsan-smoke all green"
    USES_TERMINAL
    COMMENT "Phase 8.1 — full sanitizer matrix smoke (gate for any locking change)"
)
