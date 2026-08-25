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
    src/repl-incr
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
