cmake_minimum_required(VERSION 3.16)
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
pkg_check_modules(FABRIC   REQUIRED libfabric)
pkg_check_modules(ZLIB     REQUIRED zlib)
pkg_check_modules(LIBBPF  REQUIRED libbpf)

# ── AF_XDP and KCP removed (replaced by TCP replication) ──────────────
# Phase 9.1 — Simplified replication using TCP + eBPF
# Removed dependencies: libxdp, KCP library
# Kept: eBPF filter for write command capture

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

set(CORE_SRCS
    src/core/kvstore.c
    src/core/config.c
    src/core/protocol.c
    src/core/block.c
    src/core/shared.c
)

set(ENGINE_SRCS
    src/engine/kv_dict.c
)

set(TYPE_SRCS
    src/type/kv_type_string.c
    src/type/kv_type_hash.c
)

set(NET_SRCS
    src/net/reactor.c
    src/net/proactor_ring.c
    src/net/proactor_conn.c
    src/net/proactor_event.c
    src/net/proactor_main.c
    src/net/ntyco.c
)

set(PERSIST_SRCS
    src/persist/persistence.c
    src/persist/journal.c
    src/persist/wal_persist.c
)

set(REPL_SRCS
    src/repl/rdma_repl.c
    src/repl/repl_stream.c
    src/repl/repl_backlog.c
    src/repl/repl_tcp.c
    src/repl/repl_incr.c
)

# BPF sources disabled for single-threaded build
set(BPF_SRCS
    # src/bpf/xdp_manager.c
    # src/bpf/sockmap_manager.c
    # src/bpf/bpf_pin.c
)

set(KVSTORE_SRCS
    ${UTIL_SRCS}
    ${CORE_SRCS}
    ${ENGINE_SRCS}
    ${TYPE_SRCS}
    ${NET_SRCS}
    ${PERSIST_SRCS}
    ${REPL_SRCS}
    ${BPF_SRCS}
)

# ── Compile XDP kernel BPF program ────────────────────────────────────
set(XDP_KERN_SRC  ${CMAKE_SOURCE_DIR}/src/bpf/xdp_kvs.kern.c)
set(XDP_KERN_OBJ  ${CMAKE_BINARY_DIR}/xdp_kvs.kern.o)

add_custom_command(
    OUTPUT  ${XDP_KERN_OBJ}
    COMMAND clang -O2 -g -target bpf
            -I${CMAKE_SOURCE_DIR}/src/bpf
            -I/usr/include
            -I/usr/include/x86_64-linux-gnu
            -c ${XDP_KERN_SRC}
            -o ${XDP_KERN_OBJ}
    DEPENDS ${XDP_KERN_SRC} ${CMAKE_SOURCE_DIR}/src/bpf/xdp_kvs.h
    COMMENT "Compiling XDP BPF kernel program"
)

# ── BPF kernel programs disabled for single-threaded build ────────────
# add_custom_target(xdp_kern DEPENDS ${XDP_KERN_OBJ})
# add_custom_target(sockmap_kern DEPENDS ${SOCKMAP_KERN_OBJ})

# ── Compile repl-agent BPF programs ───────────────────────────────────
# repl_observe.bpf.o — socket-layer observation for replication
set(REPL_OBSERVE_KERN_SRC ${CMAKE_SOURCE_DIR}/src/repl/repl_observe.bpf.c)
set(REPL_OBSERVE_KERN_OBJ ${CMAKE_BINARY_DIR}/repl_observe.bpf.o)

add_custom_command(
    OUTPUT  ${REPL_OBSERVE_KERN_OBJ}
    COMMAND clang -O2 -g -target bpf
            -I${CMAKE_SOURCE_DIR}/src/repl
            -I/usr/include
            -I/usr/include/x86_64-linux-gnu
            -c ${REPL_OBSERVE_KERN_SRC}
            -o ${REPL_OBSERVE_KERN_OBJ}
    DEPENDS ${REPL_OBSERVE_KERN_SRC} ${CMAKE_SOURCE_DIR}/src/repl/repl_observe.h
    COMMENT "Compiling repl_observe BPF program"
)

add_custom_target(repl_observe_kern DEPENDS ${REPL_OBSERVE_KERN_OBJ})

# ── Compile repl_filter BPF program (smart filtering) ──────────────────
set(REPL_FILTER_KERN_SRC ${CMAKE_SOURCE_DIR}/src/repl/repl_filter.bpf.c)
set(REPL_FILTER_KERN_OBJ ${CMAKE_BINARY_DIR}/repl_filter.bpf.o)

add_custom_command(
    OUTPUT  ${REPL_FILTER_KERN_OBJ}
    COMMAND clang -O2 -g -target bpf
            -I${CMAKE_SOURCE_DIR}/src/repl
            -I/usr/include
            -I/usr/include/x86_64-linux-gnu
            -c ${REPL_FILTER_KERN_SRC}
            -o ${REPL_FILTER_KERN_OBJ}
    DEPENDS ${REPL_FILTER_KERN_SRC}
    COMMENT "Compiling repl_filter.bpf.c"
)

add_custom_target(repl_filter_kern DEPENDS ${REPL_FILTER_KERN_OBJ})

# ── XDP KCP filter removed (no longer needed with TCP) ─────────────────

# ── Old XDP replication removed (replaced by KCP+AF_XDP) ─────────────

# ── kvstore target ────────────────────────────────────────────────────
add_executable(kvstore ${KVSTORE_SRCS})

target_include_directories(kvstore PRIVATE
    src/util
    src/core
    src/engine
    src/type
    src/net
    src/persist
    src/repl
    src/bpf
    ${NTYCO_DIR}/core
    ${URING_INCLUDE_DIRS}
    ${JEMALLOC_INCLUDE_DIRS}
    ${FABRIC_INCLUDE_DIRS}
    ${LIBBPF_INCLUDE_DIRS}
)

target_link_libraries(kvstore PRIVATE
    Threads::Threads
    ${NTYCO_LIB}
    ${URING_LIBRARIES}
    ${JEMALLOC_LIBRARIES}
    ${FABRIC_LIBRARIES}
    ${ZLIB_LIBRARIES}
    ${LIBBPF_LIBRARIES}
    elf
    dl
)

if(ENABLE_AF_XDP)
    target_link_libraries(kvstore PRIVATE ${LIBXDP_LIBRARIES})
    target_include_directories(kvstore PRIVATE ${LIBXDP_INCLUDE_DIRS})
endif()

add_dependencies(kvstore ntyco repl_filter_kern)

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

# ── repl-agent target (deprecated - replaced by KCP+XDP) ──────────────
# Commented out: old independent replication agent
# set(REPL_AGENT_SRCS
#     src/repl/repl_agent.c
# )
#
# add_executable(repl-agent ${REPL_AGENT_SRCS})
#
# target_include_directories(repl-agent PRIVATE
#     src/repl
#     ${LIBBPF_INCLUDE_DIRS}
# )
#
# target_link_libraries(repl-agent PRIVATE
#     Threads::Threads
#     ${LIBBPF_LIBRARIES}
#     elf
#     dl
# )
#
# add_dependencies(repl-agent repl_observe_kern)

# ── testcase target ───────────────────────────────────────────────────
add_executable(testcase test/testcase.c)

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
