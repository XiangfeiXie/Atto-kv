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
add_compile_options(-fno-omit-frame-pointer)

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

# ── XDP/AF_XDP support (Phase 11 - optional) ──────────────────────────
option(ENABLE_XDP_REPL "Enable XDP/AF_XDP zero-copy WAL replication" OFF)
if(ENABLE_XDP_REPL)
    pkg_check_modules(LIBXDP libxdp)
    add_compile_definitions(ENABLE_XDP_REPL)
    if(LIBXDP_FOUND)
        add_compile_definitions(HAVE_LIBXDP)
        message(STATUS "XDP/AF_XDP replication support enabled with libxdp")
    else()
        message(WARNING "libxdp not found; building staged AF_XDP replication manager without libxdp helpers")
    endif()
endif()

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
)

set(VECTOR_SRCS
    src/vector/vector_store.c
    src/vector/vector_math.c
)

set(STORAGE_SRCS
    src/storage/vec_engine.c
    src/storage/vec_collection.c
    src/storage/vec_segment.c
)

set(INDEX_SRCS
    src/index/flat_index.c
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
    src/net/proactor.c
    src/net/ntyco.c
)

set(PERSIST_SRCS
    src/persist/persistence.c
)

set(REPL_SRCS
    src/repl/rdma_repl.c
    src/repl/wal_repl.c
    src/repl/journal.c
    src/repl/wal_persist.c
    src/repl/repl_stream.c
    src/repl/xdp_repl_manager.c
)

set(BPF_SRCS
    src/bpf/xdp_manager.c
    src/bpf/sockmap_manager.c
    src/bpf/bpf_pin.c
)

# Replication XDP/AF_XDP sources (Phase 11 - conditional)
if(ENABLE_XDP_REPL AND LIBXDP_FOUND)
    list(APPEND REPL_SRCS
        src/repl/af_xdp_repl.c
        src/repl/xdp_wal_manager.c
    )
endif()

set(KVSTORE_SRCS
    ${UTIL_SRCS}
    ${CORE_SRCS}
    ${VECTOR_SRCS}
    ${STORAGE_SRCS}
    ${INDEX_SRCS}
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

add_custom_target(xdp_kern DEPENDS ${XDP_KERN_OBJ})

# ── Compile sockmap kernel BPF program ────────────────────────────────
set(SOCKMAP_KERN_SRC  ${CMAKE_SOURCE_DIR}/src/bpf/sockmap_kvs.kern.c)
set(SOCKMAP_KERN_OBJ  ${CMAKE_BINARY_DIR}/sockmap_kvs.kern.o)

add_custom_command(
    OUTPUT  ${SOCKMAP_KERN_OBJ}
    COMMAND clang -O2 -g -target bpf
            -I${CMAKE_SOURCE_DIR}/src/bpf
            -I/usr/include
            -I/usr/include/x86_64-linux-gnu
            -c ${SOCKMAP_KERN_SRC}
            -o ${SOCKMAP_KERN_OBJ}
    DEPENDS ${SOCKMAP_KERN_SRC} ${CMAKE_SOURCE_DIR}/src/bpf/sockmap_kvs.h
    COMMENT "Compiling sockmap BPF kernel program"
)

add_custom_target(sockmap_kern DEPENDS ${SOCKMAP_KERN_OBJ})

# ── Compile replication XDP kernel BPF program (Phase 11 - conditional) ─
if(ENABLE_XDP_REPL)
    set(WAL_XDP_KERN_SRC  ${CMAKE_SOURCE_DIR}/src/repl/xdp_repl.kern.c)
    set(WAL_XDP_KERN_OBJ  ${CMAKE_BINARY_DIR}/xdp_repl.kern.o)

    add_custom_command(
        OUTPUT  ${WAL_XDP_KERN_OBJ}
        COMMAND clang -O2 -g -target bpf
                -I${CMAKE_SOURCE_DIR}/src/bpf
                -I${CMAKE_SOURCE_DIR}/src/repl
                -I/usr/include
                -I/usr/include/x86_64-linux-gnu
                -c ${WAL_XDP_KERN_SRC}
                -o ${WAL_XDP_KERN_OBJ}
        DEPENDS ${WAL_XDP_KERN_SRC}
        COMMENT "Compiling replication XDP BPF kernel program"
    )

    add_custom_target(wal_xdp_kern DEPENDS ${WAL_XDP_KERN_OBJ})
endif()

# ── kvstore target ────────────────────────────────────────────────────
add_executable(kvstore ${KVSTORE_SRCS})

target_include_directories(kvstore PRIVATE
    src/util
    src/core
    src/engine
    src/type
    src/vector
    src/storage
    src/index
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

if(ENABLE_XDP_REPL AND LIBXDP_FOUND)
    target_include_directories(kvstore PRIVATE ${LIBXDP_INCLUDE_DIRS})
endif()

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

if(ENABLE_XDP_REPL AND LIBXDP_FOUND)
    target_link_libraries(kvstore PRIVATE ${LIBXDP_LIBRARIES})
endif()

add_dependencies(kvstore ntyco xdp_kern sockmap_kern)

if(ENABLE_XDP_REPL)
    add_dependencies(kvstore wal_xdp_kern)
endif()

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
