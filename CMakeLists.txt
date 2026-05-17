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

set(ENGINE_SRCS
    src/engine/kv_dict.c
)

set(TYPE_SRCS
    src/type/kv_type_string.c
    src/type/kv_type_list.c
    src/type/kv_type_set.c
    src/type/kv_type_hash.c
    src/type/kv_type_zset.c
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
)

set(BPF_SRCS
    src/bpf/xdp_manager.c
    src/bpf/sockmap_manager.c
    src/bpf/bpf_pin.c
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

add_dependencies(kvstore ntyco xdp_kern sockmap_kern)

# ── kvs_slowlog: uprobe slow-command monitor ──────────────────────────
set(SLOWLOG_BPF_SRC  ${CMAKE_SOURCE_DIR}/src/tools/kvs_slowlog.bpf.c)
set(SLOWLOG_BPF_OBJ  ${CMAKE_BINARY_DIR}/kvs_slowlog.bpf.o)

add_custom_command(
    OUTPUT  ${SLOWLOG_BPF_OBJ}
    COMMAND clang -O2 -g -target bpf
            -D__TARGET_ARCH_x86
            -I${CMAKE_SOURCE_DIR}/src/repl
            -I/usr/include
            -I/usr/include/x86_64-linux-gnu
            -c ${SLOWLOG_BPF_SRC}
            -o ${SLOWLOG_BPF_OBJ}
    DEPENDS ${SLOWLOG_BPF_SRC}
    COMMENT "Compiling kvs_slowlog BPF kernel program"
)

add_custom_target(kvs_slowlog_bpf DEPENDS ${SLOWLOG_BPF_OBJ})

add_executable(kvs_slowlog src/tools/kvs_slowlog.c)

target_include_directories(kvs_slowlog PRIVATE
    ${LIBBPF_INCLUDE_DIRS}
)

target_link_libraries(kvs_slowlog PRIVATE
    ${LIBBPF_LIBRARIES}
    elf
    dl
)

add_dependencies(kvs_slowlog kvs_slowlog_bpf)

# ── kvs-top: live three-layer observability dashboard ────────────────
add_executable(kvs_top
    src/tools/kvs_top.c
    src/bpf/bpf_pin.c
)

target_include_directories(kvs_top PRIVATE
    src/bpf
    ${LIBBPF_INCLUDE_DIRS}
)

target_link_libraries(kvs_top PRIVATE
    ${LIBBPF_LIBRARIES}
    elf
    dl
)

set_target_properties(kvs_top PROPERTIES OUTPUT_NAME "kvs-top")

# ── testcase target ───────────────────────────────────────────────────
add_executable(testcase test/testcase.c)
