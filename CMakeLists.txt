cmake_minimum_required(VERSION 3.16)
project(kvstore C)

set(CMAKE_C_STANDARD 11)

# ── Dependencies ──────────────────────────────────────────────────────
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

pkg_check_modules(URING    REQUIRED liburing)
pkg_check_modules(JEMALLOC REQUIRED jemalloc)
pkg_check_modules(FABRIC   REQUIRED libfabric)
pkg_check_modules(BPF      REQUIRED libbpf)
pkg_check_modules(ELF      REQUIRED libelf)
pkg_check_modules(ZLIB     REQUIRED zlib)

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
    src/repl/ebpf_incr_repl.c
)

set(KVSTORE_SRCS
    ${UTIL_SRCS}
    ${CORE_SRCS}
    ${ENGINE_SRCS}
    ${TYPE_SRCS}
    ${NET_SRCS}
    ${PERSIST_SRCS}
    ${REPL_SRCS}
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
    ${NTYCO_DIR}/core
    ${URING_INCLUDE_DIRS}
    ${JEMALLOC_INCLUDE_DIRS}
    ${FABRIC_INCLUDE_DIRS}
    ${BPF_INCLUDE_DIRS}
)

target_link_libraries(kvstore PRIVATE
    Threads::Threads
    ${NTYCO_LIB}
    ${URING_LIBRARIES}
    ${JEMALLOC_LIBRARIES}
    ${FABRIC_LIBRARIES}
    ${BPF_LIBRARIES}
    ${ELF_LIBRARIES}
    ${ZLIB_LIBRARIES}
    dl
)

add_dependencies(kvstore ntyco)

# ── testcase target ───────────────────────────────────────────────────
add_executable(testcase test/testcase.c)

# ── eBPF object ───────────────────────────────────────────────────────
add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/ebpf_incr.bpf.o
    COMMAND clang -O2 -g -target bpf -D__TARGET_ARCH_x86
            -I/usr/include/bpf
            -I${CMAKE_SOURCE_DIR}/src/repl
            -c ${CMAKE_SOURCE_DIR}/src/repl/ebpf_incr.bpf.c
            -o ${CMAKE_BINARY_DIR}/ebpf_incr.bpf.o
    DEPENDS ${CMAKE_SOURCE_DIR}/src/repl/ebpf_incr.bpf.c
            ${CMAKE_SOURCE_DIR}/src/repl/vmlinux.h
    COMMENT "Compiling eBPF program"
)

add_custom_target(bpf ALL DEPENDS ${CMAKE_BINARY_DIR}/ebpf_incr.bpf.o)
add_dependencies(kvstore bpf)
