cmake_minimum_required(VERSION 3.16)
project(kvstore_incr_sync C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_BUILD_TYPE Release)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBBPF REQUIRED libbpf)

# ── BPF kernel program (XDP filter) ───────────────────────────────────

set(BPF_SRC  ${CMAKE_SOURCE_DIR}/xdp_filter.bpf.c)
set(BPF_OBJ  ${CMAKE_BINARY_DIR}/xdp_filter.bpf.o)

add_custom_command(
    OUTPUT  ${BPF_OBJ}
    COMMAND clang -O2 -g -target bpf
            -I${CMAKE_SOURCE_DIR}
            -I/usr/include/x86_64-linux-gnu
            -c ${BPF_SRC}
            -o ${BPF_OBJ}
    DEPENDS ${BPF_SRC} ${CMAKE_SOURCE_DIR}/xdp_common.h
    COMMENT "Compiling xdp_filter.bpf.c (XDP BPF)"
)

add_custom_target(xdp_filter_bpf DEPENDS ${BPF_OBJ})

# ── User-space daemon ─────────────────────────────────────────────────

set(SRCS
    main.c
    xdp_loader.c
    xdp_reasm.c
)

set(HEADERS
    config.h
    xdp_common.h
    xdp_loader.h
    xdp_reasm.h
)

add_executable(kvstore-incr-sync ${SRCS} ${HEADERS})
target_include_directories(kvstore-incr-sync PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${LIBBPF_INCLUDE_DIRS}
)
target_link_libraries(kvstore-incr-sync PRIVATE
    ${LIBBPF_LIBRARIES}
    elf
)
add_dependencies(kvstore-incr-sync xdp_filter_bpf)
