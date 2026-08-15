cmake_minimum_required(VERSION 3.16)
project(kvstore_incr_sync C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_BUILD_TYPE Release)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBBPF REQUIRED libbpf)

# ── BPF kernel program (XDP filter) ───────────────────────────────────

set(BPF_SRC  ${CMAKE_SOURCE_DIR}/xdp_filter.bpf.c)
set(BPF_OBJ  ${CMAKE_BINARY_DIR}/xdp_filter.bpf.o)

# Per-event ringbuf reserve payload.  Empty → use the default from
# xdp_common.h (XDP_MAX_PAYLOAD = 65536).  For a 1500-MTU native-XDP
# deployment set e.g. 4096 to hold ~8x more events:
#   cmake .. -DXDP_RESERVE_PAYLOAD=4096
set(XDP_RESERVE_PAYLOAD "" CACHE STRING
    "Ringbuf reserve payload bytes per event (empty = header default)")

set(BPF_DEFINES "")
if(XDP_RESERVE_PAYLOAD)
    list(APPEND BPF_DEFINES "-DXDP_RESERVE_PAYLOAD=${XDP_RESERVE_PAYLOAD}")
endif()

add_custom_command(
    OUTPUT  ${BPF_OBJ}
    COMMAND clang -O2 -g -target bpf
            ${BPF_DEFINES}
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
    stream.c
    replay.c
)

set(HEADERS
    config.h
    xdp_common.h
    xdp_loader.h
    stream.h
    replay.h
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
