CLANG ?= clang
CC    ?= gcc

BPF_CFLAGS = -target bpf -D__TARGET_ARCH_x86 -g -O2 -Wall

all: repl-recv repl-recv.bpf.o

# 从本机 BTF 生成 vmlinux.h（struct pt_regs 等类型）
vmlinux.h:
	bpftool btf dump file /sys/kernel/btf/vmlinux format c > $@

repl-recv.bpf.o: repl-recv.bpf.c repl-recv.h vmlinux.h
	$(CLANG) $(BPF_CFLAGS) -c repl-recv.bpf.c -o $@

repl-recv: repl-recv.c repl-recv.h
	$(CC) -O2 -Wall -o $@ repl-recv.c -lbpf -lelf -lz

clean:
	rm -f repl-recv repl-recv.bpf.o vmlinux.h

.PHONY: all clean
