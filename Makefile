
CC    = gcc
CLANG = clang

FLAGS = -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -luring -ldl \
        -ljemalloc -lfabric -lpthread -lbpf -lelf -lz

SRCS = kvstore.c ntyco.c proactor.c kvs_array.c kvs_rbtree.c kvs_hash.c \
       kvs_skiptable.c reactor.c repl.c repl_net.c rdma_repl.c \
       persistence.c config.c ebpf_incr_repl.c

TESTCASE_SRCS = testcase.c
TARGET  = kvstore
SUBDIR  = ./NtyCo/
TESTCASE = testcase

BPF_OBJ = ebpf_incr.bpf.o

OBJS = $(SRCS:.c=.o)

all: $(SUBDIR) vmlinux.h $(BPF_OBJ) $(TARGET) $(TESTCASE)

$(SUBDIR): ECHO
	make -C $@

ECHO:
	@echo $(SUBDIR)

vmlinux.h:
	bpftool btf dump file /sys/kernel/btf/vmlinux format c > $@

$(BPF_OBJ): ebpf_incr.bpf.c vmlinux.h
	$(CLANG) -O2 -g -target bpf -D__TARGET_ARCH_x86 \
	         -I/usr/include/bpf -I. \
	         -c ebpf_incr.bpf.c -o $(BPF_OBJ)

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(FLAGS)

$(TESTCASE): $(TESTCASE_SRCS)
	$(CC) -o $@ $^

%.o: %.c
	$(CC) $(FLAGS) -c $^ -o $@

clean:
	rm -rf $(OBJS) $(TARGET) $(TESTCASE) $(BPF_OBJ)


