/* kvs_slowlog.c — standalone uprobe slow-command monitor for kvstore
 *
 * Attaches to a RUNNING kvstore process without modifying its source.
 * Reports every kvs_dispatch() call that exceeds the latency threshold.
 *
 * Usage:
 *   kvs_slowlog <pid> [threshold_ms]
 *
 * Examples:
 *   kvs_slowlog $(pgrep kvstore) 1        # report commands slower than 1ms
 *   kvs_slowlog 12345 0.5                 # report commands slower than 0.5ms
 *
 * Requires:
 *   - kvs_slowlog.bpf.o in the same directory as this binary
 *   - CAP_BPF or root (for uprobe attachment)
 *   - kvstore binary NOT fully stripped (needs .symtab for kvs_dispatch)
 *
 * Output format:
 *   [HH:MM:SS.mmm] latency=X.XXXms  cmd=<RESP fragment>
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <libgen.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>

#define MAX_CMD_CAPTURE 128

/* Must match kvs_slowlog.bpf.c */
struct slow_event {
    uint64_t latency_ns;
    uint32_t cmd_len;
    uint8_t  cmd[MAX_CMD_CAPTURE];
};

static volatile int g_stop = 0;

static void sig_handler(int sig) { (void)sig; g_stop = 1; }

/* Suppress libbpf log noise unless KVS_SLOWLOG_DEBUG=1 is set */
static int libbpf_print(enum libbpf_print_level level,
                        const char *fmt, va_list args)
{
    if (level == LIBBPF_DEBUG && !getenv("KVS_SLOWLOG_DEBUG"))
        return 0;
    return vfprintf(stderr, fmt, args);
}

static int handle_event(void *ctx, void *data, size_t sz)
{
    (void)ctx; (void)sz;
    const struct slow_event *e = data;

    /* Timestamp */
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm *tm = localtime(&ts.tv_sec);
    char tsbuf[16];
    strftime(tsbuf, sizeof(tsbuf), "%H:%M:%S", tm);

    /* Render RESP fragment: replace non-printable bytes with '.' to keep
     * the output on a single line; \r\n separators become visible dots. */
    char display[MAX_CMD_CAPTURE + 1];
    uint32_t dlen = e->cmd_len < MAX_CMD_CAPTURE ? e->cmd_len : MAX_CMD_CAPTURE;
    for (uint32_t i = 0; i < dlen; i++) {
        uint8_t c = e->cmd[i];
        display[i] = (c >= 0x20 && c < 0x7f) ? (char)c : '.';
    }
    display[dlen] = '\0';

    printf("[%s.%03ld] latency=%7.3fms  cmd=%s\n",
           tsbuf,
           ts.tv_nsec / 1000000L,
           (double)e->latency_ns / 1e6,
           display);
    fflush(stdout);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr,
            "Usage: %s <kvstore_pid> [threshold_ms]\n"
            "  threshold_ms defaults to 1.0\n"
            "  Set KVS_SLOWLOG_DEBUG=1 for verbose libbpf output\n",
            argv[0]);
        return 1;
    }

    pid_t   target_pid    = (pid_t)atoi(argv[1]);
    double  threshold_ms  = argc >= 3 ? atof(argv[2]) : 1.0;
    uint64_t threshold_ns = (uint64_t)(threshold_ms * 1e6);

    /* Resolve target binary path through /proc/pid/exe */
    char bin_path[512];
    ssize_t bin_len = readlink("/proc/self/exe", bin_path, sizeof(bin_path) - 1);
    if (bin_len < 0) {
        perror("readlink /proc/self/exe");
        return 1;
    }
    bin_path[bin_len] = '\0';

    /* BPF object lives next to this binary */
    char bpf_obj_path[512];
    char *dir = dirname(bin_path);  /* dirname may modify bin_path */
    snprintf(bpf_obj_path, sizeof(bpf_obj_path), "%s/kvs_slowlog.bpf.o", dir);

    /* Re-read the target binary path after dirname modified bin_path */
    char target_bin[512];
    snprintf(target_bin, sizeof(target_bin), "/proc/%d/exe", target_pid);

    /* Verify target process exists */
    if (access(target_bin, F_OK) != 0) {
        fprintf(stderr, "Process %d not found (or no permission)\n", target_pid);
        return 1;
    }
    if (access(bpf_obj_path, R_OK) != 0) {
        fprintf(stderr, "BPF object not found: %s\n", bpf_obj_path);
        fprintf(stderr, "Build it with: make kvs_slowlog_bpf\n");
        return 1;
    }

    libbpf_set_print(libbpf_print);

    /* ── Load BPF object ── */
    struct bpf_object *obj = bpf_object__open(bpf_obj_path);
    if (!obj || libbpf_get_error(obj)) {
        fprintf(stderr, "bpf_object__open(%s): %s\n",
                bpf_obj_path, strerror(errno));
        return 1;
    }

    if (bpf_object__load(obj) != 0) {
        fprintf(stderr, "bpf_object__load failed: %s\n", strerror(errno));
        bpf_object__close(obj);
        return 1;
    }

    /* ── Configure threshold ── */
    struct bpf_map *cfg = bpf_object__find_map_by_name(obj, "cfg_map");
    if (cfg) {
        uint32_t key = 0;
        bpf_map_update_elem(bpf_map__fd(cfg), &key, &threshold_ns, BPF_ANY);
    }

    /* ── Find BPF programs ── */
    struct bpf_program *enter_prog =
        bpf_object__find_program_by_name(obj, "on_dispatch_enter");
    struct bpf_program *exit_prog  =
        bpf_object__find_program_by_name(obj, "on_dispatch_exit");

    if (!enter_prog || !exit_prog) {
        fprintf(stderr, "BPF programs not found in object\n");
        bpf_object__close(obj);
        return 1;
    }

    /* ── Attach uprobes to target process ── */
    LIBBPF_OPTS(bpf_uprobe_opts, enter_opts,
        .func_name = "kvs_dispatch",
        .retprobe  = false,
    );
    LIBBPF_OPTS(bpf_uprobe_opts, exit_opts,
        .func_name = "kvs_dispatch",
        .retprobe  = true,
    );

    struct bpf_link *enter_link =
        bpf_program__attach_uprobe_opts(enter_prog, target_pid,
                                        target_bin, 0, &enter_opts);
    struct bpf_link *exit_link =
        bpf_program__attach_uprobe_opts(exit_prog, target_pid,
                                        target_bin, 0, &exit_opts);

    if (!enter_link || libbpf_get_error(enter_link) ||
        !exit_link  || libbpf_get_error(exit_link)) {
        fprintf(stderr, "Failed to attach uprobes: %s\n"
                        "  Is the process running? Does the binary have .symtab?\n"
                        "  Try: nm %s | grep kvs_dispatch\n",
                strerror(errno), target_bin);
        goto cleanup;
    }

    /* ── Set up ring buffer consumer ── */
    struct bpf_map *rb_map = bpf_object__find_map_by_name(obj, "slow_rb");
    if (!rb_map) {
        fprintf(stderr, "Ring buffer map 'slow_rb' not found\n");
        goto cleanup;
    }

    struct ring_buffer *rb = ring_buffer__new(bpf_map__fd(rb_map),
                                              handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "ring_buffer__new: %s\n", strerror(errno));
        goto cleanup;
    }

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);

    printf("[kvs_slowlog] pid=%d  threshold=%.3fms  bpf=%s\n",
           target_pid, threshold_ms, bpf_obj_path);
    printf("[kvs_slowlog] monitoring started — Ctrl+C to stop\n");
    printf("%-15s  %-14s  %s\n", "time", "latency", "command (RESP fragment)");
    printf("%-15s  %-14s  %s\n", "---------------", "--------------",
           "-------------------------------------------------------");

    while (!g_stop) {
        int err = ring_buffer__poll(rb, 100 /* ms */);
        if (err < 0 && err != -EINTR) {
            fprintf(stderr, "ring_buffer__poll: %d\n", err);
            break;
        }
    }

    printf("\n[kvs_slowlog] detaching uprobes...\n");
    ring_buffer__free(rb);

cleanup:
    if (enter_link) bpf_link__destroy(enter_link);
    if (exit_link)  bpf_link__destroy(exit_link);
    bpf_object__close(obj);
    printf("[kvs_slowlog] done\n");
    return 0;
}
