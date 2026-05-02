#include "repl.h"
#include "repl_net.h"
#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/time.h>

// 全局变量
int repl_role = REPL_MASTER;
int repl_listen_fd = -1;
pthread_t repl_thread;

char master_runid[40] = {0};
long long master_offset = 0;
SlaveInfo slaves[MAX_SLAVES];
int slave_count = 0;

char repl_backlog[REPL_BACKLOG_SIZE];
long long backlog_base = 0;
int backlog_len = 0;

char repl_master_ip[64] = {0};
int repl_master_port = 0;
int repl_master_fd = -1;
char master_runid_slave[40] = {0};
long long slave_offset = 0;

// 生成Redis格式的40位16进制runid（无uuid依赖）
static void repl_generate_runid(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    srand(tv.tv_sec ^ tv.tv_usec ^ getpid());
    
    for (int i = 0; i < 20; i++) {
        unsigned char r = rand() & 0xFF;
        sprintf(master_runid + i*2, "%02x", r);
    }
    master_runid[40] = '\0';
    printf("✅ 主节点 runId: %s\n", master_runid);
}

void repl_init(void) {
    memset(slaves, -1, sizeof(slaves));
    memset(repl_backlog, 0, sizeof(repl_backlog));
    slave_count = 0;
    master_offset = 0;
    backlog_base = 0;
    backlog_len = 0;

    if (repl_role == REPL_MASTER) {
        repl_generate_runid();
        pthread_create(&repl_thread, NULL, repl_master_thread, NULL);
    }
}

void repl_add_slave(int fd) {
    if (fd < 0 || slave_count >= MAX_SLAVES) return;
    for (int i = 0; i < slave_count; i++) {
        if (slaves[i].fd == fd) return;
    }
    slaves[slave_count].fd = fd;
    slaves[slave_count].offset = 0;
    slave_count++;
    printf("✅ 从机已连接: fd=%d 总数=%d\n", fd, slave_count);
}

// Redis标准环形积压缓冲区写入
void repl_feed_backlog(const char *cmd, int len) {
    if (len <= 0) return;
    
    // 计算写入位置
    int pos = backlog_len % REPL_BACKLOG_SIZE;
    int remain = REPL_BACKLOG_SIZE - pos;
    int write_len = (len > remain) ? remain : len;

    // 写入数据
    memcpy(repl_backlog + pos, cmd, write_len);
    if (len > remain) {
        memcpy(repl_backlog, cmd + write_len, len - write_len);
    }

    // 更新缓冲区信息
    backlog_len += len;
    if (backlog_len > REPL_BACKLOG_SIZE) {
        backlog_base += backlog_len - REPL_BACKLOG_SIZE;
        backlog_len = REPL_BACKLOG_SIZE;
    }
    master_offset += len;
}

void repl_propagate(const char *cmd, int len) {
    if (slave_count == 0 || len <= 0) return;
    for (int i = 0; i < slave_count; i++) {
        int fd = slaves[i].fd;
        if (fd < 0) continue;
        if (repl_net_send(fd, cmd, len) <= 0) {
            close(fd);
            slaves[i].fd = -1;
            printf("❌ 从机断开: fd=%d\n", fd);
        } else {
            slaves[i].offset += len;
        }
    }
}

// Redis标准：发送RDB全量同步 + 补发增量命令
static void repl_send_rdb_full_sync(int slave_fd) {
    long long old_offset = master_offset;
    rdb_save_sync();

    FILE *fp = fopen("dump.rdb", "rb");
    if (!fp) return;
    fseek(fp, 0, SEEK_END);
    long rdb_len = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    // 发送 +FULLRESYNC runid offset
    char reply[128];
    snprintf(reply, sizeof(reply), "+FULLRESYNC %s %lld\r\n", master_runid, master_offset);
    repl_net_send(slave_fd, reply, strlen(reply));

    // 发送 $rdb_len\r\n
    char head[64];
    snprintf(head, sizeof(head), "$%ld\r\n", rdb_len);
    repl_net_send(slave_fd, head, strlen(head));

    // 流式发送RDB文件
    char buf[4096];
    int n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        repl_net_send(slave_fd, buf, n);
    }
    fclose(fp);
    printf("✅ RDB 全量发送完成，大小: %ld bytes\n", rdb_len);

    // 补发RDB生成期间的增量命令（Redis核心逻辑）
    long long missing = master_offset - old_offset;
    if (missing > 0 && missing <= REPL_BACKLOG_SIZE) {
        int start = (backlog_len - missing) % REPL_BACKLOG_SIZE;
        if (start + missing <= REPL_BACKLOG_SIZE) {
            repl_net_send(slave_fd, repl_backlog + start, missing);
        } else {
            int part1 = REPL_BACKLOG_SIZE - start;
            repl_net_send(slave_fd, repl_backlog + start, part1);
            repl_net_send(slave_fd, repl_backlog, missing - part1);
        }
        printf("✅ 增量命令补发完成: %lld bytes\n", missing);
    }
}

// 处理PSYNC命令（Redis标准逻辑）
void repl_handle_psync(int slave_fd, char **tokens, DynBuf *wb) {
    char *runid = tokens[1];
    long long offset = atoll(tokens[2]);

    // 首次同步：PSYNC ? -1
    if (strcmp(runid, "?") == 0 && offset == -1) {
        repl_send_rdb_full_sync(slave_fd);
        return;
    }

    // 断线重连：检查runid和offset是否在积压缓冲区
    if (strcmp(runid, master_runid) == 0 && offset >= backlog_base) {
        char reply[128];
        snprintf(reply, sizeof(reply), "+CONTINUE\r\n");
        dynbuf_append(wb, reply, strlen(reply));
        repl_net_send(slave_fd, reply, strlen(reply));
        printf("✅ 断线重连，增量同步成功\n");
    } else {
        // offset不在积压缓冲区，执行全量同步
        repl_send_rdb_full_sync(slave_fd);
    }
}

void repl_handle_replconf(char **tokens, DynBuf *wb) {
    if (strcmp(tokens[1], "ACK") == 0) {
        long long offset = atoll(tokens[2]);
        printf("❤️ 心跳 ACK offset: %lld\n", offset);
        dynbuf_append(wb, "+OK\r\n", 5);
    }
}

// 主机复制线程
void* repl_master_thread(void *arg) {
    repl_listen_fd = repl_net_init(kvs_config.replport);
    printf("✅ 主机复制线程启动 端口=%d\n", kvs_config.replport);

    while (1) {
        int slave_fd = repl_net_accept(repl_listen_fd);
        if (slave_fd < 0) continue;
        repl_add_slave(slave_fd);

        char buf[4096];
        DynBuf wb;
        dynbuf_init(&wb, BUF_INIT_CAP);

        while (1) {
            int n = repl_net_recv(slave_fd, buf, sizeof(buf));
            if (n <= 0) break;

            char *tokens[KVS_MAX_TOKENS] = {0};
            resp_unpack(buf, n, tokens);

            if (strcmp(tokens[0], "PSYNC") == 0) {
                repl_handle_psync(slave_fd, tokens, &wb);
            } else if (strcmp(tokens[0], "REPLCONF") == 0) {
                repl_handle_replconf(tokens, &wb);
                repl_net_send(slave_fd, wb.buf, wb.len);
            }

            free_tokens(tokens, KVS_MAX_TOKENS);
            dynbuf_clear(&wb);
        }
        dynbuf_free(&wb);
        close(slave_fd);
    }
    return NULL;
}

// 从机复制线程
void* repl_slave_thread(void *arg) {
    repl_master_fd = repl_net_connect(repl_master_ip, kvs_config.replport);
    if (repl_master_fd < 0) {
        printf("❌ 连接主机失败\n");
        repl_switch_to_master();
        return NULL;
    }

    // 动态缓冲区（解决长文本）
    DynBuf recv_buf;
    dynbuf_init(&recv_buf, 4096);
    int r_off = 0;
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);

    // 1. 发送PSYNC ? -1
    char psync_cmd[128];
    snprintf(psync_cmd, sizeof(psync_cmd), "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
    repl_net_send(repl_master_fd, psync_cmd, strlen(psync_cmd));
    printf("✅ 已发送PSYNC全量同步请求\n");

    // 2. 解析响应（+FULLRESYNC 或 +CONTINUE）
    char resp_header[128] = {0};
    int header_idx = 0;
    while (1) {
        int n = repl_net_recv(repl_master_fd, resp_header + header_idx, 1);
        if (n <= 0) break;
        header_idx++;
        if (header_idx >= 2 && resp_header[header_idx-1] == '\n' && resp_header[header_idx-2] == '\r') break;
    }

    // 3. 如果是FULLRESYNC，接收RDB
    if (strstr(resp_header, "+FULLRESYNC") != NULL) {
        // 解析RDB头
        char rdb_header[64] = {0};
        int rdb_idx = 0;
        while (1) {
            int n = repl_net_recv(repl_master_fd, rdb_header + rdb_idx, 1);
            if (n <= 0) break;
            rdb_idx++;
            if (rdb_idx >= 2 && rdb_header[rdb_idx-1] == '\n' && rdb_header[rdb_idx-2] == '\r') break;
        }

        long long rdb_total_len = atoll(rdb_header + 1);
        printf("✅ 从机接收RDB大小: %lld\n", rdb_total_len);

        FILE *rdb_fp = fopen("dump.rdb", "wb");
        long long recv_len = 0;
        char buf[4096];
        while (recv_len < rdb_total_len) {
            int n = repl_net_recv(repl_master_fd, buf, sizeof(buf));
            if (n <= 0) break;
            fwrite(buf, 1, n, rdb_fp);
            recv_len += n;
        }
        fclose(rdb_fp);

        // 加载RDB
        kvs_flush_all();
        rdb_load_mmap();
        printf("✅ 全量同步完成，进入增量同步\n");
    }

    // 4. 增量同步（支持长文本）
    while (1) {
        // 动态扩容
        if (recv_buf.len + 4096 > recv_buf.cap) {
            char *new_buf = realloc(recv_buf.buf, recv_buf.cap * 2);
            if (!new_buf) break;
            recv_buf.buf = new_buf;
            recv_buf.cap *= 2;
        }

        // 读取数据
        int n = repl_net_recv(repl_master_fd, recv_buf.buf + recv_buf.len, recv_buf.cap - recv_buf.len);
        if (n <= 0) {
            printf("❌ 与主机断开连接\n");
            close(repl_master_fd);
            repl_master_fd = -1;
            break;
        }
        recv_buf.len += n;

        /* 流式解析完整命令（偏移指针版，无 memmove） */
        char *p = recv_buf.buf + r_off;
        int remaining = recv_buf.len;
        while (remaining > 0) {
            int cmd_len = resp_try_parse_full_cmd(p, remaining);
            if (cmd_len <= 0) break;
            dynbuf_clear(&wb);
            kvs_protocol(p, cmd_len, &wb);
            slave_offset += cmd_len;
            p += cmd_len;
            remaining -= cmd_len;
            r_off += cmd_len;
        }
        recv_buf.len = remaining;
        if (recv_buf.len == 0) r_off = 0;

        // 定时发送心跳ACK
        static long long last_ack = 0;
        if (master_offset - last_ack > REPL_PING_INTERVAL) {
            char offset_str[32];
            snprintf(offset_str, sizeof(offset_str), "%lld", slave_offset);
            char ack[128];
            snprintf(ack, sizeof(ack), "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%zu\r\n%s\r\n",
                     strlen(offset_str), offset_str);
            repl_net_send(repl_master_fd, ack, strlen(ack));
            last_ack = master_offset;
        }

        dynbuf_clear(&wb);
    }

    dynbuf_free(&recv_buf);
    dynbuf_free(&wb);
    return NULL;
}

void repl_switch_to_master(void) {
    if (repl_role == REPL_MASTER) return;
    if (repl_master_fd >= 0) { close(repl_master_fd); repl_master_fd = -1; }
    repl_role = REPL_MASTER;
    repl_init();
    printf("🔄 已切换 → 主节点\n");
}

void repl_switch_to_slave(const char *ip, int port) {
    if (repl_role == REPL_SLAVE) return;
    if (repl_listen_fd >= 0) { close(repl_listen_fd); repl_listen_fd = -1; }
    strncpy(repl_master_ip, ip, sizeof(repl_master_ip)-1);
    repl_master_port = port;
    repl_role = REPL_SLAVE;
    pthread_create(&repl_thread, NULL, repl_slave_thread, NULL);
    printf("🔄 已切换 → 从节点 主机: %s:%d\n", ip, port);
}