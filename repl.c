#include "repl.h"
#include "repl_net.h"
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

// 全局复制变量
int repl_role = REPL_MASTER;
int repl_listen_fd = -1;
pthread_t repl_thread;
char repl_master_ip[64] = {0};
int repl_master_port = 0;
int repl_master_fd = -1;

char repl_backlog[REPL_BACKLOG_SIZE];
long long repl_offset = 0;
long long backlog_start_offset = 0;
int backlog_len = 0;
int slave_fds[MAX_SLAVES];
int slave_count = 0;

// ------------------------------
// 初始化：自动启动复制网络线程
// ------------------------------
void repl_init(void) {
    memset(slave_fds, -1, sizeof(slave_fds));
    memset(repl_backlog, 0, sizeof(repl_backlog));
    repl_offset = 0;
    backlog_start_offset = 0;
    backlog_len = 0;
    slave_count = 0;

    // 主机：启动复制监听线程
    if (repl_role == REPL_MASTER) {
        pthread_create(&repl_thread, NULL, repl_master_thread, NULL);
    }
}

// ------------------------------
// 主机：复制网络独立线程（6380端口）
// ------------------------------
void* repl_master_thread(void *arg) {
    repl_listen_fd = repl_net_init(REPL_LISTEN_PORT);
    printf("✅ 主机复制线程启动，监听端口: %d\n", REPL_LISTEN_PORT);

    while (1) {
        int slave_fd = repl_net_accept(repl_listen_fd);
        if (slave_fd < 0) continue;

        repl_add_slave(slave_fd);
        printf("✅ 从机连接成功，开始发送RDB全量同步\n");

        // 发送RDB全量数据（复制专用链路）
        rdb_save_async();
        waitpid(-1, NULL, WNOHANG);

        FILE *fp = fopen("dump.rdb", "rb");
        if (!fp) continue;
        fseek(fp, 0, SEEK_END);
        long rdb_len = ftell(fp);
        fseek(fp, 0, SEEK_SET);

        // 发送RDB协议头
        char head[64];
        sprintf(head, "$%ld\r\n", rdb_len);
        repl_net_send(slave_fd, head, strlen(head));

        // 发送RDB文件
        char buf[4096];
        int n;
        while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
            repl_net_send(slave_fd, buf, n);
        }
        fclose(fp);
        printf("✅ RDB全量同步完成\n");
    }
    return NULL;
}

// ------------------------------
// 从机：复制网络线程（接收RDB+命令）
// ------------------------------
void* repl_slave_thread(void *arg) {
    // 连接主机复制端口（6380）
    repl_master_fd = repl_net_connect(repl_master_ip, REPL_LISTEN_PORT);
    if (repl_master_fd < 0) {
        printf("❌ 从机连接主机复制网络失败\n");
        repl_switch_to_master();
        return NULL;
    }

    char buf[4096];
    DynBuf wb;
    dynbuf_init(&wb, BUF_INIT_CAP);

    // 1. 解析RDB头
    char rdb_header[64] = {0};
    int header_idx = 0;
    while (1) {
        int n = repl_net_recv(repl_master_fd, rdb_header + header_idx, 1);
        if (n <= 0) break;
        header_idx++;
        if (header_idx >= 2 && rdb_header[header_idx-1] == '\n' && rdb_header[header_idx-2] == '\r') break;
    }

    // 2. 接收并写入RDB
    long long rdb_total_len = atoll(rdb_header + 1);
    printf("✅ 从机开始接收RDB，大小: %lld\n", rdb_total_len);

    FILE *rdb_fp = fopen("dump.rdb", "wb");
    long long recv_len = 0;
    while (recv_len < rdb_total_len) {
        int n = repl_net_recv(repl_master_fd, buf, sizeof(buf));
        if (n <= 0) break;
        fwrite(buf, 1, n, rdb_fp);
        recv_len += n;
    }
    fclose(rdb_fp);

    // 3. 加载RDB恢复数据
    kvs_flush_all();
    rdb_load();
    printf("✅ 从机全量同步完成，进入增量同步\n");

    // 4. 增量命令同步
    while (1) {
        int n = repl_net_recv(repl_master_fd, buf, sizeof(buf));
        if (n <= 0) {
            printf("❌ 与主机断开连接\n");
            close(repl_master_fd);
            repl_master_fd = -1;
            break;
        }
        kvs_protocol(buf, n, &wb);
        dynbuf_clear(&wb);
    }

    dynbuf_free(&wb);
    return NULL;
}

// ------------------------------
// 通用工具函数
// ------------------------------
void repl_add_slave(int fd) {
    if (fd < 0 || slave_count >= MAX_SLAVES) return;
    for (int i=0; i<slave_count; i++) if (slave_fds[i]==fd) return;
    slave_fds[slave_count++] = fd;
}

void repl_feed_backlog(const char *cmd, int len) {
    if (backlog_len + len > REPL_BACKLOG_SIZE) {
        int remove = backlog_len + len - REPL_BACKLOG_SIZE;
        backlog_start_offset += remove;
        backlog_len -= remove;
        memmove(repl_backlog, repl_backlog + remove, backlog_len);
    }
    memcpy(repl_backlog + backlog_len, cmd, len);
    backlog_len += len;
    repl_offset += len;
}

void repl_propagate(const char *cmd, int len) {
    for (int i=0; i<slave_count; i++) {
        int fd = slave_fds[i];
        if (fd >= 0) repl_net_send(fd, cmd, len);
    }
}

// ------------------------------
// 主从切换
// ------------------------------
void repl_switch_to_master(void) {
    if (repl_role == REPL_MASTER) return;

    // 关闭从机连接
    if (repl_master_fd >= 0) {
        close(repl_master_fd);
        repl_master_fd = -1;
    }
    repl_role = REPL_MASTER;
    pthread_create(&repl_thread, NULL, repl_master_thread, NULL);
    printf("🔄 已切换为【主机】\n");
}

void repl_switch_to_slave(const char *ip, int port) {
    if (repl_role == REPL_SLAVE) return;

    // 关闭主机复制网络
    if (repl_listen_fd >= 0) {
        close(repl_listen_fd);
        repl_listen_fd = -1;
    }

    strncpy(repl_master_ip, ip, sizeof(repl_master_ip)-1);
    repl_role = REPL_SLAVE;
    pthread_create(&repl_thread, NULL, repl_slave_thread, NULL);
    printf("🔄 已切换为【从机】，连接主机: %s\n", ip);
}