/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2010-2011 YAMAMOTO Naoki
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "dinio.h"

#define CMD_SET           1   /* データの保存(キーが存在している場合は置換) */
#define CMD_ADD           2   /* データの保存(キーが既に存在しない場合のみ) */
#define CMD_REPLACE       3   /* データの保存(キーが既に存在する場合のみ) */
#define CMD_APPEND        4   /* 値への後方追加 */
#define CMD_PREPEND       5   /* 値への前方追加 */
#define CMD_CAS           6   /* データの保存(バージョン排他制御) */
#define CMD_GET           7   /* データの取得 */
#define CMD_GETS          8   /* データの取得(バージョン付き) */
#define CMD_DELETE        9   /* データの削除 */
#define CMD_INCR          10  /* 値への加算 */
#define CMD_DECR          11  /* 値への減算 */
#define CMD_STATS         12  /* 各種ステータスを表示 */
#define CMD_VERSION       13  /* バージョンを表示 */
#define CMD_VERBOSITY     14  /* 動作確認 */
#define CMD_QUIT          30  /* 終了(コネクション切断) */
#define CMD_STATUS        100 /* ステータス確認 */
#define CMD_SHUTDOWN      110 /* 終了(シャットダウン) */
#define CMD_ADDSERVER     120 /* データストア追加 */
#define CMD_REMOVESERVER  121 /* データストア削除 */
#define CMD_UNLOCKSERVER  122 /* データストアロック解除 */
#define CMD_HASHSERVER    130 /* キーのサーバー算出 */
#define CMD_IMPORTDATA    131 /* データのインポート */

#define STAT_FIN       0x01
#define STAT_CLOSE     0x02
#define STAT_SHUTDOWN  0x04

#ifdef WIN32
static HANDLE memcached_queue_cond;
#else
static pthread_mutex_t memcached_queue_mutex;
static pthread_cond_t memcached_queue_cond;
#endif

static int noreply(int n, const char** p)
{
    if (n > 1)
        return (stricmp(p[n-1], "noreply") == 0);
    return 0;
}

static int parse_command(const char* str)
{
    if (stricmp(str, "set") == 0)
        return CMD_SET;
    if (stricmp(str, "get") == 0)
        return CMD_GET;
    if (stricmp(str, "delete") == 0)
        return CMD_DELETE;
    if (stricmp(str, "gets") == 0)
        return CMD_GETS;
    if (stricmp(str, "cas") == 0)
        return CMD_CAS;
    if (stricmp(str, "add") == 0)
        return CMD_ADD;
    if (stricmp(str, "replace") == 0)
        return CMD_REPLACE;
    if (stricmp(str, "append") == 0)
        return CMD_APPEND;
    if (stricmp(str, "prepend") == 0)
        return CMD_PREPEND;
    if (stricmp(str, "incr") == 0)
        return CMD_INCR;
    if (stricmp(str, "decr") == 0)
        return CMD_DECR;
    if (stricmp(str, "stats") == 0)
        return CMD_STATS;
    if (stricmp(str, "version") == 0)
        return CMD_VERSION;
    if (stricmp(str, "verbosity") == 0)
        return CMD_VERBOSITY;
    if (stricmp(str, "quit") == 0)
        return CMD_QUIT;

    if (strcmp(str, SHUTDOWN_CMD) == 0)
        return CMD_SHUTDOWN;
    if (strcmp(str, STATUS_CMD) == 0)
        return CMD_STATUS;
    if (strcmp(str, ADDSERVER_CMD) == 0)
        return CMD_ADDSERVER;
    if (strcmp(str, REMOVESERVER_CMD) == 0)
        return CMD_REMOVESERVER;
    if (strcmp(str, UNLOCKSERVER_CMD) == 0)
        return CMD_UNLOCKSERVER;
    if (strcmp(str, HASHSERVER_CMD) == 0)
        return CMD_HASHSERVER;
    if (strcmp(str, IMPORTDATA_CMD) == 0)
        return CMD_IMPORTDATA;

    return -1;
}

static void dust_recv_buffer(struct sock_buf_t* sb)
{
    int end_flag = 0;

    while (! end_flag) {
        char buf[BUF_SIZE];

        if (! sockbuf_wait_data(sb, RCV_TIMEOUT_NOWAIT))
            break;  /* empty */
        if (sockbuf_gets(sb, buf, sizeof(buf), LINE_DELIMITER, 0, &end_flag) < 1)
            break;
    }
}

static int datablock_recv(struct sock_buf_t* sb, char* buf, int bytes, int noreply_flag)
{
    int bufsize;
    int len;
    int line_flag;
    int data_err = 0;

    /* bufsize: (bytes + strlen(CRLF) + NULL terminate) */
    bufsize = bytes  + strlen(LINE_DELIMITER) + 1;
    len = sockbuf_gets(sb, buf, bufsize, LINE_DELIMITER, 0, &line_flag);
    if (len < 1)
        return -1;
    if (! line_flag) {
        /* 行末(CRLF)まで読み捨てます。*/
        dust_recv_buffer(sb);
        data_err = 1;
    }
    if (len != bytes)
        data_err = 1;

    if (data_err) {
        if (! noreply_flag) {
            char msg[256];

            snprintf(msg, sizeof(msg), "<data block> size error, bytes=%d", bytes);
            reply_error(sb->socket, msg);
        }
        return -1;
    }
    return 0;
}

/* set <key> <flags> <exptime> <bytes> [noreply]
 * <data block>
 */
static int set_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    int dsize;
    int bytes = 0;
    char* data = NULL;
    int result;

    if (cn < 5 || cn > 6) {
        if (! noreply(cn, cl))
            reply_error(sb->socket, "illegal parameter.");
        return -1;
    }

    dsize = atoi(cl[4]);
    if (dsize > MAX_MEMCACHED_DATASIZE) {
        if (! noreply(cn, cl))
            reply_error(sb->socket, "data size too large.");
        return -1;
    }

    if (dsize > 0) {
        /* データブロックを受信します。*/
        bytes = dsize + strlen(LINE_DELIMITER);
        data = (char*)malloc(bytes+1);
        if (data == NULL) {
            err_write("set_command: no memory bytes=%d.", bytes);
            if (! noreply(cn, cl))
                reply_error(sb->socket, "data recv no memory.");
            return -1;
        }
        if (datablock_recv(sb, data, dsize, noreply(cn, cl)) < 0) {
            free(data);
            return 0;
        }
        /* CRLF + '\0' を付加します。*/
        memcpy(&data[dsize], LINE_DELIMITER, sizeof(LINE_DELIMITER));
    }
    result = dispatch_event_entry(sb->socket, CMDGRP_SET, cmdline, cn, cl, bytes, data);
    if (data)
        free(data);
    return result;
}

/* add <key> <flags> <exptime> <bytes> [noreply]
 * <data block>
 */
static int add_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    return set_command(sb, cmdline, cn, cl);
}

/* replace <key> <flags> <exptime> <bytes> [noreply]
 * <data block>
 */
static int replace_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    return set_command(sb, cmdline, cn, cl);
}

/* append <key> <flags> <exptime> <bytes> [noreply]
 * <data block>
 */
static int append_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    return set_command(sb, cmdline, cn, cl);
}

/* prepend <key> <flags> <exptime> <bytes> [noreply]
 * <data block>
 */
static int prepend_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    return set_command(sb, cmdline, cn, cl);
}

/* cas <key> <flags> <exptime> <bytes> <cas unqiue> [noreply]
 * <data block>
 */
static int cas_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    if (cn < 6) {
        if (! noreply(cn, cl))
            reply_error(sb->socket, "illegal parameter.");
        return -1;
    }
    return set_command(sb, cmdline, cn, cl);
}

/* get <key[ key1 key2 ...]>
 * VALUE <key> <flags> <bytes>
 * <data block>
 * ...
 * END
 */
static int get_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    if (cn < 2)
        return reply_error(sb->socket, "illegal parameter.");

    return dispatch_event_entry(sb->socket, CMDGRP_GET, cmdline, cn, cl, 0, NULL);
}

/* gets <key[ key1 key2 ...]>
 * VALUE <key> <flags> <bytes> <cas unique>
 * <data block>
 * ...
 * END
 */
static int gets_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    if (cn < 2)
        return reply_error(sb->socket, "illegal parameter.");

    return dispatch_event_entry(sb->socket, CMDGRP_GET, cmdline, cn, cl, 0, NULL);
}

/* delete <key> [<time>] [noreply]
 */
static int delete_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    if (cn < 2 || cn > 4) {
        if (! noreply(cn, cl))
            reply_error(sb->socket, "illegal parameter.");
        return -1;
    }
    return dispatch_event_entry(sb->socket, CMDGRP_DELETE, cmdline, cn, cl, 0, NULL);
}

/* incr <key> <value> [noreply]
 */
static int incr_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    if (cn < 3 || cn > 4) {
        if (! noreply(cn, cl))
            reply_error(sb->socket, "illegal parameter.");
        return -1;
    }
    return dispatch_event_entry(sb->socket, CMDGRP_SET, cmdline, cn, cl, 0, NULL);
}

/* decr <key> <value> [noreply]
 */
static int decr_command(struct sock_buf_t* sb, const char* cmdline, int cn, const char** cl)
{
    if (cn < 3 || cn > 4) {
        if (! noreply(cn, cl))
            reply_error(sb->socket, "illegal parameter.");
        return -1;
    }
    return dispatch_event_entry(sb->socket, CMDGRP_SET, cmdline, cn, cl, 0, NULL);
}

/* stats
 */
static int stats_command(SOCKET socket)
{
    struct membuf_t* mb;
    char str[256];
    int pid;
    int64 nowtime;
    unsigned int nowsec;
    int i;
    int64 set_count = 0;
    int64 get_count = 0;

    mb = mb_alloc(1024);
    if (mb == NULL) {
        err_write("memc_gateway: stats no memory.");
        snprintf(str, sizeof(str), "%s%sEND%s", "no memory.", LINE_DELIMITER, LINE_DELIMITER);
        if (send_data(socket, str, strlen(str)) < 0) {
            err_write("memc_gateway: stats send error.");
            return -1;
        }
    }

    nowtime = system_time();
    nowsec = system_seconds();
    pid = getpid();

    for (i = 0; i < g_dss->num_server; i++) {
        set_count += g_dss->server_list[i]->set_count;
        get_count += g_dss->server_list[i]->get_count;
    }

    snprintf(str, sizeof(str), "STAT pid %d%s", pid, LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT uptime %u%s", (nowsec - (unsigned int)(g_start_time/1000000)), LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT time %u%s", nowsec, LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT version %s%s", PROGRAM_VERSION, LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT pointer_size %u%s", (unsigned int)sizeof(void*), LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT rusage_user %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT rusage_system %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT curr_connections %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT total_connections %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT connection_structures %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT cmd_get %lld%s", get_count, LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT cmd_set %lld%s", set_count, LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT cmd_flush %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT get_hits %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT get_misses %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT delete_misses %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT delete_hits %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT incr_misses %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT incr_hits %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT decr_misses %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT decr_hits %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT cas_misses %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT cas_hits %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT cas_badval %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT auth_cmds %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT auth_errors %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT bytes_read %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT bytes_written %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT limit_maxbytes %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT accepting_conns %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT listen_disabled_num %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT threads %d%s", g_conf->worker_threads, LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT conn_yields %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT bytes %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT curr_items %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT total_items %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT evictions %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));
    snprintf(str, sizeof(str), "STAT reclaimed %s%s", "N/A", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));

    snprintf(str, sizeof(str), "END%s", LINE_DELIMITER);
    mb_append(mb, str, strlen(str));

    /* 応答データ */
    if (send_data(socket, mb->buf, mb->size) < 0)
        err_write("memc_gateway: stats send error.");
    mb_free(mb);
    return 0;
}

/* version
 */
static int version_command(SOCKET socket)
{
    char verstr[256];

    snprintf(verstr, sizeof(verstr), "%s%s", PROGRAM_VERSION, LINE_DELIMITER);

    /* 応答データ */
    if (send_data(socket, verstr, strlen(verstr)) < 0) {
        err_write("memc_gateway: version send error.");
        return -1;
    }
    return 0;
}

/* verbosity
 */
static int verbosity_command(SOCKET socket)
{
    char str[256];

    snprintf(str, sizeof(str), "OK%s", LINE_DELIMITER);

    /* 応答データ */
    if (send_data(socket, str, strlen(str)) < 0) {
        err_write("memc_gateway: verbosity send error.");
        return -1;
    }
    return 0;
}

static int cmdline_recv(struct sock_buf_t* sb, char* buf, int size, int* line_flag)
{
    int len;

    len = sockbuf_gets(sb, buf, size, LINE_DELIMITER, 0, line_flag);
    if (len < 1)
        return len;
    if (! *line_flag) {
        /* 行末(CRLF)までを読み捨てます。*/
        dust_recv_buffer(sb);
        return 0;
    }
    return len;
}

static unsigned command_gateway(struct sock_buf_t* sb,
                                struct in_addr addr)
{
    unsigned stat = 0;
    int result = 0;
    int len;
    char buf[BUF_SIZE];
    char cmdbuf[CMDLINE_SIZE];
    int line_flag;
    char** clp;
    int cc;
    int cmd;

    /* コマンド行を受信します。*/
    len = cmdline_recv(sb, buf, sizeof(buf), &line_flag);
    if (len < 0)
        return STAT_FIN|STAT_CLOSE;    /* FIN受信 */
    if (len == 0) {
        if (line_flag)
            return 0;    /* 空文字受信 */
        return STAT_FIN|STAT_CLOSE;    /* FIN受信 */
    }
    if (! line_flag) {
        reply_error(sb->socket, NULL);
        return 0;
    }
    if (strlen(buf) > CMDLINE_SIZE) {
        reply_error(sb->socket, NULL);
        return 0;
    }
    strcpy(cmdbuf, buf);
    TRACE("request command: %s ...", buf);

    clp = split(buf, ' ');
    if (clp == NULL) {
        reply_error(sb->socket, NULL);
        return 0;
    }
    cc = list_count((const char**)clp);
    if (cc <= 0) {
        list_free(clp);
        reply_error(sb->socket, NULL);
        return 0;
    }

    cmd = parse_command(trim(clp[0]));
    switch (cmd) {
        case CMD_SET:
            result = set_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_ADD:
            result = add_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_REPLACE:
            result = replace_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_APPEND:
            result = append_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_PREPEND:
            result = prepend_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_CAS:
            result = cas_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_GET:
            result = get_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_GETS:
            result = gets_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_DELETE:
            result = delete_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_INCR:
            result = incr_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_DECR:
            result = decr_command(sb, cmdbuf, cc, (const char**)clp);
            break;
        case CMD_STATS:
            result = stats_command(sb->socket);
            break;
        case CMD_VERSION:
            result = version_command(sb->socket);
            break;
        case CMD_VERBOSITY:
            result = verbosity_command(sb->socket);
            break;
        case CMD_QUIT:
            stat = STAT_CLOSE;
            break;
        case CMD_SHUTDOWN:
        case CMD_STATUS:
        case CMD_ADDSERVER:
        case CMD_REMOVESERVER:
        case CMD_UNLOCKSERVER: {
            char ip_addr[256];

            mt_inet_ntoa(addr, ip_addr);
            if (strcmp(ip_addr, "127.0.0.1") == 0) {
                if (cmd == CMD_SHUTDOWN) {
                    shutdown_command(sb->socket);
                    stat |= STAT_SHUTDOWN;
                } else if (cmd == CMD_STATUS)
                    status_command(sb->socket);
                else if (cmd == CMD_ADDSERVER)
                    result = add_server_command(sb->socket, cc, (const char**)clp);
                else if (cmd == CMD_REMOVESERVER)
                    result = remove_server_command(sb->socket, cc, (const char**)clp);
                else if (cmd == CMD_UNLOCKSERVER)
                    result = unlock_server_command(sb->socket, cc, (const char**)clp);
            } else {
                if (reply_error(sb->socket, "illegal command.") < 0)
                    result = -1;
            }
            stat |= STAT_CLOSE;
            break;
        }
        case CMD_HASHSERVER:
            result = hash_command(sb->socket, cc, (const char**)clp);
            break;
        case CMD_IMPORTDATA:
            result = import_command(sb->socket, cc, (const char**)clp);
            break;
        default: {
            /* エラー応答データ */
            if (reply_error(sb->socket, "illegal command.") < 0)
                result = -1;
            break;
        }
    }
    list_free(clp);
    return stat;
}

static void break_signal()
{
    SOCKET c_socket;
    const char dummy = 0x30;

    c_socket = sock_connect_server("127.0.0.1", g_conf->port_no);
    if (c_socket == INVALID_SOCKET) {
        err_write("break_signal: can't open socket: %s", strerror(errno));
        return;
    }
    send_data(c_socket, &dummy, sizeof(dummy));
    SOCKET_CLOSE(c_socket);
}

static struct sock_buf_t* socket_buffer(SOCKET socket)
{
    struct sock_buf_t* sb;
    char sockkey[16];

    snprintf(sockkey, sizeof(sockkey), "%d", socket);
    sb = (struct sock_buf_t*)hash_get(g_sockbuf_hash, sockkey);
    if (sb == NULL) {
        err_write("socket_buffer: not found hash key=%d", socket);
        return NULL;
    }
    if (sb->socket != socket) {
        err_write("socket_buffer: illegal socket %d -> %d", socket, sb->socket);
        return NULL;
    }
    return sb;
}

static void socket_cleanup(struct sock_buf_t* sb)
{
    char sockkey[16];

    sock_event_delete(g_sock_event, sb->socket);

    shutdown(sb->socket, 2);  /* 2: RDWR stop */
    SOCKET_CLOSE(sb->socket);

    snprintf(sockkey, sizeof(sockkey), "%d", sb->socket);
    if (hash_delete(g_sockbuf_hash, sockkey) < 0)
        err_write("socket_cleanup: hash_delete fail, key=%s", sockkey);
    sockbuf_free(sb);
}

static void memcached_gateway_thread(void* argv)
{
    /* argv unuse */
    struct thread_args_t* th_args;
    SOCKET socket;
    struct in_addr addr;
    struct sock_buf_t* sb;
    int stat;
    int end_flag;

    while (! g_shutdown_flag) {
#ifndef WIN32
        pthread_mutex_lock(&memcached_queue_mutex);
#endif
        /* キューにデータが入るまで待機します。*/
        while (que_empty(g_queue)) {
#ifdef WIN32
            WaitForSingleObject(memcached_queue_cond, INFINITE);
#else
            pthread_cond_wait(&memcached_queue_cond, &memcached_queue_mutex);
#endif
        }
#ifndef WIN32
        pthread_mutex_unlock(&memcached_queue_mutex);
#endif
        /* キューからデータを取り出します。*/
        th_args = (struct thread_args_t*)que_pop(g_queue);
        if (th_args == NULL)
            continue;

        addr = th_args->sockaddr.sin_addr;
        socket = th_args->client_socket;

        sb = socket_buffer(socket);
        if (sb == NULL)
            continue;

        do {
            end_flag = 0;
            /* コマンドを受信して処理します。*/
            /* 'quit'コマンドが入力されると STAT_CLOSE が真になります。*/
            /* 'shutdown'コマンドが入力されると STAT_SHUTDOWN と
                STAT_CLOSE が真になります。*/
            stat = command_gateway(sb, addr);

            if (stat & STAT_CLOSE) {
                /* ソケットをクローズします。*/
                if (g_trace_mode) {
                    char ip_addr[256];

                    mt_inet_ntoa(addr, ip_addr);
                    TRACE("disconnect to %s, socket=%d, done.\n", ip_addr, sb->socket);
                }
                /* ソケットをクローズします。*/
                socket_cleanup(sb);
                end_flag = 1;
            }

            if (! end_flag) {
                if (sb->cur_size < 1)
                    end_flag = 1;
            }
        } while (! end_flag);

        if (! (stat & STAT_CLOSE)) {
            /* コマンド処理が終了したのでイベント通知を有効にします。*/
            sock_event_enable(g_sock_event, sb->socket);
        }
        /* パラメータ領域の解放 */
        free(th_args);

        if (stat & STAT_SHUTDOWN) {
            g_shutdown_flag = 1;
            break_signal();
        }
    }

    /* スレッドを終了します。*/
#ifdef _WIN32
    _endthread();
#endif
}

static void create_worker_thread()
{
    int i;

    for (i = 0; i < g_conf->worker_threads; i++) {
#ifdef _WIN32
        uintptr_t thread_id;
#else
        pthread_t thread_id;
#endif
        /* スレッドを作成します。
           生成されたスレッドはリクエストキューが空のため、
           待機状態に入ります。*/
#ifdef _WIN32
        thread_id = _beginthread(memcached_gateway_thread, 0, NULL);
#else
        pthread_create(&thread_id, NULL, (void*)memcached_gateway_thread, NULL);
        /* スレッドの使用していた領域を終了時に自動的に解放します。*/
        pthread_detach(thread_id);
#endif
    }
}

int memcached_gateway_event(SOCKET socket, struct sockaddr_in sockaddr)
{
    struct thread_args_t* th_args;

    /* スレッドへ渡す情報を作成します */
    th_args = (struct thread_args_t*)malloc(sizeof(struct thread_args_t));
    if (th_args == NULL) {
        err_log(sockaddr.sin_addr, "no memory.");
        SOCKET_CLOSE(socket);
        return 0;
    }
    th_args->client_socket = socket;
    th_args->sockaddr = sockaddr;

    /* リクエストされた情報をキューイング(push)します。*/
    que_push(g_queue, th_args);

    /* キューイングされたことをスレッドへ通知します。*/
#ifdef WIN32
    SetEvent(memcached_queue_cond);
#else
    pthread_mutex_lock(&memcached_queue_mutex);
    pthread_cond_signal(&memcached_queue_cond);
    pthread_mutex_unlock(&memcached_queue_mutex);
#endif
    return 0;
}

int memcached_gateway_start()
{
    struct sockaddr_in sockaddr;
    char ip_addr[256];

    /* メッセージキューの作成 */
    g_queue = que_initialize();
    if (g_queue == NULL)
        return -1;
    TRACE("%s initialized.\n", "event queue");

    /* イベントリスニングソケットの作成 */
    g_listen_socket = sock_listen(INADDR_ANY,
                                  g_conf->port_no,
                                  g_conf->backlog,
                                  &sockaddr);
    if (g_listen_socket == INVALID_SOCKET)
        return -1;  /* error */

    /* 自分自身の IPアドレスを取得します。*/
    sock_local_addr(ip_addr);

    /* スターティングメッセージの表示 */
    TRACE("%s port: %d on %s listening ... %d threads\n",
        PROGRAM_NAME, g_conf->port_no, ip_addr, g_conf->worker_threads);

    /* キューイング制御の初期化 */
#ifdef WIN32
    memcached_queue_cond = CreateEvent(NULL, FALSE, FALSE, NULL);
#else
    pthread_mutex_init(&memcached_queue_mutex, NULL);
    pthread_cond_init(&memcached_queue_cond, NULL);
#endif

    /* ワーカースレッドを生成します。 */
    create_worker_thread();
    return 0;
}

void memcached_gateway_end()
{
    if (g_listen_socket != INVALID_SOCKET) {
        shutdown(g_listen_socket, 2);  /* 2: RDWR stop */
        SOCKET_CLOSE(g_listen_socket);
    }

    if (g_queue != NULL) {
        que_finalize(g_queue);
        TRACE("%s terminated.\n", "event queue");
    }

#ifdef WIN32
    CloseHandle(memcached_queue_cond);
#else
    pthread_cond_destroy(&memcached_queue_cond);
    pthread_mutex_destroy(&memcached_queue_mutex);
#endif
}
