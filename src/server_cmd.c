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

/*
 * 分散ロックメカニズム
 *
 * データストアサーバーの追加／削除が行われる場合には
 * 分散ロックが行われます。サーバーの追加／削除で影響の受ける
 * データストアサーバーをロックするためのコマンドが分散サーバーに
 * TCP通信にて送信されます。
 * すべてのロックが取得できた時点でデータストアの追加／削除が
 * 実行されます。
 *
 * データストアの追加と削除
 *
 * replications: 2 の場合
 *
 * DS(A)のデータは DS(B) と DS(C) が保持
 * DS(B)のデータは DS(C) と DS(D) が保持
 * DS(C)のデータは DS(D) と DS(A) が保持
 * DS(D)のデータは DS(A) と DS(B) が保持
 *
 *               +-------+
 *       +-------| DS(A) |------+
 *       |       +-------+      |
 *   +-------+              +-------+
 *   | DS(D) |              | DS(B) |
 *   +-------+              +-------+
 *       |       +-------+      |
 *       +-------| DS(C) |------+
 *               +-------+
 *
 * [DS(A')を追加]
 *
 *               +-------+
 *       +-------| DS(A) |------+
 *       |       +-------+      |
 *       |                  +-------+
 *       |                  | DS(A')|
 *       |                  +-------+
 *       |                      |
 *   +-------+              +-------+
 *   | DS(D) |              | DS(B) |
 *   +-------+              +-------+
 *       |       +-------+      |
 *       +-------| DS(C) |------+
 *               +-------+
 *
 * DS(B)とDS(D)を分散ロック
 * DS(B)のデータをDS(A')に再配分
 * DS(A')に配分されたデータのレプリカをDS(D)から削除
 *
 * [DS(B)を削除]
 *               +-------+
 *       +-------| DS(A) |------+
 *       |       +-------+      |
 *   +-------+              +-------+
 *   | DS(D) |              | DS(A')|
 *   +-------+              +-------+
 *       |       +-------+      |
 *       +-------| DS(C) |------+
 *               +-------+
 *
 * 削除するDS(B)を分散ロック
 * DS(B)のデータはDS(C)とDS(D)にもある
 * DS(C)からDS(B)のデータをDS(A)にコピー
 *
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "dinio.h"

static char* get_local_datetime(int64 usec, char* buf, size_t bufsize)
{
    time_t sec;
    struct tm t;

    sec = (time_t)(usec / 1000000);
    mt_localtime(&sec, &t);
    snprintf(buf, bufsize, "%d/%02d/%02d %02d:%02d:%02d",
             t.tm_year+1900, t.tm_mon+1, t.tm_mday,
             t.tm_hour, t.tm_min, t.tm_sec);
    return buf;
}

static char* server_status(struct server_t* server)
{
    if (server->status == DSS_PREPARE)
        return "pre";
    if (server->status == DSS_INACTIVE)
        return "x";
    if (server->status == DSS_LOCKED)
        return "lock";
    return "OK";
}

/*
 * ./dinio -status
 *
 * 分散サーバーとデータストアサーバーの状態を表示します。
 */
void status_command(SOCKET socket)
{
    struct membuf_t* mbuf;
    char buf[128];
    char stimebuf[128];
    int i;
    int rep_n;

    mbuf = mb_alloc(1024);
    if (mbuf == NULL) {
        if (reply_error(socket, "no memory.") < 0)
            return;
    }

    get_local_datetime(g_start_time, stimebuf, sizeof(stimebuf));
    snprintf(buf, sizeof(buf), "start %s  running %d datastore servers.\n",
            stimebuf, g_dss->num_server);
    mb_append(mbuf, buf, strlen(buf));
    strcpy(buf, "Status IP------------- PORT  #NODE #CONN #set------ #get------ #del------\n");
    mb_append(mbuf, buf, strlen(buf));
    for (i = 0; i < g_dss->num_server; i++) {
        char* status;
        int pool_conns = 0;

        status = server_status(g_dss->server_list[i]);
        if (g_dss->server_list[i]->pool)
            pool_conns = pool_count(g_dss->server_list[i]->pool);
        snprintf(buf, sizeof(buf), "[%-4s] %-15s %5u   %3d   %3d %10lld %10lld %10lld\n",
                 status,
                 g_dss->server_list[i]->ip,
                 g_dss->server_list[i]->port,
                 g_dss->server_list[i]->scale_factor,
                 pool_conns,
                 g_dss->server_list[i]->set_count,
                 g_dss->server_list[i]->get_count,
                 g_dss->server_list[i]->del_count);
        mb_append(mbuf, buf, strlen(buf));
    }

    /* レプリケーション情報 */
    rep_n = replication_queue_count();
    if (rep_n > 0) {
        snprintf(buf, sizeof(buf), "\nreplicating ... %d\n", rep_n);
        mb_append(mbuf, buf, strlen(buf));
    }

    mb_append(mbuf, LINE_DELIMITER, strlen(LINE_DELIMITER));

    if (send_data(socket, mbuf->buf, mbuf->size) < 0)
        err_write("status_command: send error: %s", strerror(errno));
    mb_free(mbuf);
}

/*
 * ./dinio -shutdown
 *
 * 分散サーバーを停止します。
 */
void shutdown_command(SOCKET socket)
{
    char sendbuf[256];

    /* g_shutdown_flag は呼び出し側でセットされる。*/
    snprintf(sendbuf, sizeof(sendbuf), "stopped.\n%s", LINE_DELIMITER);
    if (send_data(socket, sendbuf, strlen(sendbuf)) < 0)
        err_write("shutdown_command: send error: %s", strerror(errno));
}

/*
 * ./dinio -remove server port
 *
 * データストアサーバーを分散サーバに追加します。
 */
int add_server_command(SOCKET socket, int cn, const char** cl)
{
    int result = 0;
    struct server_t* server;
    char err_msg[256];

    if (cn < 4) {
        snprintf(err_msg, sizeof(err_msg),
                 "illegal parameter %s command.%s", cl[0], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    /* データストアの存在チェック */
    server = ds_get_server(cl[1], atoi(cl[2]));
    if (server) {
        snprintf(err_msg, sizeof(err_msg),
                 "server %s:%s already exists.%s", cl[1], cl[2], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    /* データストアを作成します。*/
    server = ds_create_server(cl[1], atoi(cl[2]), atoi(cl[3]));
    if (server == NULL) {
        snprintf(err_msg, sizeof(err_msg),
                 "don't create server %s:%s%s", cl[1], cl[2], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    CS_START(&g_dss->critical_section);

    /* データストアを追加します。*/
    if (ds_attach_server(server) < 0) {
        snprintf(err_msg, sizeof(err_msg),
                 "don't add server %s:%s%s", cl[1], cl[2], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        result = -1;
        goto final;
    }

    /* データを再配分します。*/
    if (g_dss->num_server > 1) {
        struct server_t* nserver;
        struct server_t* dserver;

        if (add_redist_target(server, &nserver, &dserver) == 0) {
            /* 分散ロックの取得 */
            if (lock_servers(nserver, dserver) < 0) {
                snprintf(err_msg, sizeof(err_msg),
                         "don't lock server %s:%d%s", server->ip, server->port, LINE_DELIMITER);
                send_data(socket, err_msg, strlen(err_msg));
                result = -1;
                goto final;
            }

            /* データの再配分 */
            add_redistribution(server, nserver, dserver);

            /* 分散ロックの解放 */
            unlock_servers(nserver, dserver);
        }
    }

    /* 分散サーバーにデータストアの追加を通知します。*/
    friend_add_server(g_friend_list, server);

final:
    CS_END(&g_dss->critical_section);

    if (result == 0) {
        /* データストアの状態を返します。*/
        status_command(socket);
    }
    return result;
}

/*
 * ./dinio -remove server port
 *
 * データストアサーバーを分散サーバーから取り除きます。
 */
int remove_server_command(SOCKET socket, int cn, const char** cl)
{
    int result = 0;
    int lock_flag = 0;
    struct server_t* server;
    char err_msg[256];

    if (cn < 3) {
        snprintf(err_msg, sizeof(err_msg),
                 "illegal parameter %s command.%s", cl[0], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    /* データストアを検索します。*/
    server = ds_get_server(cl[1], atoi(cl[2]));
    if (server == NULL) {
        snprintf(err_msg, sizeof(err_msg),
                 "not found server %s:%s%s", cl[1], cl[2], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    CS_START(&g_dss->critical_section);

    /* 分散ロックの取得 */
    if (lock_servers(server, NULL) < 0) {
        snprintf(err_msg, sizeof(err_msg),
                 "don't lock server %s:%d%s", server->ip, server->port, LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        result = -1;
        goto final;
    }
    lock_flag = 1;

    /* レプリケーション数を維持するために別のサーバーにデータを転送します。*/
    if (g_conf->replications > 0 &&
        g_dss->num_server-1 > g_conf->replications) {
        struct server_t* nserver;
        struct server_t* tserver;

        if (remove_redist_target(server, &nserver, &tserver) == 0) {
            /* 再配分 */
            remove_redistribution(server, nserver, tserver);
        }
    }

    /* 分散サーバーにデータストアの削除を通知します。*/
    friend_remove_server(g_friend_list, server);

    /* サーバーが削除されるので分散ロックを解放する必要はありません。*/
    if (ds_detach_server(server) < 0) {
        snprintf(err_msg, sizeof(err_msg),
                 "don't detach server %s:%s%s", cl[1], cl[2], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        result = -1;
        goto final;
    }
    lock_flag = 0;

final:
    if (lock_flag)
        unlock_servers(server, NULL);

    CS_END(&g_dss->critical_section);
    if (result == 0) {
        /* データストアの状態を返します。*/
        status_command(socket);
    }
    return result;
}

/*
 * ./dinio -unlock server port
 *
 * ロックされているデータストアサーバーを解除します。
 */
int unlock_server_command(SOCKET socket, int cn, const char** cl)
{
    struct server_t* server;
    char err_msg[256];

    if (cn < 3) {
        snprintf(err_msg, sizeof(err_msg),
                 "illegal parameter %s command.%s", cl[0], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    /* データストアを検索します。*/
    server = ds_get_server(cl[1], atoi(cl[2]));
    if (server == NULL) {
        snprintf(err_msg, sizeof(err_msg),
                 "not found server %s:%s%s", cl[1], cl[2], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    CS_START(&g_dss->critical_section);

    if (server->status == DSS_LOCKED) {
        ds_unlock_server(server);

        /* 分散サーバーにデータストアのロック解除を通知します。*/
        friend_unlock_server(g_friend_list, server);
    }

    CS_END(&g_dss->critical_section);

    /* データストアの状態を返します。*/
    status_command(socket);
    return 0;
}

/*
 * ./dinio -hash key ...
 *
 * キーが割り当てられるデータストアサーバーの
 * IPアドレスとポート番号を表示します。
 */
int hash_command(SOCKET socket, int cn, const char** cl)
{
    int result = 0;
    int keys;
    int i;
    struct membuf_t* mbuf;
    char err_msg[256];

    mbuf = mb_alloc(1024);
    if (mbuf == NULL) {
        snprintf(err_msg, sizeof(err_msg), "%s: %s%s",
                 cl[0], "no memory.", LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    if (cn < 3) {
        snprintf(err_msg, sizeof(err_msg),
                 "illegal parameter %s command.%s", cl[0], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        result = -1;
        goto final;
    }

    keys = atoi(cl[1]);
    for (i = 0; i < keys; i++) {
        const char* key;
        struct server_t* server;
        char buf[1024];

        key = cl[2+i];
        server = ds_key_server(key, strlen(key));
        if (server == NULL) {
            snprintf(buf, sizeof(buf),
                     "not found server key=%s\n", key);
        } else {
            snprintf(buf, sizeof(buf),
                     "key=%s -> server %s:%d\n", key, server->ip, server->port);
        }
        mb_append(mbuf, buf, strlen(buf));
    }
    mb_append(mbuf, LINE_DELIMITER, strlen(LINE_DELIMITER));

    /* 応答データ */
    if (send_data(socket, mbuf->buf, mbuf->size) < 0) {
        err_write("memcached: hash send error.");
        result = -1;
    }

final:
    mb_free(mbuf);
    return result;
}

static int valid_import_command(const char* cmd)
{
    return (strcmp(cmd, "set") == 0 ||
            strcmp(cmd, "add") == 0 ||
            strcmp(cmd, "replace") == 0 ||
            strcmp(cmd, "append") == 0 ||
            strcmp(cmd, "prepend") == 0);
}

static int remove_last_crlf(char* datap)
{
    int len;

    len = strlen(datap);
    if (len > 0) {
        if (datap[len-1] == '\n')
            datap[--len] = '\0';    /* 最後の LF コードを削除 */
    }
    if (len > 0) {
        if (datap[len-1] == '\r')
            datap[--len] = '\0';    /* 最後の CR コードを削除 */
    }
    return len;
}

/*
 * ./dinio -import file-path
 *
 * 指定されたファイルのデータをデータストアサーバーに挿入します。
 *
 * ファイルフォーマットは以下の通り
 *
 *  <command> <key> <flags> <exptime>\n
 *  <datablock>\n
 *
 *  ※ <command> は set, add, replace, append, prepend
 *  ※ データサイズは <datablock> のサイズになります。
 */
int import_command(SOCKET socket, int cn, const char** cl)
{
    FILE *fp;
    char fpath[MAX_PATH+1];
    char buf[1024];
    char* datap = NULL;
    char cmdbuf[CMDLINE_SIZE];
    char* cmd_line[6]; /* cmd key flags exptime bytes noreply */
    int bytes;
    char cbytes[16];
    char err_msg[256];
    int count = 0;
    int lineno = 0;
    int result = 0;

    if (cn < 2) {
        snprintf(err_msg, sizeof(err_msg),
                 "no input file-path.%s", LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    get_abspath(fpath, (const char*)cl[1], MAX_PATH);
    if ((fp = fopen(fpath, "r")) == NULL) {
        snprintf(err_msg, sizeof(err_msg),
                 "file open error: %s.%s", cl[1], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        return -1;
    }

    datap = (char*)malloc(MAX_MEMCACHED_DATASIZE+3); /* add <CRLF>\0 */
    if (datap == NULL) {
        snprintf(err_msg, sizeof(err_msg),
                 "data block buffer no memory: %s.%s", cl[1], LINE_DELIMITER);
        send_data(socket, err_msg, strlen(err_msg));
        result = -1;
        goto final;
    }

    cmd_line[5] = "noreply";
    while (fgets(buf, sizeof(buf), fp) != NULL) {
        int i;
        char** list;
        int len;

        lineno++;
        if (strlen(buf) == 0)
            continue;

        /* コマンド行 */
        list = split(buf, ' ');
        if (list == NULL) {
            snprintf(err_msg, sizeof(err_msg),
                     "no memory: %s.%s", cl[1], LINE_DELIMITER);
            send_data(socket, err_msg, strlen(err_msg));
            result = -1;
            break;
        }
        if (list_count((const char**)list) != 4) {
            list_free(list);
            snprintf(err_msg, sizeof(err_msg),
                     "illegal file format: %s line=%d.%s", cl[1], lineno, LINE_DELIMITER);
            send_data(socket, err_msg, strlen(err_msg));
            result = -1;
            break;
        }

        for (i = 0; i < 4; i++)
            cmd_line[i] = trim(list[i]);

        if (! valid_import_command(cmd_line[0])) {
            list_free(list);
            snprintf(err_msg, sizeof(err_msg),
                     "illegal command error: %s line=%d.%s", cl[0], lineno, LINE_DELIMITER);
            send_data(socket, err_msg, strlen(err_msg));
            result = -1;
            break;
        }

        /* データブロック行 */
        if (fgets(datap, MAX_MEMCACHED_DATASIZE, fp) == NULL) {
            list_free(list);
            snprintf(err_msg, sizeof(err_msg),
                     "data block error: %s line=%d.%s", cl[0], lineno, LINE_DELIMITER);
            send_data(socket, err_msg, strlen(err_msg));
            result = -1;
            break;
        }
        len = remove_last_crlf(datap);    /* 最後の改行コードを削除 */
        snprintf(cbytes, sizeof(cbytes), "%d", len);
        cmd_line[4] = cbytes;
        strcat(datap, LINE_DELIMITER);
        bytes = len + strlen(LINE_DELIMITER);

        snprintf(cmdbuf, sizeof(cmdbuf), "%s %s %s %s %s %s",
                 cmd_line[0], cmd_line[1], cmd_line[2], cmd_line[3], cmd_line[4], cmd_line[5]);

        /* データストアに挿入します。*/
        if (dispatch_event_entry(socket,
                                 CMDGRP_SET,
                                 cmdbuf,
                                 6, (const char**)cl,
                                 bytes, datap) < 0) {
            list_free(list);
            snprintf(err_msg, sizeof(err_msg),
                     "command dispatch error: %s line=%d.%s", cl[0], lineno, LINE_DELIMITER);
            send_data(socket, err_msg, strlen(err_msg));
            result = -1;
            break;
        }
        list_free(list);
        lineno++;
        count++;
    }

final:
    if (datap)
        free(datap);
    fclose(fp);

    if (result == 0) {
        char buf[256];

        /* 応答データ */
        snprintf(buf, sizeof(buf), "\nimported %d data.\n%s", count, LINE_DELIMITER);
        send_data(socket, buf, strlen(buf));
    }
    return result;
}
