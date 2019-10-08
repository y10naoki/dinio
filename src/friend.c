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

#define R_BUF_SIZE 256

static int parse_friend_line(const char* sline,
                             struct friend_t* fsvr)
{
    char* delim = " ";
    char* tok;
    char* port;
    char* endptr = 0;

    /* ip-addr */
    tok = strtok((char*)sline, delim);
    if ((strlen(tok)) > 15) {
        err_write("invalid IP-ADDR value in friend definitions.");
        return -1;
    }
    strncpy(fsvr->ip, tok, strlen(tok));

    /* port */
    tok = strtok(NULL, delim);
    port = (char*)alloca(strlen(tok)+1);
    strncpy(port, tok, strlen(tok));
    port[strlen(tok)] = '\0';

    errno = 0;
    fsvr->port = strtol(port, &endptr, 10);
    if (errno == ERANGE || endptr == port) {
        err_write("invalid port number value in friend definitions.");
        return -1;
    }
    return 0;
}

static struct friend_t* read_friend_file(const char* filename)
{
    FILE* fp;
    char buf[R_BUF_SIZE];
    struct friend_t* friend_list = NULL;
    unsigned int num_friends = 0;

    fp = fopen(filename, "r");
    if (! fp) {
        err_write("can't open friend define file = %s", filename);
        return NULL;
    }

    while (fgets(buf, sizeof(buf), fp) != NULL) {
        struct friend_t fsvr;

        trim(buf);
        if (strlen(buf) < 2 || buf[0] == '#')
            continue;

        memset(&fsvr, '\0', sizeof(struct friend_t));
        if (parse_friend_line(buf, &fsvr) < 0)
            continue;

        if (strlen(fsvr.ip) > 0) {
            friend_list =
                (struct friend_t*)realloc(friend_list,
                                          sizeof(struct friend_t) * (num_friends+2));
            if (friend_list == NULL) {
                err_write("friend no memory count is %d.", num_friends);
                break;
            }
            memcpy(&friend_list[num_friends], &fsvr, sizeof(struct friend_t));
            num_friends++;
            memset(&friend_list[num_friends], '\0', sizeof(struct friend_t));
            TRACE("define friend server %s:%d\n", fsvr.ip, fsvr.port);
        }
    }
    fclose(fp);
    return friend_list;
}

/*
 * 分散サーバー構造体のリストを作成します。
 *  +---------------+---------------+-----+--------+
 *  |struct friend_t|struct friend_t| ... |all zero|
 *  +---------------+---------------+-----+--------+
 *
 * 分散サーバーに接続してコネクションを確立します。
 * 相手のサーバーがまだ起動していない可能性もあるので
 * エラーにはしません。
 *
 * [定義ファイルのフォーマット]
 * # ip-addr    port
 * 192.168.10.1 15432
 * 192.168.10.2 15432
 * 192.168.10.3 15432
 *
 * def_fname: サーバー定義ファイル名
 *
 * 戻り値
 *  成功すると分散サーバーリストのポインタを返します。
 *  エラーの場合は NULL を返します。
 */
struct friend_t* friend_create(const char* def_fname)
{
    struct friend_t* friend_list;

    if (def_fname == NULL || strlen(def_fname) == 0)
        return NULL;

    /* 定義ファイルを読み込みます。*/
    friend_list = read_friend_file(def_fname);

    if (friend_list == NULL)
        return NULL;
    return friend_list;
}

/*
 * 分散サーバーをクローズします。
 *
 * friend_list: 分散サーバー構造体のリスト
 *
 * 戻り値
 *  なし
 */
void friend_close(struct friend_t* friend_list)
{
    if (friend_list == NULL)
        return;

    /* メモリの解放 */
    free(friend_list);
}

/* +--------+----------+-----------+---------+-----------------+
 * | cmd(1) | iplen(1) | ip(iplen) | port(2) | scale-factor(2) |
 * +--------+----------+-----------+---------+-----------------+
 *
 * scale-factor は FRIEND_ADD_SERVER の場合のみ
 */
static int make_command(unsigned char cmd, struct server_t* server, char* buf)
{
    char* p;
    unsigned char len;
    unsigned short port;
    unsigned short scale;

    p = buf;
    memcpy(p, &cmd, sizeof(cmd));
    p += sizeof(cmd);

    len = (unsigned char)strlen(server->ip);
    memcpy(p, &len, sizeof(len));
    p += sizeof(len);

    memcpy(p, server->ip, len);
    p += len;

    port = (unsigned short)server->port;
    memcpy(p, &port, sizeof(port));
    p += sizeof(port);

    if (cmd == FRIEND_ADD_SERVER) {
        scale = (unsigned short)server->scale_factor;
        memcpy(p, &scale, sizeof(scale));
        p += sizeof(scale);
    }
    return p - buf;
}

static int friend_command(int cmd,
                          struct friend_t* friend_list,
                          struct server_t* server)
{
    struct friend_t* fsvr;

    if (friend_list == NULL)
        return 0;

    fsvr = friend_list;
    while (fsvr->ip[0]) {
        SOCKET socket;
        char cmdbuf[256];
        int len;

        /* コネクションを作成します。*/
        socket = sock_connect_server(fsvr->ip, fsvr->port);
        if (socket == INVALID_SOCKET) {
            /* 分散サーバーがダウンしている。*/
            fsvr++;
            continue;
        }

        /* 送信するコマンドを作成します。*/
        len = make_command((unsigned char)cmd, server, cmdbuf);
        if (len > 0) {
            char ack;
            int status;

            /* 分散サーバーにコマンドを送信します。*/
            if (send_data(socket, cmdbuf, len) < 0) {
                SOCKET_CLOSE(socket);
                err_write("friend_command(%d): send error %s:%d.",
                          cmd, fsvr->ip, fsvr->port);
                return -1;
            }
            /* 応答を確認します。*/
            if (! wait_recv_data(socket, FRIEND_WAIT_TIME)) {
                SOCKET_CLOSE(socket);
                err_write("friend_command(%d): timeout %s:%d.",
                          cmd, fsvr->ip, fsvr->port);
                return -1;
            }
            /* Ackを受信します。*/
            if (recv_nchar(socket, &ack, sizeof(char), &status) != sizeof(char)) {
                SOCKET_CLOSE(socket);
                err_write("friend_command(%d): ack recv error %s:%d.",
                          cmd, fsvr->ip, fsvr->port);
                return -1;
            }
            if (ack != FRIEND_ACK) {
                SOCKET_CLOSE(socket);
                err_write("friend_command(%d): ack error(%c) %s:%d.",
                          cmd, ack, fsvr->ip, fsvr->port);
                return -1;
            }
            SOCKET_CLOSE(socket);
        }
        fsvr++;
    }
    return 0;
}

/*
 * 分散サーバーのすべてにデータストアサーバーの追加コマンドを送信します。
 *
 * friend_list: 分散サーバーリストのポインタ
 * server: 追加するサーバー構造体のポインタ
 *
 * 戻り値
 *  成功するとゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int friend_add_server(struct friend_t* friend_list, struct server_t* server)
{
    return friend_command(FRIEND_ADD_SERVER, friend_list, server);
}

/*
 * 分散サーバーのすべてにデータストアサーバーの削除コマンドを送信します。
 *
 * friend_list: 分散サーバーリストのポインタ
 * server: 削除するサーバー構造体のポインタ
 *
 * 戻り値
 *  成功するとゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int friend_remove_server(struct friend_t* friend_list, struct server_t* server)
{
    return friend_command(FRIEND_REMOVE_SERVER, friend_list, server);
}

/*
 * 分散サーバーのすべてにデータストアサーバーのロックコマンドを送信します。
 * すべての分散サーバーがロックできた場合にのみ成功します。
 *
 * friend_list: 分散サーバーリストのポインタ
 * server: ロックするサーバー構造体のポインタ
 *
 * 戻り値
 *  成功するとゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int friend_lock_server(struct friend_t* friend_list, struct server_t* server)
{
    if (friend_command(FRIEND_LOCK_SERVER, friend_list, server) < 0) {
        err_write("friend_lock: error to be unlock.");
        friend_command(FRIEND_UNLOCK_SERVER, friend_list, server);
        return -1;
    }
    return 0;
}

/*
 * 分散サーバーのすべてにデータストアサーバーのロック解放コマンドを送信します。
 *
 * friend_list: 分散サーバーリストのポインタ
 * server: 解放するサーバー構造体のポインタ
 *
 * 戻り値
 *  成功するとゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int friend_unlock_server(struct friend_t* friend_list, struct server_t* server)
{
    return friend_command(FRIEND_UNLOCK_SERVER, friend_list, server);
}

