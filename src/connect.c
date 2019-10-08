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

static int dust_recv(SOCKET socket)
{
    if (! wait_recv_data(socket, RCV_TIMEOUT_NOWAIT))
        return 0;   /* empty */

    while (1) {
        char buf[1024];
        int len;
        int status;

        len = recv_char(socket, buf, sizeof(buf), &status);
        if (len == 0 || status != 0)
            break;
        if (! wait_recv_data(socket, RCV_TIMEOUT_NOWAIT))
            break;
    }
    return 1;
}

/* 
 * サーバーに接続するコールバック関数です。
 * pool_initialize()から呼び出されます。
 *
 * s: サーバー構造体のポインタ
 *
 * 戻り値
 *  サーバーソケット構造体のポインタ
 */
static void* cb_conn(void* s)
{
    struct server_socket_t* ss;
    struct server_t* svr;
    SOCKET socket;

    if (s == NULL)
        return NULL;

    svr = (struct server_t*)s;
    ss = (struct server_socket_t*)malloc(sizeof(struct server_socket_t));
    if (ss == NULL) {
        err_write("connect: conn(): no memory.");
        return NULL;
    }
    /* 接続します。*/
    socket = sock_connect_server(svr->ip, svr->port);
    if (socket == INVALID_SOCKET) {
        free(ss);
        return NULL;
    }
    ss->server = svr;
    ss->socket = socket;
    return ss;
}

/* 
 * 接続を切断するコールバック関数です。
 * pool_finalize()から呼び出されます。
 *
 * s: サーバーソケット構造体のポインタ
 *
 * 戻り値
 *  なし
 */
static void cb_disconn(void* s)
{
    struct server_socket_t* ss;

    if (s == NULL)
        return;

    ss = (struct server_socket_t*)s;
    shutdown(ss->socket, 2);
    SOCKET_CLOSE(ss->socket);
    free(ss);
}

/*
 * データストアサーバーへのコネクションプールを作成します。
 *
 * コネクションをプール数などの情報はグローバル変数から取得します。
 *
 * server: サーバー構造体のポインタ
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int ds_connect_server(struct server_t* server)
{
    struct pool_t* p;

    p = pool_initialize(g_conf->pool_init_conns,
                        g_conf->pool_ext_conns,
                        cb_conn,
                        cb_disconn,
                        POOL_NOTIMEOUT,
                        g_conf->pool_ext_release_time,
                        server);
    if (p == NULL)
        return -1;
    server->pool = p;
    return 0;
}

/*
 * データストアサーバーへのコネクションプールを解放します。
 *
 * server: サーバー構造体のポインタ
 *
 * 戻り値
 *  なし
 */
void ds_disconnect_server(struct server_t* server)
{
    if (server->pool) {
        pool_finalize(server->pool);
        server->pool = NULL;
    }
}

/*
 * データストアサーバー数分の接続を行います。
 * 接続したソケットはプールされます。
 *
 * 接続が失敗したデータストアサーバーはリストから外されます。
 *
 *
 * 戻り値
 *  データストアのサーバー数を返します。
 *  エラーの場合は -1 を返します。
 */
int ds_connect()
{
    int i;

    if (g_dss == NULL)
        return -1;

    TRACE("%d data store servers connecting ...\n", g_dss->num_server);

    /* コネクションを確立します。*/
    for (i = 0; i < g_dss->num_server; i++)
        ds_connect_server(g_dss->server_list[i]);

    /* コネクションのチェックを行います。*/
    for (i = g_dss->num_server-1; i >= 0; i--) {
        if (g_dss->server_list[i]->pool == NULL) {
            int shift_n;

            TRACE("don't connect to data store server (%s:%d).\n",
                  g_dss->server_list[i]->ip, g_dss->server_list[i]->port);
            err_write("ds_connect: don't connect to data store server (%s:%d).",
                      g_dss->server_list[i]->ip, g_dss->server_list[i]->port);

            /* 動的に確保したメモリを解放します。*/
            free(g_dss->server_list[i]);

            shift_n = g_dss->num_server - i - 1;
            if (shift_n > 0) {
                memmove(&g_dss->server_list[i],
                        &g_dss->server_list[i+1],
                        sizeof(struct server_t*) * shift_n);
            }
            g_dss->num_server--;
        }
    }
    return g_dss->num_server;
}

/*
 * 接続したデータストアサーバーの切断を行います。
 *
 * 戻り値
 *  なし
 */
void ds_disconnect()
{
    int i;

    for (i = 0; i < g_dss->num_server; i++) {
        if (g_dss->server_list[i]->pool)
            pool_finalize(g_dss->server_list[i]->pool);
    }
}

/*
 * サーバーへのコネクションをプーリングから取得します。
 *
 * server: サーバー構造体のポインタ
 *
 * 戻り値
 *  サーバーソケット構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
struct server_socket_t* ds_server_socket(struct server_t* server)
{
    if (server->pool == NULL)
        return NULL;
    return (struct server_socket_t*)pool_get(server->pool, g_conf->pool_wait_time);
}

/*
 * サーバーへのコネクションをプーリングへ返却します。
 *
 * server: サーバー構造体のポインタ
 * ss: サーバーソケット構造体のポインタ
 * reset: ソケットのリセットフラグ（ゼロは正常、ゼロ以外はエラー）
 *
 * 戻り値
 *  なし
 */
void ds_release_socket(struct server_t* server,
                       struct server_socket_t* ss,
                       int reset)
{
    if (reset) {
        /* バッファにデータがあればすべて捨てます。*/
        dust_recv(ss->socket);
        /* コネクションをクローズして再オープンします。*/
        pool_reset(server->pool, ss);
    } else {
        /* コネクションの使用を解放します。*/
        pool_release(server->pool, ss);
    }
}
