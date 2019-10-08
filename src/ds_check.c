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

static void change_status(struct server_t* server, int status)
{
    CS_START(&g_dss->critical_section);
    server->status = status;
    CS_END(&g_dss->critical_section);
}

static int active_check(struct server_t* server)
{
    char* check_cmd = "version\r\n";
    SOCKET socket;
    int len;
    char buf[128];
    int status = DSS_INACTIVE;

    /* データストアの稼働チェック中にデータストアが削除される可能性があるため
       プールされているコネクションは使用しません。
       データストアが削除されるとコネクションプールが解放されてしまい
       不正アクセスになるためです。 */
    socket = sock_connect_server(server->ip, server->port);
    if (socket == INVALID_SOCKET) {
        err_write("active_check: %s:%d connect error.",
                  server->ip, server->port);
        return DSS_INACTIVE;
    }

    /* 稼動チェックのために"version"コマンドを送信します。*/
    if (send_data(socket, check_cmd, strlen(check_cmd)) < 0) {
        err_write("active_check: %s:%d server inactive.",
                  server->ip, server->port);
        goto final;
    }

    if (g_conf->datastore_timeout >= 0) {
        /* サーバーからの応答を指定ミリ秒待ちます。*/
        if (! wait_recv_data(socket, g_conf->datastore_timeout)) {
            /* サーバーからの応答がない。*/
            err_write("active_check: (%s) %s:%d data store server timeout.",
                      "version", server->ip, server->port);
            goto final;
        }
    }

    /* データストアからの応答を受信します。*/
    len = recv_line(socket, buf, sizeof(buf), LINE_DELIMITER);
    if (len < 0) {
        err_write("active_check: %s:%d connection closed.",
                  server->ip, server->port);
        goto final;    /* FIN受信 */
    }

    /* 正常に稼動している。*/
    status = DSS_ACTIVE;

final:
    /* コネクションをクローズします。*/
    SOCKET_CLOSE(socket);
    return status;
}

static int auto_detach_node(struct server_t* server)
{
    int result = 0;
    int lock_flag = 0;

    CS_START(&g_dss->critical_section);

    /* 分散ロックの取得 */
    if (lock_servers(server, NULL) < 0) {
        err_write("auto_detach_node: don't lock server %s:%d",
                  server->ip, server->port);
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
        err_write("auto_detach_node: don't detach server %s:%s",
                  server->ip, server->port);
        result = -1;
        goto final;
    }
    lock_flag = 0;

final:
    if (lock_flag)
        unlock_servers(server, NULL);

    CS_END(&g_dss->critical_section);
    return result;
}

void ds_active_check_thread(void* argv)
{
    /* unuse argv, value is NULL. */
    int stime;

    if (g_conf->active_check_interval <= 0)
        return;

    /* チェック間隔（秒）の間スリープします。*/
    stime = g_conf->active_check_interval;
#ifdef _WIN32
    /* windowsのSleep()はミリ秒になります。*/
    stime *= 1000;
#endif

    while (! g_shutdown_flag) {
        int i;

        /* 指定時間眠りに入りますzzz。*/
        sleep(stime);

        /* ds_close()が呼ばれた場合はスレッドを終了します。*/
        if (g_dss == NULL)
            break;

        /* 物理ノードをチェックします。*/
        for (i = 0; i < g_dss->num_server; i++) {
            struct server_t* server;
            int cur_status;

            server = g_dss->server_list[i];

            if (server->status == DSS_INACTIVE) {
                /* ノードダウンの場合は再起動している可能性があるため
                   コネクションを再作成します。*/
                ds_disconnect_server(server);
                if (ds_connect_server(server) < 0)
                    continue;
            }

            /* データストアが稼動しているか調べます。*/
            cur_status = active_check(server);
            if (cur_status == DSS_INACTIVE) {
                /* ノードがダウンしている。*/
                if (server->status != DSS_INACTIVE) {
                    change_status(server, cur_status);
                    if (g_conf->auto_detach) {
                        /* 自動的にノードを削除します。*/
                        auto_detach_node(server);
                    }
                }
            } else if (cur_status == DSS_ACTIVE) {
                if (server->status == DSS_PREPARE ||
                    server->status == DSS_INACTIVE) {
                    change_status(server, cur_status);
                }
            }
        }
    }
#ifdef _WIN32
    _endthread();
#endif
}
