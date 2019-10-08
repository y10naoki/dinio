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
 * I/O戦略
 *
 * ソケットを利用したデータ処理は多重I/Oを使用して行います。
 * 実装は nestalib/sockevent.c で行われています。
 *
 * 多重I/Oはメインスレッドで処理してクライアントリクエストは
 * ワーカスレッドで処理します。
 * データストアへの入出力はディスパッチスレッドが担当します。
 *
 * 1. listenソケットを監視対象に登録します。
 * 2. 多重I/Oで受信したソケットが listenソケットであれば
 *    accept にてクライアントソケット取得して監視対象に登録します。
 * 3. 多重I/Oで受信したソケットがクライアントソケットであれば
 *    スレッドキューに登録してワーカスレッドで処理します。
 * 4. ワーカスレッドはコマンドを解析してデータストアへの
 *    問い合わせはディスパッチスレッドのキューに登録します。
 * 5. ワーカスレッド内でソケットのクローズが行われた場合は
 *    監視対象から外します。
 *
 * [main thread]
 *     |
 *     +- 多重I/O にて受付
 *     |
 *   [worker thread]
 *       |
 *       +- コマンドの解析
 *       |
 *     [dispatch thread]
 *       |
 *       +- データストアへのコマンド送信
 *       +- データストアからの結果をクライアントへ送信
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "dinio.h"

static int is_shutdown()
{
    return g_shutdown_flag;
}

static int sock_event_cb(SOCKET socket)
{
    struct sockaddr_in sockaddr;
    int n;
    SOCKET client_socket;

    if (socket == g_listen_socket) {
        char sockkey[16];
        struct sock_buf_t* sockbuf;

        n = sizeof(struct sockaddr);
        client_socket = accept(g_listen_socket, (struct sockaddr*)&sockaddr, (socklen_t*)&n);
        if (client_socket < 0)
            return 0;
        if (g_shutdown_flag) {
            SOCKET_CLOSE(client_socket);
            return -1;
        }

        if (g_trace_mode) {
            char ip_addr[256];

            mt_inet_ntoa(sockaddr.sin_addr, ip_addr);
            TRACE("connect from %s, socket=%d ... \n", ip_addr, client_socket);
        }
        if (sock_event_add(g_sock_event, client_socket) < 0) {
            SOCKET_CLOSE(client_socket);
            return -1;
        }
        /* ソケットバッファを作成します。*/
        sockbuf = sockbuf_alloc(client_socket);
        if (sockbuf == NULL) {
            err_write("sock_event_cb: sockbuf_alloc no memory");
            SOCKET_CLOSE(client_socket);
            return -1;
        }
        snprintf(sockkey, sizeof(sockkey), "%d", client_socket);
        if (hash_put(g_sockbuf_hash, sockkey, sockbuf) < 0) {
            err_write("sock_event_cb: hsh_put failed");
            SOCKET_CLOSE(client_socket);
            return -1;
        }
    } else if (socket == g_informed_socket) {
        n = sizeof(struct sockaddr);
        client_socket = accept(g_informed_socket, (struct sockaddr*)&sockaddr, (socklen_t*)&n);
        if (client_socket < 0)
            return 0;
        if (g_shutdown_flag) {
            SOCKET_CLOSE(client_socket);
            return -1;
        }
        friend_informed_event(client_socket, sockaddr);
    } else {
        n = sizeof(struct sockaddr);
        getpeername(socket, (struct sockaddr*)&sockaddr, (socklen_t*)&n);

        /* リクエスト処理中はイベント通知を無効にします。*/
        sock_event_disable(g_sock_event, socket);
        /* リクエストを処理します。*/
        memcached_gateway_event(socket, sockaddr);
    }
    return 0;
}

static int sock_init()
{
    g_sock_event = sock_event_create();
    if (g_sock_event == NULL)
        return -1;
    if (sock_event_add(g_sock_event, g_listen_socket) < 0)
        return -1;
    if (g_friend_list) {
        if (sock_event_add(g_sock_event, g_informed_socket) < 0)
            return -1;
    }
    g_sockbuf_hash = hash_initialize(1031);
    if (g_sockbuf_hash == NULL) {
        err_write("dinio_server: hash_initialize failure.");
        return -1;
    }
    return 0;
}

static void sock_final()
{
    if (g_sockbuf_hash)
        hash_finalize(g_sockbuf_hash);
    if (g_sock_event)
        sock_event_close(g_sock_event);
}

void dinio_server()
{
    g_start_time = system_time();

    /* 分散サーバーリストを作成します。*/
    g_friend_list = friend_create(g_conf->friend_file);
    if (g_friend_list) {
        /* 分散サーバーからの通知を受けるスレッドを開始します。*/
        if (friend_informed_start() < 0)
            return;
    }

    /* データストアサーバーを作成します。
       この中でコンシステントハッシュも作成します。*/
    if (ds_create(g_conf->server_file) < 0)
        return;

    if (g_conf->replication_threads > 0) {
        /* replicationを実行するスレッドを開始します。*/
        if (replication_server_start() < 0)
            return;
    }

    /* dispatchを実行するスレッドを開始します。*/
    if (dispatch_server_start() < 0)
        return;

    /* memcachedプロトコルを橋渡しするスレッドを開始します。*/
    if (memcached_gateway_start() < 0)
        return;

    if (sock_init() < 0)
        goto final;

    sock_event_loop(g_sock_event, sock_event_cb, is_shutdown);

final:
    sock_final();

    /* memcachedプロトコルを処理するスレッドを終了します。*/
    memcached_gateway_end();

    if (g_conf->replication_threads > 0) {
        /* replicationを実行するスレッドを終了します。*/
        replication_server_end();
    }

    /* dispatchを実行するスレッドを終了します。*/
    dispatch_server_end();

    /* 分散サーバーからの通知を受けるスレッドを終了します。*/
    if (g_friend_list)
        friend_informed_end();

    /* データストアサーバーを終了します。*/
    ds_close();

    /* 分散サーバーリストを解放します。*/
    if (g_friend_list)
        friend_close(g_friend_list);
}
