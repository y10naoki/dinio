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
 * 分散サーバーのコマンドを処理します。
 *
 * コマンドフォーマットは以下の通りです。
 * scale-factor は FRIEND_ADD_SERVER の場合のみ設定されます。
 * +--------+----------+-----------+---------+-----------------+
 * | cmd(1) | iplen(1) | ip(iplen) | port(2) | scale-factor(2) |
 * +--------+----------+-----------+---------+-----------------+
 *
 * cmd(1byte)
 *   FRIEND_ADD_SERVER     1
 *   FRIEND_REMOVE_SERVER  2
 *   FRIEND_LOCK_SERVER    3
 *   FRIEND_UNLOCK_SERVER  4
 * iplen(1byte)
 *   後続のipアドレスのバイト数
 * ip(nbyte)
 *   データストアのIPアドレス
 * port(2byte)
 *   データストアのポート番号
 * scale-factor(2byte)
 *   仮想ノード数
 *
 * 処理が正常に終了した場合はFRIEND_ACK('A')を送信します。
 * それ以外はFREIEND_REJECT('R')を送信します。
 *
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "dinio.h"

#ifdef WIN32
static HANDLE informed_queue_cond;
#else
static pthread_mutex_t informed_queue_mutex;
static pthread_cond_t informed_queue_cond;
#endif

static int informed_add_server(const char* ip, int port, int scale_factor)
{
    struct server_t* server;

    server = ds_create_server(ip, port, scale_factor);
    if (server == NULL)
        return -1;
    if (ds_attach_server(server) < 0)
        return -1;
    return 0;
}

static int informed_command(SOCKET socket, struct in_addr addr)
{
    int result = 0;
    int status;
    unsigned char cmd;
    unsigned char iplen;
    char ip[16];
    unsigned short port;
    char rch;

    /* cmd(1)を受信します。*/
    if (recv_nchar(socket, (char*)&cmd, sizeof(cmd), &status) != sizeof(cmd)) {
        err_write("informed_command: cmd recv error.");
        return -1;
    }

    /* iplen(1)を受信します。*/
    if (recv_nchar(socket, (char*)&iplen, sizeof(iplen), &status) != sizeof(iplen)) {
        err_write("informed_command: iplen recv error.");
        return -1;
    }
    if (iplen > sizeof(ip)-1) {
        err_write("informed_command: iplen(%d) error.", (int)iplen);
        return -1;
    }

    /* ip(n)を受信します。*/
    if (recv_nchar(socket, ip, iplen, &status) != iplen) {
        err_write("informed_command: ip recv error.");
        return -1;
    }
    ip[iplen] = '\0';

    /* port(2)を受信します。*/
    port = (unsigned short)recv_short(socket, &status);
    if (status != 0) {
        err_write("informed_command: port recv error.");
        return -1;
    }

    if (cmd == FRIEND_ADD_SERVER) {
        unsigned short scale_factor;

        /* scale-factor(2)を受信します。*/
        scale_factor = (unsigned short)recv_short(socket, &status);
        if (status != 0) {
            err_write("informed_command: scale_factor recv error.");
            return -1;
        }
        result = informed_add_server(ip, port, scale_factor);
    } else {
        struct server_t* server;

        /* サーバーの取得 */
        server = ds_get_server(ip, port);
        if (server == NULL) {
            err_write("informed_command: not found server %s:%d.", ip, port);
            result = -1;
        } else {
            if (cmd == FRIEND_REMOVE_SERVER)
                result = ds_detach_server(server);
            else if (cmd == FRIEND_LOCK_SERVER)
                ds_lock_server(server);
            else if (cmd == FRIEND_UNLOCK_SERVER)
                ds_unlock_server(server);
            else {
                err_write("informed_command: illegal cmd (%c) error.", cmd);
                result = -1;
            }
        }
    }

    /* 応答データの送信 */
    rch = (result == 0)? FRIEND_ACK : FRIEND_REJECT;
    if (send_data(socket, &rch, sizeof(char)) < 0) {
        err_write("informed_command: result(%c) send error: %s", rch, strerror(errno));
        return -1;
    }
    return 0;
}

static void informed_thread(void* argv)
{
    /* argv unuse */
    struct thread_args_t* th_args;
    SOCKET socket;
    struct in_addr addr;

    while (! g_shutdown_flag) {
#ifndef WIN32
        pthread_mutex_lock(&informed_queue_mutex);
#endif
        /* キューにデータが入るまで待機します。*/
        while (que_empty(g_informed_queue)) {
#ifdef WIN32
            WaitForSingleObject(informed_queue_cond, INFINITE);
#else
            pthread_cond_wait(&informed_queue_cond, &informed_queue_mutex);
#endif
        }
#ifndef WIN32
        pthread_mutex_unlock(&informed_queue_mutex);
#endif
        /* キューからデータを取り出します。*/
        th_args = (struct thread_args_t*)que_pop(g_informed_queue);
        if (th_args == NULL)
            continue;

        addr = th_args->sockaddr.sin_addr;
        socket = th_args->client_socket;

        /* コマンドを受信して処理します。*/
        informed_command(socket, addr);

        /* パラメータ領域の解放 */
        free(th_args);

        /* ソケットをクローズします。*/
        SOCKET_CLOSE(socket);
    }

    /* スレッドを終了します。*/
#ifdef _WIN32
    _endthread();
#endif
}

static void create_worker_thread()
{
#ifdef _WIN32
    uintptr_t thread_id;
#else
    pthread_t thread_id;
#endif
    /* スレッドを作成します。
       生成されたスレッドはリクエストキューが空のため、
       待機状態に入ります。*/
#ifdef _WIN32
    thread_id = _beginthread(informed_thread, 0, NULL);
#else
    pthread_create(&thread_id, NULL, (void*)informed_thread, NULL);
    /* スレッドの使用していた領域を終了時に自動的に解放します。*/
    pthread_detach(thread_id);
#endif
}

int friend_informed_event(SOCKET socket, struct sockaddr_in sockaddr)
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
    que_push(g_informed_queue, th_args);

    /* キューイングされたことをスレッドへ通知します。*/
#ifdef WIN32
    SetEvent(informed_queue_cond);
#else
    pthread_mutex_lock(&informed_queue_mutex);
    pthread_cond_signal(&informed_queue_cond);
    pthread_mutex_unlock(&informed_queue_mutex);
#endif
    return 0;
}

int friend_informed_start()
{
    struct sockaddr_in sockaddr;
    char ip_addr[256];

    /* メッセージキューの作成 */
    g_informed_queue = que_initialize();
    if (g_informed_queue == NULL)
        return -1;
    TRACE("%s initialized.\n", "informed queue");

    /* 分散サーバーからのリスニングソケットの作成 */
    g_informed_socket = sock_listen(INADDR_ANY,
                                    g_conf->informed_port,
                                    g_conf->backlog,
                                    &sockaddr);
    if (g_informed_socket == INVALID_SOCKET)
        return -1;  /* error */

    /* 自分自身の IPアドレスを取得します。*/
    sock_local_addr(ip_addr);

    /* スターティングメッセージの表示 */
    TRACE("%s port: %d on %s listening ... %d thread\n",
        PROGRAM_NAME, g_conf->informed_port, ip_addr, 1);

    /* キューイング制御の初期化 */
#ifdef WIN32
    informed_queue_cond = CreateEvent(NULL, FALSE, FALSE, NULL);
#else
    pthread_mutex_init(&informed_queue_mutex, NULL);
    pthread_cond_init(&informed_queue_cond, NULL);
#endif

    /* ワーカースレッドを生成します。 */
    create_worker_thread();
    return 0;
}

void friend_informed_end()
{
    if (g_informed_socket != INVALID_SOCKET) {
        shutdown(g_informed_socket, 2);  /* 2: RDWR stop */
        SOCKET_CLOSE(g_informed_socket);
    }
    if (g_informed_queue != NULL) {
        que_finalize(g_informed_queue);
        TRACE("%s terminated.\n", "informed queue");
    }

#ifdef WIN32
    CloseHandle(informed_queue_cond);
#else
    pthread_cond_destroy(&informed_queue_cond);
    pthread_mutex_destroy(&informed_queue_mutex);
#endif
}
