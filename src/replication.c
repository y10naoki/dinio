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

/* データストアのレプリケーションをバックエンドのスレッドで実行します。
 *
 * レプリケートするサーバー数は g_conf->replications で指定されます。
 *
 * レプリケートするサーバーが g_conf->replications 以下の場合は
 * すべてのサーバーにレプリケーションされます。
 * 
 * レプリケートするサーバーはコンシステントハッシュの円周上で
 * 時計回りに決定します。
 *
 * レプリケーション方法は memcached コマンドによって変わります。
 *
 * deleteコマンドは noreply を付加してサーバーに送信します。
 *
 * 以下の更新系コマンドは独自プロトコルにてキーで更新データを
 * 取得してレプリケート対象のサーバーを更新します。
 * キーが存在しない場合は追加されます。
 * すでにキーが存在する場合は <cas> を含めて更新されます。
 *  set, add, replace, append, prepend, cas, incl, decr
 *
 * レプリケーションでは bget でデータを取得して bset で設定します。
 * データが圧縮されていても展開すること関係なく、そのまま bset に
 * 引き渡します。
 *
 * 参照系のコマンドはレプリケーションの対象外になります。
 *  get, gets
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "dinio.h"

struct replication_event_t {
    struct server_t* server;
    int cmd_grp;
    char key[MAX_MEMCACHED_KEYSIZE+1];
};

static struct queue_t* replication_queue;

#ifdef WIN32
static HANDLE replication_queue_cond;
#else
static pthread_mutex_t replication_queue_mutex;
static pthread_cond_t replication_queue_cond;
#endif

/*
 * レプリケーションを実行します。
 *
 * org_server: 更新を行ったサーバー構造体のポインタ
 * cmd_grp: コマンドグループ
 * key: キーのポインタ
 *
 * 戻り値
 *  レプリケートした件数を返します。
 *  エラーの場合は -1 を返します。
 */
int do_replication(struct server_t* org_server,
                   int cmd_grp,
                   const char* key)
{
    int rep_num = 0;
    struct server_t* cur_server;
    int i;
    char* datablock = NULL;
    int dbsize = 0;

    if (cmd_grp == CMDGRP_GET || g_conf->replications < 1)
        return 0;   /* ignore */

    TRACE("replication: start %s:%d\n", org_server->ip, org_server->port);
    if (cmd_grp == CMDGRP_SET) {
        struct server_socket_t* org_ss;
        int reset_conn = 0;

        /* サーバーの状態をチェックします。*/
        if (ds_check_server(org_server) < 0) {
            err_write("replication: %s:%d was locked/inactive.", org_server->ip, org_server->port);
            return -1;
        }

        /* サーバーのソケットをプールから取得します。*/
        org_ss = ds_server_socket(org_server);
        if (org_ss) {
            /* サーバーから更新データを取得します。*/
            datablock = bget_command(org_ss, key, &dbsize);
            if (datablock == NULL) {
                /* dbsize がゼロの場合は NOT FOUND */
                if (dbsize == -1)
                    reset_conn = -1;
            }
            /* ソケットを解放します。*/
            ds_release_socket(org_server, org_ss, reset_conn);
        }
        if (datablock == NULL)
            return 0;
    }

    cur_server = org_server;
    for (i = 0; i < g_conf->replications; i++) {
        struct server_t* server;
        struct server_socket_t* ss;
        int result = 0;

        /* レプリケートするサーバーを決定します。*/
        server = ds_next_server(cur_server);
        if (server == NULL) {
            err_write("replication: ds_next_server() is NULL.");
            goto final;
        }
        if (server == org_server)
            break;

        /* サーバーの状態をチェックします。*/
        if (ds_check_server(server) < 0) {
            err_write("replication: %s:%d was locked/inactive.", server->ip, server->port);
            goto final;
        }

        /* サーバーのソケットをプールから取得します。*/
        ss = ds_server_socket(server);
        if (ss == NULL) {
            err_write("replication: %s:%d ds_server_socket() is NULL.", server->ip, server->port);
            goto final;
        }

        if (cmd_grp == CMDGRP_SET) {
            /* サーバーのデータを更新します。*/
            result = bset_command(ss, key, dbsize, datablock);
        } else if (cmd_grp == CMDGRP_DELETE) {
            /* noreply を付加した delete コマンドを送信します。*/
            delete_noreply_command(ss, key);
        }

        /* ソケットを解放します。*/
        ds_release_socket(server, ss, result);

        cur_server = server;
        rep_num++;
    }

final:
    if (datablock)
        free(datablock);
    TRACE("replication: end   %s:%d rep_num=%d\n", org_server->ip, org_server->port, rep_num);
    return rep_num;
}

static void replication_thread(void* argv)
{
    /* argv unuse */
    struct replication_event_t* rep_ev;

    while (! g_shutdown_flag) {
#ifndef WIN32
        pthread_mutex_lock(&replication_queue_mutex);
#endif
        /* キューにデータが入るまで待機します。*/
        while (que_empty(replication_queue)) {
#ifdef WIN32
            WaitForSingleObject(replication_queue_cond, INFINITE);
#else
            pthread_cond_wait(&replication_queue_cond, &replication_queue_mutex);
#endif
        }
#ifndef WIN32
        pthread_mutex_unlock(&replication_queue_mutex);
#endif
        /* キューからデータを取り出します。*/
        rep_ev = (struct replication_event_t*)que_pop(replication_queue);
        if (rep_ev == NULL)
            continue;

        /* レプリケーションの開始を遅延させます。*/
        if (g_conf->replication_delay_time > 0) {
#ifdef _WIN32
            Sleep(g_conf->replication_delay_time);
#else
            usleep(g_conf->replication_delay_time * 1000);
#endif
        }

        /* レプリケーションを実行します。*/
        do_replication(rep_ev->server, rep_ev->cmd_grp, rep_ev->key);

        /* パラメータ領域の解放 */
        free(rep_ev);
    }

    /* スレッドを終了します。*/
#ifdef _WIN32
    _endthread();
#endif
}

static void create_replication_threads()
{
    int i;

    for (i = 0; i < g_conf->replication_threads; i++) {
#ifdef _WIN32
        uintptr_t thread_id;
#else
        pthread_t thread_id;
#endif
        /* スレッドを作成します。
           生成されたスレッドはリクエストキューが空のため、
           待機状態に入ります。*/
#ifdef _WIN32
        thread_id = _beginthread(replication_thread, 0, NULL);
#else
        pthread_create(&thread_id, NULL, (void*)replication_thread, NULL);
        /* スレッドの使用していた領域を終了時に自動的に解放します。*/
        pthread_detach(thread_id);
#endif
    }
}

/*
 * レプリケーションキューの数を返します。
 *
 * 戻り値
 *  レプリケーションキューの件数を返します。
 *  レプリケーションが有効でない場合はゼロを返します。
 */
int replication_queue_count()
{
    if (replication_queue == NULL)
        return 0;
    return que_count(replication_queue);
}

int replication_event_entry(struct server_t* org_server,
                            int cmd_grp,
                            const char* key)
{
    struct replication_event_t* rep_ev;

    if (cmd_grp == CMDGRP_GET || g_conf->replications < 1)
        return 0;   /* ignore */

    /* スレッドへ渡す情報を作成します */
    rep_ev = (struct replication_event_t*)malloc(sizeof(struct replication_event_t));
    if (rep_ev == NULL) {
        err_write("replication_event: no memory.");
        return -1;
    }
    rep_ev->server = org_server;
    rep_ev->cmd_grp = cmd_grp;
    strcpy(rep_ev->key, key);

    /* レプリケーション情報をキューイング(push)します。*/
    que_push(replication_queue, rep_ev);

    /* キューイングされたことをスレッドへ通知します。*/
#ifdef WIN32
    SetEvent(replication_queue_cond);
#else
    pthread_mutex_lock(&replication_queue_mutex);
    pthread_cond_signal(&replication_queue_cond);
    pthread_mutex_unlock(&replication_queue_mutex);
#endif
    return 0;
}

int replication_server_start()
{
    if (g_conf->replications < 1)
        return 0;

    /* メッセージキューの作成 */
    replication_queue = que_initialize();
    if (replication_queue == NULL)
        return -1;
    TRACE("%s initialized.\n", "replication queue");

    /* キューイング制御の初期化 */
#ifdef WIN32
    replication_queue_cond = CreateEvent(NULL, FALSE, FALSE, NULL);
#else
    pthread_mutex_init(&replication_queue_mutex, NULL);
    pthread_cond_init(&replication_queue_cond, NULL);
#endif

    /* ワーカースレッドを生成します。 */
    create_replication_threads();
    return 0;
}

void replication_server_end()
{
    if (g_conf->replications < 1)
        return;

    if (replication_queue != NULL) {
        que_finalize(replication_queue);
        TRACE("%s terminated.\n", "replication queue");
    }

#ifdef WIN32
    CloseHandle(replication_queue_cond);
#else
    pthread_cond_destroy(&replication_queue_cond);
    pthread_mutex_destroy(&replication_queue_mutex);
#endif
}
