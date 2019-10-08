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

struct dispatch_event_t {
    SOCKET csocket;
    int cmd_grp;
    char cmdline[CMDLINE_SIZE];
    char key[MAX_MEMCACHED_KEYSIZE+1];
    int cn;
    int dsize;
    char* data;
    int noreply_flag;
};

static struct queue_t* dispatch_queue;

#ifdef WIN32
static HANDLE dispatch_queue_cond;
#else
static pthread_mutex_t dispatch_queue_mutex;
static pthread_cond_t dispatch_queue_cond;
#endif

static int last_error()
{
#ifdef WIN32
    return WSAGetLastError();
#else
    return errno;
#endif
}

static void incl_command(int grp, struct server_t* server)
{
    if (grp == CMDGRP_SET)
        mt_increment64(&server->set_count);
    else if (grp == CMDGRP_GET)
        mt_increment64(&server->get_count);
    else if (grp == CMDGRP_DELETE)
        mt_increment64(&server->del_count);
}

static int noreply(int cn, const char** cl)
{
    if (cn > 1)
        return (stricmp(cl[cn-1], "noreply") == 0);
    return 0;
}

static int ds_datablock_size(SOCKET socket,
                             char* buf,
                             int bufsize,
                             int* bytes)
{
    int len;
    char* tbuf;
    char** glp;
    int glc;

    /* VALUE <key> <flags> <bytes> [<cas>]<CRLF> */
    *bytes = -1;
    len = recv_line(socket, buf, bufsize, LINE_DELIMITER);
    if (len <= 0)
        return -1;    /* FIN受信 */

    tbuf = (char*)alloca(len+1);
    strcpy(tbuf, buf);
    glp = split(tbuf, ' ');
    if (glp) {
        glc = list_count((const char**)glp);
        if (glc > 3)
            *bytes = atoi(glp[3]);
        list_free(glp);
    }

    /* <CRLF>を付加したバッファを返します。*/
    strcat(buf, LINE_DELIMITER);
    return strlen(buf);
}

static int ds_datablock_recv(SOCKET socket,
                             char* buf,
                             int bytes,
                             const char* delim)
{
    char* str;

    /* bytes = datablock + delim */
    str = recv_str(socket, delim, 1);
    if (str == NULL)
        return -1;
    if (strlen(str) != bytes) {
        recv_free(str);
        return -1;
    }
    memcpy(buf, str, bytes);
    recv_free(str);
    return bytes;
}

/* サーバーからの応答をクライアントに送信します。*/
static int client_reply(SOCKET csocket,
                        struct server_socket_t* ss,
                        int cmd_grp,
                        const char* cmdline,
                        const char* term_word,
                        int send_term_word_flag,
                        struct membuf_t* mb)
{
    char buf[BUF_SIZE];
    int len;
    char* delim = LINE_DELIMITER;
    int skip_recv_flag = 0;

    if (term_word) {
        delim = (char*)alloca(strlen(term_word) + strlen(LINE_DELIMITER) + 1);
        strcpy(delim, term_word);
        strcat(delim, LINE_DELIMITER);
    }

    if (g_conf->datastore_timeout >= 0) {
        /* サーバーからの応答を指定ミリ秒待ちます。*/
        if (! wait_recv_data(ss->socket, g_conf->datastore_timeout)) {
            /* サーバーからの応答がない。*/
            err_write("client_reply: (%s) %s:%d data store server timeout.",
                      cmdline, ss->server->ip, ss->server->port);
            return -1;
        }
    }

    if (cmd_grp == CMDGRP_GET) {
        int bytes;

        /* VALUE <key> <flags> <bytes> [<cas>]<CRLF>
           <data block><CRLF>
           END<CRLF> */
        len = ds_datablock_size(ss->socket, buf, sizeof(buf), &bytes);
        if (len < 0) {
            err_write("client_reply: (%s) %s:%d ds_datablock_size() error[%d].",
                      cmdline, ss->server->ip, ss->server->port, last_error());
            return -1;
        }
        if (bytes > 0) {
            char* rbuf;

            mb_append(mb, buf, len);
            bytes += strlen(LINE_DELIMITER);
            rbuf = (char*)malloc(bytes + strlen(delim));
            if (rbuf == NULL) {
                err_write("client_reply: (%s) %s:%d no memory %d bytes.",
                          cmdline, ss->server->ip, ss->server->port, bytes);
                return -1;
            }
            if (ds_datablock_recv(ss->socket, rbuf, bytes+strlen(delim), delim) < 0) {
                err_write("client_reply: (%s) %s:%d ds_datablock_recv() error[%d].",
                          cmdline, ss->server->ip, ss->server->port, last_error());
                free(rbuf);
                return -1;
            }
            mb_append(mb, rbuf, bytes);
            free(rbuf);
        }
        /* "END<CRLF>" を受信したので読まないように制御します。 */
        skip_recv_flag = 1;
    }

    /* "get key1 key2<CRLF>" のように複数の問い合わせは別のコマンドとして
        実行されるため、buf に "END<CRLF>" は含めないように制御しています。
        delim を buf に含めるかは send_term_word_flag で決まります。*/
    if (! skip_recv_flag) {
        len = recv_line(ss->socket, buf, sizeof(buf), delim);
        if (len < 0) {
            err_write("client_reply: (%s) %s:%d recv_line() error[%d].",
                      cmdline, ss->server->ip, ss->server->port, last_error());
            return -1;
        }

        if (cmd_grp == CMDGRP_SET) {
            if (strnicmp(buf, "STORED", 6) != 0) {
                err_write("client_reply: (%s) %s:%d recv_line(STORED)=%s.",
                           cmdline, ss->server->ip, ss->server->port, buf);
                return -1;
            }
        } else if (cmd_grp == CMDGRP_DELETE) {
            if (strnicmp(buf, "DELETED", 7) != 0) {
                err_write("client_reply: (%s) %s:%d recv_line(DELETED)=%s.",
                           cmdline, ss->server->ip, ss->server->port, buf);
                return -1;
            }
        }
        if (len > 0)
            mb_append(mb, buf, len);
    }
    if (send_term_word_flag)
        mb_append(mb, delim, strlen(delim));

    /* 結果をクライアントへ送信します。*/
    if (send_data(csocket, mb->buf, mb->size) < 0) {
        /* クライアントが終了している可能性があるのでエラーにはしません。*/
        err_write("client_reply: (%s) %d bytes %s:%d -> %d client send error[%d].",
                  cmdline, mb->size, ss->server->ip, ss->server->port, csocket, last_error());
    }
    return 0;
}

static int do_command(SOCKET csocket,
                      int cmd_grp,
                      struct membuf_t* mb,
                      struct server_t* server,
                      const char* cmdline,
                      int noreply_flag,
                      const char* term_word,
                      int send_term_word_flag)
{
    int result = -1;
    struct server_socket_t* ss;

    /* サーバーの状態をチェックします。*/
    if (ds_check_server(server) < 0) {
        err_write("dispatch_command: %s:%d was locked/inactive.", server->ip, server->port);
        return -1;
    }

    /* サーバーのソケットをプールから取得します。*/
    ss = ds_server_socket(server);
    if (ss == NULL) {
        err_write("dispatch_command: (%s) ds_server_socket() is NULL.", cmdline);
        goto final;
    }

    /* サーバーにコマンドを送信します。*/
    if (send_data(ss->socket, mb->buf, mb->size) < 0) {
        err_write("dispatch_command: (%s) %s:%d send error[%d].",
                  cmdline, server->ip, server->port, last_error());
        goto final;
    }
    if (! noreply_flag) {
        struct membuf_t* mbr;

        /* 編集用のバッファを確保します。*/
        mbr = mb_alloc(BUF_SIZE);
        if (mbr == NULL) {
            err_write("dispatch_command: mb_alloc() no memory.");
            goto final;
        }

        /* サーバーから応答を待ってクライアントに送信します。*/
        result = client_reply(csocket,
                              ss,
                              cmd_grp,
                              cmdline,
                              term_word,
                              send_term_word_flag,
                              mbr);
        mb_free(mbr);
        if (result < 0)
            goto final;
    }
    result = 0;

final:
    /* サーバーのソケットをプールへ返却します。*/
    if (server && ss)
        ds_release_socket(server, ss, result);
    return result;
}

static int do_dispatch(SOCKET csocket,
                       int cmd_grp,
                       const char* cmdline,
                       const char* key,
                       int dsize,
                       const char* data,
                       int noreply_flag,
                       const char* term_word,
                       int send_term_word_flag)
{
    struct membuf_t* mb;
    struct server_t* key_server = NULL;
    struct server_t* server = NULL;
    int retry;
    int result = -1;

    mb = mb_alloc(BUF_SIZE);
    if (mb == NULL) {
        err_write("do_dispatch: (%s) mb_alloc() no memory.", cmdline);
        if (! noreply_flag)
            reply_error(csocket, "no memory.");
        return -1;
    }

    /* コマンドを編集します。*/
    mb_append(mb, cmdline, strlen(cmdline));
    /* <CRLF> を付加します。*/
    mb_append(mb, LINE_DELIMITER, strlen(LINE_DELIMITER));

    if (dsize > 0) {
        /* data の最後には <CRLF> が付加されている。*/
        mb_append(mb, data, dsize);
    }

    /* キーから該当のサーバーを求めます。*/
    key_server = ds_key_server(key, strlen(key));
    retry = g_conf->replications + 1;
    while (retry > 0) {
        if (key_server && key_server->status != DSS_INACTIVE)
            break;
        retry--;
        if (retry > 0)
            key_server = ds_next_server(key_server);
    }

    if (key_server == NULL || key_server->status == DSS_INACTIVE) {
        err_write("do_dispatch: (%s) ds_key_server() is NULL.", cmdline);
        if (! noreply_flag)
            reply_error(csocket, NULL);
        goto final;
    }

    server = key_server;
    while (retry > 0) {
        /* コマンドを実行します。*/
        result = do_command(csocket,
                            cmd_grp,
                            mb,
                            server,
                            cmdline,
                            noreply_flag,
                            term_word,
                            send_term_word_flag);
        if (result == 0)
            break;
        retry--;
        /* エラーの場合は次のサーバーから取得します。*/
        if (retry > 0) {
            server = ds_next_server(server);
            if (server == NULL || server == key_server) {
                if (! noreply_flag)
                    reply_error(csocket, NULL);
                goto final;
            }
        }
    }

    if (result != 0) {
        if (! noreply_flag)
            reply_error(csocket, NULL);
        goto final;
    }

    /* コマンド実行数をインクリメントします。*/
    incl_command(cmd_grp, key_server);

    if (g_conf->replications > 0) {
        if (g_conf->replication_threads > 0) {
            /* バックエンドのスレッドでレプリケーションを実行します。*/
            replication_event_entry(key_server, cmd_grp, key);
        } else {
            /* レプリケーションを実行します。*/
            do_replication(key_server, cmd_grp, key);
        }
    }

final:
    mb_free(mb);
    return result;
}

static void dis_ev_free(struct dispatch_event_t* dis_ev)
{
    if (dis_ev == NULL)
        return;

    if (dis_ev->data)
        free(dis_ev->data);
    free(dis_ev);
}

static void dispatch_thread(void* argv)
{
    /* argv unuse */
    struct dispatch_event_t* dis_ev;

    while (! g_shutdown_flag) {
#ifndef WIN32
        pthread_mutex_lock(&dispatch_queue_mutex);
#endif
        /* キューにデータが入るまで待機します。*/
        while (que_empty(dispatch_queue)) {
#ifdef WIN32
            WaitForSingleObject(dispatch_queue_cond, INFINITE);
#else
            pthread_cond_wait(&dispatch_queue_cond, &dispatch_queue_mutex);
#endif
        }
#ifndef WIN32
        pthread_mutex_unlock(&dispatch_queue_mutex);
#endif
        /* キューからデータを取り出します。*/
        dis_ev = (struct dispatch_event_t*)que_pop(dispatch_queue);
        if (dis_ev == NULL)
            continue;

        /* dispatchを実行します。*/
        if (dis_ev->cmd_grp == CMDGRP_GET) {
            if (dis_ev->cn > 2) {
                int i;
                char** cl;

                /* key が複数指定されたときは指定された key が
                   同じデータストアに保存されている保障がないので
                   別々にコマンドを発行します。 */
                cl = split(dis_ev->cmdline, ' ');
                if (cl == NULL) {
                    err_write("dispatch_thread: no memory");
                    if (! dis_ev->noreply_flag)
                        reply_error(dis_ev->csocket, "no memory");
                    dis_ev_free(dis_ev);
                    continue;
                }
                for (i = 1; i < dis_ev->cn; i++) {
                    char cmdbuf[CMDLINE_SIZE];
                    int term_flag = 0;

                    snprintf(cmdbuf, sizeof(cmdbuf), "%s %s", cl[0], cl[i]);
                    if (i+1 == dis_ev->cn)
                        term_flag = 1;
                    do_dispatch(dis_ev->csocket,
                                dis_ev->cmd_grp,
                                cmdbuf,
                                cl[i],
                                dis_ev->dsize,
                                dis_ev->data,
                                dis_ev->noreply_flag,
                                "END",
                                term_flag);
                }
                list_free(cl);
            } else {
                /* single get, gets */
                do_dispatch(dis_ev->csocket,
                            dis_ev->cmd_grp,
                            dis_ev->cmdline,
                            dis_ev->key,
                            dis_ev->dsize,
                            dis_ev->data,
                            dis_ev->noreply_flag,
                            "END",
                            1);
            }
        } else {
            /* other get, gets command */
            do_dispatch(dis_ev->csocket,
                        dis_ev->cmd_grp,
                        dis_ev->cmdline,
                        dis_ev->key,
                        dis_ev->dsize,
                        dis_ev->data,
                        dis_ev->noreply_flag,
                        NULL,
                        1);
        }

        /* パラメータ領域の解放 */
        dis_ev_free(dis_ev);
    }

    /* スレッドを終了します。*/
#ifdef _WIN32
    _endthread();
#endif
}

static void create_dispatch_threads()
{
    int i;

    for (i = 0; i < g_conf->dispatch_threads; i++) {
#ifdef _WIN32
        uintptr_t thread_id;
#else
        pthread_t thread_id;
#endif
        /* スレッドを作成します。
           生成されたスレッドはリクエストキューが空のため、
           待機状態に入ります。*/
#ifdef _WIN32
        thread_id = _beginthread(dispatch_thread, 0, NULL);
#else
        pthread_create(&thread_id, NULL, (void*)dispatch_thread, NULL);
        /* スレッドの使用していた領域を終了時に自動的に解放します。*/
        pthread_detach(thread_id);
#endif
    }
}

int dispatch_event_entry(SOCKET csocket,
                         int cmd_grp,
                         const char* cmdline,
                         int cn,
                         const char** cl,
                         int dsize,
                         const char* data)
{
    struct dispatch_event_t* dis_ev;

    /* スレッドへ渡す情報を作成します */
    dis_ev = (struct dispatch_event_t*)calloc(1, sizeof(struct dispatch_event_t));
    if (dis_ev == NULL) {
        err_write("dispatch_event_entry: no memory.");
        return -1;
    }
    dis_ev->csocket = csocket;
    dis_ev->cmd_grp = cmd_grp;
    strcpy(dis_ev->cmdline, cmdline);
    strcpy(dis_ev->key, cl[1]);
    dis_ev->cn = cn;
    dis_ev->dsize = dsize;
    if (data) {
        dis_ev->data = (char*)malloc(dsize);
        if (dis_ev->data == NULL) {
            err_write("dispatch_event_entry: no memory.");
            dis_ev_free(dis_ev);
            return -1;
        }
        memcpy(dis_ev->data, data, dsize);
    }
    dis_ev->noreply_flag = noreply(cn, cl);

    /* dispatch情報をキューイング(push)します。*/
    que_push(dispatch_queue, dis_ev);

    /* キューイングされたことをスレッドへ通知します。*/
#ifdef WIN32
    SetEvent(dispatch_queue_cond);
#else
    pthread_mutex_lock(&dispatch_queue_mutex);
    pthread_cond_signal(&dispatch_queue_cond);
    pthread_mutex_unlock(&dispatch_queue_mutex);
#endif
    return 0;
}

int dispatch_server_start()
{
    /* メッセージキューの作成 */
    dispatch_queue = que_initialize();
    if (dispatch_queue == NULL)
        return -1;
    TRACE("%s initialized.\n", "dispatch queue");

    /* キューイング制御の初期化 */
#ifdef WIN32
    dispatch_queue_cond = CreateEvent(NULL, FALSE, FALSE, NULL);
#else
    pthread_mutex_init(&dispatch_queue_mutex, NULL);
    pthread_cond_init(&dispatch_queue_cond, NULL);
#endif

    /* ワーカースレッドを生成します。 */
    create_dispatch_threads();
    return 0;
}

void dispatch_server_end()
{
    if (dispatch_queue != NULL) {
        que_finalize(dispatch_queue);
        TRACE("%s terminated.\n", "dispatch queue");
    }

#ifdef WIN32
    CloseHandle(dispatch_queue_cond);
#else
    pthread_cond_destroy(&dispatch_queue_cond);
    pthread_mutex_destroy(&dispatch_queue_mutex);
#endif
}

int reply_error(SOCKET csocket, const char* msg)
{
    char buf[256];

    if (msg)
        snprintf(buf, sizeof(buf), "ERROR %s\r\n", msg);
    else
        strcpy(buf, "ERROR\r\n");

    if (send_data(csocket, buf, strlen(buf)) < 0) {
        err_write("reply_error: client send error=[%d].", last_error());
        return -1;
    }
    return 0;
}
