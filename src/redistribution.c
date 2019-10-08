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

/* 独自コマンドにてキーとデータの再配分を行います。
 *
 * 【データストアが追加された場合】
 *     1. 追加されたサーバーの次のサーバーが管理するキーをすべて取得します。
 *
 *     2. キーのコンシステントハッシュ値を求めて追加されたサーバーに
 *        転送するか判定します。
 *
 *     3. 転送する場合はサーバーにデータを追加します。
 *
 *     4. レプリケーション数に応じて転記したデータをサーバーから削除します。
 *
 * 【データストアが削除された場合】
 *     条件 (num_server-1 > replications) が成り立つ場合のみ
 *
 *     1. 削除するサーバーの次のサーバーが保持するキーをすべて取得します。
 *
 *     2. キーのコンシステントハッシュ値を求めて削除するサーバーが
 *        管理していたデータか判定します。
 *
 *     3. 削除するサーバーが管理していたデータの場合は
 *        レプリケーション数プラス１先のサーバーにデータを追加します。
 *
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "dinio.h"

#define MAX_MEMCACHED_KEYSIZE   250

static struct server_t* replica_delete_server(struct server_t* nserver)
{
    int n;
    struct server_t* dserver;

    n = g_conf->replications;
    dserver = nserver;
    while (dserver && n > 0) {
        dserver = ds_next_server(dserver);
        n--;
    }
    if (dserver == nserver)
        dserver = NULL;
    return dserver;
}

static int recv_key(SOCKET socket, char* key)
{
    unsigned char ksize;
    int keysize;
    int status;

    /* キーサイズを受信します。*/
    if (recv_nchar(socket, (char*)&ksize, sizeof(ksize), &status) != sizeof(ksize))
        return -1;
    if (ksize == 0)
        return 0;
    keysize = ksize;
    if (keysize > MAX_MEMCACHED_KEYSIZE)
        return 0;

    /* キーを受信します。*/
    if (recv_nchar(socket, key, keysize, &status) != keysize)
        return -1;
    return keysize;
}

/*
 * データストアを追加するときにデータの再配分を行う
 * サーバーを取得します。
 *
 * データを保持する必要のないサーバーがない場合は *dserver に
 * NULL が設定されます。
 *
 * server: 追加するサーバー構造体のポインタ
 * nserver: 再配分を行うサーバー構造体のポインタアドレス
 * dserver: データを保持する必要のないサーバー構造体のポインタアドレス
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int add_redist_target(struct server_t* server,
                      struct server_t** nserver,
                      struct server_t** dserver)
{
    *nserver = ds_next_server(server);
    if (*nserver == NULL)
        return -1;

    /* データを保持する必要のないサーバーを求めます。*/
    *dserver = NULL;
    if (g_conf->replications > 0) {
        /* 配分したことでレプリカを保持する必要のない
           サーバーを求めます。*/
        *dserver = replica_delete_server(*nserver);
        if (*dserver == server)
            *dserver = NULL;
    } else {
        /* レプリケーションは行われないため、
           転送したデータは元のサーバーから削除します。*/
        *dserver = *nserver;
    }
    return 0;
}

/*
 * データストアが追加されたときにデータの再配分を行います。
 *
 * 次に配置されているサーバーからキーをすべて取得して再配分します。
 *
 * server: 追加したサーバー構造体のポインタ
 * nserver: 再配分を行うサーバー構造体のポインタ
 * dserver: データを保持する必要のないサーバー構造体のポインタ
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int add_redistribution(struct server_t* server,
                       struct server_t* nserver,
                       struct server_t* dserver)
{
    int result = 0;
    struct server_socket_t* ss = NULL;
    struct server_socket_t* nss = NULL;
    struct server_socket_t* dss = NULL;
    struct server_socket_t* rss = NULL;
    int reset_ss = 0;
    int reset_nss = 0;
    int reset_dss = 0;
    int reset_rss = 0;
    int target_num = 0;
    int redist_num = 0;

    TRACE("redistribution(add): start %s:%d\n", server->ip, server->port);

    ss = ds_server_socket(server);
    if (ss == NULL) {
        result = -1;
        goto final;
    }

    nss = ds_server_socket(nserver);
    if (nss == NULL) {
        result = -1;
        goto final;
    }

    rss = ds_server_socket(nserver);
    if (rss == NULL) {
        result = -1;
        goto final;
    }

    if (dserver)
        dss = ds_server_socket(dserver);

    /* すべてのキーを取得するコマンドを次サーバーへ送信します。*/
    if (bkeys_command(rss) < 0) {
        result = -1;
        reset_rss = -1;
        goto final;
    }

    /* +-------+------+-------+------+-----+------+
     * |size(1)|key(n)|size(1)|key(n)| ... | 0(1) |
     * +-------+------+-------+------+-----+------+
     */
    while (result == 0) {
        int keysize;
        char key[MAX_MEMCACHED_KEYSIZE+1];

        keysize = recv_key(rss->socket, key);
        if (keysize < 0) {
            result = -1;
            reset_rss = -1;
            break;
        }
        if (keysize == 0)
            break;  /* end */

        /* キーの配置先を判定します。*/
        if (ds_key_server(key, keysize) == server) {
            char* data;
            int dsize;

            /* 転送します。*/
            key[keysize] = '\0';
            data = bget_command(nss, key, &dsize);
            if (data) {
                result = bset_command(ss, key, dsize, data);
                if (result == 0) {
                    /* 必要のないキーのレプリカは削除します。*/
                    if (dss) {
                        if (delete_noreply_command(dss, key) < 0) {
                            result = -1;
                            reset_dss = -1;
                        }
                    }
                } else {
                    reset_ss = -1;
                }
                free(data);
            }
            redist_num++;
        }
        target_num++;
    }

final:
    if (dss)
        ds_release_socket(dserver, dss, reset_dss);
    if (rss)
        ds_release_socket(nserver, rss, reset_rss);
    if (nss)
        ds_release_socket(nserver, nss, reset_nss);
    if (ss)
        ds_release_socket(server, ss, reset_ss);

    logout_write("redistribution(add): %s:%d -> %s:%d result=%d (%d/%d)",
                 nserver->ip, nserver->port, server->ip, server->port,
                 result, redist_num, target_num);
    TRACE("redistribution(add): end   %s:%d result=%d (%d/%d)\n",
          server->ip, server->port, result, redist_num, target_num);
    return result;
}

/*
 * データストアを削除するときにデータの再配分を行う
 * サーバーを取得します。
 *
 * server: 削除するサーバー構造体のポインタ
 * nserver: データを保持するサーバー構造体のポインタアドレス
 * tserver: データを転送するサーバー構造体のポインタアドレス
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int remove_redist_target(struct server_t* server,
                         struct server_t** nserver,
                         struct server_t** tserver)
{
    int i;

    /* 削除する次のサーバーを求めます。*/
    *nserver = ds_next_server(server);
    if (*nserver == NULL)
        return -1;

    /* データを転送するサーバーを求めます。*/
    *tserver = *nserver;
    for (i = 0; i < g_conf->replications; i++) {
        *tserver = ds_next_server(*tserver);
        if (*tserver == NULL)
            return -1;
    }
    return 0;
}

/*
 * データストアを削除するときにデータの再配分を行います。
 *
 * レプリケーション数を維持するためにデータを転記します。
 *
 * 例)
 * replications: 2
 * servers: 4    (a) - (b) - (c) - (d)
 *   - (b) を削除する
 *   - (b) のデータは (c) と (d) にもある
 *   - (c) のデータを (a) に複写する
 *
 * server: 削除するサーバー構造体のポインタ
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int remove_redistribution(struct server_t* server,
                          struct server_t* nserver,
                          struct server_t* tserver)
{
    int result = 0;
    struct server_socket_t* nss = NULL;
    struct server_socket_t* tss = NULL;
    struct server_socket_t* rss = NULL;
    int reset_nss = 0;
    int reset_tss = 0;
    int reset_rss = 0;
    int target_num = 0;
    int redist_num = 0;

    TRACE("redistribution(remove): start %s:%d\n", server->ip, server->port);

    nss = ds_server_socket(nserver);
    if (nss == NULL) {
        result = -1;
        goto final;
    }

    tss = ds_server_socket(tserver);
    if (tss == NULL) {
        result = -1;
        goto final;
    }

    rss = ds_server_socket(nserver);
    if (rss == NULL) {
        result = -1;
        goto final;
    }

    /* すべてのキーを取得するコマンドを次サーバーへ送信します。*/
    if (bkeys_command(rss) < 0) {
        result = -1;
        reset_rss = -1;
        goto final;
    }

    /* +-------+------+-------+------+-----+------+
     * |size(1)|key(n)|size(1)|key(n)| ... | 0(1) |
     * +-------+------+-------+------+-----+------+
     */
    while (result == 0) {
        int keysize;
        char key[MAX_MEMCACHED_KEYSIZE+1];

        keysize = recv_key(rss->socket, key);
        if (keysize < 0) {
            result = -1;
            reset_rss = -1;
            break;
        }
        if (keysize == 0)
            break;  /* end */

        /* キーの配置先を判定します。*/
        if (ds_key_server(key, keysize) == server) {
            char* data;
            int dsize;

            /* データを転送します。*/
            key[keysize] = '\0';
            data = bget_command(nss, key, &dsize);
            if (data) {
                result = bset_command(tss, key, dsize, data);
                if (result < 0)
                    reset_tss = -1;
                free(data);
            }
            redist_num++;
        }
        target_num++;
    }

final:
    if (rss)
        ds_release_socket(nserver, rss, reset_rss);
    if (tss)
        ds_release_socket(tserver, tss, reset_tss);
    if (nss)
        ds_release_socket(nserver, nss, reset_nss);

    logout_write("redistribution(remove): %s:%d -> %s:%d result=%d (%d/%d)",
                 server->ip, server->port, nserver->ip, nserver->port,
                 result, redist_num, target_num);
    TRACE("redistribution(remove): end   %s:%d result=%d (%d/%d)\n",
          server->ip, server->port, result, redist_num, target_num);
    return result;
}
