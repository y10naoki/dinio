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

static int server_index(struct server_t* server)
{
    int i;

    for (i = 0; i < g_dss->num_server; i++) {
        if (g_dss->server_list[i] == server)
            return i;
    }
    return -1;  /* not found */
}

static void free_memory()
{
    if (g_dss) {
        if (g_dss->server_list) {
            int i;

            for (i = 0; i < g_dss->num_server; i++)
                free(g_dss->server_list[i]);
            free(g_dss->server_list);
        }
        free(g_dss);
        g_dss = NULL;
    }
}

static int parse_server_line(const char* sline,
                             struct server_t* server)
{
    char* delim = " ";
    char* tok;
    char* port;
    char* factor;
    char* endptr = 0;

    /* ip-addr */
    tok = strtok((char*)sline, delim);
    if ((strlen(tok)) > 15) {
        err_write("invalid IP-ADDR value in server definitions.");
        return -1;
    }
    strncpy(server->ip, tok, strlen(tok));

    /* port */
    tok = strtok(NULL, delim);
    port = (char*)alloca(strlen(tok)+1);
    strncpy(port, tok, strlen(tok));
    port[strlen(tok)] = '\0';

    errno = 0;
    server->port = strtol(port, &endptr, 10);
    if (errno == ERANGE || endptr == port) {
        err_write("invalid port number value in server definitions.");
        return -1;
    }

    /* scale_factor */
    tok = strtok(NULL, delim);
    factor = (char*)alloca(strlen(tok)+1);
    strncpy(factor, tok, strlen(tok));
    factor[strlen(tok)] = '\0';

    errno = 0;
    server->scale_factor = strtol(factor, &endptr, 10);
    if (errno == ERANGE || endptr == factor) {
        err_write("invalid scale factor value in server definitions.");
        return -1;
    }
    return 0;
}

static struct server_t** read_server_file(const char* filename,
                                          unsigned int* server_count)
{
    FILE* fp;
    char buf[R_BUF_SIZE];
    struct server_t** server_list = NULL;
    unsigned int num_servers = 0;

    *server_count = 0;

    fp = fopen(filename, "r");
    if (! fp) {
        err_write("can't open server define file = %s", filename);
        return NULL;
    }

    server_list = (struct server_t**)calloc(MAX_SERVER_NUM, sizeof(struct server_t*));
    if (server_list == NULL) {
        err_write("server list no memory.");
        fclose(fp);
        return NULL;
    }

    while (fgets(buf, sizeof(buf), fp) != NULL) {
        struct server_t server;

        trim(buf);
        if (strlen(buf) < 2 || buf[0] == '#')
            continue;

        memset(&server, '\0', sizeof(struct server_t));
        if (parse_server_line(buf, &server) < 0)
            continue;

        if (strlen(server.ip) > 0 && server.scale_factor > 0) {
            if (num_servers+1 > MAX_SERVER_NUM) {
                err_write("server count over %d", MAX_SERVER_NUM);
                break;
            }
            server_list[num_servers] = (struct server_t*)calloc(1, sizeof(struct server_t));
            memcpy(server_list[num_servers], &server, sizeof(struct server_t));
            num_servers++;
            TRACE("define data store server %s:%d #%d\n",
                  server.ip, server.port, server.scale_factor);
        }
    }
    fclose(fp);

    *server_count = num_servers;
    return server_list;
}

static unsigned int server_node_count()
{
    int i;
    unsigned int n = 0;

    for (i = 0; i < g_dss->num_server; i++)
        n += g_dss->server_list[i]->scale_factor;
    return n;
}

/*
 * データストアサーバーの構造体を作成します。
 *
 * サーバー定義ファイルを読んで物理サーバー情報を作成します。
 * 仮想ノード数は標準で 100 とし、スケールファクタの値に応じて増分します。
 * 処理性能の高いサーバーはスケールファクタに大きな値を指定します。
 *
 * サーバーに接続してコネクションをプールします。
 *
 * [サーバー定義ファイルのフォーマット]
 * # ip-addr    port  scale-factor
 * 192.168.10.1 11211 100
 * 192.168.10.2 11211 200
 * 192.168.10.3 11211 150
 * 192.168.10.4 11211 120
 * 192.168.10.5 11211 100
 *
 * svrdef_fname: サーバー定義ファイル名
 *
 * 戻り値
 *  成功するとゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int ds_create(const char* svrdef_fname)
{
    unsigned int server_count;
    unsigned int node_count;
    int i;

    g_dss = (struct ds_server_t*)calloc(1, sizeof(struct ds_server_t));
    if (g_dss == NULL) {
        err_write("ds_create(): no memory.\n");
        return -1;
    }

    /* サーバー定義ファイルを読み込みます。*/
    g_dss->server_list = read_server_file(svrdef_fname, &server_count);

    if (g_dss->server_list == NULL) {
        free(g_dss);
        return -1;
    }
    g_dss->num_server = server_count;

    /* サーバーに接続してコネクションをプールします。
       コネクションが確立できないサーバーはリストから外されます。
       コネクションが確立できたサーバー数が返されます。*/
    if (ds_connect() <= 0) {
        free_memory();
        return -1;
    }

    /* 仮想ノード数を求めます。*/
    node_count = server_node_count();

    /* コンシステントハッシュを作成します。*/
    g_dss->ch = ch_create(g_dss->num_server, g_dss->server_list, node_count);
    if (g_dss->ch == NULL) {
        ds_disconnect();
        free_memory();
        return -1;
    }

    /* サーバーのステータスを稼動中に変更します。*/
    for (i = 0; i < g_dss->num_server; i++) {
        g_dss->server_list[i]->status = DSS_ACTIVE;
        /* クリティカルセクションの初期化 */
        CS_INIT(&g_dss->server_list[i]->critical_section);
    }

    /* クリティカルセクションの初期化 */
    CS_INIT(&g_dss->critical_section);

    /* データストアの状態を監視するスレッドを作成します。*/
    {
#ifdef _WIN32
        uintptr_t thread_id;
        thread_id = _beginthread(ds_active_check_thread, 0, NULL);
#else
        pthread_t thread_id;
        pthread_create(&thread_id, NULL, (void*)ds_active_check_thread, NULL);
        /* スレッドの使用していた領域を終了時に自動的に解放します。*/
        pthread_detach(thread_id);
#endif
    }
    return 0;
}

/*
 * データストアサーバーをクローズします。
 *
 * 戻り値
 *  なし
 */
void ds_close()
{
    if (g_dss == NULL)
        return;

    /* クリティカルセクションの削除 */
    CS_DELETE(&g_dss->critical_section);

    /* コンシステントハッシュのクローズ */
    ch_close(g_dss->ch);

    /* サーバーとの接続を切断してコネクションプールを開放します。*/
    ds_disconnect();

    /* メモリを解放します。 */
    /* g_dss が NULL になり、データストアを監視するスレッドが終了します。*/
    free_memory();
}

/*
 * データストアサーバーからサーバーを検索します。
 *
 * ip: IPアドレス
 * port: ポート番号
 *
 * 戻り値
 *  サーバー構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
struct server_t* ds_get_server(const char* ip, int port)
{
    int i;

    if (g_dss == NULL)
        return NULL;

    for (i = 0; i < g_dss->num_server; i++) {
        if (strcmp(g_dss->server_list[i]->ip, ip) == 0 &&
            g_dss->server_list[i]->port == port)
            return g_dss->server_list[i];
    }
    return NULL;
}

/*
 * サーバーを作成します。
 *
 * ip: IPアドレス
 * port: ポート番号
 * scale_factor: 仮想ノード数（100を標準として指定）
 *
 * 戻り値
 *  成功したらサーバー構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
struct server_t* ds_create_server(const char* ip,
                                  int port,
                                  int scale_factor)
{
    struct server_t* server;

    server = (struct server_t*)calloc(1, sizeof(struct server_t));
    if (server == NULL) {
        err_write("ds_create_server(): no memory.");
        return NULL;
    }

    strcpy(server->ip, ip);
    server->port = port;
    server->scale_factor = scale_factor;

    /* クリティカルセクションの初期化 */
    CS_INIT(&server->critical_section);

    TRACE("create data store server %s:%d #%d\n",
          server->ip, server->port, server->scale_factor);
    return server;
}

/*
 * サーバーをコンシステントハッシュから取り除きます。
 * またコネクションプールが解放されます。
 *
 * データの再配分はこの関数では行われません。
 *
 * server: サーバー構造体のポインタ
 *
 * 戻り値
 *  成功したらゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int ds_detach_server(struct server_t* server)
{
    int s_index;
    int shift_n;

    if (g_dss == NULL)
        return -1;

    /* コンシステントハッシュからサーバーを削除します。*/
    ch_remove_server(g_dss->ch, server);

    /* サーバーを削除します。*/
    s_index = server_index(server);
    if (s_index < 0)
        return -1;

    /* クリティカルセクションの削除 */
    CS_DELETE(&server->critical_section);

    TRACE("detach data store server %s:%d #%d\n",
          server->ip, server->port, server->scale_factor);

    /* コネクションプールを解放します。*/
    pool_finalize(g_dss->server_list[s_index]->pool);

    /* 動的に確保したメモリを解放します。*/
    free(g_dss->server_list[s_index]);

    shift_n = g_dss->num_server - s_index - 1;
    if (shift_n > 0) {
        memmove(&g_dss->server_list[s_index],
                &g_dss->server_list[s_index+1],
                sizeof(struct server_t*) * shift_n);
    }
    g_dss->num_server--;
    return 0;
}

/*
 * サーバーをコンシステントハッシュに追加します。
 * サーバーとのコネクションプールを作成します。
 *
 * データの再配分はこの関数では行われません。
 *
 * server: サーバー構造体のポインタ
 *
 * 戻り値
 *  成功したらゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int ds_attach_server(struct server_t* server)
{
    /* サーバーリストに追加します。*/
    g_dss->server_list[g_dss->num_server] = server;

    /* コネクションプールを作成します。*/
    if (ds_connect_server(server) < 0)
        return -1;

    server->status = DSS_ACTIVE;
    g_dss->num_server++;

    /* コンシステントハッシュに追加します。*/
    if (ch_add_server(g_dss->ch, server) < 0)
        return -1;
    return 0;
}

/*
 * コンシステントハッシュから次の物理サーバーを取得します。
 *
 * server: データストア構造体のポインタ
 *
 * 戻り値
 *  次のサーバー構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
struct server_t* ds_next_server(struct server_t* server)
{
    struct consistent_hash_t* ch;
    int i;

    ch = g_dss->ch;

    for (i = 0; i < g_dss->num_server; i++) {
        if (ch->phys_node_list[i] == server) {
            if (i == g_dss->num_server-1)
                return ch->phys_node_list[0];
            return ch->phys_node_list[i+1];
        }
    }
    return NULL;
}

/*
 * コンシステントハッシュからキーに対応したサーバーを取得します。
 *
 * key: キー値
 * keysize: キー長(バイト数)
 *
 * 戻り値
 *  サーバー構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
struct server_t* ds_key_server(const char* key, int keysize)
{
    struct node_t* node;

    node = ch_get_node(g_dss->ch, key, keysize);
    if (node == NULL)
        return NULL;
    return node->server;
}

/*
 * サーバーをロック状態に設定します。
 *
 * サーバーの追加や削除でデータの再配分が行われている場合は
 * 対象サーバーが一時的にロックの状態になります。
 *
 * server: サーバー構造体のポインタ
 *
 * 戻り値
 *  なし
 */
void ds_lock_server(struct server_t* server)
{
    CS_START(&server->critical_section);
    server->status = DSS_LOCKED;
}

/*
 * サーバーのロックを解除します。
 *
 * server: サーバー構造体のポインタ
 *
 * 戻り値
 *  なし
 */
void ds_unlock_server(struct server_t* server)
{
    server->status = DSS_ACTIVE;
    CS_END(&server->critical_section);
}

/*
 * サーバーの状態をチェックします。
 * ロックされていた場合は ACTIVE になるまで指定された時間待ちます。
 * 最大待ち時間（秒）は dinio.lock_wait_time で指定します。
 * INACTIVE の場合はエラーになります。
 *
 * server: サーバー構造体のポインタ
 *
 * 戻り値
 *  ACTIVE の場合はゼロを返します。
 *  それ以外は -1 を返します。
 */
int ds_check_server(struct server_t* server)
{
    int wait_time;
    int stime = 1;

    if (server->status == DSS_ACTIVE)
        return 0;
    if (server->status == DSS_INACTIVE)
        return -1;

#ifdef _WIN32
    /* windowsのSleep()はミリ秒になります。*/
    stime *= 1000;
#endif

    wait_time = g_conf->lock_wait_time;
    do {
        if (server->status == DSS_ACTIVE)
            return 0;
        /* sleep 1 second */
        sleep(stime);
    } while (--wait_time > 0);

    /* server was locked */
    return -1;
}
