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

#include "nestalib.h"
#include "consistent_hash.h"

static int node_compare(struct node_t* a, struct node_t* b)
{
    if (a->point < b->point)
        return -1;
    if (a->point > b->point)
        return 1;
    return 0;
}

static void free_consistent_hash(struct consistent_hash_t* ch)
{
    if (ch) {
        if (ch->node_array)
            free(ch->node_array);
        if (ch->phys_node_list)
            free(ch->phys_node_list);
        free(ch);
    }
}

static int create_server_node(struct consistent_hash_t* ch,
                              struct server_t* server,
                              int ins)
{
    char skey[30];
    unsigned int h;
    int i;
    int n = 0;

    /* サーバーのハッシュ値を求めます(ip-port)。*/
    sprintf(skey, "%s-%d", server->ip, server->port);
    h = ch_hash(skey, strlen(skey));
    ch->node_array[ins+n].point = h;
    ch->node_array[ins+n].server = server;
    ch->node_array[ins+n].server_flag = 1;
    n++;

    /* スケールファクタ分の仮想ノードを作成します(ip-index)。*/
    for (i = 0; i < server->scale_factor; i++) {
        sprintf(skey, "%s-%d", server->ip, i);

        h = ch_hash(skey, strlen(skey));
        ch->node_array[ins+n].point = h;
        ch->node_array[ins+n].server = server;
        ch->node_array[ins+n].server_flag = 0;
        n++;
    }
    return n;
}

static void remove_node_server(struct consistent_hash_t* ch,
                               struct server_t* server)
{
    int i = 0;

    while (i < ch->num_node) {
        struct node_t* node;

        node = &ch->node_array[i];
        if (node->server == server) {
            int shift_n;

            shift_n = ch->num_node - i - 1;
            if (shift_n > 0) {
                memmove(&ch->node_array[i],
                        &ch->node_array[i+1],
                        sizeof(struct node_t) * shift_n);
            }
            ch->num_node--;
        } else {
            i++;
        }
    }
}

static int physical_node(struct consistent_hash_t* ch)
{
    int i;
    int n = 0;

    for (i = 0; i < ch->num_node; i++) {
        if (ch->node_array[i].server_flag)
            ch->phys_node_list[n++] = ch->node_array[i].server;
    }
    return n;
}

/*
 * コンシステントハッシュを作成します。
 *
 * server_count: 物理サーバー数
 * server_list: サーバー構造体のポインタ配列
 * node_count: 仮想ノード数
 *
 * 戻り値
 *  コンシステントハッシュ構造体のポインタを返します。
 *  エラーの場合は NULL を返します。
 */
struct consistent_hash_t* ch_create(int server_count,
                                    struct server_t** server_list,
                                    int node_count)
{
    struct consistent_hash_t* ch;
    int i;
    int n = 0;

    ch = (struct consistent_hash_t*)calloc(1, sizeof(struct consistent_hash_t));
    if (ch == NULL) {
        err_write("ch_create(): no memory.");
        return NULL;
    }

    /* スケールファクタに応じた仮想ノードを作成します。*/
    ch->num_node = server_count + node_count;
    ch->node_array = malloc(ch->num_node * sizeof(struct node_t));
    if (ch->node_array == NULL) {
        err_write("ch_create(): no memory.");
        free_consistent_hash(ch);
        return NULL;
    }

    /* 物理ノードリストを作成します。*/
    ch->phys_node_list = (struct server_t**)calloc(MAX_SERVER_NUM, sizeof(struct server_t*));
    if (ch->phys_node_list == NULL) {
        err_write("ch_create(): no memory.");
        free_consistent_hash(ch);
        return NULL;
    }

    /* ノードを作成します。 */
    for (i = 0; i < server_count; i++)
        n += create_server_node(ch, server_list[i], n);

    /* ハッシュ値を昇順にソートします。*/
    qsort((void*)ch->node_array,
          ch->num_node,
          sizeof(struct node_t),
          (CMP_FUNC)node_compare);

    /* ソートされたノードから物理ノードリストを作成します。*/
    physical_node(ch);
    return ch;
}

/*
 * コンシステントハッシュを終了します。
 *
 * ch: コンシステントハッシュ構造体のポインタ
 *
 * 戻り値
 *  なし
 */
void ch_close(struct consistent_hash_t* ch)
{
    if (ch == NULL)
        return;

    free_consistent_hash(ch);
}

/*
 * キーから符号なし整数のハッシュ値を算出します。
 * ハッシュ関数は MurmurHash2A を使用します。
 *
 * key: キー
 * keysize: キーサイズ
 *
 * 戻り値
 *  ハッシュ値
 */
unsigned int ch_hash(const char* key, int keysize)
{
    return MurmurHash2A(key, keysize, 1001);
}

/*
 * コンシステントハッシュの円周上に配置されたノードからキー値に
 * 対応したノードを求めます。
 *
 * ch: コンシステントハッシュ構造体のポインタ
 * key: キー
 * keysize: キーサイズ
 *
 * 戻り値
 *  ノード構造体のポインタ
 */
struct node_t* ch_get_node(struct consistent_hash_t* ch,
                           const char* key,
                           int keysize)
{
    unsigned int h;
    int highp, maxp, lowp, midp;
    unsigned int midval, midval1;
    struct node_t* node_array;
    struct node_t* target_node = NULL;

    if (ch == NULL)
        return NULL;

    /* キーのハッシュ値を求めます。*/
    h = ch_hash(key, keysize);

    /* 円周上に配置されたノードを２分探索で検索します。*/
    highp = ch->num_node;
    maxp = highp;
    lowp = 0;
    node_array = ch->node_array;

    while (1) {
        midp = (lowp+highp) / 2;
        if (midp == maxp) {
            if (midp == ch->num_node)
                midp = 1;
            target_node = &node_array[midp-1];
            break;
        }
        midval = node_array[midp].point;
        midval1 = (midp == 0)? 0 : node_array[midp-1].point;

        if (h <= midval && h > midval1) {
            target_node = &node_array[midp];
            break;
        }

        if (midval < h)
            lowp = midp + 1;
        else
            highp = midp - 1;

        if (lowp > highp) {
            target_node = &node_array[0];
            break;
        }
    }
    return target_node;
}

/*
 * コンシステントハッシュからサーバーを削除します。
 * 仮想ノード上のサーバーも削除されます。
 *
 * ch: コンシステントハッシュ構造体のポインタ
 * server: データストアサーバー構造体のポインタ
 *
 * 戻り値
 *  成功したらゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int ch_remove_server(struct consistent_hash_t* ch,
                     struct server_t* server)
{
    if (ch == NULL)
        return -1;

    /* 仮想ノードを削除します。*/
    remove_node_server(ch, server);

    /* 物理ノードリストを作成します。*/
    physical_node(ch);
    return 0;
}

/*
 * コンシステントハッシュに仮想ノードを追加します。
 *
 * ch: コンシステントハッシュ構造体のポインタ
 * server: データストアサーバー構造体のポインタ
 *
 * 戻り値
 *  成功したらゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int ch_add_server(struct consistent_hash_t* ch,
                  struct server_t* server)
{
    size_t new_size;
    int n;
    struct node_t* tp;

    /* 仮想ノードを作成します。*/
    new_size = sizeof(struct node_t) * (ch->num_node + server->scale_factor + 1);
    tp = (struct node_t*)realloc(ch->node_array, new_size);
    if (tp == NULL) {
        err_write("ch_add_server(): no memory.");
        return -1;
    }
    ch->node_array = tp;
    n = create_server_node(ch, server, ch->num_node);
    ch->num_node += n;

    /* ハッシュ値を昇順にソートします。*/
    qsort((void*)ch->node_array,
          ch->num_node,
          sizeof(struct node_t),
          (CMP_FUNC)node_compare);

    /* ソートされたノードから物理ノードリストを作成します。*/
    physical_node(ch);
    return 0;
}
