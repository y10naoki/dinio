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

/* 独自コマンドにてキーとデータの転送を行います。
 *
 *  【データ取得コマンド】
 *    bget <key><CRLF>
 *
 *    （応答フォーマット）
 *    +-+---------+---------+--------+------------+
 *    |V|<size>(4)|<stat>(1)|<cas>(8)|<data>(size)|
 *    +-+---------+---------+--------+------------+
 *    |*|<-------------- datablock -------------->|
 *
 *    先頭バイトが "V" でその後に32ビットの<size>が続きます。
 *    <size>は<data>のバイト数になります。
 *    <stat>に DATA_COMPRESS_Z ビットが立っている場合は<data>が
 *    zlib で圧縮されています。
 *    この場合の<size>は圧縮後のバイト数が設定されます。
 *
 *    key が not found の場合は先頭バイトに "n" が返されます。
 *    その他のエラーの場合は "e" が返されます。
 *
 *  【データ設定コマンド】
 *    bset <key><CRLF>
 *    <datablock>
 *
 *    （datablockフォーマット）
 *    +---------+---------+--------+------------+
 *    |<size>(4)|<stat(1)>|<cas(8)>|<data>(size)|
 *    +---------+---------+--------+------------+
 *    <size>は<data>のバイト数になります。
 *    <stat>に DATA_COMPRESS_Z ビットが立っている場合は<data>が
 *    zlib で圧縮されています。
 *    この場合の<size>は圧縮後のバイト数が設定されます。
 *
 *    <datablock> の後には <CRLF> は付きません。
 *
 *    （応答フォーマット）
 *    +--+
 *    |OK|
 *    +--+
 *    エラーの場合は"ER"が返されます。
 *
 *  【キー取得コマンド】
 *    bkeys<CRLF>
 *
 *    （応答フォーマット）
 *    +------------+--------+------------+--------+-----+---+
 *    |<keysize>(1)|<key>(n)|<keysize>(1)|<key>(n)| ... | 0 |
 *    +------------+--------+------------+--------+-----+---+
 *
 *    キー長とキーのペアが複数連続します。キー長がゼロの場合が
 *    終了にまります。
 *
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "dinio.h"

static void error_cmd(struct server_socket_t* ss,
                      const char* cmd,
                      const char* fmt)
{
    char tcmd[CMDLINE_SIZE];

    strrep(cmd, LINE_DELIMITER, "", tcmd);
    err_write(fmt, tcmd, ss->server->ip, ss->server->port);
}

static int wait_server(struct server_socket_t* ss, const char* cmd)
{
    if (g_conf->datastore_timeout >= 0) {
        /* サーバーからの応答を指定ミリ秒待ちます。*/
        if (! wait_recv_data(ss->socket, g_conf->datastore_timeout)) {
            /* サーバーからの応答がない。*/
            error_cmd(ss, cmd, "dataio: (%s) %s:%d data store server timeout.");
            return -1;
        }
    }
    return 0;
}

/*
 * データストアからキーのデータブロックを取得します。
 *
 * ss: サーバーソケット構造体のポインタ
 * key: キー領域のポインタ
 * dbsize: データブロックサイズが設定される領域のポインタ
 *         データが NOT FOUND の場合はゼロが設定されます
 *
 * 戻り値
 *  成功した場合はデータブロックのポインタを返します。
 *  エラーの場合は NULL を返します。
 */
char* bget_command(struct server_socket_t* ss,
                   const char* key,
                   int* dbsize)
{
    char cmd[CMDLINE_SIZE];
    char v;
    char stat;
    int64 cas;
    int dsize;
    char* dbuf = NULL;
    char* dbufp;
    int status;

    /* bget <key><CRLF> */
    snprintf(cmd, sizeof(cmd), "bget %s%s", key, LINE_DELIMITER);
    *dbsize = -1;

    /* サーバーにコマンドを送信します。*/
    if (send_data(ss->socket, cmd, strlen(cmd)) < 0) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d send error.");
        return NULL;
    }

    /* サーバーからの応答を待ちます。*/
    if (wait_server(ss, cmd) < 0)
        goto final;

    /* Vマークを受信します。*/
    if (recv_nchar(ss->socket, &v, sizeof(char), &status) != sizeof(char)) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d recv V mark error.");
        goto final;
    }
    if (v == 'n') {
        /* not found key */
        *dbsize = 0;
        goto final;
    }
    if (v != 'V') {
        if (v == 'e')
            error_cmd(ss, cmd, "dataio: (%s) %s:%d error 'e' mark.");
        else
            error_cmd(ss, cmd, "dataio: (%s) %s:%d illegal protocol V mark.");
        goto final;
    }

    /* データサイズを受信します。*/
    dsize = recv_int(ss->socket, &status);
    if (dsize < 1 || status != 0) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d recv data size error.");
        goto final;
    }

    /* <stat>を受信します。*/
    if (recv_nchar(ss->socket, &stat, sizeof(char), &status) != sizeof(char)) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d recv stat error.");
        goto final;
    }

    /* <cas>を受信します。*/
    cas = recv_int64(ss->socket, &status);
    if (cas < 1 || status != 0) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d recv cas error.");
        goto final;
    }

    /* データブロックの編集 */
    *dbsize = sizeof(int) + sizeof(char) + sizeof(int64) + dsize;
    dbuf = (char*)malloc(*dbsize);
    if (dbuf == NULL) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d no memory size=%d.");
        goto final;
    }
    dbufp = dbuf;
    memcpy(dbufp, &dsize, sizeof(int));
    dbufp += sizeof(int);
    memcpy(dbufp, &stat, sizeof(char));
    dbufp += sizeof(char);
    memcpy(dbufp, &cas, sizeof(int64));
    dbufp += sizeof(int64);

    /* データを受信します。*/
    if (recv_nchar(ss->socket, dbufp, dsize, &status) != dsize) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d recv data error.");
        free(dbuf);
        dbuf = NULL;
        goto final;
    }

final:
    return dbuf;
}

/*
 * データストアへキーとデータブロックを更新します。
 *
 * キーが存在しない場合は追加されます。
 * キーが存在する場合は cas 値も含めて上書きされます。
 *
 * ss: サーバーソケット構造体のポインタ
 * key: キー領域のポインタ
 * dbsize: データブロックサイズ
 * datablock: データブロックの領域のポインタ
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int bset_command(struct server_socket_t* ss,
                 const char* key,
                 int dbsize,
                 const char* datablock)
{
    char cmd[CMDLINE_SIZE];
    int cmdlen;
    char* dbuf;
    int result = 0;

    /* bset <key><CRLF>
       <datablock>
     */
    snprintf(cmd, sizeof(cmd), "bset %s%s", key, LINE_DELIMITER);
    cmdlen = strlen(cmd);

    dbuf = malloc(cmdlen + dbsize);
    if (dbuf == NULL) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d mb_alloc no memory.");
        return -1;
    }
    memcpy(dbuf, cmd, cmdlen);
    memcpy(dbuf+cmdlen, datablock, dbsize);

    /* サーバーにコマンドを送信します。*/
    if (send_data(ss->socket, dbuf, cmdlen+dbsize) < 0) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d send error.");
        result = -1;
    }
    free(dbuf);

    if (result == 0) {
        char resp_str[2];
        int status;

        /* 応答データ(OK/ER)を受信します。*/
        if (recv_nchar(ss->socket, resp_str, 2, &status) != 2) {
            error_cmd(ss, cmd, "dataio: (%s) %s:%d recv data error.");
            return -1;
        }
        if (memcmp(resp_str, "OK", 2) != 0) {
            error_cmd(ss, cmd, "dataio: (%s) %s:%d resp error.");
            return -1;
        }
    }
    return result;
}

/*
 * データストアからすべてのキーを取得するコマンドを送信します。
 *
 * ss: サーバーソケット構造体のポインタ
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int bkeys_command(struct server_socket_t* ss)
{
    char cmd[16];

    /* bkeys<CRLF>
     */
    snprintf(cmd, sizeof(cmd), "bkeys%s", LINE_DELIMITER);

    /* サーバーにコマンドを送信します。*/
    if (send_data(ss->socket, cmd, strlen(cmd)) < 0) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d send error.");
        return -1;
    }

    /* サーバーからの応答を待ちます。*/
    return wait_server(ss, cmd);
}

/*
 * データストアからキーを削除するコマンドを送信します。
 *
 * ss: サーバーソケット構造体のポインタ
 * key: キー
 *
 * 戻り値
 *  成功した場合はゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int delete_noreply_command(struct server_socket_t* ss, const char* key)
{
    int result = 0;
    char cmd[CMDLINE_SIZE];

    snprintf(cmd, sizeof(cmd), "delete %s noreply%s", key, LINE_DELIMITER);

    /* サーバーにコマンドを送信します。*/
    if (send_data(ss->socket, cmd, strlen(cmd)) < 0) {
        error_cmd(ss, cmd, "dataio: (%s) %s:%d send error.");
        result = -1;
    }
    return result;
}
