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

/*
 * データストアをロックします。
 * ロックされたことを他の分散サーバーに通知します。
 *
 * server: サーバー構造体のポインタ
 * oserver: 一緒にロックするサーバー構造体のポインタ
 *
 * 戻り値
 *  成功したらゼロを返します。
 *  エラーの場合は -1 を返します。
 */
int lock_servers(struct server_t* server, struct server_t* oserver)
{
    if (friend_lock_server(g_friend_list, server) < 0)
        return -1;
    ds_lock_server(server);

    if (oserver) {
        if (friend_lock_server(g_friend_list, oserver) < 0) {
            friend_unlock_server(g_friend_list, server);
            ds_unlock_server(server);
            return -1;
        }
        ds_lock_server(oserver);
    }
    return 0;
}

/*
 * データストアのロックを解放します。
 * ロックが解放されたことを他の分散サーバーに通知します。
 *
 * server: サーバー構造体のポインタ
 * oserver: 一緒に解放するサーバー構造体のポインタ
 *
 * 戻り値
 *  なし
 */
void unlock_servers(struct server_t* server, struct server_t* oserver)
{
    if (oserver) {
        friend_unlock_server(g_friend_list, oserver);
        ds_unlock_server(oserver);
    }

    friend_unlock_server(g_friend_list, server);
    ds_unlock_server(server);
}
