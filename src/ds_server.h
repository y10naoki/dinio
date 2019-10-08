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
#ifndef _DS_SERVER_H_
#define _DS_SERVER_H_

#include "nestalib.h"
#include "consistent_hash.h"

#define MAX_SERVER_NUM  1000    /* max number of data-store servers */

/* server status */
#define DSS_PREPARE    0
#define DSS_ACTIVE     1
#define DSS_INACTIVE   2
#define DSS_LOCKED     3

/* physical server info */
struct server_t {
    CS_DEF(critical_section);
    int status;             /* status */
    char ip[16];            /* 255.255.255.255 */
    int port;               /* max 65535 */
    int scale_factor;       /* node scale number(standard=100) */
    struct pool_t* pool;    /* server socket pool */
    int64 set_count;        /* count of execute set command */
    int64 get_count;        /* count of execute get command */
    int64 del_count;        /* count of execute delete command */
};

/* data-store server info */
struct ds_server_t {
    CS_DEF(critical_section);
    int num_server;                     /* number of servers */
    struct server_t** server_list;      /* server list */
    struct consistent_hash_t* ch;       /* consistent hash */
};

/* server socket */
struct server_socket_t {
    struct server_t* server;
    SOCKET socket;
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

/* ds_server.c */
int ds_create(const char* svrdef_fname);
void ds_close();
struct server_t* ds_get_server(const char* ip, int port);
struct server_t* ds_create_server(const char* ip, int port, int scale_factor);
int ds_detach_server(struct server_t* server);
int ds_attach_server(struct server_t* server);
struct server_t* ds_next_server(struct server_t* server);
struct server_t* ds_key_server(const char* key, int keysize);
void ds_lock_server(struct server_t* server);
void ds_unlock_server(struct server_t* server);
int ds_check_server(struct server_t* server);

/* ds_check.c */
void ds_active_check_thread(void* argv);

/* connect.c */
int ds_connect();
void ds_disconnect();
int ds_connect_server(struct server_t* server);
void ds_disconnect_server(struct server_t* server);
struct server_socket_t* ds_server_socket(struct server_t* server);
void ds_release_socket(struct server_t* server, struct server_socket_t* ss, int result);

#ifdef __cplusplus
}
#endif

#endif  /* _DS_SERVER_H_ */
