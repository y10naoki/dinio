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
#ifndef _CONSISTENT_HASH_H_
#define _CONSISTENT_HASH_H_

#include "ds_server.h"

typedef int (*CMP_FUNC)(const void*, const void*);

/* node on continuum */
struct node_t {
    unsigned int point;         /* point on circle */
    struct server_t* server;    /* server info */
    int server_flag;            /* real server is 1, virtual server is 0 */
};

/* consistent hash(continuum) */
struct consistent_hash_t {
    int num_node;
    struct node_t* node_array;
    struct server_t** phys_node_list;   /* physical server list */
};

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

struct consistent_hash_t* ch_create(int server_count, struct server_t** server_list, int node_count);
void ch_close(struct consistent_hash_t* ch);
unsigned int ch_hash(const char* key, int keysize);
struct node_t* ch_get_node(struct consistent_hash_t* ch, const char* key, int keysize);
int ch_remove_server(struct consistent_hash_t* ch, struct server_t* server);
int ch_add_server(struct consistent_hash_t* ch, struct server_t* server);
struct server_t* ch_next_server(struct consistent_hash_t* ch, struct server_t* server);

#ifdef __cplusplus
}
#endif

#endif  /* _CONSISTENT_HASH_H_ */
