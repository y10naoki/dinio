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
#ifndef _DINIO_H_
#define _DINIO_H_

#include "nestalib.h"
#include "ds_server.h"

#define PROGRAM_NAME        "dinio"
#define PROGRAM_VERSION     "0.3.0"

#define DEFAULT_PORT                    11211   /* memcached listen port */
#define DEFAULT_BACKLOG                 100     /* listen backlog number */
#define DEFAULT_WORKER_THREADS          8       /* worker threads number */
#define DEFAULT_DISPATCH_THREADS        20      /* dispatch threads number */
#define DEFAULT_DATASTORE_TIMEOUT       3000    /* datastore request timeout(ms) */
#define DEFAULT_LOCK_WAIT_TIME          180     /* datastore lock wait time(sec) */
#define DEFAULT_ACTIVE_CHECK_INTERVAL   60      /* datastore check interval time(sec) */
#define DEFAULT_POOL_INIT_CONNECTIONS   10      /* pooling initial connections number */
#define DEFAULT_POOL_EXT_CONNECTIONS    20      /* pooling extend connections number */
#define DEFAULT_POOL_EXT_RELEASE_TIME   180     /* 3 min */
#define DEFAULT_POOL_WAIT_TIME          10      /* 10 sec */
#define DEFAULT_REPLICATIONS            2       /* two servers copied */
#define DEFAULT_REPLICATION_THREADS     3       /* replication worker threads number */
#define DEFAULT_REPLICATION_DELAY_TIME  0       /* replication delay time(ms) */
#define DEFAULT_INFORMED_PORT           15432   /* imformed port number */

#define STATUS_CMD          "__/status/__"
#define SHUTDOWN_CMD        "__/shutdown/__"
#define ADDSERVER_CMD       "__/addserver/__"
#define REMOVESERVER_CMD    "__/removeserver/__"
#define UNLOCKSERVER_CMD    "__/unlockserver/__"
#define HASHSERVER_CMD      "__/hashserver/__"
#define IMPORTDATA_CMD      "__/importdata/__"

#define CMDGRP_SET      1   /* update group */
#define CMDGRP_GET      2   /* retrieval group */
#define CMDGRP_DELETE   3   /* deletion group */

#define LINE_DELIMITER  "\r\n"

#define MAX_MEMCACHED_KEYSIZE   250
#define MAX_MEMCACHED_DATASIZE  (1*1024*1024)   /* 1MB */

#define CMDLINE_SIZE          (256+MAX_MEMCACHED_KEYSIZE)

#define FRIEND_ADD_SERVER     1
#define FRIEND_REMOVE_SERVER  2
#define FRIEND_LOCK_SERVER    3
#define FRIEND_UNLOCK_SERVER  4

#define FRIEND_ACK     'A'
#define FRIEND_REJECT  'R'

#define FRIEND_WAIT_TIME  3000   /* 3 seconds in the greatest waiting time */

/* thread argument */
struct thread_args_t {
    SOCKET client_socket;
    struct sockaddr_in sockaddr;
};

/* program configuration */
struct dinio_conf_t {
    int daemonize;                      /* execute as daemon(Linux/MacOSX only) */
    char username[256];                 /* execute as username(Linux/MacOSX only) */
    ushort port_no;                     /* listen port number */
    int backlog;                        /* listen backlog number */
    int worker_threads;                 /* memcached worker thread number */
    int dispatch_threads;               /* dispatch worker thread number */
    char error_file[MAX_PATH+1];        /* error file name */
    char output_file[MAX_PATH+1];       /* output file name */
    int datastore_timeout;              /* datastore request timeout(ms) */
    int lock_wait_time;                 /* datastore lock wait time(sec) */
    int active_check_interval;          /* datastore active check time(sec) */
    int auto_detach;                    /* datastore auto detach mode */
    int pool_init_conns;                /* pooling initial connections */
    int pool_ext_conns;                 /* pooling extend connections */
    int pool_ext_release_time;          /* pooling extend connection release time(sec) */
    int pool_wait_time;                 /* pooling connection max wait time(sec) */
    char server_file[MAX_PATH+1];       /* server(data store) define file name */
    int replications;                   /* copy server number */
    int replication_threads;            /* replication worker threads number */
    int replication_delay_time;         /* replication delay time(ms) */
    ushort informed_port;               /* port number to be informed of the state of other servers */
    char friend_file[MAX_PATH+1];       /* dinio server define file name */
};

/* friend server */
struct friend_t {
    char ip[16];            /* 255.255.255.255 */
    int port;               /* max 65535 */
};

/* macros */
#define TRACE(fmt, ...) \
    if (g_trace_mode) { \
        fprintf(stdout, fmt, __VA_ARGS__); \
    }

#ifdef _WIN32
#define get_abspath(abs_path, path, maxlen) \
    _fullpath(abs_path, path, maxlen)
#else
#define get_abspath(abs_path, path, maxlen) \
    realpath(path, abs_path)
#endif

/* global variables */
#ifndef _MAIN
    extern
#endif
struct dinio_conf_t* g_conf;  /* read only configure data */

#ifndef _MAIN
    extern
#endif
SOCKET g_listen_socket;     /* listen socket */

#ifndef _MAIN
    extern
#endif
int g_shutdown_flag;        /* not zero is shutdown mode */

#ifndef _MAIN
    extern
#endif
int g_trace_mode;           /* not zero is trace mode */

#ifndef _MAIN
    extern
#endif
struct queue_t* g_queue;    /* request queue */

#ifndef _MAIN
    extern
#endif
int64 g_start_time;         /* start time of server */

#ifndef _MAIN
    extern
#endif
struct ds_server_t* g_dss;  /* data store server */

#ifndef _MAIN
    extern
#endif
struct friend_t* g_friend_list; /* dinio friend server list */

#ifndef _MAIN
    extern
#endif
SOCKET g_informed_socket;     /* informed listen socket */

#ifndef _MAIN
    extern
#endif
struct queue_t* g_informed_queue;    /* informed request queue */

#ifndef _MAIN
    extern
#endif
void* g_sock_event;         /* socket event */

#ifndef _MAIN
    extern
#endif
struct hash_t* g_sockbuf_hash;

/* prototypes */
#ifdef __cplusplus
extern "C" {
#endif

/* config.c */
int config(const char* conf_fname);

/* dinio_server.c */
void dinio_server(void);
SOCKET socket_listen(ulong addr, ushort port, int backlog, struct sockaddr_in* sockaddr);

/* dinio_cmd.c */
void stop_server(void);
void status_server(void);
void add_server(const char* addr, const char* port, const char* scale_factor);
void remove_server(const char* addr, const char* port);
void unlock_server(const char* addr, const char* port);
void hash_server(int n, const char* keys[]);
void import_server(const char* fname);

/* memc_gateway.c */
int memcached_gateway_start(void);
int memcached_gateway_event(SOCKET socket, struct sockaddr_in sockaddr);
void memcached_gateway_end(void);

/* server_cmd.c */
void status_command(SOCKET socket);
void shutdown_command(SOCKET socket);
int add_server_command(SOCKET socket, int cn, const char** cl);
int remove_server_command(SOCKET socket, int cn, const char** cl);
int unlock_server_command(SOCKET socket, int cn, const char** cl);
int hash_command(SOCKET socket, int cn, const char** cl);
int import_command(SOCKET socket, int cn, const char** cl);

/* dispatch.c */
int dispatch_event_entry(SOCKET csocket, int cmd_grp, const char* cmdline, int cn, const char** cl, int dsize, const char* data);
int dispatch_server_start(void);
void dispatch_server_end(void);
int reply_error(SOCKET csocket, const char* msg);

/* replication.c */
int do_replication(struct server_t* org_server, int cmd_grp, const char* key);
int replication_queue_count(void);
int replication_event_entry(struct server_t* org_server, int cmd_grp, const char* key);
int replication_server_start(void);
void replication_server_end(void);

/* dataio.c */
char* bget_command(struct server_socket_t* ss, const char* key, int* dbsize);
int bset_command(struct server_socket_t* ss, const char* key, int dbsize, const char* datablock);
int bkeys_command(struct server_socket_t* ss);
int delete_noreply_command(struct server_socket_t* ss, const char* key);

/* redistribution.c */
int add_redist_target(struct server_t* server, struct server_t** nserver, struct server_t** dserver);
int add_redistribution(struct server_t* server, struct server_t* nserver, struct server_t* dserver);
int remove_redist_target(struct server_t* server, struct server_t** nserver, struct server_t** tserver);
int remove_redistribution(struct server_t* server, struct server_t* nserver, struct server_t* tserver);

/* friend.c */
struct friend_t* friend_create(const char* def_fname);
void friend_close(struct friend_t* friend_list);
int friend_add_server(struct friend_t* friend_list, struct server_t* server);
int friend_remove_server(struct friend_t* friend_list, struct server_t* server);
int friend_lock_server(struct friend_t* friend_list, struct server_t* server);
int friend_unlock_server(struct friend_t* friend_list, struct server_t* server);

/* informed.c */
int friend_informed_start(void);
int friend_informed_event(SOCKET socket, struct sockaddr_in sockaddr);
void friend_informed_end(void);

/* lock_server.c */
int lock_servers(struct server_t* server, struct server_t* oserver);
void unlock_servers(struct server_t* server, struct server_t* oserver);

#ifdef __cplusplus
}
#endif

#endif  /* _DINIO_H_ */
