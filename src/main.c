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

#define _MAIN
#include "dinio.h"

#ifndef WIN32
#include <pwd.h>
#endif

#define DEFAULT_CONF_FILE  "./conf/" PROGRAM_NAME ".conf"

#define ACT_START          0
#define ACT_STOP           1
#define ACT_STATUS         2
#define ACT_ADD_SERVER     3
#define ACT_REMOVE_SERVER  4
#define ACT_UNLOCK_SERVER  5
#define ACT_HASH           6
#define ACT_IMPORT         7

static char* conf_file = NULL;  /* config file name */
static int action = ACT_START;  /* ACT_START, ACT_STOP, ACT_STATUS,
                                   ACT_ADD_SERVER, ACT_REMOVE_SERVER,
                                   ACT_HASH, ACT_IMPORT */

static char* arg_addr = NULL;
static char* arg_port = "11211";
static char* arg_scale_factor = "100";
static char** arg_keys = NULL;
static int arg_keyc = 0;
static char* arg_impfile = NULL;

static int shutdown_done_flag = 0;  /* shutdown済みフラグ */
static int cleanup_done_flag = 0;   /* cleanup済みフラグ */

static CS_DEF(shutdown_lock);
static CS_DEF(cleanup_lock);

static void version()
{
    fprintf(stdout, "%s/%s\n", PROGRAM_NAME, PROGRAM_VERSION);
    fprintf(stdout, "Copyright (c) 2010-2011 YAMAMOTO Naoki\n\n");
}

static void usage()
{
    version();
    fprintf(stdout, "usage: %s {ACTION} [-f conf.file]\n", PROGRAM_NAME);
    fprintf(stdout, "ACTION:\n");
    fprintf(stdout, "  -add ip-addr port[11211] scale-factor[100]\n");
    fprintf(stdout, "  -remove ip-addr port[11211]\n");
    fprintf(stdout, "  -unlock ip-addr port[11211]\n");
    fprintf(stdout, "  [-start]\n");
    fprintf(stdout, "  -status\n");
    fprintf(stdout, "  -stop\n");
    fprintf(stdout, "  -hash key ...\n");
    fprintf(stdout, "  -import /path/filename\n");
    fprintf(stdout, "  -version\n\n");
}

static void cleanup()
{
    /* Ctrl-Cで終了させたときに Windowsでは dinio_server()の
       メインループでも割り込みが起きて後処理のcleanup()が
       呼ばれるため、１回だけ実行されるように制御します。*/
    CS_START(&cleanup_lock);
    if (! cleanup_done_flag) {
        logout_finalize();
        err_finalize();
        sock_finalize();
        mt_finalize();
        cleanup_done_flag = 1;
    }
    CS_END(&cleanup_lock);
}

static void sig_handler(int signo)
{
    if (signo == SIGINT || signo == SIGTERM) {
        /* Windowsでは、メインスレッドのみで割り込みがかかるが、
           古いカーネル pthread ではすべてのスレッドに割り込みがかかるため、
           プログラムの停止を１回のみ実行するように制御します。*/
        CS_START(&shutdown_lock);
        if (! shutdown_done_flag) {
            cleanup();
            shutdown_done_flag = 1;
            if (action == ACT_START)
                printf("\n%s was terminated.\n", PROGRAM_NAME);
        }
        exit(0);
        CS_END(&shutdown_lock);
#ifndef _WIN32
    } else if (signo == SIGPIPE) {
        /* ignore */
#endif
    }
}

static int startup()
{
    /* グローバル変数の初期化 */
    g_listen_socket = INVALID_SOCKET;
    g_informed_socket = INVALID_SOCKET;

    /* 割り込み処理用のクリティカルセクション初期化 */
    CS_INIT(&shutdown_lock);
    CS_INIT(&cleanup_lock);

    /* マルチスレッド対応関数の初期化 */
    mt_initialize();

    /* ソケット関数の初期化 */
    sock_initialize();

    /* エラーファイルの初期化 */
    err_initialize(g_conf->error_file);

    /* アウトプットファイルの初期化 */
    logout_initialize(g_conf->output_file);

    /* 割り込みハンドラーの登録 */
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
#ifndef _WIN32
    signal(SIGPIPE, sig_handler);
#endif
    return 0;
}

static int arg_hash_key(int argc, char* argv[], int* cur_index)
{
    int i;

    i = *cur_index;
    while (i < argc) {
        if (*argv[i] == '-')
            break;
        if (strlen(argv[i]) > MAX_MEMCACHED_KEYSIZE) {
            fprintf(stdout, "key length too large, %d > %d\n.",
                            (int)strlen(argv[i]), MAX_MEMCACHED_KEYSIZE);
            return -1;
        }
        arg_keys[arg_keyc++] = argv[i++];
    }
    if (arg_keyc == 0) {
        fprintf(stdout, "no input key.\n");
        return -1;
    }
    *cur_index = i - 1;
    return 0;
}

static int parse(int argc, char* argv[])
{
    int i;

    for (i = 1; i < argc; i++) {
        if (strcmp("-start", argv[i]) == 0) {
            action = ACT_START;
        } else if (strcmp("-stop", argv[i]) == 0) {
            action = ACT_STOP;
        } else if (strcmp("-status", argv[i]) == 0) {
            action = ACT_STATUS;
        } else if (strcmp("-add", argv[i]) == 0 ||
                   strcmp("-remove", argv[i]) == 0 ||
                   strcmp("-unlock", argv[i]) == 0) {
            if (strcmp("-add", argv[i]) == 0)
                action = ACT_ADD_SERVER;
            else if (strcmp("-remove", argv[i]) == 0)
                action = ACT_REMOVE_SERVER;
            else
                action = ACT_UNLOCK_SERVER;
            if (++i < argc)
                arg_addr = argv[i];
            else {
                fprintf(stdout, "no input ip-addr.\n");
                return -1;
            }
            if (i+1 < argc) {
                if (*argv[i+1] != '-')
                    arg_port = argv[++i];
            }
            if (action == ACT_ADD_SERVER) {
                if (i+1 < argc) {
                    if (*argv[i+1] != '-')
                        arg_scale_factor = argv[++i];
                }
            }
        } else if (strcmp("-hash", argv[i]) == 0) {
            arg_keys = malloc(sizeof(char*) * (argc - 2));
            if (arg_keys == NULL) {
                fprintf(stdout, "no memory.\n");
                return -1;
            }
            i++;
            if (arg_hash_key(argc, argv, &i) < 0)
                return 1;
            action = ACT_HASH;
        } else if (strcmp("-import", argv[i]) == 0) {
            if (++i < argc)
                arg_impfile = argv[i];
            else {
                fprintf(stdout, "no import file.\n");
                return -1;
            }
            action = ACT_IMPORT;
        } else if (strcmp("-version", argv[i]) == 0 ||
                   strcmp("--version", argv[i]) == 0) {
            version();
            return 1;
        } else if (strcmp("-f", argv[i]) == 0) {
            if (++i < argc)
                conf_file = argv[i];
            else {
                fprintf(stdout, "no config file.\n");
                return -1;
            }
        } else {
            return -1;
        }
    }
    return 0;
}

static int parse_config()
{
    /* コンフィグ領域の確保 */
    g_conf = (struct dinio_conf_t*)calloc(1, sizeof(struct dinio_conf_t));
    if (g_conf == NULL) {
        fprintf(stderr, "no memory.\n");
        return -1;
    }

    /* デフォルト値を設定します。*/
    g_conf->port_no = DEFAULT_PORT;
    g_conf->backlog = DEFAULT_BACKLOG;
    g_conf->worker_threads = DEFAULT_WORKER_THREADS;
    g_conf->dispatch_threads = DEFAULT_DISPATCH_THREADS;
    g_conf->datastore_timeout = DEFAULT_DATASTORE_TIMEOUT;
    g_conf->lock_wait_time = DEFAULT_LOCK_WAIT_TIME;
    g_conf->active_check_interval = DEFAULT_ACTIVE_CHECK_INTERVAL;
    g_conf->pool_init_conns = DEFAULT_POOL_INIT_CONNECTIONS;
    g_conf->pool_ext_conns = DEFAULT_POOL_EXT_CONNECTIONS;
    g_conf->pool_ext_release_time = DEFAULT_POOL_EXT_RELEASE_TIME;
    g_conf->pool_wait_time = DEFAULT_POOL_WAIT_TIME;
    g_conf->replications = DEFAULT_REPLICATIONS;
    g_conf->replication_threads = DEFAULT_REPLICATION_THREADS;
    g_conf->replication_delay_time = DEFAULT_REPLICATION_DELAY_TIME;
    g_conf->informed_port = DEFAULT_INFORMED_PORT;

    /* コンフィグファイル名がパラメータで指定されていない場合は
       デフォルトのファイル名を使用します。*/
    if (conf_file == NULL)
        conf_file = DEFAULT_CONF_FILE;

    /* コンフィグファイルの解析 */
    config(conf_file);
    return 0;
}

#ifndef WIN32
static int change_user()
{
    if (getuid() == 0 || geteuid() == 0) {
        struct passwd* pw;

        if (g_conf->username[0] == '\0') {
            fprintf(stderr, "can't run as root, please user switch -u\n");
            return 1;
        }
        if ((pw = getpwnam(g_conf->username)) == 0) {
            fprintf(stderr, "can't find the user %s\n", g_conf->username);
            return 1;
        }
        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
            fprintf(stderr, "change user failed, %s\n", g_conf->username);
            return 1;
        }
    }
    return 0;
}
#endif

int main(int argc, char* argv[])
{
    int ret;

    /* パラメータ解析 */
    if ((ret = parse(argc, argv)) != 0) {
        if (ret < 0)
            usage();
        return 1;
    }

    /* コンフィグファイルの処理 */
    if (parse_config() < 0)
        return 1;

#ifndef WIN32
    if (action == ACT_START) {
        /* ユーザーの切換 */
        if (change_user() != 0)
            return 1;
    }
#endif

#ifndef _WIN32
    if (action == ACT_START) {
        if (g_conf->daemonize) {
#ifdef MAC_OSX
            if (daemon(1, 0) != 0)
                fprintf(stderr, "daemon() error\n");
#else
            if (daemon(0, 0) != 0)
                fprintf(stderr, "daemon() error\n");
#endif
        }
    }
#endif

    /* 初期処理 */
    if (startup() < 0)
        return 1;

    if (action == ACT_START)
        dinio_server();
    else if (action == ACT_STOP)
        stop_server();
    else if (action == ACT_STATUS)
        status_server();
    else if (action == ACT_ADD_SERVER)
        add_server(arg_addr, arg_port, arg_scale_factor);
    else if (action == ACT_REMOVE_SERVER)
        remove_server(arg_addr, arg_port);
    else if (action == ACT_UNLOCK_SERVER)
        unlock_server(arg_addr, arg_port);
    else if (action == ACT_HASH)
        hash_server(arg_keyc, (const char**)arg_keys);
    else if (action == ACT_IMPORT)
        import_server(arg_impfile);

    /* 後処理 */
    cleanup();
    if (arg_keys)
        free(arg_keys);
    free(g_conf);

#ifdef WIN32
    _CrtDumpMemoryLeaks();
#endif
    return 0;
}
