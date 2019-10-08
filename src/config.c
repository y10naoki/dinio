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

#define R_BUF_SIZE 1024
#define CMD_INCLUDE "include"

/*
 * コンフィグファイルを読んでパラメータを設定します。
 * 既知のパラメータ以外は無視されます。
 * パラメータの形式は "name=value"の形式とします。
 *
 * conf_fname: コンフィグファイル名
 *
 * 戻り値
 *  0: 成功
 * -1: 失敗
 *
 * (config parameters)
 * dinio.daemon = 1 or 0 (default is 0, unix only)
 * dinio.username = string (default is no)
 * dinio.port_no = number (default is 11211)
 * dinio.backlog = number (default is 100)
 * dinio.worker_threads = number (default is 8)
 * dinio.dispatch_threads = number (default is 20)
 * dinio.error_file = path/file (default is stderr)
 * dinio.output_file = path/file (default is stdout)
 * dinio.trace_flag = 1 or 0 (default is 0)
 * dinio.datastore_timeout = number(default is 3000(ms))
 * dinio.active_wait_time = number(default is 180(sec))
 * dinio.active_check_interval = number(default is 30(sec))
 * dinio.auto_detach = 1 or 0 (default is 0)
 * dinio.pool_init_conns = number(default is 20)
 * dinio.pool_ext_conns = number(default is 80)
 * dinio.pool_ext_release_time = number(default is 1800(sec))
 * dinio.pool_wait_time = number(default is 10(sec))
 * dinio.server_file = path/file(defaut is no)
 * dinio.replications = number(default is 2)
 * dinio.replication_threads = number(default is 3)
 * dinio.replication_delay_time = number(default is 0(ms))
 * dinio.informed_port = number(default is 15432)
 * dinio.friend_file = path/file(default is no)
 * include = FILE_NAME
 * ...
 */
int config(const char* conf_fname)
{
    FILE *fp;
    char fpath[MAX_PATH+1];
    char buf[R_BUF_SIZE];
    int err = 0;

    get_abspath(fpath, conf_fname, MAX_PATH);
    if ((fp = fopen(fpath, "r")) == NULL) {
        fprintf(stderr, "file open error: %s\n", conf_fname);
        return -1;
    }

    while (fgets(buf, sizeof(buf), fp) != NULL) {
        int index;
        char name[R_BUF_SIZE];
        char value[R_BUF_SIZE];

        /* コメントの排除 */
        index = indexof(buf, '#');
        if (index >= 0) {
            buf[index] = '\0';
            if (strlen(buf) == 0)
                continue;
        }
        /* 名前と値の分離 */
        index = indexof(buf, '=');
        if (index <= 0)
            continue;

        substr(name, buf, 0, index);
        substr(value, buf, index+1, -1);

        /* 両端のホワイトスペースを取り除きます。*/
        trim(name);
        trim(value);

        if (strlen(name) > MAX_VNAME_SIZE-1) {
            fprintf(stderr, "parameter name too large: %s\n", buf);
            err = -1;
            break;
        }
        if (strlen(value) > MAX_VVALUE_SIZE-1) {
            fprintf(stderr, "parameter value too large: %s\n", buf);
            err = -1;
            break;
        }

        if (stricmp(name, "dinio.port_no") == 0) {
            g_conf->port_no = (ushort)atoi(value);
        } else if (stricmp(name, "dinio.backlog") == 0) {
            g_conf->backlog = atoi(value);
        } else if (stricmp(name, "dinio.worker_threads") == 0) {
            g_conf->worker_threads = atoi(value);
        } else if (stricmp(name, "dinio.dispatch_threads") == 0) {
            g_conf->dispatch_threads = atoi(value);
        } else if (stricmp(name, "dinio.daemon") == 0) {
            g_conf->daemonize = atoi(value);
        } else if (stricmp(name, "dinio.username") == 0) {
            strncpy(g_conf->username, value, sizeof(g_conf->username)-1);
        } else if (stricmp(name, "dinio.error_file") == 0) {
            if (strlen(value) > 0)
                get_abspath(g_conf->error_file, value, sizeof(g_conf->error_file)-1);
        } else if (stricmp(name, "dinio.output_file") == 0) {
            if (strlen(value) > 0)
                get_abspath(g_conf->output_file, value, sizeof(g_conf->output_file)-1);
        } else if (stricmp(name, "dinio.trace_flag") == 0) {
            g_trace_mode = atoi(value);
        } else if (stricmp(name, "dinio.datastore_timeout") == 0) {
            g_conf->datastore_timeout = atoi(value);
        } else if (stricmp(name, "dinio.lock_wait_time") == 0) {
            g_conf->lock_wait_time = atoi(value);
        } else if (stricmp(name, "dinio.active_check_interval") == 0) {
            g_conf->active_check_interval = atoi(value);
        } else if (stricmp(name, "dinio.auto_detach") == 0) {
            g_conf->auto_detach = atoi(value);
        } else if (stricmp(name, "dinio.pool_init_conns") == 0) {
            g_conf->pool_init_conns = atoi(value);
        } else if (stricmp(name, "dinio.pool_ext_conns") == 0) {
            g_conf->pool_ext_conns  = atoi(value);
        } else if (stricmp(name, "dinio.pool_ext_release_time") == 0) {
            g_conf->pool_ext_release_time = atoi(value);
        } else if (stricmp(name, "dinio.pool_wait_time") == 0) {
            g_conf->pool_wait_time = atoi(value);
        } else if (stricmp(name, "dinio.server_file") == 0) {
            if (strlen(value) > 0)
                get_abspath(g_conf->server_file, value, sizeof(g_conf->server_file)-1);
        } else if (stricmp(name, "dinio.replications") == 0) {
            g_conf->replications = atoi(value);
        } else if (stricmp(name, "dinio.replication_threads") == 0) {
            g_conf->replication_threads = atoi(value);
        } else if (stricmp(name, "dinio.replication_delay_time") == 0) {
            g_conf->replication_delay_time = atoi(value);
        } else if (stricmp(name, "dinio.informed_port") == 0) {
            g_conf->informed_port = atoi(value);
        } else if (stricmp(name, "dinio.friend_file") == 0) {
            if (strlen(value) > 0)
                get_abspath(g_conf->friend_file, value, sizeof(g_conf->friend_file)-1);
        } else if (stricmp(name, CMD_INCLUDE) == 0) {
            /* 他のconfigファイルを再帰処理で読み込みます。*/
            if (config(value) < 0)
                break;
        } else {
            /* ignore */
        }
    }

    if (g_conf->worker_threads < 1)
        g_conf->worker_threads = 1;
    if (g_conf->dispatch_threads < 1)
        g_conf->worker_threads = 1;

    fclose(fp);
    return err;
}
