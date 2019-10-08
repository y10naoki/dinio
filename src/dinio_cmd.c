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

static void server_cmd(const char* cmd)
{
    SOCKET c_socket;
    char* recvbuf = NULL;
    int okay_flag = 0;

    c_socket = sock_connect_server("127.0.0.1", g_conf->port_no);
    if (c_socket != INVALID_SOCKET) {
        char cmd_str[256];

        snprintf(cmd_str, sizeof(cmd_str), "%s\r\n", cmd);
        if (send_data(c_socket, cmd_str, strlen(cmd_str)) > 0) {
            recvbuf = recv_str(c_socket, LINE_DELIMITER, 0);
            if (recvbuf)
                okay_flag = 1;
        }
    }

    if (okay_flag) {
        fprintf(stdout, "\n%s", recvbuf);
#ifndef _WIN32
        fprintf(stdout, "\n");
#endif
    } else
        fprintf(stdout, "\n%s\n\n", "not running.");

    if (recvbuf)
        recv_free(recvbuf);
    if (c_socket != INVALID_SOCKET)
        SOCKET_CLOSE(c_socket);
}

void stop_server()
{
    server_cmd(SHUTDOWN_CMD);
}

void status_server()
{
    server_cmd(STATUS_CMD);
}

void add_server(const char* addr, const char* port, const char* scale_factor)
{
    char cmd[1024];

    if (addr == NULL)
        return;
    snprintf(cmd, sizeof(cmd), "%s %s %s %s", ADDSERVER_CMD, addr, port, scale_factor);
    server_cmd(cmd);
}

void remove_server(const char* addr, const char* port)
{
    char cmd[1024];

    if (addr == NULL)
        return;
    snprintf(cmd, sizeof(cmd), "%s %s %s", REMOVESERVER_CMD, addr, port);
    server_cmd(cmd);
}

void unlock_server(const char* addr, const char* port)
{
    char cmd[1024];

    if (addr == NULL)
        return;
    snprintf(cmd, sizeof(cmd), "%s %s %s", UNLOCKSERVER_CMD, addr, port);
    server_cmd(cmd);
}

void hash_server(int n, const char* keys[])
{
    char* cmd;
    int i;

    if (keys == NULL)
        return;

    cmd = (char*)alloca(64 + n * MAX_MEMCACHED_KEYSIZE);
    sprintf(cmd, "%s %d", HASHSERVER_CMD, n);
    for (i = 0; i < n; i++) {
        if (strlen(keys[i]) > MAX_MEMCACHED_KEYSIZE) {
            fprintf(stdout, "\n%s: %s\n", "key length too large.", keys[i]);
            return;
        }
        strcat(cmd, " ");
        strcat(cmd, keys[i]);
    }
    server_cmd(cmd);
}

void import_server(const char* fname)
{
    char cmd[64+MAX_PATH];

    if (fname == NULL)
        return;
    if (strlen(fname) > MAX_PATH) {
        fprintf(stdout, "\n%s: %s\n", "file name length too large.", fname);
        return;
    }
    snprintf(cmd, sizeof(cmd), "%s %s", IMPORTDATA_CMD, fname);
    server_cmd(cmd);
}
