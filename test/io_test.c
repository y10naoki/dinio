/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * The MIT License
 *
 * Copyright (c) 2010 YAMAMOTO Naoki
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
#ifdef _WIN32
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "nestalib.h"

#define CMD_GET    1
#define CMD_SET    2
#define CMD_DELETE 3

#define KEY_SIZE 8

struct thread_args_t {
    int tno;
};

static char* _cmd = "get";
static char* _ip = "127.0.0.1";
static int _port = 11211;
static int _threads = 1;
static int _st_num = 0;
static int _end_num = 1;
static int _dsize = 1000;
static int _noreply = 0;

static int64 _start_utime;

static void usage()
{
    printf("io_test [option]\n");
    printf("  [option]\n");
    printf("    -c command { [get] | set | delete }\n");
    printf("    -a server address [127.0.0.1]\n");
    printf("    -p server port number [11211]\n");
    printf("    -t number of thread [1]\n");
    printf("    -n number of command [1]\n");
    printf("    -s start number [0]\n");
    printf("    -l data size [1000]\n");
    printf("    -noreply\n");
}

static int args(int argc, char* argv[])
{
    int i;
    int num = 0;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-c") == 0) {
            if (++i < argc)
                _cmd = argv[i];
        } else if (strcmp(argv[i], "-a") == 0) {
            if (++i < argc)
                _ip = argv[i];
        } else if (strcmp(argv[i], "-p") == 0) {
            if (++i < argc)
                _port = atoi(argv[i]);
        } else if (strcmp(argv[i], "-t") == 0) {
            if (++i < argc)
                _threads = atoi(argv[i]);
        } else if (strcmp(argv[i], "-n") == 0) {
            if (++i < argc)
                num = atoi(argv[i]);
        } else if (strcmp(argv[i], "-s") == 0) {
            if (++i < argc)
                _st_num = atoi(argv[i]);
        } else if (strcmp(argv[i], "-l") == 0) {
            if (++i < argc)
                _dsize = atoi(argv[i]);
        } else if (strcmp(argv[i], "-noreply") == 0) {
            _noreply = 1;
        } else
            return -1;
    }
    if (num > 0)
        _end_num = _st_num + num;
    return 0;
}

static int read_data(SOCKET socket)
{
    while (1) {
        char buf[1024];
        int len;
        int status;

        len = recv_char(socket, buf, sizeof(buf), &status);
        if (len == 0 || status != 0)
            break;
        if (! wait_recv_data(socket, RCV_TIMEOUT_NOWAIT))
            break;
    }
    return 0;
}

static int read_result(SOCKET socket, int count)
{
    while (count > 0) {
        char buf[1024];
        int len;

        len = recv_line(socket, buf, sizeof(buf), "\r\n");
        if (len < 0) {
            printf("recv_line() : error\n");
            break;
        }
        count--;
    }
    return 0;
}

static int set_command(SOCKET socket, const char* key, int dsize, const char* data)
{
    char cmd[128];

    if (_noreply)
        snprintf(cmd, sizeof(cmd), "set %s 0 0 %d noreply\r\n", key, dsize);
    else
        snprintf(cmd, sizeof(cmd), "set %s 0 0 %d\r\n", key, dsize);

    if (send_data(socket, cmd, strlen(cmd)) < 0) {
        printf("send cmd error %s", cmd);
        return -1;
    }
    if (send_data(socket, data, dsize+2) < 0) {  /* add CR/LF */
        printf("send cmd error %s", cmd);
        return -1;
    }
    if (! _noreply) {
        if (read_result(socket, 1) < 0)
            return -1;
    }
    return 0;
}

static int get_command(SOCKET socket, const char* key)
{
    char cmd[128];

    snprintf(cmd, sizeof(cmd), "get %s\r\n", key);
    if (send_data(socket, cmd, strlen(cmd)) < 0) {
        printf("send cmd error %s", cmd);
        return -1;
    }
    if (read_data(socket) < 0)
        return -1;
    return 0;
}

static int delete_command(SOCKET socket, const char* key)
{
    char cmd[128];

    if (_noreply)
        snprintf(cmd, sizeof(cmd), "delete %s noreply\r\n", key);
    else
        snprintf(cmd, sizeof(cmd), "delete %s\r\n", key);

    if (send_data(socket, cmd, strlen(cmd)) < 0) {
        printf("send cmd error %s", cmd);
        return -1;
    }
    if (! _noreply) {
        if (read_result(socket, 1) < 0)
            return -1;
    }
    return 0;
}

static int quit_command(SOCKET socket)
{
    char* cmd = "quit\r\n";

    if (send_data(socket, cmd, strlen(cmd)) < 0) {
        printf("send error %s", cmd);
        return -1;
    }
    return 0;
}

static void do_thread(void* argv)
{
    struct thread_args_t* args;
    int tno;
    SOCKET socket;
    int cmdno = 0;
    int i;
    char* data = NULL;
    int64 cur_utime, elapsed_utime;
    int64 prev_utime = _start_utime;

    args = (struct thread_args_t*)argv;
    tno = args->tno;
    free(args);

    if (strcmp(_cmd, "get") == 0)
        cmdno = CMD_GET;
    else if (strcmp(_cmd, "set") == 0)
        cmdno = CMD_SET;
    else if (strcmp(_cmd, "delete") == 0)
        cmdno = CMD_DELETE;
    else {
        printf("[%d] cmd error=%s\n", tno, _cmd);
        return;
    }

    socket = sock_connect_server(_ip, _port);
    if (socket == INVALID_SOCKET) {
        printf("%s:%d can't connect server.\n", _ip, _port);
        return;
    }

    if (cmdno == CMD_SET) {
        data = (char*)malloc(_dsize+2);  /* append data area CR/LF */
        memset(data, 'c', _dsize);
        memcpy(data+_dsize, "\r\n", 2);
    }

    printf("[%d] start %s command\n", tno, _cmd);
    for (i = _st_num; i < _end_num; i++) {
        char key[KEY_SIZE+1];

        snprintf(key, sizeof(key), "%08d", i);
        if (cmdno == CMD_GET) {
            if (get_command(socket, key) < 0)
                break;
        } else if (cmdno == CMD_SET) {
            if (_dsize >= KEY_SIZE)
                memcpy(data, key, strlen(key));
            if (set_command(socket, key, _dsize, data) < 0)
                break;
        } else if (cmdno == CMD_DELETE) {
            if (delete_command(socket, key) < 0)
                break;
        }
        if ((i-_st_num) > 0 && (i%1000) == 0) {
            cur_utime = system_time();
            elapsed_utime = cur_utime - prev_utime;
            printf("[%d] %d completed. elapsed time:%lld(usec)\n",
                   tno, i-_st_num, elapsed_utime);
            prev_utime = cur_utime;
        }
    }
    cur_utime = system_time();
    elapsed_utime = cur_utime - _start_utime;
    printf("[%d] %d completed. time:%lld(usec)\n", tno, i-_st_num, elapsed_utime);

    quit_command(socket);
    if (data)
        free(data);
    SOCKET_CLOSE(socket);
}

int main(int argc, char* argv[])
{
    int i;
#ifdef _WIN32
    uintptr_t* thread_ids;
#else
    pthread_t* thread_ids;
#endif

    if (args(argc, argv) < 0) {
        usage();
        return 0;
    }
    printf("[-c %s -a %s -p %d -t %d -s %d -n %d -l %d %s]\n",
           _cmd, _ip, _port, _threads, _st_num, (_end_num - _st_num), _dsize,
           ((_noreply)? "-noreply" : ""));

    sock_initialize();

#ifdef _WIN32
    thread_ids = (uintptr_t*)malloc(sizeof(uintptr_t) * _threads);
#else
    thread_ids = (pthread_t*)malloc(sizeof(pthread_t) * _threads);
#endif
    if (thread_ids == NULL) {
        printf("no memory\n");
        return 1;
    }

    _start_utime = system_time();

    /* create threads */
    for (i = 0; i < _threads; i++) {
        struct thread_args_t* args;

        args = (struct thread_args_t*)malloc(sizeof(struct thread_args_t));
        if (args == NULL) {
            printf("no memory\n");
            return 1;
        }
        args->tno = i + 1;

#ifdef _WIN32
        thread_ids[i] = _beginthread(do_thread, 0, (void*)args);
#else
        pthread_create(&thread_ids[i], NULL, (void*)do_thread, (void*)args);
#endif
    }

    /* join threads */
    for (i = 0; i < _threads; i++) {
#ifdef _WIN32
        WaitForSingleObject((HANDLE)thread_ids[i], INFINITE);
#else
        void* result;
        pthread_join(thread_ids[i], &result);
#endif
    }

    free(thread_ids);
    sock_finalize();

#ifdef _WIN32
    _CrtDumpMemoryLeaks();
#endif
    return 0;
}
