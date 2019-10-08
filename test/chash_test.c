#ifdef _WIN32
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define _MAIN
#include "../dinio.h"

static void print_server_node()
{
    struct consistent_hash_t* ch;
    int i;

    ch = g_dss->ch;

    printf("number of server %d\n", g_dss->num_server);
    for (i = 0; i < g_dss->num_server; i++) {
        struct server_t* server;

        server = g_dss->server_list[i];
        printf("[%d] ip=%s port=%u scale_factor=%d\n",
               i, server->ip, server->port, server->scale_factor);
    }

    printf("\nnumber of node %d\n", ch->num_node);
    for (i = 0; i < ch->num_node; i++) {
        struct node_t* node;

        node = &ch->node_array[i];
        printf("[%d] point=%u ip=%s port=%d flag=%d\n",
               i, node->point,
               node->server->ip, node->server->port, node->server_flag);
    }
}

static void create_key_test(struct consistent_hash_t* ch)
{
    int i;

    for (i = 0; i < 100; i++) {
        char key[10];
        unsigned int kh;
        struct node_t* node;

        sprintf(key, "%d", i);
        kh = ch_hash(key, strlen(key));
        node = ch_get_node(ch, key, strlen(key));
        printf("[%d] kh=%u point=%u ip=%s port=%d flag=%d\n",
               i, kh, node->point,
               node->server->ip, node->server->port, node->server_flag);
    }
}

int main(int argc, char* argv[])
{
    struct server_t* server;
    struct server_t* server_next;

    mt_initialize();
    sock_initialize();

    g_conf = (struct dinio_conf_t*)calloc(1, sizeof(struct dinio_conf_t));
    if (g_conf == NULL) {
        fprintf(stderr, "no memory.\n");
        return -1;
    }
    g_conf->pool_init_conns = 1;
    get_abspath(g_conf->server_file, "../../server.def", sizeof(g_conf->server_file)-1);

    ds_create(g_conf->server_file);

    print_server_node(g_dss);
    create_key_test(g_dss->ch);

    server = ds_get_server("192.168.30.80", 11222);
    if (server) {
        ds_detach_server(server);
        print_server_node(g_dss);
    }

    server = ds_create_server("192.168.30.80", 11222, 100);
    ds_attach_server(server);
    print_server_node(g_dss);
    server_next = ds_next_server(server);

    ds_close();

    free(g_conf);

    sock_finalize();
    mt_finalize();

#ifdef WIN32
    _CrtDumpMemoryLeaks();
#endif
    return 0;
}
