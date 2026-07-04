#include <stdlib.h>
#include "segel.h"
#include "udp_queue.h"

void udp_mailbox_init(udp_mailbox_t *box) {
    box->head = NULL;
    box->tail = NULL;
    Pthread_mutex_init(&box->mutex, NULL);
}

void udp_mailbox_destroy(udp_mailbox_t *box) {
    Pthread_mutex_lock(&box->mutex);
    udp_ping_node_t *cur = box->head;
    while (cur) {
        udp_ping_node_t *next = cur->next;
        free(cur);
        cur = next;
    }
    box->head = box->tail = NULL;
    Pthread_mutex_unlock(&box->mutex);
    Pthread_mutex_destroy(&box->mutex);
}

void udp_mailbox_push(udp_mailbox_t *box, struct sockaddr_in *addr, socklen_t addrlen) {
    udp_ping_node_t *node = Malloc(sizeof(udp_ping_node_t));
    node->addr = *addr;
    node->addrlen = addrlen;
    node->next = NULL;

    Pthread_mutex_lock(&box->mutex);
    if (box->tail == NULL) {
        box->head = box->tail = node;
    } else {
        box->tail->next = node;
        box->tail = node;
    }
    Pthread_mutex_unlock(&box->mutex);
}

int udp_mailbox_try_pop(udp_mailbox_t *box, udp_ping_node_t *out) {
    int found = 0;

    Pthread_mutex_lock(&box->mutex);
    if (box->head != NULL) {
        udp_ping_node_t *node = box->head;
        box->head = node->next;
        if (box->head == NULL) {
            box->tail = NULL;
        }
        *out = *node;
        out->next = NULL;
        free(node);
        found = 1;
    }
    Pthread_mutex_unlock(&box->mutex);

    return found;
}