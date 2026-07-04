#ifndef __UDP_QUEUE_H__
#define __UDP_QUEUE_H__

#include <pthread.h>
#include <netinet/in.h>

// HW3 — Task 4: a single pending UDP "ping" waiting to be answered by one
// specific worker thread (identified by the id it targets).
typedef struct udp_ping_node {
    struct sockaddr_in addr;
    socklen_t addrlen;
    struct udp_ping_node *next;
} udp_ping_node_t;

// An unbounded, thread-safe FIFO mailbox holding the pending UDP pings for
// ONE worker thread. The master thread (after recvfrom-ing a ping and
// parsing its target id) pushes onto the matching thread's mailbox; that
// same worker thread later drains it with udp_mailbox_try_pop().
//
// Deliberately NOT a condition-variable queue: the owning worker checks
// this mailbox once per loop iteration (see server.c's worker_thread),
// so a simple mutex-protected linked list is all that's needed — no
// blocking/waiting happens here.
typedef struct {
    udp_ping_node_t *head;
    udp_ping_node_t *tail;
    pthread_mutex_t mutex;
} udp_mailbox_t;

void udp_mailbox_init(udp_mailbox_t *box);
void udp_mailbox_destroy(udp_mailbox_t *box);

// Master-side: enqueue a pending ping for this thread (non-blocking).
void udp_mailbox_push(udp_mailbox_t *box, struct sockaddr_in *addr, socklen_t addrlen);

// Worker-side: pop the oldest pending ping, if any (non-blocking).
// Returns 1 and fills *out if one was available, 0 if the mailbox is empty.
int udp_mailbox_try_pop(udp_mailbox_t *box, udp_ping_node_t *out);

#endif /* __UDP_QUEUE_H__ */