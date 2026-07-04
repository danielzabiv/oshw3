#ifndef __QUEUE_H__
#define __QUEUE_H__

#include <pthread.h>
#include <sys/time.h>

// A single pending connection: its accepted socket fd plus the time it was
// accepted by the master thread (used for Stat-Req-Arrival / Task 3 later).
typedef struct {
    int connfd;
    struct timeval arrival;
} conn_request_t;

// A bounded, thread-safe FIFO queue used to hand accepted TCP connections
// from the master (producer) thread to the worker (consumer) thread pool.
//
// - queue_push() blocks the caller while the queue is full.
// - queue_pop()  blocks the caller while the queue is empty.
// Both use condition variables, so no thread ever busy-waits/spins.
typedef struct {
    conn_request_t *buf;         // circular buffer holding pending connections
    int capacity;                // = queue_size (max pending connections)
    int count;                   // current number of items in the queue
    int head;                    // index of the oldest item (next to pop)
    int tail;                    // index where the next item will be pushed
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} conn_queue_t;

// Initializes the queue with the given bounded capacity.
void queue_init(conn_queue_t *q, int capacity);

// Frees all resources held by the queue.
void queue_destroy(conn_queue_t *q);

// Pushes a connection request onto the tail of the queue (FIFO).
// Blocks (without spinning) if the queue is currently full.
void queue_push(conn_queue_t *q, conn_request_t item);

// Pops the oldest connection request from the head of the queue (FIFO).
// Blocks (without spinning) if the queue is currently empty.
conn_request_t queue_pop(conn_queue_t *q);

#endif /* __QUEUE_H__ */
