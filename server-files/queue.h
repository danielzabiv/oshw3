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
// Per the assignment's "Queue Management" note: pending + active requests
// together must not exceed queue_size. "count" therefore tracks pending
// (not-yet-picked-up) items, while "active_count" tracks connections a
// worker has popped but not yet finished processing (queue_task_done() not
// yet called). queue_push() blocks while count + active_count == capacity.
//
// - queue_push() blocks the caller while the queue+active total is full.
// - queue_pop()  blocks the caller while the queue is empty.
// Both use condition variables, so no thread ever busy-waits/spins.
typedef struct {
    conn_request_t *buf;         // circular buffer holding pending connections
    int capacity;                // = queue_size (max pending + active connections)
    int count;                   // number of pending (not yet popped) items
    int active_count;            // number of items popped but not yet task_done()
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
// Blocks (without spinning) while pending + active == capacity.
void queue_push(conn_queue_t *q, conn_request_t item);

// Pops the oldest connection request from the head of the queue (FIFO).
// Blocks (without spinning) if the queue is currently empty.
// The popped item counts as "active" until queue_task_done() is called.
conn_request_t queue_pop(conn_queue_t *q);

// Marks a previously-popped request as finished processing, freeing its
// slot in the pending+active capacity for a new connection.
void queue_task_done(conn_queue_t *q);

#endif /* __QUEUE_H__ */

// Pops the oldest connection request from the head of the queue (FIFO).
// Blocks (without spinning) if the queue is currently empty.
// The popped item counts as "active" until queue_task_done() is called.
conn_request_t queue_pop(conn_queue_t *q);

// Marks a previously-popped request as fin
