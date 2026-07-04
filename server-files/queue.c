#include <stdlib.h>
#include "segel.h"
#include "queue.h"

// Initializes the bounded FIFO queue.
void queue_init(conn_queue_t *q, int capacity) {
    q->buf = (conn_request_t *)malloc(sizeof(conn_request_t) * capacity);
    q->capacity = capacity;
    q->count = 0;
    q->head = 0;
    q->tail = 0;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_full, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}

// Frees all resources held by the queue.
void queue_destroy(conn_queue_t *q) {
    free(q->buf);
    q->buf = NULL;
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
}

// Pushes a connection request onto the tail of the queue; blocks (via
// condvar, no spinning) while the queue is full.
void queue_push(conn_queue_t *q, conn_request_t item) {
    pthread_mutex_lock(&q->mutex);

    while (q->count == q->capacity) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }

    q->buf[q->tail] = item;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

// Pops the oldest connection request from the head of the queue (FIFO
// order); blocks (via condvar, no spinning) while the queue is empty.
conn_request_t queue_pop(conn_queue_t *q) {
    pthread_mutex_lock(&q->mutex);

    while (q->count == 0) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }

    conn_request_t item = q->buf[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);

    return item;
}
