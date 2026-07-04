#include "segel.h"
#include "queue.h"

// Initializes the bounded FIFO queue.
void queue_init(conn_queue_t *q, int capacity) {
    q->buf = (conn_request_t *)Malloc(sizeof(conn_request_t) * capacity);
    q->capacity = capacity;
    q->count = 0;
    q->active_count = 0;
    q->head = 0;
    q->tail = 0;
    Pthread_mutex_init(&q->mutex, NULL);
    Pthread_cond_init(&q->not_full, NULL);
    Pthread_cond_init(&q->not_empty, NULL);
}

// Frees all resources held by the queue.
void queue_destroy(conn_queue_t *q) {
    free(q->buf);
    q->buf = NULL;
    Pthread_mutex_destroy(&q->mutex);
    Pthread_cond_destroy(&q->not_full);
    Pthread_cond_destroy(&q->not_empty);
}

// Pushes a connection request onto the tail of the queue; blocks (via
// condvar, no spinning) while pending + active requests == capacity (see
// the "Queue Management" edge case in the assignment).
void queue_push(conn_queue_t *q, conn_request_t item) {
    Pthread_mutex_lock(&q->mutex);

    while (q->count + q->active_count == q->capacity) {
        Pthread_cond_wait(&q->not_full, &q->mutex);
    }

    q->buf[q->tail] = item;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    Pthread_cond_signal(&q->not_empty);
    Pthread_mutex_unlock(&q->mutex);
}

// Non-blocking peek at whether queue_push() would currently block.
// A single boolean read under the mutex -- no waiting either way.
int queue_has_space(conn_queue_t *q) {
    Pthread_mutex_lock(&q->mutex);
    int has_space = (q->count + q->active_count < q->capacity);
    Pthread_mutex_unlock(&q->mutex);
    return has_space;
}

// Pops the oldest connection request from the head of the queue (FIFO
// order); blocks (via condvar, no spinning) while the queue is empty.
// The popped item is counted as "active" (still occupying a capacity slot)
// until the caller reports completion via queue_task_done().
conn_request_t queue_pop(conn_queue_t *q) {
    Pthread_mutex_lock(&q->mutex);

    while (q->count == 0) {
        Pthread_cond_wait(&q->not_empty, &q->mutex);
    }

    conn_request_t item = q->buf[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    q->active_count++;

    Pthread_mutex_unlock(&q->mutex);

    return item;
}

// Same as queue_pop(), but gives up after waiting up to timeout_ms
// milliseconds (still via condvar -- no spinning). Returns 1 and fills
// *out if an item became available, 0 if the timeout elapsed first.
int queue_pop_timed(conn_queue_t *q, conn_request_t *out, int timeout_ms) {
    struct timeval now;
    struct timespec deadline;

    gettimeofday(&now, NULL);
    long nsec = (long)now.tv_usec * 1000 + (long)(timeout_ms % 1000) * 1000000;
    deadline.tv_sec = now.tv_sec + (timeout_ms / 1000) + (nsec / 1000000000);
    deadline.tv_nsec = nsec % 1000000000;

    Pthread_mutex_lock(&q->mutex);

    while (q->count == 0) {
        int rc = Pthread_cond_timedwait(&q->not_empty, &q->mutex, &deadline);
        if (rc == ETIMEDOUT) {
            Pthread_mutex_unlock(&q->mutex);
            return 0;
        }
    }

    *out = q->buf[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    q->active_count++;

    Pthread_mutex_unlock(&q->mutex);

    return 1;
}

// Marks a previously-popped request as finished processing, freeing its
// capacity slot for a new connection to be pushed.
void queue_task_done(conn_queue_t *q) {
    Pthread_mutex_lock(&q->mutex);

    q->active_count--;
    Pthread_cond_signal(&q->not_full);

    Pthread_mutex_unlock(&q->mutex);
}