#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "segel.h"
#include "log.h"

#define LOG_INITIAL_CAPACITY 4096

// Opaque struct definition
struct Server_Log {
    // TODO: Implement internal log storage (e.g., dynamic buffer, linked list, etc.)
    char *buf;
    int len;
    int capacity;

    pthread_mutex_t mutex;
    pthread_cond_t readers_cond;
    pthread_cond_t writers_cond;
    int active_readers;
    int active_writers;
    int waiting_writers;
};

// Creates a new server log instance (stub)
server_log create_log() {
    // TODO: Allocate and initialize internal log structure
    struct Server_Log *log = Malloc(sizeof(struct Server_Log));

    log->capacity = LOG_INITIAL_CAPACITY;
    log->buf = Malloc(log->capacity);
    log->buf[0] = '\0';
    log->len = 0;

    pthread_mutex_init(&log->mutex, NULL);
    pthread_cond_init(&log->readers_cond, NULL);
    pthread_cond_init(&log->writers_cond, NULL);
    log->active_readers = 0;
    log->active_writers = 0;
    log->waiting_writers = 0;

    return log;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    // TODO: Free all internal resources used by the log
    free(log->buf);
    pthread_mutex_destroy(&log->mutex);
    pthread_cond_destroy(&log->readers_cond);
    pthread_cond_destroy(&log->writers_cond);
    free(log);
}

// Writer-priority reader-writer lock helpers
static void reader_lock(server_log log) {
    pthread_mutex_lock(&log->mutex);
    while (log->active_writers > 0 || log->waiting_writers > 0) {
        pthread_cond_wait(&log->readers_cond, &log->mutex);
    }
    log->active_readers++;
    pthread_mutex_unlock(&log->mutex);
}

static void reader_unlock(server_log log) {
    pthread_mutex_lock(&log->mutex);
    log->active_readers--;
    if (log->active_readers == 0) {
        pthread_cond_signal(&log->writers_cond);
    }
    pthread_mutex_unlock(&log->mutex);
}

static void writer_lock(server_log log) {
    pthread_mutex_lock(&log->mutex);
    log->waiting_writers++;
    while (log->active_writers > 0 || log->active_readers > 0) {
        pthread_cond_wait(&log->writers_cond, &log->mutex);
    }
    log->waiting_writers--;
    log->active_writers = 1;
    pthread_mutex_unlock(&log->mutex);
}

static void writer_unlock(server_log log) {
    pthread_mutex_lock(&log->mutex);
    log->active_writers = 0;
    pthread_cond_broadcast(&log->readers_cond); 
    pthread_cond_signal(&log->writers_cond);
    pthread_mutex_unlock(&log->mutex);
}

// Returns dummy log content as string (stub)
int get_log(server_log log, char** dst) {
    // TODO: Return the full contents of the log as a dynamically allocated string
    // This function should handle concurrent access
    reader_lock(log);

    int len = log->len;
    *dst = (char*)Malloc(len + 1); // Allocate for caller
    if (*dst != NULL) {
        memcpy(*dst, log->buf, len);
        (*dst)[len] = '\0';
    }

    reader_unlock(log);
    return len;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len) {
    // TODO: Append the provided data to the log
    // This function should handle concurrent access
    writer_lock(log);

    int needed = log->len + data_len + 2; // + '#' separator + '\0'
    if (needed > log->capacity) {
        while (log->capacity < needed) {
            log->capacity *= 2;
        }
        log->buf = (char*)Realloc(log->buf, log->capacity);
    }

    if (log->len > 0) {
        log->buf[log->len] = '#';
        log->len += 1;
    }
    memcpy(log->buf + log->len, data, data_len);
    log->len += data_len;
    log->buf[log->len] = '\0';

    writer_unlock(log);
}