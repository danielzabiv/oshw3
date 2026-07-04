#include "segel.h"
#include "log.h"
#include "request.h"

#define LOG_INITIAL_CAPACITY 4096

// Opaque struct definition
struct Server_Log {
    char *buf;
    int len;
    int capacity;

    pthread_mutex_t mutex;
    pthread_cond_t readers_cond;
    pthread_cond_t writers_cond;
    int active_readers;
    int active_writers;
    int waiting_writers;
    
    // HW3 Task 5: debug sleep time in seconds (can be fractional)
    double debug_sleep_time;
};

// Creates a new server log instance
server_log create_log(double debug_sleep_time) {
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
    
    // HW3 Task 5: store debug sleep time
    log->debug_sleep_time = debug_sleep_time;

    return log;
}

// Destroys and frees the log
void destroy_log(server_log log) {
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

// HW3 Task 5: helper to perform debug sleep if configured
// Called while holding the lock (inside critical section)
static void perform_debug_sleep(server_log log) {
    if (log->debug_sleep_time > 0) {
        // Convert debug_sleep_time (in seconds) to microseconds
        unsigned long sleep_us = (unsigned long)(log->debug_sleep_time * 1000000.0);
        usleep(sleep_us);
    }
}

// Returns the log contents as a string (null-terminated)
// NOTE: caller is responsible for freeing dst
int get_log(server_log log, char** dst, void* time_stats_ptr) {
    reader_lock(log);

    // HW3 Task 5: perform debug sleep inside critical section
    perform_debug_sleep(log);
    
    // HW3 Task 5: Stat-Log-Dispatch recorded AFTER sleep, inside critical section
    // Note: log_enter is already recorded by caller BEFORE requesting lock
    time_stats *tm_stats = (time_stats *)time_stats_ptr;
    if (tm_stats != NULL) {
        gettimeofday(&tm_stats->log_exit, NULL);
    }

    int len = log->len;
    *dst = (char*)Malloc(len + 1);
    if (*dst != NULL) {
        memcpy(*dst, log->buf, len);
        (*dst)[len] = '\0';
    }

    reader_unlock(log);
    return len;
}

// Appends a new entry to the log
void add_to_log(server_log log, const char* data, int data_len, void* time_stats_ptr) {
    writer_lock(log);

    // HW3 Task 5: perform debug sleep inside critical section
    perform_debug_sleep(log);
    
    // HW3 Task 5: Stat-Log-Dispatch recorded AFTER sleep, inside critical section
    // Note: log_enter is already recorded by caller BEFORE requesting lock
    time_stats *tm_stats = (time_stats *)time_stats_ptr;
    if (tm_stats != NULL) {
        gettimeofday(&tm_stats->log_exit, NULL);
    }

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