#include "segel.h"
#include "request.h"
#include "log.h"
#include "queue.h"

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <tcp_portnum> <udp_portnum> <threads> <queue_size> <debug_sleep_time>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//
// HW3 — Task 1: the server is now multi-threaded. One master thread (the
// producer, in main()'s while loop below) accepts connections and pushes
// them onto a bounded FIFO queue (queue.c); a fixed pool of worker threads
// (the consumers, see worker_thread() below) pop connections off that
// queue — oldest first — and hand them to requestHandle(). Both
// queue_push()/queue_pop() block on condition variables, so no thread ever
// busy-waits.
//

// Shared between the master thread and every worker thread.
// NOTE: named g_log (not "log") because segel.h pulls in <math.h>, which
// already declares a global function called log(); a global variable with
// that exact name would conflict with it.
static server_log g_log;
static conn_queue_t g_queue;

// Parses command-line arguments
// HW3 — Task 1: extended to parse the full argument list (was: just <port>).
void getargs(int *tcp_port, int *udp_port, int *num_threads, int *queue_size,
             double *debug_sleep_time, int argc, char *argv[])
{
    if (argc < 6) {
        fprintf(stderr, "Usage: %s <tcp_portnum> <udp_portnum> <threads> <queue_size> <debug_sleep_time>\n", argv[0]);
        exit(1);
    }
    *tcp_port = atoi(argv[1]);
    *udp_port = atoi(argv[2]);
    *num_threads = atoi(argv[3]);
    *queue_size = atoi(argv[4]);
    *debug_sleep_time = atof(argv[5]);

    if (*tcp_port <= 1024 || *tcp_port > 65535) {
        fprintf(stderr, "Error: tcp_portnum must be an integer in (1024, 65535]\n");
        exit(1);
    }
    if (*udp_port <= 1024 || *udp_port > 65535) {
        fprintf(stderr, "Error: udp_portnum must be an integer in (1024, 65535]\n");
        exit(1);
    }
    if (*tcp_port == *udp_port) {
        fprintf(stderr, "Error: tcp_portnum and udp_portnum must be different\n");
        exit(1);
    }
    if (*num_threads <= 0) {
        fprintf(stderr, "Error: threads must be a positive integer\n");
        exit(1);
    }
    if (*queue_size <= 0) {
        fprintf(stderr, "Error: queue_size must be a positive integer\n");
        exit(1);
    }
}

// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).

// HW3 — Task 1: worker threads (the consumers).
// Created once at startup; each loops forever: block on the shared queue
// until a connection is available (FIFO order), process exactly one HTTP
// request, then go back to waiting.
void *worker_thread(void *arg)
{
    int thread_id = *(int *)arg;
    free(arg);

    threads_stats t = Malloc(sizeof(struct Threads_stats));
    t->id = thread_id;    // Thread ID
    t->stat_req = 0;       // Static request count
    t->dynm_req = 0;       // Dynamic request count
    t->post_req = 0;       // POST request count
    t->total_req = 0;      // Total request count

    while (1) {
        conn_request_t req = queue_pop(&g_queue);

        time_stats tm_stats;
        tm_stats.task_arrival = req.arrival;
        gettimeofday(&tm_stats.task_dispatch, NULL);

        requestHandle(req.connfd, tm_stats, t, g_log);

        Close(req.connfd);
        queue_task_done(&g_queue);
    }

    free(t); // unreachable, kept for completeness
    return NULL;
}

int main(int argc, char *argv[])
{
    // Create the global server log
    g_log = create_log();

    int listenfd, connfd, tcp_port, udp_port, num_threads, queue_size, clientlen;
    double debug_sleep_time;
    struct sockaddr_in clientaddr;

    getargs(&tcp_port, &udp_port, &num_threads, &queue_size, &debug_sleep_time, argc, argv);

    // HW3 — Task 1: initialize the thread pool and request queue.
    queue_init(&g_queue, queue_size);
    pthread_t *workers = Malloc(sizeof(pthread_t) * num_threads);
    int i;
    for (i = 0; i < num_threads; i++) {
        int *id = Malloc(sizeof(int));
        *id = i + 1;
        Pthread_create(&workers[i], NULL, worker_thread, id);
    }

    listenfd = Open_listenfd(tcp_port);
    while (1) {
        clientlen = sizeof(clientaddr);
        connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

        // HW3 — Task 1: record the request arrival time, then hand the
        // connection off to the worker pool via the bounded queue instead
        // of processing it here in the master thread (previously a DEMO
        // ONLY block that called requestHandle() directly).
        conn_request_t req;
        req.connfd = connfd;
        gettimeofday(&req.arrival, NULL);

        queue_push(&g_queue, req);
    }

    // Clean up the server log before exiting
    destroy_log(g_log);

    // TODO: HW3 — Add cleanup code for the thread pool and queue.
}