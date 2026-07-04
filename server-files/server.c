#include "segel.h"
#include "request.h"
#include "log.h"
#include "queue.h"
#include "udp_queue.h"

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

// HW3 — Task 4: one UDP "mailbox" per worker thread (indexed by id-1);
// the single shared UDP socket both the master (recvfrom, in main()'s
// select() loop below) and every worker (sendto, replying to a ping) use;
// and the thread count, so the master can bounds-check a ping's requested
// id before routing it.
static udp_mailbox_t *g_udp_mailboxes;
static int g_udpfd;

// HW3 — Task 4: a worker re-checks its own mailbox at least this often
// (queue_pop_timed()'s bounded wait), so a pending UDP ping is never kept
// waiting longer than this before being answered -- without busy-waiting:
// the wait itself is a genuine (timed) condition-variable block.
#define UDP_POLL_INTERVAL_MS 100

// HW3 — Task 4 (Piazza clarification): while the TCP queue is full, the
// master must not bury itself inside a blocking queue_push() call -- it
// still needs to keep returning to select() to collect UDP pings that
// arrive concurrently during that wait. So instead of always blocking
// select() indefinitely, the master re-checks queue_has_space() at least
// this often (still a genuine blocking select(), just bounded -- no
// busy-waiting) whenever the queue was last seen full.
#define QUEUE_SPACE_POLL_INTERVAL_MS 100

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

    // HW3 — Task 4: this thread's own inbox for pending UDP pings, pushed
    // by the master thread's select() loop in main().
    udp_mailbox_t *mailbox = &g_udp_mailboxes[thread_id - 1];

    while (1) {
        // "First we respond to UDP and only then we handle TCP in the
        // worker thread" -- fully drain (FIFO) every ping currently
        // waiting for us before even trying to pick up a TCP job.
        udp_ping_node_t ping;
        while (udp_mailbox_try_pop(mailbox, &ping)) {
            char reply[MAXBUF];
            reply[0] = '\0';
            append_thread_log(reply, t);
            UDP_Write(g_udpfd, &ping.addr, reply, strlen(reply));
        }

        // Wait for a TCP job, but only up to UDP_POLL_INTERVAL_MS: still a
        // genuine (non-spinning) condition-variable block -- it just times
        // out periodically so we come back and re-check the mailbox above
        // instead of blocking on queue_pop() indefinitely.
        conn_request_t req;
        if (!queue_pop_timed(&g_queue, &req, UDP_POLL_INTERVAL_MS)) {
            continue;
        }

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
    int listenfd, connfd, tcp_port, udp_port, num_threads, queue_size, clientlen;
    double debug_sleep_time;
    struct sockaddr_in clientaddr;

    getargs(&tcp_port, &udp_port, &num_threads, &queue_size, &debug_sleep_time, argc, argv);

    // HW3 Task 5: Create the global server log with debug_sleep_time
    g_log = create_log(debug_sleep_time);

    // HW3 — Task 1: initialize the thread pool and request queue.
    queue_init(&g_queue, queue_size);

    // HW3 — Task 4: one mailbox per worker thread, plus the single shared
    // UDP socket used both to receive pings here in main() and (by every
    // worker) to send back replies.
    g_udp_mailboxes = Malloc(sizeof(udp_mailbox_t) * num_threads);
    int m;
    for (m = 0; m < num_threads; m++) {
        udp_mailbox_init(&g_udp_mailboxes[m]);
    }
    g_udpfd = UDP_Open(udp_port);

    pthread_t *workers = Malloc(sizeof(pthread_t) * num_threads);
    int i;
    for (i = 0; i < num_threads; i++) {
        int *id = Malloc(sizeof(int));
        *id = i + 1;
        Pthread_create(&workers[i], NULL, worker_thread, id);
    }

    listenfd = Open_listenfd(tcp_port);

    // HW3 — Task 4: the master thread multiplexes between the TCP listen
    // socket and the UDP ping socket with select(), so UDP pings are
    // "collected by the Master Thread concurrently with the TCP requests"
    // without ever busy-waiting or blocking exclusively on either one.
    int maxfd = (listenfd > g_udpfd ? listenfd : g_udpfd) + 1;
    while (1) {
        fd_set read_set;
        FD_ZERO(&read_set);
        FD_SET(g_udpfd, &read_set);

        // HW3 — Task 4 (Piazza clarification): only watch listenfd (i.e.
        // only offer to Accept()+queue_push() this round) if the TCP
        // queue actually has room. Otherwise queue_push() below would
        // block the master indefinitely, starving select() and thus the
        // UDP channel -- exactly the bug flagged on Piazza ("the master
        // thread must still respond to a UDP ping that arrives during
        // the wait"). Any TCP connection that's ready but not accepted
        // this round simply stays in the kernel's own listen() backlog
        // until a slot frees up; nothing is lost.
        int watching_tcp = queue_has_space(&g_queue);
        if (watching_tcp) {
            FD_SET(listenfd, &read_set);
        }

        // When the queue is full we can't rely on anything waking us up
        // the moment space frees (queue_task_done() only signals its own
        // condition variable, which the master isn't blocked on here) --
        // so bound the wait and periodically re-check queue_has_space().
        // Still a genuine blocking select(), never a busy spin.
        struct timeval timeout;
        struct timeval *timeout_ptr = NULL;
        if (!watching_tcp) {
            timeout.tv_sec = QUEUE_SPACE_POLL_INTERVAL_MS / 1000;
            timeout.tv_usec = (QUEUE_SPACE_POLL_INTERVAL_MS % 1000) * 1000;
            timeout_ptr = &timeout;
        }

        Select(maxfd, &read_set, NULL, NULL, timeout_ptr);

        if (FD_ISSET(g_udpfd, &read_set)) {
            // A UDP ping holds the <id> of the thread it wants to read;
            // route it to that thread's mailbox (pings are explicitly
            // "not considered jobs in the queue").
            char udpbuf[MAXLINE];
            struct sockaddr_in pingaddr;
            int n = UDP_Read(g_udpfd, &pingaddr, udpbuf, MAXLINE - 1);
            if (n > 0) {
                udpbuf[n] = '\0';
                int target_id = atoi(udpbuf);
                if (target_id >= 1 && target_id <= num_threads) {
                    udp_mailbox_push(&g_udp_mailboxes[target_id - 1], &pingaddr, sizeof(pingaddr));
                }
            }
        }

        if (FD_ISSET(listenfd, &read_set)) {
            clientlen = sizeof(clientaddr);
            connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

            // HW3 — Task 1: record the request arrival time, then hand the
            // connection off to the worker pool via the bounded queue.
            conn_request_t req;
            req.connfd = connfd;
            gettimeofday(&req.arrival, NULL);

            queue_push(&g_queue, req);
        }
    }

    // Clean up the server log before exiting
    destroy_log(g_log);

    // TODO: HW3 — Add cleanup code for the thread pool, queue, and UDP mailboxes.
}
