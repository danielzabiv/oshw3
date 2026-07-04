#ifndef SERVER_LOG_H
#define SERVER_LOG_H

#include <time.h>

typedef struct Server_Log* server_log;

// Creates a new server log instance
server_log create_log(double debug_sleep_time);

// Destroys and frees the log
void destroy_log(server_log log);

// Returns the log contents as a string (null-terminated)
// NOTE: caller is responsible for freeing dst
// time_stats is a pointer to struct Time_stats (defined in request.h)
int get_log(server_log log, char** dst, void* time_stats);

// Appends a new entry to the log
// time_stats is a pointer to struct Time_stats (defined in request.h)
void add_to_log(server_log log, const char* data, int data_len, void* time_stats);

#endif // SERVER_LOG_H
