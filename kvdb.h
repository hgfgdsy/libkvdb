#ifndef __KVDB_H__
#define __KVDB_H__

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <pthread.h>


struct kvdb {
	int data_fd;
	int log_fd;
	pthread_mutex_t mlock;
	struct flock dlock;
	struct flock llock;
	char dataname[400];
	char logname[400];
	int status;
};
typedef struct kvdb kvdb_t;

int kvdb_open(kvdb_t *db, const char *filename);
int kvdb_close(kvdb_t *db);
int kvdb_put(kvdb_t *db, const char *key, const char *value);
char *kvdb_get(kvdb_t *db, const char *key);

#endif
