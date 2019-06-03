#include "kvdb.h"

void *func(void *arg){
	kvdb_t *db = (kvdb_t *)arg;
	int fd = kvdb_open(db,"cap.db");
	printf("%d %d %d\n",fd,db->data_fd,db->log_fd);
	return NULL;
}

int main()
{
	kvdb_t db;
	fork();
/*	int fd = kvdb_open(&db,"cap.db");
	printf("pid = %d : %d %d\n",getpid(),fd , db.data_fd);
*/	pthread_t *t1 = malloc(sizeof(pthread_t));
	pthread_create(t1,NULL,func,&db);
	pthread_t *t2 = malloc(sizeof(pthread_t));
	pthread_create(t2,NULL,func,&db);
	pthread_join(*t1,NULL);
	pthread_join(*t2,NULL);
	return 0;
}

