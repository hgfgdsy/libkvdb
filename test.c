#include "kvdb.h"

void *func(void *arg){
	kvdb_t *db = (kvdb_t *)arg;
	int fd = kvdb_open(db,"cap.db");
	int pd = kvdb_put(db,"123","ascvewvet");
	printf("%d %d %d %d\n",fd,db->data_fd,db->log_fd,pd);
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
	kvdb_close(&db);
	char *temp = kvdb_get(&db,"123");
	if(temp == NULL) printf("hello\n");
	else printf("%s\n",temp);
	return 0;
}

