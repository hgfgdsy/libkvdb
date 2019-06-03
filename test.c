#include "kvdb.h"
#include <sys/wait.h>
/*
void *func(void *arg){
	kvdb_t *db = (kvdb_t *)arg;
	int fd = kvdb_open(db,"cap.db");
	int pd = kvdb_put(db,"0","fgbfbsb");
	printf("%d %d %d %d\n",fd,db->data_fd,db->log_fd,pd);
	return NULL;
}
*/
char *rand_key(){
	int len = rand()%127 +1;
	char *ret = (char *)malloc(len);
	for(int i=0;i<len;i++){
		int c = rand()%26+0;
		*(ret+i) = 'a'+c;
	}
	return ret;
}

char *rand_value(){
	int len = rand()%(1<<5) +1;
	char *ret = (char *)malloc(len);
	for(int i=0;i<len;i++){
		int c = rand()%26+0;
		*(ret+i) = 'a'+c;
	}
	return ret;
}

char *rand_filename(){
	int len = rand()%20+1;
	char *ret = (char *)malloc(len+3);
	for(int i=0;i<len;i++){
		int c = rand()%26+0;
		*(ret+i) = 'a'+c;

	}
	*(ret+len) = '.';
	*(ret+len+1) = 'd';
	*(ret+len+2) = 'b';
	return ret;
}


int main()
{
/*	kvdb_t db;
	srand(time(NULL));
	pid_t pid = fork();
	if(pid == 0){
	pthread_t *t1 = malloc(sizeof(pthread_t));
	pthread_create(t1,NULL,func,&db);
	pthread_t *t2 = malloc(sizeof(pthread_t));
	pthread_create(t2,NULL,func,&db);
	pthread_join(*t1,NULL);
	pthread_join(*t2,NULL);
	}
	else {
		wait(NULL);
		kvdb_open(&db,"cap.db");
	        char *temp = kvdb_get(&db,"0");
	        if(temp == NULL) printf("hello\n");
	        else printf("%s\n",temp);
	}
	return 0;
	*/
	srand(time(NULL));
	for(int i=1;i<=3;i++){
		fork();
	}
	printf("hello\n");
	kvdb_t db;
	kvdb_open(&db,rand_filename());
	kvdb_put(&db,rand_key(),rand_value());
	kvdb_close(&db);
	return 0;

}

