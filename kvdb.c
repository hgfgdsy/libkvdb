#include "kvdb.h"

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;  


void may_crash() {
	int x = rand()%6 +0;
	if(x == 0){
		exit(0);
	}
}


int file_lock(kvdb_t *db) 
{
	struct flock *datalock = &db->dlock;
	datalock->l_type = F_WRLCK;
	datalock->l_whence = SEEK_SET;
	datalock->l_start = 0;
	datalock->l_len = 0;
	struct flock *loglock = &db->dlock;
	loglock->l_type = F_WRLCK;
	loglock->l_whence = SEEK_SET;
	loglock->l_start = 0;
	loglock->l_len = 0;
	int ret1,ret2,ret=0;
	ret1 = fcntl(db->data_fd,F_SETLKW,datalock);
	ret2 = fcntl(db->log_fd,F_SETLKW,loglock);
	if(ret1 < ret) ret = ret1;
	if(ret2 < ret) ret = ret2;
	return ret;
}

int file_unlock(kvdb_t *db)
{
	struct flock *datalock = &db->dlock;
	datalock->l_type = F_UNLCK;
	datalock->l_whence = SEEK_SET;
	datalock->l_start = 0;
	datalock->l_len = 0;
	struct flock *loglock = &db->dlock;
	loglock->l_type = F_UNLCK;
	loglock->l_whence = SEEK_SET;
	loglock->l_start = 0;
	loglock->l_len = 0;
	int ret1,ret2,ret=0;
	ret1 = fcntl(db->data_fd,F_SETLK,datalock);
	ret2 = fcntl(db->log_fd,F_SETLK,loglock);
	if(ret1 < ret) ret = ret1;
	if(ret2 < ret) ret = ret2;
	return ret;
}



int kvdb_open(kvdb_t *db, const char *filename) {
	int len = strlen(filename);
	if(*(filename+len-3)!='.' || *(filename+len-2)!='d' || *(filename+len-1) != 'b')
			return -1;
	pthread_mutex_lock(&mutex);

	if(db->status == 1) {
		pthread_mutex_unlock(&mutex);
		return -1;
	}

	int label = 0;
	int fd = open(filename,O_RDWR|O_CREAT,0777);
	strcpy(db->dataname,filename);
	db->dataname[len] = '\0';
	strcpy(db->logname,filename);
	strcpy(db->logname+len-2,"log");
	db->logname[len+1] = '\0';
	int fdl = open(db->logname,O_RDWR|O_CREAT,0777);
	pthread_mutex_init(&db->mlock,PTHREAD_PROCESS_PRIVATE);
	db->data_fd = fd;
	db->log_fd = fdl;
	db->status = 1;

	/*
	int fd = open(filename,O_RDWR);
	int fdl = -1;
	int label = 0;
	if(fd < 0) {
		fd = open(filename,O_RDWR|O_CREAT,0777);
		strcpy(db->dataname,filename);
		db->dataname[len] = '\0';
		strcpy(db->logname,filename);
		strcpy(db->logname+len-2,"log");
		db->logname[len+1] = '\0';
		fdl = open(db->logname,O_RDWR|O_CREAT,0777);
		pthread_spin_init(&db->slock,PTHREAD_PROCESS_PRIVATE);
		db->data_fd = fd;
		db->log_fd = fdl;
		db->status = 1;
	}
	else {
		if(db->inited != 1){
			strcpy(db->dataname,filename);
		        strcpy(db->logname,filename);
		        strcpy((db->logname)+len-2,"log");
		        db->logname[len+1] = '\0';
			fdl = open(db->logname,O_RDWR);
		        pthread_spin_init(&db->slock,PTHREAD_PROCESS_PRIVATE);
		        
		        db->data_fd = fd;
		        db->log_fd = fdl;
		}
		else {
			fd = db->data_fd;
			fd = db->log_fd;
		}

	}*/
	pthread_mutex_unlock(&mutex);
	if(fd < 0) label = fd;
	if(fdl < 0) label = fdl; 
	return label;
}
	
	
	
	
int kvdb_close(kvdb_t *db) {
	int label = 0;
        int cd,cl;
	pthread_mutex_lock(&mutex);
	if(db->status == 0) {
		return -1;
		pthread_mutex_unlock(&mutex);
	}
	db->status = 0;
	cd = close(db->data_fd);
	cl = close(db->log_fd);
	pthread_mutex_unlock(&mutex);
	if(cd < 0) label = cd;
	if(cl < 0) label = cl;
	return label;
}








int kvdb_put(kvdb_t *db, const char *key, const char *value) {
	pthread_mutex_lock(&mutex);
	if(db->status != 1) {return -1; pthread_mutex_unlock(&mutex);}
	pthread_mutex_unlock(&mutex);

	pthread_mutex_lock(&db->mlock);
	file_lock(db);

	char full_key[32];
	char full_len[32];
	for(int i=0;i<32;i++) full_key[i] = 0x20;
	for(int i=0;i<32;i++) full_len[i] = 0x20;
	int key_len = strlen(key);
	int value_len = strlen(value);
	char key_buf[32];
	memset(key_buf,0,sizeof(key_buf));
	sprintf(key_buf,"%d",key_len);
	char value_buf[32];
	memset(value_buf,0,sizeof(value_buf));
	sprintf(value_buf,"%d",value_len);
	int rkl = strlen(key_buf);
	int rvl = strlen(value_buf);
	strncpy(full_key+32-rkl,key_buf,rkl);
	strncpy(full_len+32-rvl,value_buf,rvl);

	int pace = 66;
	int k;
	off_t offset = 0;
	for(k = 0; ; k++) {
		lseek(db->log_fd, pace*k, SEEK_SET);
		char *log_buf = (char *)malloc(70);
		int realin = read(db->log_fd, log_buf, 66);
		if(realin < 66) break;
		if(log_buf[65] != '\n') break;
		memset(key_buf,0,sizeof(key_buf));
		memset(value_buf,0,sizeof(value_buf));
		int kcnt = 0;
		int vcnt = 0;
		for(int i = 0; i < 32; i++) {
			if(log_buf[i] != 0x20) key_buf[kcnt++] = log_buf[i];
		}
		for(int i = 33; i < 65; i++) {
			if(log_buf[i] != 0x20) value_buf[vcnt++] = log_buf[i];
		}
		int l1 = atoi(key_buf);
		int l2 = atoi(value_buf);
		offset = offset + l1 + l2 + 2;
		free((void *)log_buf);
	}
	char mao[1] = {'\t'};
	char end[1] = {'\n'};

	int ret = 0;

	lseek(db->data_fd,offset,SEEK_SET);

	int d1,d2,d3,d4;
	d1 = write(db->data_fd,(void *)key,key_len);
	d2 = write(db->data_fd,(void *)mao, 1);
	may_crash();
        d3 = write(db->data_fd,(void *)value,value_len);
	d4 = write(db->data_fd,(void *)end, 1);

	sync();
	may_crash();

	if((d1+d2+d3+d4) < (key_len+value_len+2)) ret = -1;

	int k1,k2,k3,k4;
	lseek(db->log_fd, pace*k, SEEK_SET);
	k1 = write(db->log_fd, (void *)full_key, 32);
	k2 = write(db->log_fd, (void *)mao, 1);
	may_crash();
	k3 = write(db->log_fd, (void *)full_len, 32);
	k4 = write(db->log_fd, (void *)end, 1);

	if((k1+k2+k3+k4) < 66) ret = -1;
	may_crash();
	sync();

	file_unlock(db);
	pthread_mutex_unlock(&db->mlock);
	return ret;
}

char *kvdb_get(kvdb_t *db, const char *key) {
	pthread_mutex_lock(&mutex);
	if(db->status != 1) {return NULL; pthread_mutex_unlock(&mutex);}
	pthread_mutex_unlock(&mutex);

	pthread_mutex_lock(&db->mlock);
	file_lock(db);

	char key_buf[32];
	char value_buf[32];

	int k;
	off_t offset = 0;
	int pace = 66;

	int target = strlen(key);

	for(k = 0; ; k++) {
		printf("down\n");
		lseek(db->log_fd, pace*k, SEEK_SET);
		char *log_buf = (char *)malloc(70);
		int realin = read(db->log_fd, log_buf, 66);
		if(realin < 66) break;
		if(log_buf[65] != '\n') break;
		memset(key_buf,0,sizeof(key_buf));
		memset(value_buf,0,sizeof(value_buf));
		int kcnt = 0;
		int vcnt = 0;
		for(int i = 0; i < 32; i++) {
			if(log_buf[i] != 0x20) key_buf[kcnt++] = log_buf[i];
		}
		for(int i = 33; i < 65; i++) {
			if(log_buf[i] != 0x20) value_buf[vcnt++] = log_buf[i];
		}
		int l1 = atoi(key_buf);
		int l2 = atoi(value_buf);
		offset = offset + l1 + l2 + 2;
		free((void *)log_buf);
	}

	char *ret = NULL;

	for(int j=k-1; j>=0; j--) {
		printf("up\n");
		lseek(db->log_fd, pace*j, SEEK_SET);
		char *log_buf = (char *)malloc(70);
		read(db->log_fd, log_buf, 66);
		memset(key_buf,0,sizeof(key_buf));
		memset(value_buf,0,sizeof(value_buf));
		int kcnt = 0;
		int vcnt = 0;
		for(int i = 0; i < 32; i++) {
			if(log_buf[i] != 0x20) key_buf[kcnt++] = log_buf[i];
		}
		for(int i = 33; i < 65; i++) {
			if(log_buf[i] != 0x20) value_buf[vcnt++] = log_buf[i];
		}
		int l1 = atoi(key_buf);
		int l2 = atoi(value_buf);
		offset = offset - l1 - l2 - 2;
		if(l1 != target) continue;
		lseek(db->data_fd, offset, SEEK_SET);
		char *diff_key = (char *)(malloc(l1+1));
		int diff_key_read = read(db->data_fd, diff_key, l1);
		if(diff_key_read < l1) continue;
		if(strncmp(key,diff_key,l1) == 0) {
			ret = (char *)malloc(l2+1);
			if(ret == NULL) {
				printf("malloc_fail\n");
				continue;
			}
			lseek(db->data_fd, offset+l1+1, SEEK_SET);
			int diff_value_read = read(db->data_fd, ret, l2);
			if(diff_value_read < l2) ret = NULL;
			else break;
		}
	}

	file_unlock(db);
	pthread_mutex_unlock(&db->mlock);
	return ret;
}

