/*
 * buff.h
 *
 *  Created on: 2014-4-10
 *      Author: zhangsong
 */
#include <pthread.h>
#include <string>
#include <queue>
#include <iostream>
#define BUFF_SIZE 1024*1024
using namespace std;
#ifndef BUFF_H_
#define BUFF_H_

class Buff{
private:
	long size;
	deque<string> q;
	pthread_rwlock_t rw;
public:
	Buff(){
		size = 0;
		pthread_rwlock_init(&rw, NULL);
	}
	int msgToBuff(string msg) {
		while(getSize() + msg.length() > BUFF_SIZE);
		pthread_rwlock_wrlock(&rw);
		q.push_back(msg);
		size += msg.length();
		pthread_rwlock_unlock(&rw);
		return 0;
	}
	long getSize() {
		int res;
		pthread_rwlock_rdlock(&rw);
		res = size;
		pthread_rwlock_unlock(&rw);
		return res;
	}
	bool isEmpty() {
		pthread_rwlock_rdlock(&rw);
		bool res = q.empty();
		pthread_rwlock_unlock(&rw);
		return res;
	}
	string pop() {
		pthread_rwlock_wrlock(&rw);
		string s = q.front();
		q.pop_front();
		size = size - s.size();
		pthread_rwlock_unlock(&rw);
		return s;
	}
};
#endif /* BUFF_H_ */
