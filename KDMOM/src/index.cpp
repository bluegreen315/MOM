/*
 * index.cpp
 *
 *  Created on: Apr 6, 2014
 *      Author: root
 */

#include "index.h"
#include <iostream>
#include <string.h>
using namespace std;

////////////////////////////////////////////////////////////
IndexElem::IndexElem() {
	threadState = DEAD;
	queueState = DEAD;
	queue = new MsgQueue<string>();
	topic.clear();
	persist = NO_Persistent;

	/* modified by Zhang Song*/
	buffState = DEAD;
	pBuff = NULL;
	/* modify end*/
}

IndexElem::~IndexElem() {
	topic.clear();
}

SHORT IndexElem::getThreadState() {
	return threadState;
}
SHORT IndexElem::getQueueState() {
	return queueState;
}
SHORT IndexElem::getIsPersistent() {
	return persist;
}
void IndexElem::setThreadState(State _s) {
	threadState = _s;
}
void IndexElem::setQueueState(State _s) {
	queueState = _s;
}

string IndexElem::getTopic() {
	return topic;
}
void IndexElem::setTopic(string _topic) {
	topic = _topic;
}
void IndexElem::setIsPersitent(IsPersist _isPersist) {
	persist = _isPersist;
}

/* modified by Zhang Song */
SHORT IndexElem::getBuffState() {
    return buffState;
}
void IndexElem::setBuffState(State _s) {
    buffState = _s;
}
void IndexElem::createBuff() {
    pBuff = new Buff();
}
Buff* IndexElem::getBuff() {
    return pBuff;
}

/* modify end */
////////////////////////////////////////////////////////////////////

QueueIndexVector::~QueueIndexVector() {
	vec.clear();
}
int QueueIndexVector::push_elem(string topic, string msg) {
	vector<IndexElem>::size_type ix = 0;

	// if the topic is new, create a queue
	if(!findQueue(topic, topic.length(), ix)) {
		IndexElem elem;
		elem.setTopic(topic);

		pthread_rwlock_wrlock(&rwl);
		vec.push_back(elem);
		ix = vec.size() - 1;
		pthread_rwlock_unlock(&rwl);
	}

	vec[ix].queue->push_msg(msg);

	pthread_rwlock_rdlock(&rwl);
	if(vec[ix].getQueueState() == DEAD)	{
		pthread_rwlock_unlock(&rwl);
		pthread_rwlock_wrlock(&rwl);
		vec[ix].setQueueState(ALIVE);
	}
	pthread_rwlock_unlock(&rwl);

	return 0;

}

SHORT QueueIndexVector::get_elem(vector<IndexElem>::size_type ix, IndexElem &ie) {

	SHORT flag = 0;

	// read lock. then,
	// check whether the queue of element in vec[ix] has message, and
	// no thread gets message from the queue.
	pthread_rwlock_rdlock(&rwl);
	if(vec[ix].getQueueState() == ALIVE
			&& vec[ix].getThreadState() == DEAD) {
		flag = 1;
		pthread_rwlock_unlock(&rwl);


		// write lock. then
		// reset the vec[ix]'s threadState to identity the queue of
		// element in vec[ix] is busy, a thread gets message from the queue.
		pthread_rwlock_wrlock(&rwl);
		vec[ix].setThreadState(ALIVE);

		/* modified by zhangsong */
		if(vec[ix].getBuffState() == DEAD)
		{

			vec[ix].createBuff();
			vec[ix].setBuffState(ALIVE);
		}
		/* modified end */

		ie = vec[ix];
		cout << "G: " << ie.getTopic() << endl;

	}
	pthread_rwlock_unlock(&rwl);
	return flag;
}

vector<IndexElem>::size_type QueueIndexVector::size() {
	SHORT size;
	pthread_rwlock_rdlock(&rwl);
	size = vec.size();
	pthread_rwlock_unlock(&rwl);

	return size;
}

/* modified by zhangsong */
SHORT QueueIndexVector::findQueue(string topic, SHORT sizeOfTopic, vector<IndexElem>::size_type &ix) {

	topic[sizeOfTopic] = 0;

	pthread_rwlock_rdlock(&rwl);
	while(!vec.empty() && ix != vec.size()
			&& strcmp(vec[ix].getTopic().c_str(),topic.c_str()) != 0 ) {
			ix++;
	}

	//did't find,return false
	if(vec.empty() || ix == vec.size()) {
			pthread_rwlock_unlock(&rwl);
			return FALSE;
	}
	pthread_rwlock_unlock(&rwl);

	return TRUE;
}

SHORT QueueIndexVector::resetQueueState(vector<IndexElem>::size_type ix) {
	pthread_rwlock_wrlock(&rwl);
	vec[ix].setQueueState(DEAD);
	pthread_rwlock_unlock(&rwl);

	return 0;
}
SHORT QueueIndexVector::resetThreadState(vector<IndexElem>::size_type ix) {
	pthread_rwlock_wrlock(&rwl);
	vec[ix].setThreadState(DEAD);
	pthread_rwlock_unlock(&rwl);

	return 0;
}
