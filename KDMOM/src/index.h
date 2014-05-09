/*
 * index.h
 *
 *  Created on: Apr 6, 2014
 *      Author: root
 */
#include <vector>
#include <string>
#include "msgQueue.h"
#include "buff.h"
#ifndef INDEX_H_
#define INDEX_H_

//////////////////////////////////////////////////////////////
typedef enum State{
	DEAD = 0,
	ALIVE = 1
} State;
typedef enum IsPersist {
	NO_Persistent = 0,
	Persistent = 1
}IsPersist;
//////////////////////////////////////////////////////////////

/*
 * Class IndexElem
 * the element of vector
 */
class IndexElem {
public:
	friend class QueueIndexVector;

	IndexElem();
	virtual ~IndexElem();

	// Return the threadState value
	SHORT getThreadState();

	// Return the queueState value
	SHORT getQueueState();

	// Return 1 if the topic is persistent, else return 0
	SHORT getIsPersistent();

	// Return the topic value
	string getTopic();

	// set the threadState
	void setThreadState(State s);

	// set the queueState
	void setQueueState(State s);

	// set persistence of topic
	void setIsPersitent(IsPersist isPersist);

	// set the topic
	void setTopic(string topic);

	/* modified by Zhang Song*/
	SHORT getBuffState();
	void setBuffState(State s);
	void createBuff();
	Buff *getBuff();
	/* modify end */

	// queue points to the MsgQueue
	MsgQueue<string> *queue;
private:
	// Message's topic
	string topic;

	//MsgQueue<string> *queue;

	// Thread's state associated with this element's MsgQueue.
	// If 0, no thread get message from this MsgQueue.
	SHORT threadState;

	// Queue's state associated with this element's MsgQueue.
	// if 0, no message in this queue.
	SHORT queueState;

	// Topic's persistence.
	// if 1, persist this topic into disk
	SHORT persist;

	/* modified by Zhang Song*/
	SHORT buffState;
	Buff *pBuff;
	/* modify end */
};

class QueueIndexVector
{
public:

	// Initialize the read-write lock
	QueueIndexVector() {
		pthread_rwlock_init(&rwl, NULL);
	}

	virtual ~QueueIndexVector();

	// Push the IndexElement into vector.
    // Push the message into queue.
	int push_elem(string topic, string msg);

	// Return 0, it is not available to create a thread for vec[ix].
	// Else, get the IndexElement from vector.
	SHORT get_elem(vector<IndexElem>::size_type ix, IndexElem &ie);

	// Return the size of vector.
	vector<IndexElem>::size_type size();

	SHORT resetQueueState(vector<IndexElem>::size_type ix);
	SHORT resetThreadState(vector<IndexElem>::size_type ix);
protected:
	// Return the the index of topic in the vector
//	vector<IndexElem>::size_type findQueue(CHAR *topic, SHORT sizeOfTopic);
	SHORT findQueue(string topic, SHORT sizeOfTopic, vector<IndexElem>::size_type &ix);

private:
	// vector, it stores IndexElements
	vector<IndexElem> vec;

	// read-write lock
	pthread_rwlock_t rwl;
};

#endif /* INDEX_H_ */
