/*
 * leader.cpp
 *
 *  Created on: Apr 3, 2014
 *      Author: root
 */
#include <cmath>
#include "../src/zhelpers.hpp"
#include "../src/index.h"
#include <pthread.h>
#include "../src/utils.h"
#include <fstream>
#include <time.h>
#include "zmq_utils.h"

#define SAFE_SIZE 10
using namespace std;

#ifndef SHORT
typedef short SHORT;
#endif
#ifndef CHAR
typedef char CHAR;
#endif

#ifndef DOUBLE
typedef double DOUBLE;
#endif

/**********************************************************************/
// Prepare ZMQ context and socket
zmq::context_t context(1);
zmq::socket_t frontend(context, ZMQ_PULL);
zmq::socket_t backend(context, ZMQ_PUB);
/**********************************************************************/

struct SendArgs {
	IndexElem ie;
	vector<IndexElem>::size_type ix;
};

/**********************************************************************/
// queue index
QueueIndexVector vec;
/**********************************************************************/

/**********************************************************************/
// function declaration
void *recv(void *arg);
void *send(void *arg);
void *flush(void *arg);
//void *flush1(void *arg);
//SHORT flushQueue(string str, const string filePath);
/**********************************************************************/

int main()
{
	/**********************************************************************/
	// Bind the frontend & backend socket
	int hwm = 2000000000;
	frontend.bind("tcp://*:5556");
	backend.setsockopt(ZMQ_SNDHWM, &hwm, sizeof(hwm));
	backend.bind("tcp://*:5557");
	/**********************************************************************/

	/**********************************************************************/
	// Pthread ID
	pthread_t recv_thread_ID;
	pthread_t send_thread_ID;
	pthread_t flush_thread_ID;
	/**********************************************************************/

	/**********************************************************************/
	// Create recv thread
	SHORT res = pthread_create(&recv_thread_ID, NULL, recv, NULL);
	if(res != 0) {
		perror("Thread creation failed");
		exit(EXIT_FAILURE);
	}
	/**********************************************************************/

	/**********************************************************************/
	// Create send thread & flush thread
	IndexElem ie;
	while(1){
		for(vector<IndexElem>::size_type ix = 0; ix != vec.size(); ix++) {

			SHORT flag = vec.get_elem(ix, ie);

			if(flag) {
				IndexElem *ieTemp = new IndexElem();
				*ieTemp = ie;

				res = pthread_create(&send_thread_ID, NULL, send, ieTemp);
				if(res != 0) {
					perror("Thread creation failed");
					exit(EXIT_FAILURE);
				}

				res = pthread_create(&flush_thread_ID, NULL, flush, ieTemp);
				if(res != 0) {
					perror("Thread creation failed");
					exit(EXIT_FAILURE);
				}
//				s_sleep(1);
			}

		}
	}
	/**********************************************************************/

	/**********************************************************************/
	// Create send thread & flush thread
//	SendArgs args;
//	IndexElem ieTemp;
//	while(1){
//		for(vector<IndexElem>::size_type ix = 0; ix != vec.size(); ix++) {
//
//			SHORT flag = vec.get_elem(ix, ieTemp);
//
//			if(flag) {
//				args.ie = ieTemp;
//				args.ix = ix;
////				cout <<"new topic"<<endl;
//				res = pthread_create(&send_thread_ID, NULL, send, &args);
//				if(res != 0) {
//					perror("Thread creation failed");
//					exit(EXIT_FAILURE);
//				}
//				res = pthread_create(&flush_thread_ID, NULL, flush, &args);
//				if(res != 0) {
//					perror("Thread creation failed");
//					exit(EXIT_FAILURE);
//				}
//				s_sleep(10);
//			}
//		}
//	}
	/**********************************************************************/
	pthread_join(recv_thread_ID, NULL);
	pthread_join(send_thread_ID, NULL);
	pthread_join(flush_thread_ID, NULL);
	return 0;
}


void *recv (void *arg) {
    int recv_nbr = 0;
    static int s_interrupted = 0;
    s_catch_signals();
    while(!s_interrupted) {
    		if(recv_nbr == 3000000)
    			break;
            std::string str = s_recv(frontend);
//            cout << str << endl;
            //下面对消息进行解析
			// msg's topic
			char  topic_temp[24];
			int i;
			for(i = 0;i < 24 && str[i] != ' ';i++)
			{
							topic_temp[i] = str[i];
			}
			topic_temp[i] = 0;//问题1****************************
			string topic = topic_temp;
			////////////////////////////////////////////////////////////////////////
			//msg's size
			i = 24;
			int size = 0;int k = 0;
			while(str[i] != 'a' && i < 32)
			{
				size = size + (SHORT)pow((DOUBLE)10,k)*(str[i]-48);
				i++;
				k++;
			}
			// msg's content
			char *temp = new char[size + 1];
			for(int j = 0;j < size;j++)
			{
							temp[j] = str[32 + j];
			}
			temp[size] = 0;
			string msg = temp;

//////////////////////////////////////////////////////////////////////////////////

		vec.push_elem(topic, msg);

		recv_nbr ++;

//		cout << "R:" << recv_nbr << " "<< flush;

		//////////////////////////////////////////////////////////////////////////
		//  Send reply back to client
/*		zmq::message_t reply (5);
		memcpy ((void *) reply.data (), "World", 5);
		frontend.send (reply);
		*/
		///////////////////////////////////////////////////////////////////////////
	}
	cout << "R:" << recv_nbr << endl;
}




void *send(void *_args) {

//	SendArgs args;
//	args = *(SendArgs *)_args;
//	IndexElem ie = args.ie;
//	vector<IndexElem>::size_type ix = args.ix;
////	cout << ix << endl;
//	string topic = ie.getTopic();

/////////////////////////////////////////////////////////////////////////////////////////////////
	IndexElem ie = *(IndexElem*) _args;
	string topic = ie.getTopic();
/////////////////////////////////////////////////////////////////////////////////////////////////
	cout << "ST: "<< topic << endl;

	SHORT send_nbr = 0;
	string str;

	static int s_interrupted = 0;
	s_catch_signals();
	while(!s_interrupted) {
/////////////////////////////////////////////////////////////////////////////////////////////////

//		int sleep_nbr = 0;
//		while(ie.queue->empty()) {
//
//			if(sleep_nbr == 5) {
//				vec.resetThreadState(ix);
//				cout << "thread" << ix << endl;
//				pthread_exit(NULL);
//			}
//			sleep_nbr ++;
//
//			s_sleep(1000);
//
//		}

//////////////////////////////////////////////////////////////////////////////////////////////////
		ie.queue->pop_msg(str);
//		cout << str << endl;
		ie.getBuff()->msgToBuff(str);
//////////////////////////////////////////////////////////////////////////////////////////////////

		// Send message to all subscribers who subscribe this topic
		int size = topic.size() + str.size();
		zmq::message_t message(size);
		snprintf((char *) message.data(), size, "%s %s", topic.c_str(), str.c_str());
		backend.send(message);

		send_nbr ++;
		cout << "S:"<< send_nbr << endl;
//		if(send_nbr == 10) break;

	}

	cout << topic << send_nbr << endl;
}

void *flush(void *_args) {
//	SendArgs args;
//	args = *(SendArgs *)_args;
//	IndexElem ie = args.ie;

	////////////////////////////////////////////////////////

	IndexElem ie = *(IndexElem*) _args;
	string topic = ie.getTopic();

	cout << "FT: " << ie.getTopic() << endl;

	Buff *pb = ie.getBuff();

	clock_t start,finish;
	start = clock();
	while(1) {
		finish = clock();
		while(pb->getSize() > SAFE_SIZE || (finish - start) / CLOCKS_PER_SEC > 3) {

			ofstream fout(topic.c_str(), ios::app);
			if(!fout) {
				cerr << "Cannot open file !" << endl;
				exit(1);
			}
			while(!pb->isEmpty()) {
				fout << pb->pop()<< endl;
			}
			fout.close();

			start = clock();
		}
	}
}
/*
void *flush1(void *arg)
{
	cout<< "I'm running!"<<endl;
	while(1);
}
*/

/*
SHORT flushQueue(string str, const string filePath)
{
	// ------------Open persistent file--------------
	ofstream fout(filePath.c_str(), ios::app);
	if(!fout)
	{
		cerr << "Cannot open file !" << endl;
		exit(1);
	}

	fout << str << endl;

	// Close file
	fout.close();

	return 0;
}*/
