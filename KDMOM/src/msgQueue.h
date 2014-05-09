#include "declaration.h"
#include <deque>
#include <pthread.h>
using namespace std;

#ifndef SHORT
typedef short SHORT;
#endif

template<class DataType>
class MsgQueue {
public:
	MsgQueue():_nready(0){
    	 pthread_mutex_init(&_mutex, NULL);
    	 pthread_mutex_init(&_ready_mutex, NULL);
    	 pthread_cond_init(&_cond, NULL);
    }
    int push_msg(DataType &d) {

        pthread_mutex_lock(&_mutex);
         _queue.push_back(d);
        pthread_mutex_unlock(&_mutex);

        pthread_mutex_lock(&_ready_mutex);
        if (!_nready) {
        	 pthread_cond_signal(&_cond);
        }
        _nready++;

        pthread_mutex_unlock(&_ready_mutex);

        return 0;
    }
    int pop_msg(DataType &d) {
        pthread_mutex_lock(&_ready_mutex);
        while (_nready == 0) {
        	pthread_cond_wait(&_cond, &_ready_mutex);

        }
        _nready--;

        pthread_mutex_unlock(&_ready_mutex);

        pthread_mutex_lock(&_mutex);
         d = _queue.front();
      //   cout << "QQQQ:" << d << endl;
         _queue.pop_front();
        pthread_mutex_unlock(&_mutex);

        return 0;
    }

    int empty() {
    	return _queue.empty();
    }

private:
    pthread_cond_t     _cond;
    SHORT                 _nready;
    pthread_mutex_t     _ready_mutex;
    pthread_mutex_t     _mutex;
    deque<DataType> _queue;
};

