// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include <map>
#include <mutex>
#include <condition_variable>
class lock_server {

 protected:
  enum lockstatus{free,locked};
  int nacquire;
  std::mutex mutex;
  std::condition_variable cv;
  std::map<lock_protocol::lockid_t,lockstatus> locks;
  // std::map<lock_protocol::lockid_t,std::pair<std::shared_ptr<std::mutex>,std::shared_ptr<std::condition_variable>>> mutex_and_cvs;

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 