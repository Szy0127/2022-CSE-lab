// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  // {
  std::unique_lock<std::mutex> l(mutex);
  auto item = locks.find(lid);
  if(item == locks.end()){
    locks.emplace(lid,locked);
    std::cout<<clt<<"acquire "<<lid<<std::endl;
    return ret;
  }
  // if(item->second == -1){
  //   item->second = clt;
  //   std::cout<<clt<<"acquire "<<lid<<std::endl;
  //   return ret;
  // }
  // if(item->second == clt){
  //   std::cout<<clt<<"acquire "<<lid<<" already owned"<<std::endl;
  //   return ret;
  // }
  // auto cv_items = mutex_and_cvs.find(lid);
  // if(cv_items == mutex_and_cvs.end()){
  //   mutex_and_cvs.emplace(lid,std::make_pair(std::make_shared<std::mutex>(),std::make_shared<std::condition_variable>()));
  // }
  // }
  std::cout<<clt<<"wait for "<<lid<<std::endl;
  cv.wait(l,[this,lid,clt](){return this->locks.count(lid)==0 ;});
  assert(locks.count(lid)==0);
  locks.emplace(lid,locked);
  // locks[lid] = clt;
  std::cout<<clt<<"finish wait and acquire "<<lid<<std::endl;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  std::cout<<clt<<"release "<<lid<<std::endl;
  std::unique_lock<std::mutex> l(mutex);
  auto item = locks.find(lid);
  if(item == locks.end()){
    std::cout<<clt<<"release "<<lid<<"but not acquired"<<std::endl;
    return ret;
  }
  // locks[lid] = -1;
  locks.erase(item);
  cv.notify_all();//all thread waiting for global lock but not same lock id
  return ret;
}