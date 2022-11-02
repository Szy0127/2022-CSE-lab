// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <atomic>
#include "extent_protocol.h"

#include "inode_manager.h"
#include "persister.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  chfs_persister *_persister;
  std::atomic<chfs_command::txid_t> _txid{0};

  void checkpoint();
  void redo(const chfs_command& log);
  void undo(const chfs_command& log);

 public:
  extent_server();

  int create(chfs_command::txid_t txid,uint32_t type, extent_protocol::extentid_t &id);
  int put(chfs_command::txid_t txid,extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(chfs_command::txid_t txid,extent_protocol::extentid_t id, int &);

  // Your code here for lab2A: add logging APIs
  int begin(int,chfs_command::txid_t &txid);
  int commit(chfs_command::txid_t txid,int&);

};

#endif 







