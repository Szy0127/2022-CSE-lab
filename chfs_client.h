#ifndef chfs_client_h
#define chfs_client_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "extent_client.h"
#include <vector>


class chfs_client {
  extent_client *ec;
  lock_client *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct slinkinfo {
    unsigned long long size; // target path size
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    chfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

  std::string entries2str(chfs_client::inum ,const std::list<chfs_client::dirent> &);
  int _create(inum , const char *, extent_protocol::types , inum &,chfs_command::txid_t);

  int lookup_lockfree(inum, const char *, bool &, inum &);
  int readdir_lockfree(inum, std::list<dirent> &);
  int read_lockfree(inum, size_t, off_t, std::string &);


 public:
  chfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);
  bool isslink(inum);
  
  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int getslink(inum, slinkinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);

  int readlink(inum,std::string &);
  int symlink(inum,const char*,const char*,inum &);
  
  /** you may need to add symbolic link related methods here.*/
};

#endif 
