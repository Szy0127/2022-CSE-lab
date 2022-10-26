// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"



extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  char *buf = new char[DISK_SIZE];
  auto exist = _persister->restore_checkpoint(buf,DISK_SIZE);
  if(exist){
    im->set_data(buf);
  }
  delete[] buf;
  // Your code here for Lab2A: recover data on startup
  std::vector<chfs_command> log_entries;
  std::set<chfs_command::txid_t> commited;
  _persister->restore_logdata(log_entries,commited);
  for(const auto &log:log_entries){
    if(!log.in_checkpoint){// commited:redo  uncommited:ignore
      if(commited.count(log.id)){
        std::cout<<"recover "<<log.id<<" ";
        redo(log);
      }
    }else{//commited:ignore uncommited: undo
      if(!commited.count(log.id)){
        undo(log);
      }
    }
  }
}


void extent_server::redo(const chfs_command& log)
{
  switch(log.type){
    case chfs_command::CMD_CREATE:{
      inode_t inode;
      inode.type = log.inode_attr.type;
      inode.size = log.inode_attr.size;
      inode.atime = log.inode_attr.atime;
      inode.ctime = log.inode_attr.ctime;
      inode.mtime = log.inode_attr.mtime;
      im->alloc_inode(log.inum,&inode);
      std::cout<<"create"<<log.inum<<std::endl;
      break;
      }
      case chfs_command::CMD_PUT:{
      im->write_file(log.inum, log.new_val.data(), log.new_val.length());
      std::cout<<"put"<<log.inum<<" "<<log.new_val.length()<<std::endl;
      break;
      }
      case chfs_command::CMD_REMOVE:{
      im->remove_file(log.inum);
      std::cout<<"remove"<<log.inum<<std::endl;
      break;
      }
      default:break;
  }
}

void extent_server::undo(const chfs_command& log)
{
  switch(log.type){
    case chfs_command::CMD_CREATE:{
      extent_protocol::attr a;
      getattr(log.inum,a);
      if(a.type!=0){
        im->free_inode(log.inum);
      }
      break;
      }
      case chfs_command::CMD_PUT:{
        std::string buf;
        get(log.inum,buf);
        if(buf == log.new_val){
          im->write_file(log.inum, log.old_val.data(), log.old_val.length());
        }
        break;
        }
      case chfs_command::CMD_REMOVE:{
          extent_protocol::attr a;
          getattr(log.inum,a);
          if(a.type==0){
            inode_t inode;
            inode.type = log.inode_attr.type;
            inode.size = log.inode_attr.size;
            inode.atime = log.inode_attr.atime;
            inode.ctime = log.inode_attr.ctime;
            inode.mtime = log.inode_attr.mtime;
            im->alloc_inode(log.inum,&inode);
            im->write_file(log.inum, log.old_val.data(), log.old_val.length());
          }
      break;
      }
      default:break;
    }
}

chfs_command::txid_t extent_server::begin()
{
  auto cp = _persister->append_log({chfs_command::CMD_BEGIN,++_txid});
  if(cp){
    checkpoint();
  }
  return _txid;
}
void extent_server::commit(chfs_command::txid_t txid)
{
  auto cp = _persister->append_log({chfs_command::CMD_COMMIT,txid});
  if(cp){
    checkpoint();
  }
}

// old:null new:id  undo:free_inode
int extent_server::create(uint32_t type, extent_protocol::extentid_t &id,chfs_command::txid_t txid)
{
  // alloc a new inode and return inum
  // printf("extent_server: create inode\n");
  id = im->alloc_inode(type);
  extent_protocol::attr a;
  getattr(id,a);
  // std::cout<<"create"<<txid<<" "<<id<<std::endl;
  auto cp = _persister->append_log({chfs_command::CMD_CREATE,txid,static_cast<chfs_command::inum_t>(id),*reinterpret_cast<chfs_command::inode_attr_t*>(&a)});
  if(cp){
    checkpoint();
  }
  return extent_protocol::OK;
}

//old:get(id) new:buf undo:put(old)
int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &,chfs_command::txid_t txid)
{
  // printf("extent_server: put %lld\n", id);
  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  // printf("extent_server: put %lld size %d\n", id,size);
  std::string old_buf;
  get(id,old_buf);
  im->write_file(id, cbuf, size);
  extent_protocol::attr a;
  getattr(id,a);
  auto cp = _persister->append_log({chfs_command::CMD_PUT,txid,static_cast<chfs_command::inum_t>(id),*reinterpret_cast<chfs_command::inode_attr_t*>(&a),old_buf,buf});
  // std::cout<<"put"<<txid<<" "<<id<<" "<<size<<std::endl;
  if(cp){
    checkpoint();
  }
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  // printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

//old:get(id) new:null undo: create(id) put(old)
int extent_server::remove(extent_protocol::extentid_t id, int &,chfs_command::txid_t txid)
{
  // printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  std::string buf;
  get(id,buf);
  im->remove_file(id);
  extent_protocol::attr a;
  getattr(id,a);
  auto cp = _persister->append_log({chfs_command::CMD_REMOVE,txid,static_cast<chfs_command::inum_t>(id),*reinterpret_cast<chfs_command::inode_attr_t*>(&a),buf,""});
  // std::cout<<"remove"<<txid<<" "<<id<<std::endl;
  if(cp){
    checkpoint();
  }
  return extent_protocol::OK;
}

void extent_server::checkpoint()
{
  auto data = im->get_data();
  _persister->checkpoint(data,DISK_SIZE);
}
