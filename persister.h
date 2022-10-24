#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <set>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 1024

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_REMOVE

    };

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;

    using inum_t = uint32_t;
    inum_t inum = 0;
    struct inode_attr_t{
        uint32_t type;
        unsigned int atime;
        unsigned int mtime;
        unsigned int ctime;
        unsigned int size;
    }inode_attr;

    
    std::string old_val{};
    std::string new_val{};

    // constructor
    chfs_command(){}

    chfs_command(cmd_type t,txid_t tid,inum_t n,inode_attr_t inode,std::string old,std::string new_):type(t),id(tid),inum(n),inode_attr(inode),old_val(old),new_val(new_) {}
    
    //begin commit
    chfs_command(cmd_type t,txid_t tid):type(t),id(tid){}

    chfs_command(cmd_type t,txid_t tid,inum_t n,inode_attr_t inode):type(t),id(tid),inum(n),inode_attr(inode) {}



    // uint64_t size() const {
    //     uint64_t s = sizeof(cmd_type) + sizeof(txid_t);
    //     return s;
    // }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(command log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata(std::vector<command> &log_entries,std::set<chfs_command::txid_t> &commited);
    void restore_checkpoint();

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    // std::vector<command> log_entries;
    // std::set<chfs_command::txid_t> commited;

};

template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::append_log(command log) {
    // Your code here for lab2A
    std::ofstream f(file_path_logfile,std::ios::app);
    f.write((char*)&log.type,sizeof(log.type));
    f<<log.id<<" "<<log.inum<<" ";
    f.write((char*)&log.inode_attr,sizeof(chfs_command::inode_attr_t));
    if(log.type == chfs_command::CMD_REMOVE || log.type == chfs_command::CMD_PUT){
        auto len = log.old_val.length();
        f<<len<<" ";
        f.write(log.old_val.data(),len);
        if(log.type == chfs_command::CMD_PUT){
            len = log.new_val.length();
            f<<len<<" ";
            f.write(log.new_val.data(),len);
        }
    }
    f<<'\n';
}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A

}

template<typename command>
void persister<command>::restore_logdata(std::vector<command> &log_entries,std::set<chfs_command::txid_t> &commited) {
    // Your code here for lab2A
    std::ifstream f(file_path_logfile);
    command log;
    while(!f.eof()){
        f.read((char*)&log.type,sizeof(log.type));
        f>>log.id>>log.inum;
        f.read((char*)&log.inode_attr,sizeof(chfs_command::inode_attr_t));
        if(log.type == chfs_command::CMD_REMOVE || log.type == chfs_command::CMD_PUT){
            int len;
            f>>len;
            char c[len+1];
            f.read(c,len);
            log.old_val.assign(c, len);
            if(log.type == chfs_command::CMD_PUT){
                f>>len;
                char c1[len+1];
                f.read(c1,len);
                log.new_val.assign(c1, len);
            }
        }
    }
    if(log.type==chfs_command::CMD_COMMIT){
        commited.insert(log.id);
    }
    log_entries.push_back(std::move(log));
};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A

};


using chfs_persister = persister<chfs_command>;

#endif // persister_h