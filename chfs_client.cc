// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        return IOERR; \
    } \
} while (0)

/* 
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create', 
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log 
 * to achive all-or-nothing for these transactions.
 */

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
    // if (ec->put(1, "",1) != extent_protocol::OK)
    //     printf("error init root dir\n"); // XYB: init root dir
    chfs_command::txid_t txid;
    if(ec->begin(txid)!= extent_protocol::OK){
        printf("error init root dir\n"); // XYB: init root dir
        return;
    }
    lc->acquire(1);
    if (ec->put(1, "",txid) != extent_protocol::OK){
        lc->release(1);
        printf("error init root dir\n"); // XYB: init root dir
        return;
    }
    if(ec->commit(txid)!= extent_protocol::OK){
        lc->release(1);
        printf("error init root dir\n"); // XYB: init root dir
        return;
    }
    lc->release(1);

}



chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);
    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

int chfs_client::readlink(inum ino,std::string &path){
    extent_protocol::attr a;
    lc->acquire(ino);
    EXT_RPC(ec->getattr(ino,a));
    read(ino,a.size,0,path);
    lc->release(ino);
    return OK;
}

int chfs_client::_create(inum parent, const char *name, extent_protocol::types type, inum &ino_out,chfs_command::txid_t txid){
    std::cout<<"_create"<<std::endl;
    int r = OK;

    bool found;
    lc->acquire(parent);
    lookup(parent,name,found,ino_out);
    if(found){
        return EXIST;
    }
    

    //create an inode
    //format of this inode stays same, buf content of blocks is the format of a dir that we design by ourselves

    EXT_RPC(ec->create(type,ino_out,txid));
    std::list<dirent> entries;
    readdir(parent,entries);
    dirent d;
    d.name = std::string(name);
    d.inum = ino_out;
    entries.push_back(d);

    EXT_RPC(ec->put(parent,entries2str(parent,entries),txid));
    lc->release(parent);
    std::cout<<"create:"<<ino_out<<" "<<type<<std::endl;
    return r;
}
//parent/name --> link
int chfs_client::symlink(inum parent,const char *link,const char *name,inum &ino_out){
    std::cout<<"symlink"<<std::endl;
    chfs_command::txid_t txid;
    EXT_RPC(ec->begin(txid));
    _create(parent,name,extent_protocol::T_SLINK,ino_out,txid);
    std::cout<<"symlink:"<<link<<" "<<ino_out<<std::endl;
    lc->acquire(ino_out);
    EXT_RPC(ec->put(ino_out,link,txid));
    lc->release(ino_out);
    EXT_RPC(ec->commit(txid));
    return OK;
}
bool
chfs_client::isslink(inum inum){
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);
    if (a.type == extent_protocol::T_SLINK) {
        printf("isfile: %lld is a symbolic link\n", inum);
        return true;
    } 
    return false;
}
bool
chfs_client::isdir(inum inum)
{

    std::cout<<"isdir"<<std::endl;
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    } 
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        lc->release(inum);
        goto release;
    }
    lc->release(inum);

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        lc->release(inum);
        goto release;
    }
    lc->acquire(inum);
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

int
chfs_client::getslink(inum inum, slinkinfo &sin)
{
    int r = OK;

    printf("getslink %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        lc->release(inum);
        goto release;
    }
    lc->release(inum);
    sin.atime = a.atime;
    sin.mtime = a.mtime;
    sin.ctime = a.ctime;
    sin.size = a.size;

release:
    return r;
}

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    std::cout<<"setattr"<<std::endl;
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    assert(size>=0);
    // extent_protocol::attr a;
    // EXT_RPC(ec->getattr(ino,a));
    // a.size = size;
    std::string data;
    chfs_command::txid_t txid;
    EXT_RPC(ec->begin(txid));
    lc->acquire(ino);
    EXT_RPC(ec->get(ino,data));
    data.resize(size,'\0');

    EXT_RPC(ec->put(ino,data,txid));
    lc->release(ino);
    EXT_RPC(ec->commit(txid));

    return r;
}
std::string chfs_client::entries2str(inum parent,const std::list<dirent> &entries){
    std::stringstream ss;
    for(auto const &entry:entries){
        ss << entry.name;
        ss.put('\0');// << or string will auto remove \0 , other character is considered valid in file name
        ss << entry.inum;
    }
    return ss.str();
}
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    // int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    chfs_command::txid_t txid;
    EXT_RPC(ec->begin(txid));
    auto res = _create(parent,name,extent_protocol::T_FILE,ino_out, txid);
    EXT_RPC(ec->commit(txid));
    return res;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    // int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */



    //create an inode
    //format of this inode stays same, buf content of blocks is the format of a dir that we design by ourselves

    chfs_command::txid_t txid;
    EXT_RPC(ec->begin(txid));
    auto res = _create(parent,name,extent_protocol::T_DIR,ino_out,txid);
    EXT_RPC(ec->commit(txid));
    return res;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    std::cout<<"lookup"<<std::endl;
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    // the job of parsing path is done by fuse,which we can't see the detail
    // we just save data in symlink and read it in readlink
    assert(isdir(parent));
    lc->acquire(parent);
    std::list<dirent> entries;
    readdir(parent,entries);
    lc->release(parent);
    found = false;
    for(auto const &entry:entries){
        if(strcmp(name,entry.name.c_str())==0){
            found = true;
            ino_out = entry.inum;
            break;
        }
    }
    // std::cout<<"lookup:"<<parent<<" "<<name<<" "<<found<<std::endl;

    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    std::cout<<"readdir"<<std::endl;
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    //a\012b\013c\014

    assert(list.empty());
    std::string buf;
    lc->acquire(dir);
    EXT_RPC(ec->get(dir,buf));
    lc->release(dir);
    std::stringstream ss(buf);
    dirent entry;
    while(std::getline(ss,entry.name,'\0')){
        ss >> entry.inum;
        list.push_back(entry);
    }


    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    std::cout<<"read:"<<ino<<" "<<size<<std::endl;
    assert(size >=0);
    assert(off >=0);
    lc->acquire(ino);
    EXT_RPC(ec->get(ino,data));
    // assert(size+off <= data.length());// it's valid and substr work normally
    lc->release(ino);
    data = data.substr(off,size);

    // std::cout<<"read"<<ino<<" "<<size<<" "<<off<<std::endl;
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    
    std::cout<<"write:"<<ino<<" "<<size<<std::endl;
    assert(size >=0);
    assert(off >=0);
    std::string origin;
    lc->acquire(ino);
    EXT_RPC(ec->get(ino,origin));
    auto len = origin.length();

    // if use += std::string(data)
    // strange bugs may occur
    // if use erase ,test-e may fail because of strange characters
    // be if we use normal characters test-e passed
    // c_str to std::string \0 trancated?

    // std::string _data(data);
    // if(_data.length() > size){// data not end with \0 so len(data) > size?
        // _data.erase(size);
    // }
    
    // std::cout<<off<<" "<<len<<std::endl;
    if((size_t)off >= len) { // if > instead of >=   error in test-e
        origin.resize(off, '\0');
        // origin += _data;
        origin.append(data,size);
    }else{
        if(off + size <= len){
            origin.replace(off,size,data,0,size);
        }else{
            origin.replace(off,len-off,data,0,size);//auto append
            //origin += _data.substr();
        }
    }

    chfs_command::txid_t txid;
    EXT_RPC(ec->begin(txid));
    EXT_RPC(ec->put(ino,origin,txid));
    lc->release(ino);
    EXT_RPC(ec->commit(txid));
    bytes_written = size;
    // std::cout<<"write"<<ino<<" "<<size<<" "<<off<<" "<<origin.length()<<std::endl;
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    std::cout<<"unlink:"<<name<<std::endl;
    bool found;
    inum ino;
    lc->acquire(parent);
    lookup(parent,name,found,ino);
    if(!found){
        return NOENT;
    }

    chfs_command::txid_t txid;
    EXT_RPC(ec->begin(txid));
    lc->acquire(ino);
    EXT_RPC(ec->remove(ino,txid));
    lc->release(ino);
    std::list<dirent> entries;
    readdir(parent,entries);
    entries.remove_if([&name](const auto &entry){return strcmp(entry.name.c_str(),name)==0;});
    EXT_RPC(ec->put(parent,entries2str(parent,entries),txid));
    lc->release(parent);
    EXT_RPC(ec->commit(txid));
    return r;
}

