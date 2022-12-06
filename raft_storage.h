#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <list>
#include <vector>
#include <fstream>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here

    void append_log(int term,const command &cmd);
    void write_logs(std::list<std::pair<int,command>> &log);
    void recover(std::list<std::pair<int,command>> &log,std::vector<char> &snapshot,
    int &current_term,int &commit_index,int &last_applied,int &last_snapshot_index,int &last_snapshot_term,int &vote_for);
    void update_meta(int current_term,int commit_index,int last_applied,int vote_for=-1);
    void update_snapshot(std::vector<char> &snapshot,int last_snapshot_index,int last_snapshot_term); 

private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string dir_path;// not contain /;
    std::string log_file{"log.bin"};
    std::string meta_file{"meta.bin"};
    std::string snapshot_file{"snapshot.bin"};
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    dir_path = dir;

    // std::ofstream f(dir_path+"/"+log_file,std::ios::trunc);
    
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}

template <typename command>
void raft_storage<command>::append_log(int term,const command &cmd) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);
    std::ofstream f(dir_path+"/"+log_file,std::ios::app);
    // f<<term<<std::endl;
    f.write((char*)&term,sizeof(int));
    int size = cmd.size();
    char *c = new char[size];
    cmd.serialize(c,size);
    f.write((char*)&size,sizeof(int));
    f.write(c,size);
    delete[] c;
}

template <typename command>
void raft_storage<command>::recover(std::list<std::pair<int,command>> &log,std::vector<char> &snapshot,
int &current_term,int &commit_index,int &last_applied,int &last_snapshot_index,int &last_snapshot_term,int &vote_for){
    // Lab3: Your code here
    std::cout<<"start recover"<<std::endl;
    std::unique_lock<std::mutex> _(mtx);
    {
        std::ifstream f(dir_path+"/"+log_file);
        if(f.fail()){
            return;
        }
        // f<<term<<std::endl;
        int term;
        f.read((char*)&term,sizeof(int));
        while(!f.eof()){
            auto cmd = command{};
            int size;
            f.read((char*)&size,sizeof(int));
            char *c = new char[size];
            f.read(c,size);
            cmd.deserialize(c,size);
            log.emplace_back(term,cmd);
            f.read((char*)&term,sizeof(int));
            delete[] c;
        }
    }
    std::cout<<log_file<<std::endl;
    {
        std::ifstream f(dir_path+"/"+meta_file);
        if(f.fail()){
            return;
        }
        f>>current_term>>commit_index>>last_applied>>vote_for;
    }
    std::cout<<meta_file<<std::endl;
    {
        std::ifstream f(dir_path+"/"+snapshot_file);
        if(f.fail()){
            return;
        }
        int i;
        f.read((char*)&i,sizeof(int));
        last_snapshot_index = i;
        f.read((char*)&i,sizeof(int));
        last_snapshot_term = i;
        char c;
        f.read(&c,1);
        while(!f.eof()){
            snapshot.push_back(c);
            f.read(&c,1);
        }
    }
    std::cout<<"finish recover"<<std::endl;
}

template <typename command>
void raft_storage<command>::write_logs(std::list<std::pair<int,command>> &log){
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);
    std::ofstream f(dir_path+"/"+log_file,std::ios::trunc);
    // f<<term<<std::endl;
    for(const auto&entry:log){
        const auto term = entry.first;
        const auto cmd = entry.second;
        f.write((char*)&term,sizeof(int));
        int size = cmd.size();
        char *c = new char[size];
        cmd.serialize(c,size);
        f.write((char*)&size,sizeof(int));
        f.write(c,size);  
        delete[] c; 
    }
}

template <typename command>
void raft_storage<command>::update_meta(int current_term,int commit_index,int last_applied,int vote_for){
    std::unique_lock<std::mutex> _(mtx);
    std::ofstream f(dir_path+"/"+meta_file,std::ios::trunc);
    f<<current_term<<" "<<commit_index<<" "<<last_applied<<" "<<vote_for;
}

template <typename command>
void raft_storage<command>::update_snapshot(std::vector<char> &snapshot,int last_snapshot_index,int last_snapshot_term){
    std::unique_lock<std::mutex> _(mtx);
    std::ofstream f(dir_path+"/"+snapshot_file,std::ios::trunc);
    f.write((char*)&last_snapshot_index,sizeof(int));
    f.write((char*)&last_snapshot_term,sizeof(int));
    std::string data;
    data.assign(snapshot.begin(),snapshot.end());
    f<<data;
}




#endif // raft_storage_h