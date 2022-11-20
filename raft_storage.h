#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <list>
#include <fstream>
template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here

    void append_log(int term,const command &cmd);
    void write_logs(std::list<std::pair<int,command>> &log);
    void recover(std::list<std::pair<int,command>> &log,int &current_term,int &commit_index,int &last_applied);
    void update_meta(int current_term,int commit_index,int last_applied);

private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string dir_path;// not contain /;
    std::string log_file{"log.bin"};
    std::string meta_file{"meta.bin"};
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
    auto size = cmd.size();
    char c[size];
    cmd.serialize(c,size);
    f.write(c,size);
}

template <typename command>
void raft_storage<command>::recover(std::list<std::pair<int,command>> &log,
int &current_term,int &commit_index,int &last_applied){
    // Lab3: Your code here
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
            auto size = cmd.size();
            char c[size];
            f.read(c,size);
            cmd.deserialize(c,size);
            log.emplace_back(term,cmd);
            f.read((char*)&term,sizeof(int));
        }
    }

    std::ifstream f(dir_path+"/"+meta_file);
    if(f.fail()){
        return;
    }
    f>>current_term>>commit_index>>last_applied;
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
        auto size = cmd.size();
        char c[size];
        cmd.serialize(c,size);
        f.write(c,size);   
    }
}

template <typename command>
void raft_storage<command>::update_meta(int current_term,int commit_index,int last_applied){
    std::unique_lock<std::mutex> _(mtx);
    std::ofstream f(dir_path+"/"+meta_file,std::ios::trunc);
    f<<current_term<<" "<<commit_index<<" "<<last_applied;
}



#endif // raft_storage_h