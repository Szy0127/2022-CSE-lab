#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"
#include <list>
#include <set>

template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

 #define RAFT_LOG(fmt, args...) \
     do {                       \
     } while (0);
/*
#define RAFT_LOG(fmt, args...)                                                                                   \
do {                                                                                                         \
    auto now =                                                                                               \
        std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
            std::chrono::system_clock::now().time_since_epoch())                                             \
            .count();                                                                                        \
    printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
} while (0);
*/
public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;
    std::atomic_bool heartbeat;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */
    int vote_for;//-1 for none
    std::set<int> grand;//nodeid grand
    std::list<std::pair<int,command>> log;//term cmd

    /* ---- Volatile state on all server----  */
    int commit_index;
    int last_applied;

    /* ---- Volatile state on leader----  */
    std::map<int,int> next_index;//clientid index of next log
    std::map<int,int> match_index;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    vote_for(-1),
    commit_index(0),
    last_applied(0),
    role(follower) {
    std::unique_lock<std::mutex> _(mtx);
    storage->recover(log,current_term,commit_index,last_applied,vote_for);
    if(log.empty()){
        auto cmd = command{};
        storage->append_log(0,cmd);
        log.emplace_back(0,cmd);
    }else{
        auto log_it = log.begin();
        log_it++;//skip empty log
        auto log_end = log.begin();
        std::advance(log_end,last_applied+1);
        for(;log_it!=log_end;log_it++){
            RAFT_LOG("recover state,term=%d",log_it->first);
            state->apply_log(log_it->second);
        }
    }
    srand(time(nullptr));
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    std::unique_lock<std::mutex> _(mtx);
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);
    term = current_term;
    if(role!=leader){
        return false;
    }
    index = log.size();//last one?
    RAFT_LOG("get new log[%d]",log.size());
    storage->append_log(current_term,cmd);
    log.emplace_back(current_term,cmd);
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    // RAFT_LOG("request_vote");
    std::unique_lock<std::mutex> _(mtx);
    if(args.term < current_term){
        reply.term = current_term;
        reply.vote_granted = false;
        RAFT_LOG("not vote for %d because to old",args.candidate_id);
        return OK;
    }
    if(current_term < args.term){//vote for a new leader
        role = follower;    //may be candidate itself
        vote_for = -1;
        current_term = args.term;
    }
    if(vote_for == -1 || vote_for == args.candidate_id){
        auto last_log_term = log.back().first;
        auto last_log_index = log.size()-1;
        if(args.last_log_term > last_log_term || 
            (args.last_log_term == last_log_term && args.last_log_index >= last_log_index)
        ){
            reply.term = current_term;//useless?
            // current_term = args.term;
            reply.vote_granted = true;
            vote_for = args.candidate_id;
            storage->update_meta(current_term,commit_index,last_applied,vote_for);
            RAFT_LOG("vote for node%d,lastlogterm:%d,candidates lastlogterm:%d",args.candidate_id,last_log_term,args.last_log_term);
            heartbeat.store(true);
            return OK;
        }else{
            RAFT_LOG("cant vote for node%d,lt:%d,clt:%d,li:%d,cli:%d",args.candidate_id,last_log_term,args.last_log_term,last_log_index,args.last_log_index);
        }
    }else{
        RAFT_LOG("cant vote for node%d,already vote for node%d",args.candidate_id,vote_for);
    }
    reply.term = current_term;
    reply.vote_granted = false;
    return OK;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    // RAFT_LOG("handle_request_vote_reply");
    std::unique_lock<std::mutex> _(mtx);
    if(role==leader){
        return;
    }
    if(reply.vote_granted){
        RAFT_LOG("received vote from node%d",target);
        grand.emplace(target);
        if(grand.size() > rpc_clients.size()/2){
            role = leader;
            RAFT_LOG("become leader");
            auto log_size = log.size();
            for(auto i = 0 ; i < rpc_clients.size();i++){
                next_index.emplace(i,log_size);
                match_index.emplace(i,0);
            }
        }
    }else{
        current_term = reply.term;
        role = follower;
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    RAFT_LOG("append_entries");

    heartbeat.store(true);
    std::unique_lock<std::mutex> _(mtx);

    reply.term = current_term;    

    // RAFT_LOG("receive log from node%d",arg.leader_id);
    if(arg.term < current_term){
        reply.success = false;
        RAFT_LOG("current term:%d > %d,append fail from node%d",current_term,arg.term,arg.leader_id);
        return OK;
    }
    if(role==leader){
        if(arg.leader_id==my_id){
            reply.success = true;
            return OK;
        }
        role = follower;
        reply.success = false;
        vote_for = -1;
        storage->update_meta(current_term,commit_index,last_applied,vote_for);
        RAFT_LOG("leader received from node%d",arg.leader_id);
        return OK;//wait for next req
    }

    leader_id = arg.leader_id;
    current_term = arg.term;
    if(arg.entries.empty()){
        reply.success = true;
        if(commit_index < arg.leader_commit){
            RAFT_LOG("ping update commit index from %d to %d",commit_index,arg.leader_commit);
            commit_index = arg.leader_commit;
            storage->update_meta(current_term,commit_index,last_applied);
        }
        return OK;
    }
    
    if(log.size()-1 < arg.prev_log_index){ 
        reply.success = false;
        RAFT_LOG("log index:%d",log.size()-1);
        return OK;
    }

    auto log_it = log.begin();
    std::advance(log_it,arg.prev_log_index);
    if(log_it->first != arg.prev_log_term){
        reply.success = false;
        RAFT_LOG("log[%d].term=%d,not %d",arg.prev_log_index,log_it->first,arg.prev_log_term);
        log.pop_back();
        if(log.size() <= commit_index){
            commit_index--;
            if(last_applied > commit_index){
                RAFT_LOG("last_applied--");
                last_applied--;
            }
        }
        storage->write_logs(log);
        return OK;
    }
    //log_it.term == prev_log_term
    // update from log_it+1
    log_it++;
    log.erase(log_it,log.end());
    log.splice(log.end(),arg.entries);
    storage->write_logs(log);
    // RAFT_LOG("updated log size:%d",log.size());
    if(arg.leader_commit > commit_index){
        auto size = log.size()-1;
        RAFT_LOG("update commit index from %d to %d,from node%d",commit_index,arg.leader_commit<=size ? arg.leader_commit : size,arg.leader_id);
        commit_index = arg.leader_commit<=size ? arg.leader_commit : size;
        storage->update_meta(current_term,commit_index,last_applied);
    }

    return OK;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here
    // RAFT_LOG("handle_append_entries_reply");
    if(arg.entries.empty()){//ping
        return;
    }
    std::unique_lock<std::mutex> _(mtx);
    if(role==follower){
        return;
    }
    if(!reply.success && role==leader){
        if(reply.term > current_term){
            RAFT_LOG("term too old,become follower,update term to %d",reply.term);
            current_term = reply.term;
            role = follower;
        }else{
            next_index[node]--;
        }
        return;
    }
    RAFT_LOG("match_index[%d]update from %d to %d",node,match_index[node],arg.prev_log_index + arg.entries.size());
    match_index[node] = arg.prev_log_index + arg.entries.size();
    next_index[node] = match_index[node]+1;

    std::map<int,int,std::greater<int>> count;
    auto node_size = rpc_clients.size();
    for(auto i = 0; i < node_size;i++){
        if(count.count(match_index[i])){
            count[match_index[i]]++;
        }else{
            count.emplace(match_index[i],1);
        }
    }
    for(const auto p:count){
        if(p.second>node_size/2){
            if(p.first > commit_index){
                RAFT_LOG("update commit index to %d",p.first);
                commit_index = p.first;
                storage->update_meta(current_term,commit_index,last_applied);
            }
            break;
        } 
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    // RAFT_LOG("send_request_vote");
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    // RAFT_LOG("send_append_entries");
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.
    
    // Work for followers and candidates.

    
    // election timeout > ping
    auto timeout = rand()%150+150;
    auto elect_time = 0;
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here

        std::this_thread::sleep_for(std::chrono::milliseconds(timeout));
        if(heartbeat.load()){
            heartbeat.store(false);
            timeout = rand()%150+150;
            continue;
        }
        std::unique_lock<std::mutex> _(mtx);
        if(role==follower){
            grand.clear();
            vote_for = -1;
            elect_time = 0;
            role = candidate;
            current_term++;
            storage->update_meta(current_term,commit_index,last_applied);
            RAFT_LOG("become candidate");
        }
        if(role==candidate){
            request_vote_args arg;
            arg.term = current_term;
            arg.candidate_id = my_id;
            arg.last_log_index = log.size()-1;
            arg.last_log_term = log.back().first;
            RAFT_LOG("send request to all,lastlogterm:%d",arg.last_log_term);
            for(auto i = 0 ;i < rpc_clients.size();i++){
                thread_pool->addObjJob(this, &raft::send_request_vote, i, arg);
            }
            // candidate may repeat sending request several times
            // otherwise
            timeout = 50; // < election timeout
            elect_time++;
            if(elect_time == 3){
                timeout = rand()%150+150;
                role = follower;
                vote_for = -1;
                storage->update_meta(current_term,commit_index,last_applied);
                RAFT_LOG("candidate become follower");
            }
        }
 
    }    
    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        //backup test may recover from 50 to 1,cause much time to wait,may be considered as not making agreement;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::unique_lock<std::mutex> _(mtx);
        if(role==leader){
            // RAFT_LOG("commit");
            for(auto i = 0 ;i < rpc_clients.size();i++){
                append_entries_args<command> arg;
                arg.term = current_term;
                arg.leader_id = my_id;
                arg.leader_commit = commit_index;
                auto prev_index = next_index[i]-1;
                arg.prev_log_index = prev_index;
                auto log_it = log.begin();
                std::advance(log_it,prev_index);
                arg.prev_log_term = log_it->first;
                log_it++;
                if(log_it==log.end()){
                    continue;
                }
                for(;log_it!=log.end();log_it++){
                    arg.entries.push_back(*log_it);
                }
                RAFT_LOG("update to node%d log[%d-%d],previndex:%d,prevterm:%d",i,next_index[i],arg.entries.size()+next_index[i]-1,arg.prev_log_index,arg.prev_log_term);
                thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
            }
        }
    }  

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        //rpc_count test limits 30rpc in election,but when get the result,many pings may already happened
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        std::unique_lock<std::mutex> _(mtx);
        // RAFT_LOG("apply");
        if(last_applied < commit_index){
            auto log_it = log.begin();
            std::advance(log_it,last_applied);
            log_it++;
            auto n = commit_index - last_applied;
            RAFT_LOG("apply log[%d-%d]",last_applied+1,last_applied+n);
            for(auto i = 0;i<n;i++,log_it++){
                state->apply_log(log_it->second);
            }
            last_applied = commit_index;
            storage->update_meta(current_term,commit_index,last_applied);
        }
    }    
    
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::unique_lock<std::mutex> _(mtx);
        if(role==leader){
            RAFT_LOG("ping all");
            append_entries_args<command> arg;
            arg.term = current_term;
            arg.leader_id = my_id;
            for(auto i = 0 ;i < rpc_clients.size();i++){
                arg.leader_commit = commit_index < match_index[i] ? commit_index : match_index[i];
                // RAFT_LOG("ping %d,commit:%d",i,arg.leader_commit);
                thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
            }
        }
    }    
    

    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

#endif // raft_h