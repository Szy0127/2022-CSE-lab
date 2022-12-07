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

    int last_snapshot_index;//logical  ammulated 
    int last_snapshot_term;
    std::vector<char> snapshot_data;

    /* ---- Volatile state on all server----  */
    int commit_index;   //physical
    int last_applied;   //physical log[last_applied]

    /* ---- Volatile state on leader----  */
    std::map<int,int> next_index;//clientid index of next log //logical index
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

    std::pair<int,int> get_last_logical();//index term
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    heartbeat(false),
    role(follower),
    current_term(0),
    leader_id(-1),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    vote_for(-1),
    last_snapshot_index(0),
    last_snapshot_term(0),
    commit_index(0),
    last_applied(0)
    {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization

    std::unique_lock<std::mutex> _(mtx);
    storage->recover(log,snapshot_data,current_term,commit_index,last_applied,last_snapshot_index,last_snapshot_term,vote_for);
    if(last_snapshot_index){
        state->apply_snapshot(snapshot_data);
        RAFT_LOG("recover snapshot,last index:%d,last term:%d",last_snapshot_index,last_snapshot_term);
    }
    if(log.empty()){
        if(!last_snapshot_index){
            auto cmd = command{};
            log.emplace_back(0,cmd);
            storage->write_logs(log);
        }
    }else{
        auto log_it = log.begin();
        auto log_end = log.begin();
        if(!last_snapshot_index){
            log_it++;//skip empty log
            std::advance(log_end,last_applied+1);
        }else{
            std::advance(log_end,last_applied-last_snapshot_index);
        }
        for(;log_it!=log_end;log_it++){
            RAFT_LOG("recover state,term=%d,last_applied:%d,log size:%ld",log_it->first,last_applied,log.size());
            state->apply_log(log_it->second);
        }
    }
    srand(time(nullptr));
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


    // test get index from new_command, index start from 1
    // test wait for this log to be applied
    // logical index
    storage->append_log(current_term,cmd);
    log.emplace_back(current_term,cmd);
    index = get_last_logical().first;
    RAFT_LOG("get new log[%ld]",index);
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);
    snapshot_data = state->snapshot();
    
    auto log_it = log.begin();
    std::advance(log_it,last_applied - last_snapshot_index);
    RAFT_LOG("save snapshot,advance:%d",last_applied - last_snapshot_index);
    bool empty = log_it == log.begin();
    if(last_snapshot_index && !empty){
        log_it--;
    }
    last_snapshot_index = last_applied;
    last_snapshot_term = empty ? last_snapshot_term : log_it->first;
    // std::list<std::pair<int,command>> temp;
    // temp.splice(temp.begin(),log,log.begin(),log_it);//remove [begin,log_it)
    log_it++;
    log.erase(log.begin(),log_it);//remove [begin,it)
    RAFT_LOG("save snapshot,last index:%d,term:%d,logs remain:%ld",last_snapshot_index,last_snapshot_term,log.size());
    state->apply_snapshot(snapshot_data);//clear state,to match newcommand index
    storage->update_snapshot(snapshot_data,last_snapshot_index,last_snapshot_term);
    storage->write_logs(log);

    if(role!=leader){
        return true;
    }
    install_snapshot_args arg;
    arg.term = current_term;
    arg.leader_id = my_id;
    arg.last_included_index = last_snapshot_index;
    arg.last_included_term = last_snapshot_term;
    arg.data = snapshot_data;
    for(auto i = 0 ;i < num_nodes();i++){
        thread_pool->addObjJob(this, &raft::send_install_snapshot, i, arg);
    }


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
        // auto [last_log_index,last_log_term] = get_last_logical();
        auto p = get_last_logical();
        auto last_log_index = p.first;
        auto last_log_term = p.second;
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
            RAFT_LOG("cant vote for node%d,lastterm:%d,candidatelt:%d,lindex:%d,cli:%d",args.candidate_id,last_log_term,args.last_log_term,last_log_index,args.last_log_index);
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
        if((int)grand.size() > num_nodes()/2){
            role = leader;
            auto last_logical_index = get_last_logical().first;
            RAFT_LOG("become leader,next_index:%d",last_logical_index+1);
            next_index.clear();
            match_index.clear();
            for(auto i = 0 ; i < num_nodes();i++){
                next_index.emplace(i,last_logical_index+1);
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
    // RAFT_LOG("append_entries");

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
    if(vote_for==-1){
        vote_for = arg.leader_id;
    }
    if(current_term==arg.term &&  vote_for!=arg.leader_id){
        reply.success = false;
        reply.term = 0;
        RAFT_LOG("already voted for node%d,cant append from %d",vote_for,arg.leader_id);
        return OK;
    }
    heartbeat.store(true);

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
    auto p = get_last_logical();
    auto last_logical_index = p.first;
    // auto last_logical_term = p.second;
    if(last_logical_index < arg.prev_log_index){ 
        reply.success = false;
        RAFT_LOG("last log index:%d,not %d",last_logical_index,arg.prev_log_index);
        return OK;
    }

    //log_it.term == prev_log_term
    // update from log_it+1
    auto log_it = log.begin();
    std::advance(log_it,arg.prev_log_index-last_snapshot_index);
    // RAFT_LOG("%d %d",last_logical_index,last_snapshot_index);
    bool empty = false;
    if(last_snapshot_index){
        if(log_it !=log.begin()){
            log_it--;
        }else{
            empty = true;
        }
    }
    auto prev_log_index_term = empty ? last_snapshot_term : log_it->first;
    if(prev_log_index_term != arg.prev_log_term){
        reply.success = false;
        RAFT_LOG("log[%d].term=%d,not %d",arg.prev_log_index,prev_log_index_term,arg.prev_log_term);
        if(last_logical_index <= commit_index){
            if(!log.empty()){
                log.pop_back();   
            }
            commit_index--;
            if(last_applied > commit_index){
                RAFT_LOG("last_applied--");
                last_applied--;
            }
            storage->update_meta(current_term,commit_index,last_applied);
        }
        storage->write_logs(log);
        return OK;
    }

    if(!empty){
        log_it++;
    }else{
        log_it = log.begin();
    }

    log.erase(log_it,log.end());//remove [log_it,end)
    log.splice(log.end(),arg.entries);
    storage->write_logs(log);
    RAFT_LOG("updated log size:%ld",log.size());
    if(arg.leader_commit > commit_index){
        auto size = get_last_logical().first;
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

    std::unique_lock<std::mutex> _(mtx);
    if(reply.term > current_term){
        RAFT_LOG("term too old,become follower,update term to %d",reply.term);
        current_term = reply.term;
        role = follower;
        return;
    }
    if(arg.entries.empty()){//ping
        return;
    }
    if(role==follower || role == candidate){
        return;
    }
    if(!reply.success){
        if(reply.term == 0){
            RAFT_LOG("node%d reject append,become follower",node);
            role = follower;
            return;
        }
        if(reply.term > current_term){
            RAFT_LOG("term too old,become follower,update term to %d",reply.term);
            current_term = reply.term;
            role = follower;
        }else{
            if(arg.prev_log_index <= last_snapshot_index){
                install_snapshot_args s_arg;
                s_arg.term = current_term;
                s_arg.leader_id = my_id;
                s_arg.last_included_index = last_snapshot_index;
                s_arg.last_included_term = last_snapshot_term;
                s_arg.data = snapshot_data;
                thread_pool->addObjJob(this, &raft::send_install_snapshot, node, s_arg);
                next_index[node] = last_snapshot_index+1;
                RAFT_LOG("not enough log to sync,send snapshot %d to node %d",last_snapshot_index,node);
            }else{
                // next_index[node]--; // not idempotent
                next_index[node] = arg.prev_log_index;
                RAFT_LOG("node%d dont contain correct log[%d]",node,arg.prev_log_index);
            }
        }
        return;
    }
    RAFT_LOG("match_index[%d]update from %d to %ld",node,match_index[node],arg.prev_log_index + arg.entries.size());
    match_index[node] = arg.prev_log_index + arg.entries.size();
    next_index[node] = match_index[node]+1;

    std::map<int,int,std::greater<int>> count;
    for(auto i = 0; i < num_nodes();i++){
        if(count.count(match_index[i])){
            count[match_index[i]]++;
        }else{
            count.emplace(match_index[i],1);
        }
    }
    for(const auto p:count){
        if(p.second>num_nodes()/2){
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
    std::unique_lock<std::mutex> _(mtx);
    reply.term = current_term;
    if(args.leader_id == my_id){
        return OK;
    }
    if(args.term < current_term){
        return OK;
    }
    if(args.leader_id != leader_id){
        return OK;
    }
    auto last_logical_index = get_last_logical().first;
    if(last_logical_index < args.last_included_index){
        log.clear();
        RAFT_LOG("install_snapshot,clear all logs");
    }else{
        auto log_it = log.begin();
        std::advance(log_it,args.last_included_index - last_snapshot_index);
        if(!last_snapshot_index){
            log_it++;//empty log
        }
        if(log_it->first == args.last_included_term){
            log_it++;
        }
        log.erase(log.begin(),log_it);//remove [begin,it)
        RAFT_LOG("install_snapshot,logs remain %ld",log.size());
    }

    last_snapshot_index = args.last_included_index;
    last_snapshot_term = args.last_included_term;
    last_applied = last_snapshot_index;
    RAFT_LOG("snapshot,index:%d,term:%d",last_snapshot_index,last_snapshot_term);
    state->apply_snapshot(args.data);
    storage->update_snapshot(args.data,last_snapshot_index,last_snapshot_term);
    storage->update_meta(current_term,commit_index,last_applied);
    storage->write_logs(log);
    return OK;

}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(mtx);
    if(reply.term > current_term){
        role = follower;
    }
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
            auto p = get_last_logical();
            arg.last_log_index = p.first;
            arg.last_log_term = p.second; 
            RAFT_LOG("send request to all,lastlogterm:%d",arg.last_log_term);
            for(auto i = 0 ;i < num_nodes();i++){
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
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::unique_lock<std::mutex> _(mtx);
        if(role==leader){
            // RAFT_LOG("commit");
            for(auto i = 0 ;i < num_nodes();i++){
                if(last_snapshot_index > 0 && next_index[i]-1 < last_snapshot_index){
                    install_snapshot_args s_arg;
                    s_arg.term = current_term;
                    s_arg.leader_id = my_id;
                    s_arg.last_included_index = last_snapshot_index;
                    s_arg.last_included_term = last_snapshot_term;
                    s_arg.data = snapshot_data;
                    thread_pool->addObjJob(this, &raft::send_install_snapshot, i, s_arg);
                    next_index[i] = last_snapshot_index+1;
                    RAFT_LOG("not enough log to sync,send snapshot %d to node%d",last_snapshot_index,i);
                    continue;
                }
                append_entries_args<command> arg;
                arg.term = current_term;
                arg.leader_id = my_id;
                arg.leader_commit = commit_index;
                auto prev_index = next_index[i]-1;
                arg.prev_log_index = prev_index;
                auto log_it = log.begin();
                std::advance(log_it,prev_index-last_snapshot_index);
                arg.prev_log_term = log_it == log.begin() ? last_snapshot_term:log_it->first;
                if(!last_snapshot_index){
                    log_it++;
                }
                if(log_it==log.end()){
                    continue;
                }
                for(;log_it!=log.end();log_it++){
                    arg.entries.push_back(*log_it);
                }
                RAFT_LOG("update to node%d log[%d-%ld],previndex:%d,prevterm:%d",i,next_index[i],arg.entries.size()+next_index[i]-1,arg.prev_log_index,arg.prev_log_term);
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
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::unique_lock<std::mutex> _(mtx);
        // RAFT_LOG("apply");
        if(last_applied < commit_index){
            auto log_it = log.begin();
            if(!last_snapshot_index){
                std::advance(log_it,last_applied);
                log_it++;
            }else{
                std::advance(log_it,last_applied-last_snapshot_index);
            }
            auto n = commit_index - last_applied;
            RAFT_LOG("apply log[%d~%d],phisical[%d~%d]",last_applied+1,last_applied+n,last_applied-last_snapshot_index+1,last_applied-last_snapshot_index+n);
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
            // RAFT_LOG("ping all");
            append_entries_args<command> arg;
            arg.term = current_term;
            arg.leader_id = my_id;
            for(auto i = 0 ;i < num_nodes();i++){
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
template <typename state_machine, typename command>
std::pair<int,int> raft<state_machine, command>::get_last_logical() {
    if(last_snapshot_index){
        auto last_logical_index = log.size() + last_snapshot_index;
        auto last_logical_term = log.empty() ? last_snapshot_term : log.back().first;
        return {last_logical_index,last_logical_term};
    }else{
        auto last_logical_index = log.size() - 1;
        auto last_logical_term = log.back().first;
        return {last_logical_index,last_logical_term};
    }
}
#endif // raft_h