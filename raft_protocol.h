#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"
#include <list>
#include <vector>
enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    // Lab3: Your code here
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    // Lab3: Your code here
    int term;
    bool vote_granted;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

// use pair,convenient for raft.h to construct
template <typename command>
class log_entry {
public:
    // Lab3: Your code here
    int term;
    command cmd;
};

template <typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    // Lab3: Your code here
    m<<entry.term;
    m<<entry.cmd;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    // Lab3: Your code here
    u>>entry.term;
    u>>entry.cmd;
    return u;
}

template <typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::list<std::pair<int,command>> entries;
    int leader_commit;
};

template <typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Lab3: Your code here
    m<<args.term;
    m<<args.leader_id;
    m<<args.prev_log_index;
    m<<args.prev_log_term;
    m<<(unsigned int)(args.entries.size());
    for(const auto&entry:args.entries){
        m<<entry.first<<entry.second;
    }
    m<<args.leader_commit;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    // Lab3: Your code here
    u>>args.term;
    u>>args.leader_id;
    u>>args.prev_log_index;
    u>>args.prev_log_term;
    int len;
    u>>len;
    int term;
    command cmd;
    for(auto i = 0; i < len ;i++){
        u>>term>>cmd;
        args.entries.emplace_back(term,cmd);
    }
    u>>args.leader_commit;
    return u;
}

class append_entries_reply {
public:
    // Lab3: Your code here
    int term;
    bool success;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
public:
    // Lab3: Your code here
    int term;
    int leader_id;
    int last_included_index;
    int last_included_term;
    std::vector<char> data;
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply {
public:
    // Lab3: Your code here
    int term;
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h