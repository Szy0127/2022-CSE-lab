#include "raft_protocol.h"

marshall &operator<<(marshall &m, const request_vote_args &args) {
    // Lab3: Your code here
    m<<args.term;
    m<<args.candidate_id;
    m<<args.last_log_index;
    m<<args.last_log_term;
    return m;
}
unmarshall &operator>>(unmarshall &u, request_vote_args &args) {
    // Lab3: Your code here
    u>>args.term;
    u>>args.candidate_id;
    u>>args.last_log_index;
    u>>args.last_log_term;
    return u;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply) {
    // Lab3: Your code here
    m<<reply.term;
    m<<reply.vote_granted;
    return m;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply) {
    // Lab3: Your code here
    u>>reply.term;
    u>>reply.vote_granted;
    return u;
}

marshall &operator<<(marshall &m, const append_entries_reply &args) {
    // Lab3: Your code here
    m<<args.term;
    m<<args.success;
    return m;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &args) {
    // Lab3: Your code here
    m>>args.term;
    m>>args.success;
    return m;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args) {
    // Lab3: Your code here
    m<<args.term;
    m<<args.leader_id;
    m<<args.last_included_index;
    m<<args.last_included_term;
    m<<(unsigned long long)(args.data.size());
    // std::string str;
    // str.assign(args.data.begin(), args.data.end());
    // m<<str;
    for(auto c:args.data){
        m<<c;
    }
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args) {
    // Lab3: Your code here
    u>>args.term;
    u>>args.leader_id;
    u>>args.last_included_index;
    u>>args.last_included_term;
    unsigned long long size;
    u>>size;
    char c;
    for(auto i = 0 ;i < size ;i++){
        u>>c;
        args.data.push_back(c);
    }
    return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    m<<reply.term;
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply) {
    // Lab3: Your code here
    u>>reply.term;
    return u;
}