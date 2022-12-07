#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft():cmd_tp(CMD_NONE),type(0),id(0),buf{},res(std::make_shared<result>()) {
    // Lab3: Your code here
    res->start = std::chrono::system_clock::now();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    //4cmd_tp 4type 8id
    return 4+4+8+buf.size();
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    int loc = 0;
    for(auto i  = 0 ;i < 4 ;i++){
        buf_out[loc+i] = ((char*)&cmd_tp)[i];
    }
    loc += 4;
    for(auto i  = 0 ;i < 4 ;i++){
        buf_out[loc+i] = ((char*)&type)[i];
    }
    loc += 4;
    for(auto i  = 0 ;i < 8 ;i++){
        buf_out[loc+i] = ((char*)&id)[i];
    }
    loc += 8;
    for(auto i = 0; i < size-4-4-8;i++){
        buf_out[loc+i] = buf[i];
    }

    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    int loc = 0;
    for(auto i  = 0 ;i < 4 ;i++){
        ((char*)&cmd_tp)[i] = buf_in[loc+i] ;
    }
    loc += 4;
    for(auto i  = 0 ;i < 4 ;i++){
        ((char*)&type)[i] = buf_in[loc+i] ;
    }
    loc += 4;
    for(auto i  = 0 ;i < 8 ;i++){
        ((char*)&id)[i] = buf_in[loc+i] ;
    }
    loc += 8;
    // for(auto i = 0; i < size-4-4-8;i++){
    //     buf[i] = buf_in[loc+i] ;
    // }
    buf.assign(buf_in+loc,buf_in+size);

    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    m<<static_cast<int>(cmd.cmd_tp);
    m<<cmd.type;
    m<<cmd.id;
    m<<cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int t;
    u>>t;
    cmd.cmd_tp = static_cast<chfs_command_raft::command_type>(t);
    u>>cmd.type;
    u>>cmd.id;
    u>>cmd.buf;
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    std::unique_lock<std::mutex> _(chfs_cmd.res->mtx);
    int t;
    switch (chfs_cmd.cmd_tp)
    {
    case chfs_command_raft::CMD_CRT:
        es.create(chfs_cmd.type,chfs_cmd.res->id);
        break;
    case chfs_command_raft::CMD_PUT:
        es.put(chfs_cmd.id,chfs_cmd.buf,t);
        break;
    case chfs_command_raft::CMD_GET:
        es.get(chfs_cmd.id,chfs_cmd.res->buf);
        break;
    case chfs_command_raft::CMD_GETA:
        es.getattr(chfs_cmd.id,chfs_cmd.res->attr);
        break;
    case chfs_command_raft::CMD_RMV:
        es.remove(chfs_cmd.id,t);
        break;
    default:
        break;
    }

    chfs_cmd.res->done = true;
    chfs_cmd.res->cv.notify_all();
    return;
}


