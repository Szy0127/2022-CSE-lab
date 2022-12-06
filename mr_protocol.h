#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab4: Your definition here.
		int taskType;
		int index;//number of files for map, number of reducer for reduce
		string map_filename;
	};

	struct AskTaskRequest {
		// Lab4: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab4: Your definition here.
	};

};

marshall &operator<<(marshall &m, const mr_protocol::AskTaskResponse &args) {

	m<<args.taskType;
	m<<args.index;
	// m<<static_cast<int>(args.map_filename.size());
	m<<args.map_filename;
    return m;
}


unmarshall &operator>>(unmarshall &u, mr_protocol::AskTaskResponse &args) {

	u>>args.taskType;
	u>>args.index;
	// int size;
	// u>>size;
	u>>args.map_filename;

    return u;
}

#endif

