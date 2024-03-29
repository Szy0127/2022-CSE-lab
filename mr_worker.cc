#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
	// Copy your code from mr_sequential.cc here.
	vector<KeyVal> ret;
    int last = 0;
    int current = 0;
    int size = content.size();
    // std::cout<<size<<std::endl;
    while(current < size){
        auto c = content[current];
        if((c>='a'&& c<='z') || (c>='A'&&c<='Z')){
            current++;
            continue;
        }
        // if(c == ' ' || c == '\n' || c=='\t' || c=='\0'){
            if(last == current){
                current++;
                last = current;
                continue;
            }
            auto word = content.substr(last,current-last);
            // std::cout<<"$"<<word<<"&";
            KeyVal kv{word,"1"};
            ret.push_back(kv);
            current++;
            last = current;
            continue;
        // }
        // current++;
        // last = current;
    }
    if(last<current){
        auto word = content.substr(last,current-last);
        // std::cout<<"$"<<word<<"&";
        KeyVal kv{word,"1"};
        ret.push_back(kv);
    }

    // std::cout<<"\nmap finish"<<std::endl;
    return ret;

}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.
	int i = 0;
    for(const auto &v:values){
        i += stoi(v);
    }
    return to_string(i);

}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const vector<string> &filenames);
	void doReduce(int index,int file_number);
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

void Worker::doMap(int index, const vector<string> &filenames)
{
	// Lab4: Your code goes here.
	unique_lock<mutex> _(mtx);
	// cout<<"do map"<<index<<endl;
	auto filename = filenames[0];
	string intermediate_path_prefix{basedir + "mr-" + to_string(index) + "-"};
	string content;
	ifstream input_file(filename);
	if(input_file.fail()){
		cout<<"map"<<index<<"open fail"<<endl;
		return;
	}
	getline(input_file,content,'\0');
	vector<KeyVal> KVA = Map(filename,content);
	// cout<<"map function"<<index<<"finished"<<endl;
	// vector<ofstream> intermediates;
	// for(int i = 0; i < REDUCER_COUNT;i++){
		// cout<<i<<endl;
		// ofstream(intermediate_path_prefix+to_string(i+10),ios::app);
		// cout<<i<<endl;
		// intermediates.emplace_back(intermediate_path_prefix+to_string(i),ios::app);
	// }
	// cout<<"open files"<<index<<"finished"<<endl;


	//filesystem is based on raft,which is too slow
	vector<string> file_data(4,"");
	for(const auto&kv:KVA){
		int hash = 0;
		for(const auto&c:kv.key){
			hash = (hash * 256 + static_cast<int>(c))%REDUCER_COUNT;
		}
		file_data[hash] += kv.key + " " + kv.val + "\n";
		// intermediates[hash] << kv.key << " " << kv.val << endl;
	}
	for(int i = 0 ; i < REDUCER_COUNT;i++){
		ofstream f(intermediate_path_prefix+to_string(i),ios::app);
		// intermediates[i]<<file_data[i];
		f<<file_data[i];
	}
	// cout<<"worker map"<<index<<"finished"<<endl;
}

void Worker::doReduce(int index,int file_number)
{
	// Lab4: Your code goes here.
	unique_lock<mutex> _(mtx);
	// cout<<"do reduce"<<index<<endl;
	string intermediate_path_prefix{basedir+"mr-"};
	vector<KeyVal> intermediate;
	// int file_index = 0;
	// while(true){
	// 	string filename = intermediate_path_prefix + to_string(file_index) + "-" + to_string(index);
	// 	ifstream f(filename);
	// 	if(f.fail()){
	// 		break;
	// 	}
	// 	string content;
	// 	getline(f,content,'\0');
	// 	stringstream ss(content);
	// 	KeyVal kv;
	// 	while(ss>>kv.key>>kv.val){
	// 		intermediate.push_back(kv);
	// 	}
	// 	file_index++;
	// }
	for(auto i = 0 ;i < file_number;i++){
		string filename = intermediate_path_prefix + to_string(i) + "-" + to_string(index);
		ifstream f(filename);
		// if(f.fail()){
		// 	break;
		// }
		string content;
		getline(f,content,'\0');
		stringstream ss(content);
		KeyVal kv;
		while(ss>>kv.key>>kv.val){
			intermediate.push_back(kv);
		}
	}
	// cout<<"reducer"<<index<<" finish get intermediate"<<endl;
	sort(intermediate.begin(), intermediate.end(),
    	[](KeyVal const & a, KeyVal const & b) {
		return a.key < b.key;
	});

	// ofstream f(basedir+"mr-out",ios::app);
	stringstream file_data;
    for (unsigned int i = 0; i < intermediate.size();) {
        unsigned int j = i + 1;
        for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key;)
            j++;

        vector < string > values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(intermediate[k].val);
        }

        string output = Reduce(intermediate[i].key, values);
        // printf("%s %s\n", intermediate[i].key.data(), output.data());
		file_data<<intermediate[i].key.data()<<" "<<output.data()<<endl;
        i = j;
    }
	ofstream f(basedir+"mr-out-"+to_string(index),ios::app);
	f<<file_data.str();
	// cout<<"worker reduce"<<index<<"finished"<<endl;

}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab4: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//
		int a;
		mr_protocol::AskTaskResponse res;
		// cout<<"worker asktask"<<endl;
		cl->call(mr_protocol::asktask,a,res);
        switch (static_cast<mr_tasktype>(res.taskType)) {
            case mr_tasktype::MAP:{
				vector<string> filenames{res.map_filename};//why vector?
                doMap(res.index, filenames);
                doSubmit(MAP, res.index);
                break;
			}
            case mr_tasktype::REDUCE:
                doReduce(res.index,res.file_number);
                doSubmit(REDUCE, res.index);
                break;
            case mr_tasktype::NONE:
                sleep(5);
                break;
        }

	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}

