
#include "abdv2.grpc.pb.h"
#include <bits/stdc++.h>
#include <mutex> 
#include <grpcpp/grpcpp.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using abd_algo::ABDImpl;
using abd_algo::ReadGetArg;
using abd_algo::ReadGetRet;
using namespace std;

std::unordered_map<std::string, shared_register> kv_store;
int client_id;
std::mutex mtx; 
//-----------------------------------------------------------------------------

// Logic and data behind the server's behavior.
Status ABDReplica::ReadGet(ServerContext* context,
                           const abd_algo::ReadGetArg* request,
                            abd_algo::ReadGetRet* reply) {
  // cout << "In Replica Read Get" << endl;
  auto it = kv_store.find(request->key());
  if(it==kv_store.end()){
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Key not found");
  }
  reply->set_val(it->second.value);
  reply->set_timestamp(it->second.ts);
  // cout << "Replica Read Get Done." << endl;
  return Status::OK;
}

Status ABDReplica::ReadSet(ServerContext* context,
                           const abd_algo::ReadSetArg* request,
                           abd_algo::ReadSetRet* reply) {
  // cout << "In Replica Read Set" << endl;
  mtx.lock();
  // std::cout << "Replica: Read Set got lock" << endl;
  auto it = kv_store.find(request->key());
  if(it==kv_store.end()){
    mtx.unlock();
    // std::cout << "Replica: Read Set released lock" << endl;
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Key not found");
  }
  if(it->second.ts<request->timestamp()){
    
    kv_store[request->key()].ts = request->timestamp();
  }
  mtx.unlock();
  // std::cout << "Replica: Read Set released lock" << endl;
  // cout << "Replica Read Set Done." << endl;

  return Status::OK;
}

Status ABDReplica::WriteGet(ServerContext* context,
                const abd_algo::WriteGetArg* request,
                abd_algo::WriteGetRet* reply){
  // std::cout << "In Replica Write Get" << endl;
  std::string search_key = request->key();
  auto it = kv_store.find(search_key);
  //If key does not exist
  if(it==kv_store.end()){
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Key not found");
  }

  struct shared_register sr = it->second;
  reply->set_val(sr.value);
  reply->set_timestamp(sr.ts);
  reply->set_client_id(0);
  // cout << search_key << " " << sr.value << " " << sr.ts << " : Replica write get" << endl;
  // std::cout << "Replica Write Get Done." << endl;
  return Status::OK;
}

Status ABDReplica::WriteSet(grpc::ServerContext* context,
                         const abd_algo::WriteSetArg* request,
                         abd_algo::WriteSetRet* reply){
  // std::cout << "In Replica Write Get" << endl;
  struct shared_register reg = {
    request->val(),
    request->timestamp()
  };
  mtx.lock();
  // std::cout << "Replica: Write Set got lock" << endl;
  kv_store[request->key()] = reg;
  mtx.unlock();
  // std::cout << "Replica: Write Set released lock" << endl;
  // cout << kv_store.find(request->key())->second.value << endl;
  // std::cout << "Replica Write Get Done." << endl;
  return Status::OK;
}
//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <server_address>" << std::endl;
        return 1;
    }

    std::string server_address = argv[1];
    std::cout << "Starting ABDServer on " << server_address << std::endl;


    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
}
