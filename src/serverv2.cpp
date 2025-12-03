/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "abdv2.grpc.pb.h"
#include <bits/stdc++.h>
#include <mutex> 
#include <grpcpp/grpcpp.h>


using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using abdv2::ABDImpl;
using abdv2::ReadGetArg;
using abdv2::ReadGetRet;
using namespace std;

std::unordered_map<std::string, shared_register> kv_store;
int client_id;
std::mutex mtx; 
//-----------------------------------------------------------------------------

// Logic and data behind the server's behavior.
Status ABDReplica::ReadGet(ServerContext* context,
                           const abdv2::ReadGetArg* request,
                            abdv2::ReadGetRet* reply) {
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

// Logic and data behind the server's behavior.
Status ABDReplica::ReadSet(ServerContext* context,
                           const abdv2::ReadSetArg* request,
                           abdv2::ReadSetRet* reply) {
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
                const abdv2::WriteGetArg* request,
                abdv2::WriteGetRet* reply){
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
  reply->set_client_id(client_id);
  // cout << search_key << " " << sr.value << " " << sr.ts << " : Replica write get" << endl;
  // std::cout << "Replica Write Get Done." << endl;
  return Status::OK;
}

Status ABDReplica::WriteSet(grpc::ServerContext* context,
                         const abdv2::WriteSetArg* request,
                         abdv2::WriteSetRet* reply){
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

void RunServer() {
  std::string server_address("0.0.0.0:50052");
  ABDReplica service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  client_id = std::stoi(argv[1]);  
  RunServer();
  return 0;
}