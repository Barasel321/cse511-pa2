#include "proto/abd.grpc.pb.h"
#include "proto/abd.pb.h"
#include <grpcpp/grpcpp.h>

#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>

// Simple struct to hold (tag, value) per key
struct Entry {
    abd::Tag tag;
    std::string value;
};

class ABDServer final : public abd::ABDService::Service {
public:
    ABDServer() {}

    // Handle WriteQuery: return current tag for key
    grpc::Status WriteQuery(grpc::ServerContext* ctx,
                            const abd::WriteQueryRequest* req,
                            abd::WriteQueryReply* rep) override {
        std::lock_guard<std::mutex> lock(mu_);
        const std::string& key = req->key();

        auto it = table_.find(key);
        if (it == table_.end()) {
            // If key not present, respond with tag=(0, "")
            abd::Tag* t = rep->mutable_tag();
            t->set_counter(0);
            t->set_client_id("");
        } else {
            *rep->mutable_tag() = it->second.tag;
        }
        return grpc::Status::OK;
    }

    // Handle ReadQuery: return current (tag, value) for key
    grpc::Status ReadQuery(grpc::ServerContext* ctx,
                           const abd::ReadQueryRequest* req,
                           abd::ReadQueryReply* rep) override {
        std::lock_guard<std::mutex> lock(mu_);
        const std::string& key = req->key();

        auto it = table_.find(key);
        if (it == table_.end()) {
            // default tag & empty value
            abd::Tag* t = rep->mutable_tag();
            t->set_counter(0);
            t->set_client_id("");
            rep->set_value("");
        } else {
            *rep->mutable_tag() = it->second.tag;
            rep->set_value(it->second.value);
        }
        return grpc::Status::OK;
    }

    // Handle WriteProp: update stored (tag,value) if the new tag is greater
    grpc::Status WriteProp(grpc::ServerContext* ctx,
                           const abd::WritePropRequest* req,
                           abd::Ack* ack) override {
        std::lock_guard<std::mutex> lock(mu_);
        const std::string& key = req->key();

        const abd::Tag& incoming = req->tag();

        auto it = table_.find(key);
        if (it == table_.end()) {
            Entry entry;
            entry.tag = incoming;
            entry.value = req->value();
            table_[key] = entry;
        } else {
            abd::Tag& current = it->second.tag;
            bool replace = false;
            if (incoming.counter() > current.counter() ||
                (incoming.counter() == current.counter() &&
                 incoming.client_id() > current.client_id())) {
                replace = true;
            }

            if (replace) {
                it->second.tag = incoming;
                it->second.value = req->value();
            }
        }

        ack->set_ok(true);
        return grpc::Status::OK;
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, Entry> table_;
};

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <server_address>" << std::endl;
        return 1;
    }

    std::string server_address = argv[1];
    std::cout << "Starting ABDServer on " << server_address << std::endl;

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    ABDServer service;
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server running...\n";
    server->Wait();

    return 0;
}
