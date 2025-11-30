#include "proto/abd.grpc.pb.h"
#include "proto/abd.pb.h"
#include <grpcpp/grpcpp.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

class ABDClient {
public:
    explicit ABDClient(const std::string& server_addr)
    {
        channel_ = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
        stub_ = abd::ABDService::NewStub(channel_);
        std::cout << "ABDClient connecting to " << server_addr << "\n";
    }

    bool Put(const std::string& key, const std::string& value)
    {
        abd::WriteQueryRequest qreq;
        qreq.set_key(key);
        abd::WriteQueryReply qrep;
        grpc::ClientContext qctx;
        grpc::Status qstatus = stub_->WriteQuery(&qctx, qreq, &qrep);
        if (!qstatus.ok()) {
            std::cerr << "WriteQuery failed for PUT " << key << ": " << qstatus.error_message() << "\n";
            return false;
        }

        abd::WritePropRequest wreq;
        wreq.set_key(key);
        abd::Tag* t = wreq.mutable_tag();
        t->set_counter(qrep.tag().counter() + 1);
        t->set_client_id("client1");
        wreq.set_value(value);

        abd::Ack wrep;
        grpc::ClientContext wctx;
        grpc::Status wstatus = stub_->WriteProp(&wctx, wreq, &wrep);
        if (!wstatus.ok() || !wrep.ok()) {
            std::cerr << "WriteProp failed for PUT " << key << ": " << wstatus.error_message() << "\n";
            return false;
        }

        std::cout << "PUT " << key << " = " << value << "\n";
        return true;
    }

    bool Get(const std::string& key, std::string& value_out)
    {
        abd::ReadQueryRequest rreq;
        rreq.set_key(key);
        abd::ReadQueryReply rrep;
        grpc::ClientContext rctx;
        grpc::Status rstatus = stub_->ReadQuery(&rctx, rreq, &rrep);
        if (!rstatus.ok()) {
            std::cerr << "ReadQuery failed for GET " << key << ": " << rstatus.error_message() << "\n";
            return false;
        }
        value_out = rrep.value();
        std::cout << "GET " << key << " -> " << value_out << "\n";
        return true;
    }

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<abd::ABDService::Stub> stub_;
};

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <input_file>" << std::endl;
        return 1;
    }

    const std::string input_path = argv[1];
    std::ifstream in(input_path);
    if (!in.is_open()) {
        std::cerr << "Failed to open input file: " << input_path << "\n";
        return 1;
    }

    std::string server_addr = "localhost:50051";
    ABDClient client(server_addr);

    std::string line;
    while (std::getline(in, line)) {
        std::string trimmed = line;
        auto start = trimmed.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;
        auto end = trimmed.find_last_not_of(" \t\r\n");
        trimmed = trimmed.substr(start, end - start + 1);
        if (trimmed.empty()) continue;
        if (trimmed[0] == '#') continue;

        std::istringstream iss(trimmed);
        std::string cmd;
        iss >> cmd;
        if (cmd == "PUT" || cmd == "put") {
            std::string key;
            iss >> key;
            std::string value;
            std::getline(iss, value);
            auto vstart = value.find_first_not_of(" \t");
            if (vstart != std::string::npos) {
                value = value.substr(vstart);
            } else {
                value.clear();
            }
            if (!client.Put(key, value)) {
                std::cerr << "PUT failed for key " << key << "\n";
            }
        } else if (cmd == "GET" || cmd == "get") {
            std::string key;
            iss >> key;
            std::string val;
            if (!client.Get(key, val)) {
                std::cerr << "GET failed for key " << key << "\n";
            }
        } else {
            std::cerr << "Unknown command in input file: " << cmd << "\n";
        }
    }

    return 0;
}