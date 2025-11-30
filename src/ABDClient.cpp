#include "proto/abd.grpc.pb.h"
#include "proto/abd.pb.h"
#include <grpcpp/grpcpp.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// ---------------- Config file loader ----------------

// Simple: each non-empty, non-comment line is a server address "host:port".
std::vector<std::string> LoadServerAddresses(const std::string& config_path) {
    std::vector<std::string> addresses;
    std::ifstream in(config_path);
    if (!in.is_open()) {
        std::cerr << "Failed to open config file: " << config_path << "\n";
        return addresses;
    }

    std::string line;
    while (std::getline(in, line)) {
        // Trim leading/trailing whitespace (simple version)
        auto start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;  // empty line
        auto end = line.find_last_not_of(" \t\r\n");
        std::string trimmed = line.substr(start, end - start + 1);

        if (trimmed.empty()) continue;
        if (trimmed[0] == '#') continue; // comment line

        // For now we assume whole line is just "host:port"
        addresses.push_back(trimmed);
    }

    return addresses;
}

// ---------------- Basic ABDClient ----------------

class ABDClient {
public:
    explicit ABDClient(const std::string& server_addr)
    {
        channel_ = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
        stub_ = abd::ABDService::NewStub(channel_);

        std::cout << "ABDClient connecting to " << server_addr << "\n";
    }

    // Basic test RPC: just call WriteQuery on a single server
    bool TestWriteQuery(const std::string& key)
    {
        abd::WriteQueryRequest req;
        req.set_key(key);

        abd::WriteQueryReply rep;
        grpc::ClientContext ctx;

        grpc::Status status = stub_->WriteQuery(&ctx, req, &rep);

        if (!status.ok()) {
            std::cerr << "WriteQuery RPC failed: " << status.error_message() << "\n";
            return false;
        }

        std::cout << "Server returned tag: counter="
                  << rep.tag().counter()
                  << " client_id=" << rep.tag().client_id()
                  << "\n";

        return true;
    }

private:
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<abd::ABDService::Stub> stub_;
};

// ---------------- main: use config file to pick address ----------------

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file_path>" << std::endl;
        return 1;
    }

    const std::string config_path = argv[1];
    auto addresses = LoadServerAddresses(config_path);

    if (addresses.empty()) {
        std::cerr << "No server addresses found in config file: " << config_path << "\n";
        return 1;
    }

    // For this basic version, just use the first server in the config.
    const std::string& server_addr = addresses[0];

    ABDClient client(server_addr);
    client.TestWriteQuery("x");

    return 0;
}
