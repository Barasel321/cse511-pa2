#include "proto/abd.grpc.pb.h"
#include "proto/abd.pb.h"
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

class ABDClient {
public:
    explicit ABDClient(const std::vector<std::string>& server_addrs)
    {
        for (const auto& addr : server_addrs) {
            std::shared_ptr<grpc::Channel> ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            std::unique_ptr<abd::ABDService::Stub> stub = abd::ABDService::NewStub(ch);
            replicas_.push_back({addr, std::move(ch), std::move(stub)});
            std::cout << "ABDClient connecting to " << addr << "\n";
        }
        N_ = static_cast<int>(replicas_.size());
        R_ = N_ / 2 + 1;
        W_ = N_ / 2 + 1;
        client_id_ = std::to_string(getpid());
    }

    bool Put(const std::string& key, const std::string& value)
    {
        auto op_start = std::chrono::steady_clock::now();

        abd::Tag max_tag;
        max_tag.set_counter(0);
        max_tag.set_client_id("");
        bool have_tag = false;
        int success_count = 0;

        for (auto& r : replicas_) {
            abd::WriteQueryRequest qreq;
            qreq.set_key(key);
            abd::WriteQueryReply qrep;
            grpc::ClientContext ctx;
            grpc::Status status = r.stub->WriteQuery(&ctx, qreq, &qrep);
            if (!status.ok()) {
                std::cerr << "WriteQuery to " << r.address << " failed for PUT " << key << ": " << status.error_message() << "\n";
                continue;
            }
            success_count++;
            const abd::Tag& t = qrep.tag();
            if (!have_tag || TagGreater(t, max_tag)) {
                max_tag = t;
                have_tag = true;
            }
        }

        if (success_count < W_) {
            auto op_end = std::chrono::steady_clock::now();
            auto latency_us = std::chrono::duration_cast<std::chrono::milliseconds>(op_end - op_start).count();
            std::cerr << "PUT " << key << " failed: did not reach write quorum in WriteQuery phase (" << success_count << " < " << W_ << ") latency_ms=" << latency_us << "\n";
            return false;
        }

        abd::Tag new_tag;
        new_tag.set_counter(max_tag.counter() + 1);
        new_tag.set_client_id(client_id_);

        int ack_count = 0;
        for (auto& r : replicas_) {
            abd::WritePropRequest wreq;
            wreq.set_key(key);
            *wreq.mutable_tag() = new_tag;
            wreq.set_value(value);
            abd::Ack wrep;
            grpc::ClientContext ctx;
            grpc::Status status = r.stub->WriteProp(&ctx, wreq, &wrep);
            if (!status.ok() || !wrep.ok()) {
                std::cerr << "WriteProp to " << r.address << " failed for PUT " << key << ": " << status.error_message() << "\n";
                continue;
            }
            ack_count++;
        }

        if (ack_count < W_) {
            auto op_end = std::chrono::steady_clock::now();
            auto latency_us = std::chrono::duration_cast<std::chrono::milliseconds>(op_end - op_start).count();
            std::cerr << "PUT " << key << " failed: did not reach write quorum in WriteProp phase (" << ack_count << " < " << W_ << ") latency_ms=" << latency_us << "\n";
            return false;
        }

        auto op_end = std::chrono::steady_clock::now();
        auto latency_us = std::chrono::duration_cast<std::chrono::milliseconds>(op_end - op_start).count();
        std::cout << " PUT " << key << " = " << value << " (tag.counter=" << new_tag.counter() << ", tag.client_id=" << new_tag.client_id() << ") latency_ms=" << latency_us << "\n";
        return true;
    }

    bool Get(const std::string& key, std::string& value_out)
    {
        auto op_start = std::chrono::steady_clock::now();

        abd::Tag max_tag;
        max_tag.set_counter(0);
        max_tag.set_client_id("");
        std::string max_value;
        bool have_value = false;
        int success_count = 0;

        for (auto& r : replicas_) {
            abd::ReadQueryRequest rreq;
            rreq.set_key(key);
            abd::ReadQueryReply rrep;
            grpc::ClientContext ctx;
            grpc::Status status = r.stub->ReadQuery(&ctx, rreq, &rrep);
            if (!status.ok()) {
                std::cerr << "ReadQuery to " << r.address << " failed for GET " << key << ": " << status.error_message() << "\n";
                continue;
            }
            success_count++;
            const abd::Tag& t = rrep.tag();
            if (!have_value || TagGreater(t, max_tag)) {
                max_tag = t;
                max_value = rrep.value();
                have_value = true;
            }
        }

        if (success_count < R_ || !have_value) {
            auto op_end = std::chrono::steady_clock::now();
            auto latency_us = std::chrono::duration_cast<std::chrono::milliseconds>(op_end - op_start).count();
            std::cerr << "GET " << key << " failed: did not reach read quorum in ReadQuery phase (" << success_count << " < " << R_ << ") latency_ms=" << latency_us << "\n";
            return false;
        }

        int ack_count = 0;
        for (auto& r : replicas_) {
            abd::WritePropRequest wreq;
            wreq.set_key(key);
            *wreq.mutable_tag() = max_tag;
            wreq.set_value(max_value);
            abd::Ack wrep;
            grpc::ClientContext ctx;
            grpc::Status status = r.stub->WriteProp(&ctx, wreq, &wrep);
            if (!status.ok() || !wrep.ok()) {
                std::cerr << "WriteProp (read write-back) to " << r.address << " failed for GET " << key << ": " << status.error_message() << "\n";
                continue;
            }
            ack_count++;
        }

        if (ack_count < R_) {
            auto op_end = std::chrono::steady_clock::now();
            auto latency_us = std::chrono::duration_cast<std::chrono::milliseconds>(op_end - op_start).count();
            std::cerr << "GET " << key << " failed: did not reach read quorum in WriteProp phase (" << ack_count << " < " << R_ << ") latency_ms=" << latency_us << "\n";
            return false;
        }

        auto op_end = std::chrono::steady_clock::now();
        auto latency_us = std::chrono::duration_cast<std::chrono::milliseconds>(op_end - op_start).count();
        value_out = max_value;
        pid_t current_pid = getpid();
        std::cout << current_pid << " GET " << key << " -> " << value_out << " (tag.counter=" << max_tag.counter() << ", tag.client_id=" << max_tag.client_id() << ") latency_ms=" << latency_us << "\n";
        return true;
    }

private:
    struct Replica {
        std::string address;
        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<abd::ABDService::Stub> stub;
    };

    static bool TagGreater(const abd::Tag& a, const abd::Tag& b)
    {
        if (a.counter() != b.counter()) return a.counter() > b.counter();
        return a.client_id() > b.client_id();
    }

    std::vector<Replica> replicas_;
    int N_ = 0;
    int R_ = 0;
    int W_ = 0;
    std::string client_id_;
};

static std::string Trim(const std::string& s)
{
    std::string result = s;
    auto start = result.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = result.find_last_not_of(" \t\r\n");
    return result.substr(start, end - start + 1);
}

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

    std::ifstream cfg("servers.conf");
    if (!cfg.is_open()) {
        std::cerr << "Failed to open servers.conf\n";
        return 1;
    }

    std::vector<std::string> server_addrs;
    std::string line_cfg;
    while (std::getline(cfg, line_cfg)) {
        std::string trimmed = Trim(line_cfg);
        if (trimmed.empty()) continue;
        if (trimmed[0] == '#') continue;
        server_addrs.push_back(trimmed);
    }

    if (server_addrs.empty()) {
        std::cerr << "No server addresses found in servers.conf\n";
        return 1;
    }

    ABDClient client(server_addrs);

    std::string line;
    while (std::getline(in, line)) {
        std::string trimmed = Trim(line);
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
            value = Trim(value);
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
