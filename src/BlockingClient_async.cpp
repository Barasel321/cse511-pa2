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

#include <thread>

class BlockingClient {
public:
    explicit BlockingClient(const std::vector<std::string>& server_addrs)
    {
        for (const auto& addr : server_addrs) {
            std::shared_ptr<grpc::Channel> ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            std::unique_ptr<abd::ABDService::Stub> stub = abd::ABDService::NewStub(ch);
            replicas_.push_back({addr, std::move(ch), std::move(stub)});
            std::cout << "BlockingClient connecting to " << addr << "\n";
        }
        N_ = static_cast<int>(replicas_.size());
        R_ = N_ / 2 + 1;
        W_ = N_ / 2 + 1;
        client_id_ = std::to_string(getpid());
    }

    bool Put(const std::string& key, const std::string& value)
    {
        // 0) Acquire locks on a write quorum
        std::vector<int> locked;
        if (!AcquireQuorumLocks(key, W_, locked)) {
            std::cerr << "PUT " << key << " failed: could not acquire " << W_ << " locks\n";
            return false;
        }

        // 1) WriteQuery to LOCKED replicas only
        abd::Tag max_tag;
        max_tag.set_counter(0);
        max_tag.set_client_id("");
        bool have_tag = false;
        int success_count = 0;

        struct AsyncWriteQueryCall {
            abd::WriteQueryReply reply;
            grpc::ClientContext ctx;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<abd::WriteQueryReply>> responder;
            std::string address;
        };

        grpc::CompletionQueue cq;
        std::vector<AsyncWriteQueryCall*> calls;
        calls.reserve(locked.size());

        for (int idx : locked) {
            auto& r = replicas_[idx];
            auto* call = new AsyncWriteQueryCall;
            call->address = r.address;

            abd::WriteQueryRequest req;
            req.set_key(key);

            call->responder = r.stub->PrepareAsyncWriteQuery(&call->ctx, req, &cq);
            call->responder->StartCall();
            call->responder->Finish(&call->reply, &call->status, call);
            calls.push_back(call);
        }

        int responses = 0;
        void* got_tag;
        bool ok = false;

        while (responses < static_cast<int>(calls.size()) && cq.Next(&got_tag, &ok)) {
            auto* call = static_cast<AsyncWriteQueryCall*>(got_tag);
            responses++;

            if (!ok || !call->status.ok()) {
                std::cerr << "WriteQuery to " << call->address
                          << " failed for PUT " << key << ": "
                          << (call->status.ok() ? "stream not ok" : call->status.error_message())
                          << "\n";
            } else {
                success_count++;
                const abd::Tag& t = call->reply.tag();
                if (!have_tag || TagGreater(t, max_tag)) {
                    max_tag = t;
                    have_tag = true;
                }
            }

            delete call;
        }

        if (success_count < W_) {
            std::cerr << "PUT " << key
                      << " failed: did not reach write quorum in WriteQuery phase ("
                      << success_count << " < " << W_ << ")\n";
            ReleaseLocks(key, locked);
            return false;
        }

        // 2) Choose new tag
        abd::Tag new_tag;
        new_tag.set_counter(max_tag.counter() + 1);
        new_tag.set_client_id(client_id_);

        // 3) WriteProp ONLY to locked replicas
        struct AsyncWritePropCall {
            abd::Ack reply;
            grpc::ClientContext ctx;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<abd::Ack>> responder;
            std::string address;
        };

        grpc::CompletionQueue cq2;
        std::vector<AsyncWritePropCall*> calls2;
        calls2.reserve(locked.size());

        for (int idx : locked) {
            auto& r = replicas_[idx];
            auto* call = new AsyncWritePropCall;
            call->address = r.address;

            abd::WritePropRequest req;
            req.set_key(key);
            *req.mutable_tag() = new_tag;
            req.set_value(value);

            call->responder = r.stub->PrepareAsyncWriteProp(&call->ctx, req, &cq2);
            call->responder->StartCall();
            call->responder->Finish(&call->reply, &call->status, call);
            calls2.push_back(call);
        }

        int ack_count = 0;
        responses = 0;

        while (responses < static_cast<int>(calls2.size()) && cq2.Next(&got_tag, &ok)) {
            auto* call = static_cast<AsyncWritePropCall*>(got_tag);
            responses++;

            if (!ok || !call->status.ok() || !call->reply.ok()) {
                std::cerr << "WriteProp to " << call->address
                          << " failed for PUT " << key << ": "
                          << (call->status.ok() ? "NOK Ack or stream not ok"
                                                : call->status.error_message())
                          << "\n";
            } else {
                ack_count++;
            }

            delete call;
        }

        // 4) Release locks before returning
        ReleaseLocks(key, locked);

        if (ack_count < W_) {
            std::cerr << "PUT " << key
                      << " failed: did not reach write quorum in WriteProp phase ("
                      << ack_count << " < " << W_ << ")\n";
            return false;
        }

        std::cout << " PUT " << key << " = " << value
                  << " (tag.counter=" << new_tag.counter()
                  << ", tag.client_id=" << new_tag.client_id() << ")\n";
        return true;
    }

    bool Get(const std::string& key, std::string& value_out)
    {
        // 0) Acquire locks on a read quorum
        std::vector<int> locked;
        if (!AcquireQuorumLocks(key, R_, locked)) {
            std::cerr << "GET " << key << " failed: could not acquire " << R_ << " locks\n";
            return false;
        }

        // 1) ReadQuery to locked replicas
        abd::Tag max_tag;
        max_tag.set_counter(0);
        max_tag.set_client_id("");
        std::string max_value;
        bool have_value = false;
        int success_count = 0;

        struct AsyncReadQueryCall {
            abd::ReadQueryReply reply;
            grpc::ClientContext ctx;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<abd::ReadQueryReply>> responder;
            std::string address;
        };

        grpc::CompletionQueue cq;
        std::vector<AsyncReadQueryCall*> calls;
        calls.reserve(locked.size());

        for (int idx : locked) {
            auto& r = replicas_[idx];
            auto* call = new AsyncReadQueryCall;
            call->address = r.address;

            abd::ReadQueryRequest req;
            req.set_key(key);

            call->responder = r.stub->PrepareAsyncReadQuery(&call->ctx, req, &cq);
            call->responder->StartCall();
            call->responder->Finish(&call->reply, &call->status, call);
            calls.push_back(call);
        }

        void* got_tag;
        bool ok = false;
        int responses = 0;

        while (responses < static_cast<int>(calls.size()) && cq.Next(&got_tag, &ok)) {
            auto* call = static_cast<AsyncReadQueryCall*>(got_tag);
            responses++;

            if (!ok || !call->status.ok()) {
                std::cerr << "ReadQuery to " << call->address
                          << " failed for GET " << key << ": "
                          << (call->status.ok() ? "stream not ok" : call->status.error_message())
                          << "\n";
            } else {
                success_count++;
                const abd::Tag& t = call->reply.tag();
                if (!have_value || TagGreater(t, max_tag)) {
                    max_tag = t;
                    max_value = call->reply.value();
                    have_value = true;
                }
            }

            delete call;
        }

        if (success_count < R_ || !have_value) {
            std::cerr << "GET " << key
                      << " failed: did not reach read quorum in ReadQuery phase ("
                      << success_count << " < " << R_ << ")\n";
            ReleaseLocks(key, locked);
            return false;
        }

        // 2) Write-back via WriteProp to locked replicas (ABD-style)
        struct AsyncWritePropCall {
            abd::Ack reply;
            grpc::ClientContext ctx;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<abd::Ack>> responder;
            std::string address;
        };

        grpc::CompletionQueue cq2;
        std::vector<AsyncWritePropCall*> calls2;
        calls2.reserve(locked.size());

        for (int idx : locked) {
            auto& r = replicas_[idx];
            auto* call = new AsyncWritePropCall;
            call->address = r.address;

            abd::WritePropRequest req;
            req.set_key(key);
            *req.mutable_tag() = max_tag;
            req.set_value(max_value);

            call->responder = r.stub->PrepareAsyncWriteProp(&call->ctx, req, &cq2);
            call->responder->StartCall();
            call->responder->Finish(&call->reply, &call->status, call);
            calls2.push_back(call);
        }

        int ack_count = 0;
        responses = 0;

        while (responses < static_cast<int>(calls2.size()) && cq2.Next(&got_tag, &ok)) {
            auto* call = static_cast<AsyncWritePropCall*>(got_tag);
            responses++;

            if (!ok || !call->status.ok() || !call->reply.ok()) {
                std::cerr << "WriteProp (read write-back) to " << call->address
                          << " failed for GET " << key << ": "
                          << (call->status.ok() ? "NOK Ack or stream not ok"
                                                : call->status.error_message())
                          << "\n";
            } else {
                ack_count++;
            }

            delete call;
        }

        // 3) Release locks before returning
        ReleaseLocks(key, locked);

        if (ack_count < R_) {
            std::cerr << "GET " << key
                      << " failed: did not reach read quorum in WriteProp phase ("
                      << ack_count << " < " << R_ << ")\n";
            return false;
        }

        value_out = max_value;
        std::cout << " GET " << key << " -> " << value_out
                  << " (tag.counter=" << max_tag.counter()
                  << ", tag.client_id=" << max_tag.client_id() << ")\n";
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

    // Acquire locks on a quorum q; block/retry if not enough are granted
    bool AcquireQuorumLocks(const std::string& key, int q, std::vector<int>& locked_indices)
    {
        locked_indices.clear();
        // We just spin until we get q locks (can be blocked by other clients)
        while (static_cast<int>(locked_indices.size()) < q) {
            struct AsyncAcquireLockCall {
                abd::AcquireLockReply reply;
                grpc::ClientContext ctx;
                grpc::Status status;
                std::unique_ptr<grpc::ClientAsyncResponseReader<abd::AcquireLockReply>> responder;
                std::string address;
                int replica_index;
            };

            grpc::CompletionQueue cq;
            std::vector<AsyncAcquireLockCall*> calls;
            calls.reserve(replicas_.size());

            // Send AcquireLock to all replicas where we *do not yet* hold the lock
            for (int i = 0; i < N_; ++i) {
                if (std::find(locked_indices.begin(), locked_indices.end(), i) != locked_indices.end()) {
                    continue; // already locked
                }

                auto& r = replicas_[i];
                auto* call = new AsyncAcquireLockCall;
                call->address = r.address;
                call->replica_index = i;

                abd::AcquireLockRequest req;
                req.set_key(key);
                req.set_client_id(client_id_);

                call->responder = r.stub->PrepareAsyncAcquireLock(&call->ctx, req, &cq);
                call->responder->StartCall();
                call->responder->Finish(&call->reply, &call->status, call);
                calls.push_back(call);
            }

            void* got_tag;
            bool ok = false;
            int responses = 0;

            while (responses < static_cast<int>(calls.size()) && cq.Next(&got_tag, &ok)) {
                auto* call = static_cast<AsyncAcquireLockCall*>(got_tag);
                responses++;

                if (!ok || !call->status.ok()) {
                    std::cerr << "AcquireLock to " << call->address
                              << " failed for key " << key << ": "
                              << (call->status.ok() ? "stream not ok" : call->status.error_message())
                              << "\n";
                } else {
                    if (call->reply.granted()) {
                        // Got the lock on this replica
                        if (std::find(locked_indices.begin(), locked_indices.end(), call->replica_index) == locked_indices.end()) {
                            locked_indices.push_back(call->replica_index);
                        }
                    } else {
                        // Lock held by someone else → this is where blocking semantics come from
                        // We don't add it; we will retry in the next outer loop iteration.
                    }
                }

                delete call;
            }

            if (static_cast<int>(locked_indices.size()) >= q) {
                return true;
            }

            // Optional: small sleep to avoid busy spinning
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }

        return true;
    }

    void ReleaseLocks(const std::string& key, const std::vector<int>& locked_indices)
    {
        for (int idx : locked_indices) {
            auto& r = replicas_[idx];
            abd::ReleaseLockRequest req;
            req.set_key(key);
            req.set_client_id(client_id_);
            abd::ReleaseLockReply rep;
            grpc::ClientContext ctx;
            grpc::Status status = r.stub->ReleaseLock(&ctx, req, &rep);
            if (!status.ok() || !rep.ok()) {
                std::cerr << "ReleaseLock to " << r.address
                          << " failed for key " << key << ": "
                          << (status.ok() ? "Reply not ok" : status.error_message())
                          << "\n";
            }
        }
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

    BlockingClient client(server_addrs);

    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&t);
    char buf[64];

    std::strftime(buf, sizeof(buf), "%d-%m-%Y_%H:%M:%S", &tm); //stackoverflow ever reliable
    std::string csv_path = "logs/" + input_path + "-" + buf + ".csv";
    std::ofstream csv(csv_path);

    if (!csv.is_open()) {
        std::cerr << "Failed to open CSV file: " << csv_path << "\n";
        return 1;
    }
    csv << "op,key,value,latency_ms,success\n";

    std::string line;

    auto tt_start = std::chrono::steady_clock::now();
    int ops = 0;

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

            //time measuring. overhead should be negligible/irrelevant, we are looking at differences most of all. Plug into R for cool plots
            auto op_start = std::chrono::steady_clock::now();
            bool ok = client.Put(key, value);
            auto op_end = std::chrono::steady_clock::now();
            auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(op_end - op_start).count();


            csv << "PUT," << key << "," << value << "," << latency_ms << "," << (ok ? 1 : 0) << "\n";
            ops++;

            if (!ok) {
                std::cerr << "PUT failed for key " << key << "\n";
            }
        } else if (cmd == "GET" || cmd == "get") {
            std::string key;
            iss >> key;
            std::string value;

            auto op_start = std::chrono::steady_clock::now();
            bool ok = client.Get(key, value);
            auto op_end = std::chrono::steady_clock::now();
            auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(op_end - op_start).count();


            csv << "GET," << key << "," << value << "," << latency_ms << "," << (ok ? 1 : 0) << "\n";
            ops++;
            if (!ok) {
                std::cerr << "GET failed for key " << key << "\n";
            }
        } else {
            std::cerr << "Unknown command in input file: " << cmd << "\n";
        }
    }
    auto tt_stop = std::chrono::steady_clock::now();
    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(tt_stop - tt_start).count();

    double total_time_sec = total_time / 1000.0;

    // Avoid division by zero
    double throughput = (total_time_sec > 0.0) ? (ops / total_time_sec) : 0.0;

    std::cout << "=== Performance Summary ===\n";
    std::cout << "Total Operations : " << ops << "\n";
    std::cout << "Total Time       : " << total_time << " ms (" 
            << total_time_sec << " s)\n";
    std::cout << "Throughput       : " << throughput << " ops/sec\n";


    return 0;
}
