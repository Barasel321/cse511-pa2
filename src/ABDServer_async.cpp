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

// Forward-declare helper for tag comparison
static bool TagGreater(const abd::Tag& a, const abd::Tag& b) {
    if (a.counter() > b.counter()) return true;
    if (a.counter() < b.counter()) return false;
    return a.client_id() > b.client_id();
}

class ABDServer {
public:
    explicit ABDServer(const std::string& server_address)
        : server_address_(server_address) {}

    void Run() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
        builder.RegisterService(&service_);

        cq_ = builder.AddCompletionQueue();
        server_ = builder.BuildAndStart();

        std::cout << "Async ABDServer listening on " << server_address_ << std::endl;

        // Kick off a CallData instance for each RPC type
        new WriteQueryCallData(&service_, cq_.get(), &table_, &mu_);
        new ReadQueryCallData(&service_, cq_.get(), &table_, &mu_);
        new WritePropCallData(&service_, cq_.get(), &table_, &mu_);

        // NEW: lock RPC handlers
        new AcquireLockCallData(&service_, cq_.get(), &lock_table_, &mu_);
        new ReleaseLockCallData(&service_, cq_.get(), &lock_table_, &mu_);

        void* tag;
        bool ok;
        while (cq_->Next(&tag, &ok)) {
            // tag is actually a pointer to a CallData instance
            static_cast<CallData*>(tag)->Proceed(ok);
        }
    }

private:
    // Base class for per-RPC state machines
    class CallData {
    public:
        virtual ~CallData() = default;
        virtual void Proceed(bool ok) = 0;
    };

    // ----- WriteQuery -----
    class WriteQueryCallData final : public CallData {
    public:
        WriteQueryCallData(abd::ABDService::AsyncService* service,
                           grpc::ServerCompletionQueue* cq,
                           std::unordered_map<std::string, Entry>* table,
                           std::mutex* mu)
            : service_(service),
              cq_(cq),
              responder_(&ctx_),
              status_(CREATE),
              table_(table),
              mu_(mu) {
            // Start the state machine
            Proceed(true);
        }

        void Proceed(bool ok) override {
            if (!ok && status_ != FINISH) {
                // Stream/ctx aborted; finish and clean up
                status_ = FINISH;
            }

            if (status_ == CREATE) {
                status_ = PROCESS;
                // Request a new incoming WriteQuery RPC
                service_->RequestWriteQuery(&ctx_, &request_, &responder_,
                                            cq_, cq_, this);
            } else if (status_ == PROCESS) {
                // Spawn a new CallData to serve the next client
                new WriteQueryCallData(service_, cq_, table_, mu_);

                // Build reply using shared ABD state
                abd::WriteQueryReply reply;
                {
                    std::lock_guard<std::mutex> lock(*mu_);
                    const std::string& key = request_.key();
                    auto it = table_->find(key);
                    if (it == table_->end()) {
                        abd::Tag* t = reply.mutable_tag();
                        t->set_counter(0);
                        t->set_client_id("");
                    } else {
                        *reply.mutable_tag() = it->second.tag;
                    }
                }

                status_ = FINISH;
                responder_.Finish(reply, grpc::Status::OK, this);
            } else {
                // FINISH
                delete this;
            }
        }

    private:
        abd::ABDService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;

        abd::WriteQueryRequest request_;
        grpc::ServerAsyncResponseWriter<abd::WriteQueryReply> responder_;

        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;

        std::unordered_map<std::string, Entry>* table_;
        std::mutex* mu_;
    };

    // ----- ReadQuery -----
    class ReadQueryCallData final : public CallData {
    public:
        ReadQueryCallData(abd::ABDService::AsyncService* service,
                          grpc::ServerCompletionQueue* cq,
                          std::unordered_map<std::string, Entry>* table,
                          std::mutex* mu)
            : service_(service),
              cq_(cq),
              responder_(&ctx_),
              status_(CREATE),
              table_(table),
              mu_(mu) {
            Proceed(true);
        }

        void Proceed(bool ok) override {
            if (!ok && status_ != FINISH) {
                status_ = FINISH;
            }

            if (status_ == CREATE) {
                status_ = PROCESS;
                service_->RequestReadQuery(&ctx_, &request_, &responder_,
                                           cq_, cq_, this);
            } else if (status_ == PROCESS) {
                new ReadQueryCallData(service_, cq_, table_, mu_);

                abd::ReadQueryReply reply;
                {
                    std::lock_guard<std::mutex> lock(*mu_);
                    const std::string& key = request_.key();
                    auto it = table_->find(key);
                    if (it == table_->end()) {
                        abd::Tag* t = reply.mutable_tag();
                        t->set_counter(0);
                        t->set_client_id("");
                        reply.set_value("");
                    } else {
                        *reply.mutable_tag() = it->second.tag;
                        reply.set_value(it->second.value);
                    }
                }

                status_ = FINISH;
                responder_.Finish(reply, grpc::Status::OK, this);
            } else {
                delete this;
            }
        }

    private:
        abd::ABDService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;

        abd::ReadQueryRequest request_;
        grpc::ServerAsyncResponseWriter<abd::ReadQueryReply> responder_;

        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;

        std::unordered_map<std::string, Entry>* table_;
        std::mutex* mu_;
    };

    // ----- WriteProp -----
    class WritePropCallData final : public CallData {
    public:
        WritePropCallData(abd::ABDService::AsyncService* service,
                          grpc::ServerCompletionQueue* cq,
                          std::unordered_map<std::string, Entry>* table,
                          std::mutex* mu)
            : service_(service),
              cq_(cq),
              responder_(&ctx_),
              status_(CREATE),
              table_(table),
              mu_(mu) {
            Proceed(true);
        }

        void Proceed(bool ok) override {
            if (!ok && status_ != FINISH) {
                status_ = FINISH;
            }

            if (status_ == CREATE) {
                status_ = PROCESS;
                service_->RequestWriteProp(&ctx_, &request_, &responder_,
                                           cq_, cq_, this);
            } else if (status_ == PROCESS) {
                new WritePropCallData(service_, cq_, table_, mu_);

                abd::Ack reply;
                {
                    std::lock_guard<std::mutex> lock(*mu_);
                    const std::string& key = request_.key();
                    const abd::Tag& incoming = request_.tag();

                    auto it = table_->find(key);
                    if (it == table_->end()) {
                        Entry entry;
                        entry.tag = incoming;
                        entry.value = request_.value();
                        (*table_)[key] = std::move(entry);
                    } else {
                        abd::Tag& current = it->second.tag;
                        if (TagGreater(incoming, current)) {
                            it->second.tag = incoming;
                            it->second.value = request_.value();
                        }
                    }
                    reply.set_ok(true);
                    reply.set_error("");
                }

                status_ = FINISH;
                responder_.Finish(reply, grpc::Status::OK, this);
            } else {
                delete this;
            }
        }

    private:
        abd::ABDService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;

        abd::WritePropRequest request_;
        grpc::ServerAsyncResponseWriter<abd::Ack> responder_;

        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;

        std::unordered_map<std::string, Entry>* table_;
        std::mutex* mu_;
    };

    // ----- AcquireLock -----
    class AcquireLockCallData final : public CallData {
    public:
        AcquireLockCallData(abd::ABDService::AsyncService* service,
                            grpc::ServerCompletionQueue* cq,
                            std::unordered_map<std::string, std::string>* lock_table,
                            std::mutex* mu)
            : service_(service),
              cq_(cq),
              responder_(&ctx_),
              status_(CREATE),
              lock_table_(lock_table),
              mu_(mu) {
            Proceed(true);
        }

        void Proceed(bool ok) override {
            if (!ok && status_ != FINISH) {
                status_ = FINISH;
            }

            if (status_ == CREATE) {
                status_ = PROCESS;
                service_->RequestAcquireLock(&ctx_, &request_, &responder_,
                                             cq_, cq_, this);
            } else if (status_ == PROCESS) {
                // Spawn next handler
                new AcquireLockCallData(service_, cq_, lock_table_, mu_);

                abd::AcquireLockReply reply;
                {
                    std::lock_guard<std::mutex> lock(*mu_);
                    const std::string& key = request_.key();
                    const std::string& client_id = request_.client_id();

                    auto it = lock_table_->find(key);
                    if (it == lock_table_->end() || it->second.empty()) {
                        // No one holds the lock: grant to this client
                        (*lock_table_)[key] = client_id;
                        reply.set_granted(true);
                        reply.set_holder(client_id);
                    } else if (it->second == client_id) {
                        // Re-entrant lock by same client: grant again
                        reply.set_granted(true);
                        reply.set_holder(client_id);
                    } else {
                        // Held by someone else
                        reply.set_granted(false);
                        reply.set_holder(it->second);
                    }
                }

                status_ = FINISH;
                responder_.Finish(reply, grpc::Status::OK, this);
            } else {
                delete this;
            }
        }

    private:
        abd::ABDService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;

        abd::AcquireLockRequest request_;
        grpc::ServerAsyncResponseWriter<abd::AcquireLockReply> responder_;

        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;

        std::unordered_map<std::string, std::string>* lock_table_;
        std::mutex* mu_;
    };

    // ----- ReleaseLock -----
    class ReleaseLockCallData final : public CallData {
    public:
        ReleaseLockCallData(abd::ABDService::AsyncService* service,
                            grpc::ServerCompletionQueue* cq,
                            std::unordered_map<std::string, std::string>* lock_table,
                            std::mutex* mu)
            : service_(service),
              cq_(cq),
              responder_(&ctx_),
              status_(CREATE),
              lock_table_(lock_table),
              mu_(mu) {
            Proceed(true);
        }

        void Proceed(bool ok) override {
            if (!ok && status_ != FINISH) {
                status_ = FINISH;
            }

            if (status_ == CREATE) {
                status_ = PROCESS;
                service_->RequestReleaseLock(&ctx_, &request_, &responder_,
                                             cq_, cq_, this);
            } else if (status_ == PROCESS) {
                // Spawn next handler
                new ReleaseLockCallData(service_, cq_, lock_table_, mu_);

                abd::ReleaseLockReply reply;
                {
                    std::lock_guard<std::mutex> lock(*mu_);
                    const std::string& key = request_.key();
                    const std::string& client_id = request_.client_id();

                    auto it = lock_table_->find(key);
                    if (it != lock_table_->end() && it->second == client_id) {
                        // Only current holder may release
                        lock_table_->erase(it);
                        reply.set_ok(true);
                    } else {
                        // Either no lock or wrong client; treat as failure
                        reply.set_ok(false);
                    }
                }

                status_ = FINISH;
                responder_.Finish(reply, grpc::Status::OK, this);
            } else {
                delete this;
            }
        }

    private:
        abd::ABDService::AsyncService* service_;
        grpc::ServerCompletionQueue* cq_;
        grpc::ServerContext ctx_;

        abd::ReleaseLockRequest request_;
        grpc::ServerAsyncResponseWriter<abd::ReleaseLockReply> responder_;

        enum CallStatus { CREATE, PROCESS, FINISH };
        CallStatus status_;

        std::unordered_map<std::string, std::string>* lock_table_;
        std::mutex* mu_;
    };

    std::string server_address_;
    abd::ABDService::AsyncService service_;
    std::unique_ptr<grpc::ServerCompletionQueue> cq_;
    std::unique_ptr<grpc::Server> server_;

    std::mutex mu_;
    std::unordered_map<std::string, Entry> table_;

    // NEW: per-key lock owner (client_id) for blocking protocol
    std::unordered_map<std::string, std::string> lock_table_;
};

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <server_address>" << std::endl;
        return 1;
    }

    std::string server_address = argv[1];
    ABDServer server(server_address);
    server.Run();

    return 0;
}
