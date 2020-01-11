#include <cstdlib>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <unistd.h>
#include <unordered_map>

#include "logging.h"
#include "notification.h"
#include "object_store.grpc.pb.h"

using objectstore::GetObjectLocationReply;
using objectstore::GetObjectLocationRequest;
using objectstore::IsReadyReply;
using objectstore::IsReadyRequest;
using objectstore::ObjectCompleteReply;
using objectstore::ObjectCompleteRequest;
using objectstore::ObjectIsReadyReply;
using objectstore::ObjectIsReadyRequest;
using objectstore::RegisterReply;
using objectstore::RegisterRequest;
using objectstore::SubscriptionReply;
using objectstore::SubscriptionRequest;
using objectstore::UnsubscriptionReply;
using objectstore::UnsubscriptionRequest;
using objectstore::WriteObjectLocationReply;
using objectstore::WriteObjectLocationRequest;

class NotificationServiceImpl final
    : public objectstore::NotificationServer::Service {
public:
  NotificationServiceImpl(const int port)
      : objectstore::NotificationServer::Service(), port_(port) {}

  grpc::Status Register(grpc::ServerContext *context,
                        const RegisterRequest *request, RegisterReply *reply) {
    std::lock_guard<std::mutex> guard(barrier_mutex_);
    number_of_nodes_ = request->num_of_nodes();
    participants_.clear();
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status IsReady(grpc::ServerContext *context,
                       const IsReadyRequest *request, IsReadyReply *reply) {
    {
      std::lock_guard<std::mutex> guard(barrier_mutex_);
      participants_.insert(request->ip());
      LOG(INFO) << "Number of participants = " << participants_.size();
      for (auto &node : participants_) {
        LOG(INFO) << "participants " << node;
      }
    }
    while (true) {
      {
        std::lock_guard<std::mutex> guard(barrier_mutex_);
        if (participants_.size() == number_of_nodes_) {
          break;
        }
      }
      usleep(100);
    }

    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status Subscribe(grpc::ServerContext *context,
                         const SubscriptionRequest *request,
                         SubscriptionReply *reply) {
    std::lock_guard<std::mutex> guard(notification_mutex_);
    ObjectID object_id = ObjectID::FromBinary(request->object_id());
    std::string ip = request->subscriber_ip();

    auto v = pendings_.find(object_id);
    if (v == pendings_.end()) {
      pendings_[object_id] = {ip};
    } else {
      pendings_[object_id].push_back(ip);
    }

    reply->set_ok(true);

    return grpc::Status::OK;
  }

  grpc::Status Unsubscribe(grpc::ServerContext *context,
                           const UnsubscriptionRequest *request,
                           UnsubscriptionReply *reply) {
    std::lock_guard<std::mutex> guard(notification_mutex_);

    // TODO: remove IP from pending_

    reply->set_ok(true);

    return grpc::Status::OK;
  }

  grpc::Status ObjectComplete(grpc::ServerContext *context,
                              const ObjectCompleteRequest *request,
                              ObjectCompleteReply *reply) {
    std::lock_guard<std::mutex> guard(notification_mutex_);
    ObjectID object_id = ObjectID::FromBinary(request->object_id());

    auto v = pendings_.find(object_id);
    if (v != pendings_.end()) {
      for (auto &ip : v->second) {
        auto reply_ok = send_notification(ip, object_id);
        DCHECK(reply_ok)
            << "[NotificationServer] Sending Notification failure.";
      }
    }

    reply->set_ok(true);

    return grpc::Status::OK;
  }

  grpc::Status WriteObjectLocation(grpc::ServerContext *context,
                                   const WriteObjectLocationRequest *request,
                                   WriteObjectLocationReply *reply) {
    std::lock_guard<std::mutex> guard(object_location_mutex_);
    ObjectID object_id = ObjectID::FromBinary(request->object_id());
    std::string ip_address = request->ip();
    if (object_location_store_.find(object_id) ==
        object_location_store_.end()) {
      object_location_store_[object_id] = {ip_address};
    } else {
      object_location_store_[object_id].push_back(ip_address);
    }
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status GetObjectLocation(grpc::ServerContext *context,
                                 const GetObjectLocationRequest *request,
                                 GetObjectLocationReply *reply) {
    std::lock_guard<std::mutex> guard(object_location_mutex_);
    ObjectID object_id = ObjectID::FromBinary(request->object_id());
    if (object_location_store_.find(object_id) ==
        object_location_store_.end()) {
      reply->set_ip("");
    } else {
      size_t num_of_copies = object_location_store_[object_id].size();
      reply->set_ip(object_location_store_[object_id][rand() % num_of_copies]);
    }
    return grpc::Status::OK;
  }

private:
  bool send_notification(const std::string &ip, ObjectID object_id) {
    auto remote_address = ip + ":" + std::to_string(port_);
    create_stub(remote_address);
    grpc::ClientContext context;
    ObjectIsReadyRequest request;
    ObjectIsReadyReply reply;
    request.set_object_id(object_id.Binary());
    notification_listener_stub_pool_[remote_address]->ObjectIsReady(
        &context, request, &reply);
    return reply.ok();
  }

  std::mutex barrier_mutex_;
  int number_of_nodes_;
  std::unordered_set<std::string> participants_;
  const int port_;
  std::mutex notification_mutex_;
  std::unordered_map<ObjectID, std::vector<std::string>> pendings_;
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_pool_;
  std::unordered_map<std::string,
                     std::unique_ptr<objectstore::NotificationListener::Stub>>
      notification_listener_stub_pool_;
  std::mutex object_location_mutex_;
  std::unordered_map<ObjectID, std::vector<std::string>> object_location_store_;
  void create_stub(const std::string &remote_grpc_address) {
    if (channel_pool_.find(remote_grpc_address) == channel_pool_.end()) {
      channel_pool_[remote_grpc_address] = grpc::CreateChannel(
          remote_grpc_address, grpc::InsecureChannelCredentials());
    }
    if (notification_listener_stub_pool_.find(remote_grpc_address) ==
        notification_listener_stub_pool_.end()) {
      notification_listener_stub_pool_[remote_grpc_address] =
          objectstore::NotificationListener::NewStub(
              channel_pool_[remote_grpc_address]);
    }
  }
};

NotificationServer::NotificationServer(const std::string &my_address,
                                       const int grpc_port,
                                       const int notification_port)
    : grpc_port_(grpc_port), notification_port_(notification_port),
      service_(std::make_shared<NotificationServiceImpl>(notification_port)) {
  std::string grpc_address = my_address + ":" + std::to_string(grpc_port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
}

void NotificationServer::worker_loop() {
  LOG(INFO) << "[NotificationServer] notification server started";

  grpc_server_->Wait();
}

int main(int argc, char **argv) {
  std::string my_address = std::string(argv[1]);

  std::unique_ptr<NotificationServer> notification_server;
  std::thread notification_server_thread;
  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);

  notification_server.reset(new NotificationServer(my_address, 7777, 8888));
  notification_server_thread = notification_server->Run();
  notification_server_thread.join();
}
