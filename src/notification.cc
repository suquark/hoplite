#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <plasma/common.h>
#include <unistd.h>
#include <unordered_map>

#include "logging.h"
#include "notification.h"
#include "object_store.grpc.pb.h"

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

using namespace plasma;

class NotificationServiceImpl final
    : public objectstore::NotificationServer::Service {
public:
  NotificationServiceImpl(const int port)
      : objectstore::NotificationServer::Service(), port_(port) {}

  grpc::Status Register(grpc::ServerContext *context,
                        const RegisterRequest *request, RegisterReply *reply) {
    number_of_nodes_ = request->num_of_nodes();
    number_of_participants_ = 0;
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status IsReady(grpc::ServerContext *context,
                       const IsReadyRequest *request, IsReadyReply *reply) {
    number_of_participants_++;
    while (true) {
      if (number_of_participants_ == number_of_nodes_) {
        break;
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
    ObjectID object_id = ObjectID::from_binary(request->object_id());
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
    ObjectID object_id = ObjectID::from_binary(request->object_id());

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

private:
  bool send_notification(const std::string &ip, ObjectID object_id) {
    auto remote_address = ip + ":" + std::to_string(port_);
    auto channel =
        grpc::CreateChannel(remote_address, grpc::InsecureChannelCredentials());
    std::unique_ptr<objectstore::NotificationListener::Stub> stub(
        objectstore::NotificationListener::NewStub(channel));
    grpc::ClientContext context;
    ObjectIsReadyRequest request;
    ObjectIsReadyReply reply;
    request.set_object_id(object_id.binary());
    stub->ObjectIsReady(&context, request, &reply);
    return reply.ok();
  }

  int number_of_nodes_;
  std::atomic<int> number_of_participants_;
  const int port_;
  std::mutex notification_mutex_;
  std::unordered_map<ObjectID, std::vector<std::string>> pendings_;
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
