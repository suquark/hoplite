#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <string.h>

#include "global_control_store.h"
#include "logging.h"

using objectstore::GetLocationAsyncAnswerReply;
using objectstore::GetLocationAsyncAnswerRequest;
using objectstore::GetLocationAsyncReply;
using objectstore::GetLocationAsyncRequest;
using objectstore::GetLocationSyncReply;
using objectstore::GetLocationSyncRequest;
using objectstore::WriteLocationReply;
using objectstore::WriteLocationRequest;

class NotificationListenerImpl final
    : public objectstore::NotificationListener::Service {
public:
  NotificationListenerImpl(
      std::unordered_map<std::string, std::shared_ptr<ObjectNotifications>>
          &object_notifications_pool,
      std::shared_ptr<std::mutex> notifications_pool_mutex)
      : objectstore::NotificationListener::Service(),
        object_notifications_pool_(object_notifications_pool),
        notifications_pool_mutex_(notifications_pool_mutex) {
    TIMELINE("NotificationListenerImpl");
  }

  grpc::Status
  GetLocationAsyncAnswer(grpc::ServerContext *context,
                         const GetLocationAsyncAnswerRequest *request,
                         GetLocationAsyncAnswerReply *reply) {
    ObjectID object_id = ObjectID::FromBinary(request->object_id());
    std::string sender_ip = request->sender_ip();
    std::string query_id = request->query_id();
    size_t object_size = request->object_size();
    std::shared_ptr<ObjectNotifications> notifications;
    {
      std::lock_guard<std::mutex> guard(*notifications_pool_mutex_);
      notifications = object_notifications_pool_[query_id];
    }
    notifications->ReceiveObjectNotification(object_id, sender_ip, object_size);
    reply->set_ok(true);
    return grpc::Status::OK;
  }

private:
  std::unordered_map<std::string, std::shared_ptr<ObjectNotifications>>
      &object_notifications_pool_;
  std::shared_ptr<std::mutex> notifications_pool_mutex_;
};

std::vector<NotificationMessage> ObjectNotifications::GetNotifications() {
  std::unique_lock<std::mutex> l(notification_mutex_);
  notification_cv_.wait(l, [this]() { return !ready_.empty(); });
  std::vector<NotificationMessage> notifications = ready_;
  ready_.clear();
  return notifications;
}

void ObjectNotifications::ReceiveObjectNotification(
    const ObjectID &object_id, const std::string &sender_ip,
    size_t object_size) {
  std::unique_lock<std::mutex> l(notification_mutex_);
  ready_.push_back({object_id, sender_ip, object_size});
  l.unlock();
  notification_cv_.notify_one();
}

GlobalControlStoreClient::GlobalControlStoreClient(
    const std::string &notification_server_address,
    const std::string &my_address, int notification_server_port,
    int notification_listen_port)
    : notification_server_address_(notification_server_address),
      my_address_(my_address),
      notification_server_port_(notification_server_port),
      notification_listen_port_(notification_listen_port),
      notifications_pool_mutex_(std::make_shared<std::mutex>()),
      service_(std::make_shared<NotificationListenerImpl>(
          notifications_pool_, notifications_pool_mutex_)) {
  TIMELINE("GlobalControlStoreClient");
  std::string grpc_address =
      my_address + ":" + std::to_string(notification_listen_port_);
  LOG(INFO) << "grpc_address " << grpc_address;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
  LOG(INFO) << "grpc_server_ started";
  auto remote_notification_server_address =
      notification_server_address_ + ":" +
      std::to_string(notification_server_port_);
  LOG(INFO) << "remote_notification_server_address "
            << remote_notification_server_address;
  notification_channel_ = grpc::CreateChannel(
      remote_notification_server_address, grpc::InsecureChannelCredentials());
  notification_stub_ =
      objectstore::NotificationServer::NewStub(notification_channel_);
  LOG(INFO) << "notification_stub_ created";
}

void GlobalControlStoreClient::WriteLocation(const ObjectID &object_id,
                                             const std::string &sender_ip,
                                             bool finished,
                                             size_t object_size) {
  TIMELINE("GlobalControlStoreClient::WriteLocation");
  LOG(INFO) << "[GlobalControlStoreClient] Adding object " << object_id.Hex()
            << " to notification server with address = " << sender_ip << ".";
  grpc::ClientContext context;
  WriteLocationRequest request;
  WriteLocationReply reply;
  request.set_object_id(object_id.Binary());
  request.set_sender_ip(sender_ip);
  request.set_finished(finished);
  request.set_object_size(object_size);
  auto status = notification_stub_->WriteLocation(&context, request, &reply);
  DCHECK(status.ok()) << status.error_message();
  DCHECK(reply.ok()) << "WriteLocation for " << object_id.ToString()
                     << " failed.";
}

SyncReply GlobalControlStoreClient::GetLocationSync(const ObjectID &object_id) {
  TIMELINE("GetLocationSync");
  grpc::ClientContext context;
  GetLocationSyncRequest request;
  GetLocationSyncReply reply;
  request.set_object_id(object_id.Binary());
  notification_stub_->GetLocationSync(&context, request, &reply);
  return {std::string(reply.sender_ip()), reply.object_size()};
}

std::shared_ptr<ObjectNotifications> GlobalControlStoreClient::GetLocationAsync(
    const std::vector<ObjectID> &object_ids, const std::string &query_id) {
  std::shared_ptr<ObjectNotifications> notifications =
      std::make_shared<ObjectNotifications>();
  {
    std::lock_guard<std::mutex> guard(*notifications_pool_mutex_);
    notifications_pool_[query_id] = notifications;
  }
  grpc::ClientContext context;
  GetLocationAsyncRequest request;
  GetLocationAsyncReply reply;
  request.set_receiver_ip(my_address_);
  request.set_query_id(query_id);
  for (auto object_id : object_ids) {
    request.add_object_ids(object_id.Binary());
  }
  notification_stub_->GetLocationAsync(&context, request, &reply);
  DCHECK(reply.ok()) << "GetLocationAsync failed.";
  return notifications;
}

void GlobalControlStoreClient::worker_loop() {
  LOG(INFO) << "[GCSClient] Gcs client " << my_address_ << " started";
  grpc_server_->Wait();
}
