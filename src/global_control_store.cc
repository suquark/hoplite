#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "global_control_store.h"
#include "logging.h"

using objectstore::ConnectListenerReply;
using objectstore::ConnectListenerRequest;
using objectstore::ConnectReply;
using objectstore::ConnectRequest;
using objectstore::GetLocationAsyncAnswerReply;
using objectstore::GetLocationAsyncAnswerRequest;
using objectstore::GetLocationAsyncReply;
using objectstore::GetLocationAsyncRequest;
using objectstore::GetLocationSyncReply;
using objectstore::GetLocationSyncRequest;
using objectstore::HandlePullObjectFailureReply;
using objectstore::HandlePullObjectFailureRequest;
using objectstore::WriteLocationReply;
using objectstore::WriteLocationRequest;

class NotificationListenerImpl final : public objectstore::NotificationListener::Service {
public:
  NotificationListenerImpl(
      std::unordered_map<std::string, std::shared_ptr<ObjectNotifications>> &object_notifications_pool,
      std::shared_ptr<std::mutex> notifications_pool_mutex)
      : objectstore::NotificationListener::Service(), object_notifications_pool_(object_notifications_pool),
        notifications_pool_mutex_(notifications_pool_mutex) {
    TIMELINE("NotificationListenerImpl");
  }

  grpc::Status GetLocationAsyncAnswer(grpc::ServerContext *context, const GetLocationAsyncAnswerRequest *request,
                                      GetLocationAsyncAnswerReply *reply) {
    TIMELINE("GetLocationAsyncAnswer");
    for (auto &object : request->objects()) {
      ObjectID object_id = ObjectID::FromBinary(object.object_id());
      std::string sender_ip = object.sender_ip();
      std::string query_id = object.query_id();
      size_t object_size = object.object_size();
      std::string inband_data = object.inband_data();
      std::shared_ptr<ObjectNotifications> notifications;
      {
        std::lock_guard<std::mutex> guard(*notifications_pool_mutex_);
        notifications = object_notifications_pool_[query_id];
      }
      notifications->ReceiveObjectNotification(object_id, sender_ip, object_size, inband_data);
    }
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status ConnectListener(grpc::ServerContext *context, const ConnectListenerRequest *request,
                               ConnectListenerReply *reply) {
    TIMELINE("ConnectListener");
    return grpc::Status::OK;
  }

private:
  std::unordered_map<std::string, std::shared_ptr<ObjectNotifications>> &object_notifications_pool_;
  std::shared_ptr<std::mutex> notifications_pool_mutex_;
};

////////////////////////////////////////////////////////////////
// The object notification structure
////////////////////////////////////////////////////////////////

std::vector<NotificationMessage> ObjectNotifications::GetNotifications(bool delete_after_get, bool no_wait) {
  TIMELINE("GetNotifications");
  std::unique_lock<std::mutex> l(notification_mutex_);
  if (!no_wait) {
    notification_cv_.wait(l, [this]() { return ready_.size() > cursor_; });
  }
  std::vector<NotificationMessage> notifications(ready_.begin() + cursor_, ready_.end());
  if (delete_after_get) {
    cursor_ = ready_.size();
  }
  return notifications;
}

void ObjectNotifications::Rewind() {
  std::lock_guard<std::mutex> l(notification_mutex_);
  cursor_ = 0;
}

size_t ObjectNotifications::EraseRecords(const std::unordered_set<ObjectID> &records) {
  std::lock_guard<std::mutex> l(notification_mutex_);
  std::vector<NotificationMessage> new_records;
  size_t cursor_shift = 0;
  size_t n_records_erased = 0;
  for (int i = 0; i < ready_.size(); i++) {
    if (records.find(ready_[i].object_id) == records.end()) {
      new_records.push_back(ready_[i]);
    } else {
      ++n_records_erased;
      if (i < cursor_) {
        ++cursor_shift;
      }
    }
  }
  // shift the cursor due to the erase
  cursor_ -= cursor_shift;
  ready_ = std::move(new_records);
  return n_records_erased;
}

void ObjectNotifications::ReceiveObjectNotification(const ObjectID &object_id, const std::string &sender_ip,
                                                    size_t object_size, const std::string &inband_data) {
  {
    std::lock_guard<std::mutex> l(notification_mutex_);
    ready_.push_back({object_id, sender_ip, object_size, inband_data});
  }
  notification_cv_.notify_one();
}

GlobalControlStoreClient::GlobalControlStoreClient(const std::string &notification_server_address,
                                                   const std::string &my_address, int notification_server_port,
                                                   int notification_listener_port)
    : notification_server_address_(notification_server_address), my_address_(my_address),
      notification_server_port_(notification_server_port), notification_listener_port_(notification_listener_port),
      notifications_pool_mutex_(std::make_shared<std::mutex>()),
      service_(std::make_shared<NotificationListenerImpl>(notifications_pool_, notifications_pool_mutex_)), pool_(2) {
  TIMELINE("GlobalControlStoreClient");
  std::string grpc_address = my_address + ":" + std::to_string(notification_listener_port_);
  LOG(DEBUG) << "grpc_address " << grpc_address;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
  LOG(DEBUG) << "grpc_server_ started";
  auto remote_notification_server_address =
      notification_server_address_ + ":" + std::to_string(notification_server_port_);
  LOG(DEBUG) << "remote_notification_server_address " << remote_notification_server_address;
  notification_channel_ = grpc::CreateChannel(remote_notification_server_address, grpc::InsecureChannelCredentials());
  notification_stub_ = objectstore::NotificationServer::NewStub(notification_channel_);
  LOG(DEBUG) << "notification_stub_ created";
}

void GlobalControlStoreClient::ConnectNotificationServer() {
  grpc::ClientContext context;
  ConnectRequest request;
  request.set_sender_ip(my_address_);
  ConnectReply reply;
  auto status = notification_stub_->Connect(&context, request, &reply);
  DCHECK(status.ok()) << status.error_message();
}

void GlobalControlStoreClient::WriteLocation(const ObjectID &object_id, const std::string &sender_ip, bool finished,
                                             size_t object_size, const uint8_t *inband_data, bool blocking) {
  TIMELINE("GlobalControlStoreClient::WriteLocation");
  LOG(DEBUG) << "[GlobalControlStoreClient] Adding object " << object_id.Hex()
             << " to notification server with address = " << sender_ip << ".";
  // FIXME(suquark): Figure out why running `WriteLocation` in a thread pool
  // would fail when we are running multicast repeatly with certain object sizes
  // (65K~300K).
  WriteLocationRequest request;
  request.set_object_id(object_id.Binary());
  request.set_sender_ip(sender_ip);
  request.set_finished(finished);
  request.set_object_size(object_size);
  // inline small data buffer into the message
  if (finished && object_size <= inband_data_size_limit && inband_data != nullptr) {
    request.set_inband_data(inband_data, object_size);
  }
  if (blocking) {
    grpc::ClientContext context;
    WriteLocationReply reply;
    auto status = notification_stub_->WriteLocation(&context, request, &reply);
    DCHECK(status.ok()) << status.error_message();
    DCHECK(reply.ok()) << "WriteLocation for " << object_id.ToString() << " failed.";
  } else {
    pool_.push(
        [this, object_id](int id, WriteLocationRequest request) {
          grpc::ClientContext context;
          WriteLocationReply reply;
          auto status = notification_stub_->WriteLocation(&context, request, &reply);
          DCHECK(status.ok()) << status.error_message();
          DCHECK(reply.ok()) << "WriteLocation for " << object_id.ToString() << " failed.";
        },
        std::move(request));
  }
}

SyncReply GlobalControlStoreClient::GetLocationSync(const ObjectID &object_id, bool occupying,
                                                    const std::string &receiver_ip) {
  TIMELINE("GetLocationSync");
  grpc::ClientContext context;
  GetLocationSyncRequest request;
  GetLocationSyncReply reply;
  request.set_object_id(object_id.Binary());
  request.set_occupying(occupying);
  request.set_receiver_ip(receiver_ip);
  notification_stub_->GetLocationSync(&context, request, &reply);
  return {std::string(reply.sender_ip()), reply.object_size(), reply.inband_data()};
}

std::shared_ptr<ObjectNotifications> GlobalControlStoreClient::GetLocationAsync(const std::vector<ObjectID> &object_ids,
                                                                                const std::string &query_id,
                                                                                bool occupying) {
  TIMELINE("GetLocationAsync");
  std::shared_ptr<ObjectNotifications> notifications = std::make_shared<ObjectNotifications>();
  {
    std::lock_guard<std::mutex> guard(*notifications_pool_mutex_);
    notifications_pool_[query_id] = notifications;
  }
  GetLocationAsyncRequest request;
  request.set_receiver_ip(my_address_);
  request.set_query_id(query_id);
  request.set_occupying(occupying);
  for (auto object_id : object_ids) {
    request.add_object_ids(object_id.Binary());
  }

  (void)pool_.push(
      [this](int id, GetLocationAsyncRequest request) {
        grpc::ClientContext context;
        GetLocationAsyncReply reply;
        notification_stub_->GetLocationAsync(&context, request, &reply);
        DCHECK(reply.ok()) << "GetLocationAsync failed.";
      },
      std::move(request));
  return notifications;
}

bool GlobalControlStoreClient::HandlePullObjectFailure(const ObjectID &object_id, const std::string &receiver_ip,
                                                       std::string *alternative_sender_ip) {
  TIMELINE("HandlePullObjectFailure");
  grpc::ClientContext context;
  HandlePullObjectFailureRequest request;
  HandlePullObjectFailureReply reply;
  request.set_object_id(object_id.Binary());
  request.set_receiver_ip(receiver_ip);
  auto status = notification_stub_->HandlePullObjectFailure(&context, request, &reply);
  DCHECK(status.ok()) << status.error_message();
  *alternative_sender_ip = reply.alternative_sender_ip();
  return reply.success();
}

void GlobalControlStoreClient::worker_loop() {
  LOG(DEBUG) << "[GCSClient] Gcs client " << my_address_ << " started";
  grpc_server_->Wait();
}
