#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <hiredis.h>
#include <string.h>

#include "global_control_store.h"
#include "logging.h"
#include "object_store.grpc.pb.h"
#include "util/plasma_utils.h"

using objectstore::ObjectCompleteReply;
using objectstore::ObjectCompleteRequest;
using objectstore::ObjectIsReadyReply;
using objectstore::ObjectIsReadyRequest;
using objectstore::SubscriptionReply;
using objectstore::SubscriptionRequest;
using objectstore::UnsubscriptionReply;
using objectstore::UnsubscriptionRequest;

using namespace plasma;

class NotificationListenerImpl final
    : public objectstore::NotificationListener::Service {
public:
  NotificationListenerImpl(
      std::unordered_set<ObjectNotifications *> &object_notifications)
      : objectstore::NotificationListener::Service(),
        object_notifications_(object_notifications) {}

  grpc::Status ObjectIsReady(grpc::ServerContext *context,
                             const ObjectIsReadyRequest *request,
                             ObjectIsReadyReply *reply) {
    for (auto notifications : object_notifications_) {
      notifications->ReceiveObjectNotification(
          ObjectID::from_binary(request->object_id()));
    }
    reply->set_ok(true);
    return grpc::Status::OK;
  }

private:
  std::unordered_set<ObjectNotifications *> &object_notifications_;
};

ObjectNotifications::ObjectNotifications(std::vector<ObjectID> object_ids) {
  for (auto object_id : object_ids) {
    pending_.insert(object_id);
  }
}

std::vector<ObjectID> ObjectNotifications::GetNotifications() {
  std::lock_guard<std::mutex> guard(notification_mutex_);
  std::vector<ObjectID> notifications;
  for (auto &object_id : ready_) {
    notifications.push_back(object_id);
  }
  ready_.clear();
  return notifications;
}

void ObjectNotifications::ReceiveObjectNotification(const ObjectID &object_id) {
  std::lock_guard<std::mutex> guard(notification_mutex_);
  if (pending_.find(object_id) == pending_.end()) {
    return;
  }
  pending_.erase(object_id);
  ready_.insert(object_id);
}

GlobalControlStoreClient::GlobalControlStoreClient(
    const std::string &redis_address, int port, const std::string &my_address,
    int notification_port, int notification_listen_port)
    : redis_address_(redis_address), my_address_(my_address),
      notification_port_(notification_port),
      notification_listen_port_(notification_listen_port),
      service_(std::make_shared<NotificationListenerImpl>(notifications_)) {
  // create a redis client
  redis_client_ = redisConnect(redis_address.c_str(), port);
  LOG(DEBUG) << "[RedisClient] Connected to Redis server running at "
             << redis_address << ":" << port << ".";

  std::string grpc_address =
      my_address + ":" + std::to_string(notification_listen_port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
}

void GlobalControlStoreClient::write_object_location(
    const ObjectID &object_id, const std::string &my_address) {
  LOG(INFO) << "[RedisClient] Adding object " << object_id.hex()
            << " to Redis with address = " << my_address << ".";
  redisReply *redis_reply =
      (redisReply *)redisCommand(redis_client_, "LPUSH %s %s",
                                 object_id.hex().c_str(), my_address.c_str());
  freeReplyObject(redis_reply);
}

void GlobalControlStoreClient::flushall() {
  redisReply *reply = (redisReply *)redisCommand(redis_client_, "FLUSHALL");
  freeReplyObject(reply);
}

std::string
GlobalControlStoreClient::get_object_location(const ObjectID &object_id) {
  redisReply *redis_reply = (redisReply *)redisCommand(
      redis_client_, "LRANGE %s 0 -1", object_id.hex().c_str());

  int num_of_copies = redis_reply->elements;
  if (num_of_copies == 0) {
    return "";
  }

  std::string address =
      std::string(redis_reply->element[rand() % num_of_copies]->str);

  freeReplyObject(redis_reply);
  return address;
}

ObjectNotifications *GlobalControlStoreClient::subscribe_object_locations(
    const std::vector<ObjectID> &object_ids, bool include_completed_objects) {
  ObjectNotifications *notifications = new ObjectNotifications(object_ids);
  {
    std::lock_guard<std::mutex> guard(gcs_mutex_);
    notifications_.insert(notifications);
  }

  for (auto object_id : object_ids) {
    auto remote_address =
        redis_address_ + ":" + std::to_string(notification_port_);
    auto channel =
        grpc::CreateChannel(remote_address, grpc::InsecureChannelCredentials());
    std::unique_ptr<objectstore::NotificationServer::Stub> stub(
        objectstore::NotificationServer::NewStub(channel));
    grpc::ClientContext context;
    SubscriptionRequest request;
    SubscriptionReply reply;
    request.set_subscriber_ip(my_address_);
    request.set_object_id(object_id.binary());
    stub->Subscribe(&context, request, &reply);

    DCHECK(reply.ok()) << "Subscribing object " << object_id.hex()
                       << " failed.";
  }

  if (include_completed_objects) {
    for (auto object_id : object_ids) {
      if ("" != get_object_location(object_id)) {
        notifications->ReceiveObjectNotification(object_id);
      }
    }
  }

  return notifications;
}

void GlobalControlStoreClient::unsubscribe_object_locations(
    ObjectNotifications *notifications) {
  std::lock_guard<std::mutex> guard(gcs_mutex_);

  notifications_.erase(notifications);

  delete notifications;
}

void GlobalControlStoreClient::PublishObjectCompletionEvent(
    const ObjectID &object_id) {

  auto remote_address =
      redis_address_ + ":" + std::to_string(notification_port_);
  auto channel =
      grpc::CreateChannel(remote_address, grpc::InsecureChannelCredentials());
  std::unique_ptr<objectstore::NotificationServer::Stub> stub(
      objectstore::NotificationServer::NewStub(channel));
  grpc::ClientContext context;
  ObjectCompleteRequest request;
  ObjectCompleteReply reply;
  request.set_object_id(object_id.binary());
  stub->ObjectComplete(&context, request, &reply);

  DCHECK(reply.ok()) << "Object completes " << object_id.hex() << " failed.";
}

void GlobalControlStoreClient::worker_loop() {
  LOG(INFO) << "[GCSClient] Gcs client " << my_address_ << " started";

  grpc_server_->Wait();
}
