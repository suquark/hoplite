#include <hiredis.h>
#include <string.h>

#include "global_control_store.h"
#include "logging.h"
#include "util/plasma_utils.h"

using namespace plasma;

ObjectNotifications::ObjectNotifications(
    std::vector<std::string> object_id_hexes) {
  for (auto object_id_hex : object_id_hexes) {
    pending_.insert(object_id_hex);
  }
}

std::vector<ObjectID> ObjectNotifications::GetNotifications() {
  std::lock_guard<std::mutex> guard(notification_mutex_);
  std::vector<ObjectID> notifications;
  for (auto &object_id_hex : ready_) {
    notifications.push_back(from_hex((char *)object_id_hex.c_str()));
  }
  ready_.clear();
  return notifications;
}

void ObjectNotifications::ReceiveObjectNotification(std::string object_id_hex) {
  std::lock_guard<std::mutex> guard(notification_mutex_);
  if (pending_.find(object_id_hex) == pending_.end()) {
    return;
  }
  pending_.erase(object_id_hex);
  ready_.insert(object_id_hex);
}

GlobalControlStoreClient::GlobalControlStoreClient(
    const std::string &redis_address, int port, int notification_port) {
  // create a redis client
  redis_client_ = redisConnect(redis_address.c_str(), port);
  LOG(DEBUG) << "[RedisClient] Connected to Redis server running at "
             << redis_address << ":" << port << ".";
  notification_client_ = redisConnect(redis_address.c_str(), notification_port);
  LOG(DEBUG)
      << "[RedisClient] Connected to Redis notification server running at "
      << redis_address << ":" << notification_port << ".";
}

void GlobalControlStoreClient::write_object_location(
    const std::string &object_id_hex, const std::string &my_address) {
  LOG(INFO) << "[RedisClient] Adding object " << object_id_hex
            << " to Redis with address = " << my_address << ".";
  redisReply *redis_reply = (redisReply *)redisCommand(
      redis_client_, "LPUSH %s %s", object_id_hex.c_str(), my_address.c_str());
  freeReplyObject(redis_reply);
}

void GlobalControlStoreClient::flushall() {
  redisReply *reply = (redisReply *)redisCommand(redis_client_, "FLUSHALL");
  freeReplyObject(reply);

  redisAppendCommand(notification_client_, "FLUSHALL");
}

std::string
GlobalControlStoreClient::get_object_location(const std::string &hex) {
  redisReply *redis_reply =
      (redisReply *)redisCommand(redis_client_, "LRANGE %s 0 -1", hex.c_str());

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
  std::vector<std::string> object_id_hexes;
  for (auto object_id : object_ids) {
    object_id_hexes.push_back(object_id.hex());
  }
  ObjectNotifications *notifications = new ObjectNotifications(object_id_hexes);
  {
    std::lock_guard<std::mutex> guard(gcs_mutex_);
    notifications_.insert(notifications);
  }

  for (auto object_id_hex : object_id_hexes) {
    redisAppendCommand(notification_client_, "SUBSCRIBE %s",
                       object_id_hex.c_str());
  }

  if (include_completed_objects) {
    for (auto object_id_hex : object_id_hexes) {
      if ("" == get_object_location(object_id_hex)) {
        notifications->ReceiveObjectNotification(object_id_hex);
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
    const std::string &object_id_hex) {
  redisAppendCommand(notification_client_, "PUB %s %s", object_id_hex.c_str(),
                     "READY");
}

void GlobalControlStoreClient::worker_loop() {
  while (true) {
    redisReply *reply;
    redisGetReply(notification_client_, (void **)&reply);
    if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 3) {
      if (strcmp(reply->element[0]->str, "SUBSCRIBE") != 0) {
        // get a notification for an object completion event, update pending
        // objects list
        std::string object_id_hex = std::string(reply->element[1]->str);
        for (auto notifications : notifications_) {
          notifications->ReceiveObjectNotification(object_id_hex);
        }
      }
    }
  }
}
