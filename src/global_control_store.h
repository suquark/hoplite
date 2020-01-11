#ifndef GLOBAL_CONTROL_STORE_H
#define GLOBAL_CONTROL_STORE_H

#include "common/id.h"
#include "object_store.grpc.pb.h"
#include <condition_variable>
#include <grpcpp/server.h>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

struct redisContext;

struct NotificationListenerImpl;

class ObjectNotifications {
public:
  ObjectNotifications(std::vector<ObjectID> object_ids);
  std::vector<ObjectID> GetNotifications();

  void ReceiveObjectNotification(const ObjectID &object_id);

private:
  std::mutex notification_mutex_;
  std::condition_variable notification_cv_;
  std::unordered_set<ObjectID> pending_;
  std::unordered_set<ObjectID> ready_;
};

class GlobalControlStoreClient {
public:
  GlobalControlStoreClient(const std::string &redis_address, int redis_port,
                           const std::string &my_address, int notification_port,
                           int notification_listen_port);

  // Write object location to Redis server.
  void write_object_location(const ObjectID &object_id,
                             const std::string &my_address);

  // Get object location from Redis server.
  std::string get_object_location(const ObjectID &object_id);

  ObjectNotifications *
  subscribe_object_locations(const std::vector<ObjectID> &object_ids,
                             bool include_completed_objects = false);

  void unsubscribe_object_locations(ObjectNotifications *notifications);

  void PublishObjectCompletionEvent(const ObjectID &object_id);

  inline std::thread Run() {
    std::thread notification_thread(&GlobalControlStoreClient::worker_loop,
                                    this);
    return notification_thread;
  }

private:
  void worker_loop();

  std::mutex gcs_mutex_;
  const std::string &redis_address_;
  const std::string &my_address_;
  const int notification_port_;
  const int notification_listen_port_;
  std::shared_ptr<grpc::Channel> notification_channel_;
  std::unique_ptr<objectstore::NotificationServer::Stub> notification_stub_;

  std::unordered_set<ObjectNotifications *> notifications_;

  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<NotificationListenerImpl> service_;
};

#endif // GLOBAL_CONTROL_STORE_H
