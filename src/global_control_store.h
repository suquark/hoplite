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
#include <utility>
#include <vector>

struct NotificationListenerImpl;

struct NotificationMessage {
  ObjectID object_id;
  std::string sender_ip;
  size_t object_size;
};
struct SyncReply {
  std::string sender_ip;
  size_t object_size;
}

class ObjectNotifications {
public:
  std::vector<NotificationMessage> GetNotifications();

  void ReceiveObjectNotification(const ObjectID &object_id,
                                 const std::string &sender_ip);

private:
  std::mutex notification_mutex_;
  std::condition_variable notification_cv_;
  std::vector<NotificationMessage> ready_;
};

class GlobalControlStoreClient {
public:
  GlobalControlStoreClient(const std::string &notification_server_address,
                           const std::string &my_address,
                           int notification_server_port,
                           int notification_listen_port);

  // Write object location to the notification server.
  void WriteLocation(const ObjectID &object_id, const std::string &my_address,
                     bool finished);

  // Get object location from the notification server.
  SyncReply GetLocationSync(const ObjectID &object_id);

  std::shared_ptr<ObjectNotifications>
  GetLocationAsync(const std::vector<ObjectID> &object_ids,
                   const std::string &query_id);

  inline std::thread Run() {
    std::thread notification_thread(&GlobalControlStoreClient::worker_loop,
                                    this);
    return notification_thread;
  }

  inline void Shutdown() { grpc_server_->Shutdown(); }

private:
  void worker_loop();

  const std::string &notification_server_address_;
  const std::string &my_address_;
  const int notification_server_port_;
  const int notification_listen_port_;
  std::shared_ptr<grpc::Channel> notification_channel_;
  std::unique_ptr<objectstore::NotificationServer::Stub> notification_stub_;

  std::shared_ptr<std::mutex> notifications_pool_mutex_;
  std::unordered_map<std::string, std::shared_ptr<ObjectNotifications>>
      notifications_pool_;

  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<NotificationListenerImpl> service_;
};

#endif // GLOBAL_CONTROL_STORE_H
