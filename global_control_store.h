#ifndef GLOBAL_CONTROL_STORE_H
#define GLOBAL_CONTROL_STORE_H

#include <mutex>
#include <plasma/common.h>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

struct redisContext;

class ObjectNotifications {
public:
  ObjectNotifications(std::vector<std::string> object_id_hexes);
  std::vector<plasma::ObjectID> GetNotifications();

  void ReceiveObjectNotification(std::string object_id_hex);

private:
  std::mutex notification_mutex_;
  std::unordered_set<std::string> pending_;
  std::unordered_set<std::string> ready_;
};

class GlobalControlStoreClient {
public:
  GlobalControlStoreClient(const std::string &redis_address, int port,
                           int notification_port);

  // Write object location to Redis server.
  void write_object_location(const std::string &object_id_hex,
                             const std::string &my_address);

  // Get object location from Redis server.
  std::string get_object_location(const std::string &hex);

  // Clean up Redis store
  void flushall();

  ObjectNotifications *subscribe_object_locations(
      const std::vector<plasma::ObjectID> &object_id_hexes,
      bool include_completed_objects = false);

  void unsubscribe_object_locations(ObjectNotifications *notifications);

  void PublishObjectCompletionEvent(const std::string &object_id_hex);

  inline std::thread Run() {
    std::thread notification_thread(&GlobalControlStoreClient::worker_loop,
                                    this);
    return notification_thread;
  }

private:
  void worker_loop();

  std::mutex gcs_mutex_;
  redisContext *redis_client_;
  redisContext *notification_client_;
  redisContext *publish_client_;
  std::unordered_set<ObjectNotifications *> notifications_;
};

#endif // GLOBAL_CONTROL_STORE_H
