#ifndef NOTIFICATION_H
#define NOTIFICATION_H

#include "common/id.h"
#include "logging.h"
#include <atomic>
#include <grpcpp/server.h>
#include <string>
#include <thread>

class NotificationServiceImpl;

class NotificationServer {
public:
  NotificationServer(const std::string &my_address, const int grpc_port,
                     const int notification_port);

  std::thread Run() {
    std::thread notification_thread(&NotificationServer::worker_loop, this);
    return notification_thread;
  }

  void PutInBandData(const ObjectID &key, const std::string &value) {
    while (directory_lock_.test_and_set(std::memory_order_acquire))
      ;
    inband_data_directory_[key] = value;
    directory_lock_.clear(std::memory_order_release);
  }

  std::string GetInbandData(const ObjectID &key) {
    while (directory_lock_.test_and_set(std::memory_order_acquire))
      ;
    // we will return an empty string if it does not exist
    auto data = inband_data_directory_[key];
    directory_lock_.clear(std::memory_order_release);
    // it's likely that return copy will be avoided by the compiler.
    return data;
  }

private:
  void worker_loop();

  const int grpc_port_;
  const int notification_port_;

  // TODO: We should implement LRU gabage collection for the inband data
  // storage. But it doesn't matter now because these data take too few
  // space.
  std::unordered_map<ObjectID, std::string> inband_data_directory_;
  std::atomic_flag directory_lock_ = ATOMIC_FLAG_INIT;

  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<NotificationServiceImpl> service_;
};

#endif // NOTIFICATION_H
