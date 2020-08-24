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

private:
  void worker_loop();

  const int notification_server_port_;
  const int notification_client_port_;
  const int object_store_port_;

  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<NotificationServiceImpl> service_;
};

#endif // NOTIFICATION_H
