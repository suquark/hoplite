#pragma once
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include <grpcpp/channel.h>
#include <grpcpp/server.h>

#include "common/id.h"
#include "object_store_state.h"
#include "receiver.h"

class NotificationListenerImpl;

class NotificationListener {
public:
  NotificationListener(const std::string &my_address, int notification_listener_port, ObjectStoreState &state,
                       Receiver &recevier);

  void Run();

  void Shutdown();

private:
  void worker_loop();

  std::string my_address_;

  ObjectStoreState &state_;
  Receiver &recevier_;

  std::thread notification_listener_thread_;
  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<NotificationListenerImpl> service_;
};
