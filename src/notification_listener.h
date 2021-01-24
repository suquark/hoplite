#pragma once
#include <memory>
#include <thread>

#include <grpcpp/channel.h>
#include <grpcpp/server.h>

#include "receiver.h"

class NotificationListenerImpl;

class NotificationListener {

  NotificationListener(const std::string &my_address, int notification_listener_port, Receiver &recevier);

  ~NotificationListener();

  void Run();

  void Shutdown();

private:
  void worker_loop();

  std::string my_address_;

  Receiver &recevier_;

  std::thread notification_listener_thread_;
  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<NotificationListenerImpl> service_;
};
