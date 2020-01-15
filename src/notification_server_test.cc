#include <cstdlib>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <unistd.h>
#include <utility>
#include <unordered_map>
#include <unordered_set>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <string>
#include <thread>

#include "logging.h"
#include "common/id.h"
#include "object_store.grpc.pb.h"

using objectstore::GetLocationAsyncAnswerRequest;
using objectstore::GetLocationAsyncAnswerReply;

class NotificationListenerImpl final
    : public objectstore::NotificationListener::Service {
public:
  NotificationListenerImpl()
      : objectstore::NotificationListener::Service() {}

  grpc::Status GetLocationAsyncAnswer(grpc::ServerContext *context,
                                      const GetLocationAsyncAnswerRequest *request,
                                      GetLocationAsyncAnswerReply *reply) {
    ObjectID object_id = ObjectID::FromBinary(request->object_id());
    std::string sender_ip = request->sender_ip();
    LOG(INFO) << "[NotificationListener] [GetLocationAsyncAnswer] ID:" << object_id.Hex() << " IP:" << sender_ip;
    reply->set_ok(true);
    return grpc::Status::OK;
  }
};

class NotificationListener {
public:
  NotificationListener(const std::string &my_address, 
                       const int notification_port)
    : notification_port_(notification_port),
      service_(std::make_shared<NotificationListenerImpl>()) {
    std::string grpc_address = my_address + ":" + std::to_string(notification_port);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&*service_);
    grpc_server_ = builder.BuildAndStart();
  }
  inline std::thread Run() {
    std::thread notification_thread(&NotificationListener::worker_loop, this);
    return notification_thread;
  }

private:
  void worker_loop() {
    LOG(INFO) << "[NotificationListener] notification listener started";
    grpc_server_->Wait();
  }

  const int notification_port_;

  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<NotificationListenerImpl> service_;
};

int main(int argc, char **argv) {
  std::string my_address = std::string(argv[1]);
  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);
  std::unique_ptr<NotificationListener> notification_listener;
  std::thread notification_listener_thread;
  notification_listener.reset(new NotificationListener(my_address, 8888));
  notification_listener_thread = notification_listener->Run();
  notification_listener_thread.join();
  return 0;
}