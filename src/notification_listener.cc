#include "notification_listener.h"

#include "object_store.grpc.pb.h"
using objectstore::ConnectListenerReply;
using objectstore::ConnectListenerRequest;

class NotificationListenerImpl final : public objectstore::NotificationListener::Service {
public:
  NotificationListenerImpl(Receiver &receiver) : objectstore::NotificationListener::Service(), receiver_(receiver) {
    TIMELINE("NotificationListenerImpl");
  }

  grpc::Status ConnectListener(grpc::ServerContext *context, const ConnectListenerRequest *request,
                               ConnectListenerReply *reply) {
    TIMELINE("ConnectListener");
    return grpc::Status::OK;
  }

  grpc::Status PullAndReduceObject(grpc::ServerContext *context, const PullAndReduceObjectRequest *request,
                                   PullAndReduceObjectReply *reply) {
    TIMELINE("PullAndReduceObject");
    ObjectID reduction_id = ObjectID::FromBinary(request->reduction_id());
    ObjectID object_id_to_reduce = ObjectID::FromBinary(request->object_id_to_reduce());
    ObjectID object_id_to_pull = ObjectID::FromBinary(request->object_id_to_pull());
    // object_id_to_reduce == reduction_id -> root
    receiver_.receive_and_reduce_object(reduction_id, request->is_tree_branch(), request->sender_ip(),
                                        request->from_left_child(), request->object_size(), object_id_to_reduce,
                                        object_id_to_pull);
    return grpc::Status::OK;
  }

  grpc::Status ReduceInbandObject(grpc::ServerContext *context, const ReduceInbandObjectRequest *request,
                                  ReduceInbandObjectReply *reply) {
    TIMELINE("ReduceInbandObject");
    // TODO(siyuan): Implement this;
    DCHECK(false) << "NotImplemented";
    return grpc::Status::OK;
  }

private:
  Receiver &receiver_;
};

NotificationListener::NotificationListener(const std::string &my_address, int notification_listener_port,
                                           Receiver &recevier)
    : my_address_(my_address), recevier_(recevier) {
  service_ = std::make_shared<NotificationListenerImpl>();
  std::string grpc_address = my_address + ":" + std::to_string(notification_listener_port_);
  LOG(DEBUG) << "grpc_address " << grpc_address;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
}

void NotificationListener::Run() {
  notification_listener_thread_ = std::thread(&NotificationListener::worker_loop, this);
}

void NotificationListener::Shutdown() { notification_thread_.join(); }

void NotificationListener::worker_loop() {
  LOG(DEBUG) << "[NotificationListener] NotificationListener " << my_address_ << " started";
  grpc_server_->Wait();
}
