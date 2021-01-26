#include "notification_listener.h"

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "object_store.grpc.pb.h"
#include "object_store_state.h"

using objectstore::ConnectListenerReply;
using objectstore::ConnectListenerRequest;
using objectstore::PullAndReduceObjectReply;
using objectstore::PullAndReduceObjectRequest;
using objectstore::ReduceInbandObjectReply;
using objectstore::ReduceInbandObjectRequest;

class NotificationListenerImpl final : public objectstore::NotificationListener::Service {
public:
  NotificationListenerImpl(ObjectStoreState &state, Receiver &receiver, LocalStoreClient &local_store_client)
      : objectstore::NotificationListener::Service(), state_(state), receiver_(receiver),
        local_store_client_(local_store_client) {
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
    std::shared_ptr<LocalReduceTask> task;
    if (object_id_to_reduce == reduction_id) {
      // we are the reduce caller/root
      task = state_.get_local_reduce_task(reduction_id);
      object_id_to_reduce = task->local_object;
    }
    receiver_.receive_and_reduce_object(reduction_id, request->is_tree_branch(), request->sender_ip(),
                                        request->from_left_child(), request->object_size(), object_id_to_reduce,
                                        object_id_to_pull, request->is_sender_leaf(), request->reset_progress(), task);
    return grpc::Status::OK;
  }

  grpc::Status ReduceInbandObject(grpc::ServerContext *context, const ReduceInbandObjectRequest *request,
                                  ReduceInbandObjectReply *reply) {
    TIMELINE("ReduceInbandObject");
    ObjectID reduction_id = ObjectID::FromBinary(request->reduction_id());
    std::shared_ptr<LocalReduceTask> task = state_.get_local_reduce_task(reduction_id);
    std::shared_ptr<Buffer> buffer;
    local_store_client_.GetBufferOrCreate(reduction_id, request->inband_data().size(), &buffer);
    buffer->CopyFrom(request->inband_data());
    task->NotifyFinished();
    return grpc::Status::OK;
  }

private:
  ObjectStoreState &state_;
  Receiver &receiver_;
  LocalStoreClient &local_store_client_;
};

NotificationListener::NotificationListener(const std::string &my_address, int notification_listener_port,
                                           ObjectStoreState &state, Receiver &recevier,
                                           LocalStoreClient &local_store_client)
    : my_address_(my_address), state_(state), recevier_(recevier), local_store_client_(local_store_client) {
  service_ = std::make_shared<NotificationListenerImpl>(state, recevier, local_store_client);
  std::string grpc_address = my_address + ":" + std::to_string(notification_listener_port);
  LOG(DEBUG) << "grpc_address " << grpc_address;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
}

void NotificationListener::Run() {
  notification_listener_thread_ = std::thread(&NotificationListener::worker_loop, this);
}

void NotificationListener::Shutdown() {
  grpc_server_->Shutdown();
  notification_listener_thread_.join();
}

void NotificationListener::worker_loop() {
  LOG(DEBUG) << "[NotificationListener] NotificationListener " << my_address_ << " started";
  grpc_server_->Wait();
}
