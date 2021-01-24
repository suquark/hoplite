#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <queue>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "dependency.h"
#include "util/logging.h"
#include "notification.h"
#include "object_store.grpc.pb.h"
#include "reduce_dependency.h"
#include "util/ctpl_stl.h"

using objectstore::BarrierReply;
using objectstore::BarrierRequest;
using objectstore::ConnectListenerReply;
using objectstore::ConnectListenerRequest;
using objectstore::ConnectReply;
using objectstore::ConnectRequest;
using objectstore::CreateReduceTaskReply;
using objectstore::CreateReduceTaskRequest;
using objectstore::ExitReply;
using objectstore::ExitRequest;
using objectstore::GetLocationSyncReply;
using objectstore::GetLocationSyncRequest;
using objectstore::HandlePullObjectFailureReply;
using objectstore::HandlePullObjectFailureRequest;
using objectstore::PullAndReduceObjectReply;
using objectstore::PullAndReduceObjectRequest;
using objectstore::ReduceInbandObjectReply;
using objectstore::ReduceInbandObjectRequest;
using objectstore::WriteLocationReply;
using objectstore::WriteLocationRequest;

class NotificationServiceImpl final : public objectstore::NotificationServer::Service {
public:
  NotificationServiceImpl(const int notification_listener_port);

  grpc::Status Barrier(grpc::ServerContext *context, const BarrierRequest *request, BarrierReply *reply);

  grpc::Status Connect(grpc::ServerContext *context, const ConnectRequest *request, ConnectReply *reply);

  grpc::Status WriteLocation(grpc::ServerContext *context, const WriteLocationRequest *request,
                             WriteLocationReply *reply);

  grpc::Status GetLocationSync(grpc::ServerContext *context, const GetLocationSyncRequest *request,
                               GetLocationSyncReply *reply);

  grpc::Status HandlePullObjectFailure(grpc::ServerContext *context, const HandlePullObjectFailureRequest *request,
                                       HandlePullObjectFailureReply *reply);

  grpc::Status CreateReduceTask(grpc::ServerContext *context, const CreateReduceTaskRequest *request,
                                CreateReduceTaskReply *reply);

  void InvokePullAndReduceObject(const Node *receiver_node, const Node *sender_node, const ObjectID &reduction_id,
                                 int64_t object_size);

  void InvokeReduceInbandObject(const std::string &receiver_ip, const ObjectID &reduction_id,
                                const std::string &inband_data);

private:
  objectstore::NotificationListener::Stub *
  create_or_get_notification_listener_stub(const std::string &remote_grpc_address);

  void handle_object_ready(const ObjectID &object_id);

  std::shared_ptr<ObjectDependency> get_dependency(const ObjectID &object_id);

  void add_object_for_reduce(const ObjectID &object_id, int64_t object_size, const std::string &owner_ip,
                             const std::string &inband_data);

  std::mutex barrier_mutex_;
  std::atomic<int> barrier_arrive_counter_;
  std::atomic<int> barrier_leave_counter_;

  const int notification_listener_port_;
  struct ReceiverQueueElement {
    enum { SYNC, ASYNC, REDUCE } type;
    // For synchronous recevier
    std::shared_ptr<std::mutex> sync_mutex;
    GetLocationSyncReply *reply;
    // For asynchronous receiver
    std::string receiver_ip;
    std::string query_id;
    // For both synchronous and asynchronous receivers
    // Whether to delete the reference to the object or not
    bool occupying;
  };
  class PendingQueue {
  public:
    void EnqueueGetLocationSync(const ObjectID &object_id, const std::shared_ptr<std::mutex> &sync_mutex,
                                GetLocationSyncReply *reply, const std::string &receiver_ip, bool occupying) {
      std::lock_guard<std::mutex> lock(mutex_);
      pending_objects_[object_id].emplace(
          ReceiverQueueElement{ReceiverQueueElement::SYNC, sync_mutex, reply, receiver_ip, {}, occupying});
    }
    void EnqueueGetLocationForReduce(const ObjectID &object_id) {
      std::lock_guard<std::mutex> lock(mutex_);
      pending_objects_[object_id].emplace(ReceiverQueueElement{ReceiverQueueElement::REDUCE, {}, NULL, {}, {}, true});
    }
    std::queue<ReceiverQueueElement> PopQueue(const ObjectID &object_id) {
      std::lock_guard<std::mutex> lock(mutex_);
      return std::move(pending_objects_[object_id]);
    }

  private:
    std::unordered_map<ObjectID, std::queue<ReceiverQueueElement>> pending_objects_;
    std::mutex mutex_;
  };

  PendingQueue pending_queue_;

  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_pool_;
  std::unordered_map<std::string, std::unique_ptr<objectstore::NotificationListener::Stub>>
      notification_listener_stub_pool_;
  std::mutex channel_pool_mutex_;

  // thread pool for launching tasks
  ctpl::thread_pool thread_pool_;

  std::mutex object_dependencies_mutex_;
  std::unordered_map<ObjectID, std::shared_ptr<ObjectDependency>> object_dependencies_;

  // for reduce tasks
  ReduceManager reduce_manager_;
  std::mutex reduce_manager_mutex_;
};

NotificationServiceImpl::NotificationServiceImpl(const int notification_listener_port)
    : objectstore::NotificationServer::Service(), notification_listener_port_(notification_listener_port),
      thread_pool_(HOPLITE_THREADPOOL_SIZE_FOR_RPC), barrier_arrive_counter_(0), barrier_leave_counter_(0) {}

grpc::Status NotificationServiceImpl::Barrier(grpc::ServerContext *context, const BarrierRequest *request,
                                              BarrierReply *reply) {
  TIMELINE("Barrier");
  int n_nodes = request->num_of_nodes();
  barrier_arrive_counter_++;
  while (barrier_arrive_counter_ < n_nodes)
    ;
  barrier_leave_counter_++;
  while (barrier_leave_counter_ < n_nodes)
    ;
  barrier_arrive_counter_--;
  while (barrier_arrive_counter_ > 0)
    ;
  barrier_leave_counter_--;
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::Connect(grpc::ServerContext *context, const ConnectRequest *request,
                                              ConnectReply *reply) {
  // Create reverse stub
  std::string sender_address = request->sender_ip() + ":" + std::to_string(notification_listener_port_);
  create_or_get_notification_listener_stub(sender_address);
  grpc::ClientContext client_context;
  ConnectListenerRequest connect_request;
  ConnectListenerReply connect_reply;
  auto status = notification_listener_stub_pool_[sender_address]->ConnectListener(&client_context, connect_request,
                                                                                  &connect_reply);
  DCHECK(status.ok()) << "Connect to " << sender_address << " failed: " << status.error_message();

  LOG(INFO) << "Create succeeds on the notification server";
  return grpc::Status::OK;
}

std::shared_ptr<ObjectDependency> NotificationServiceImpl::get_dependency(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock(object_dependencies_mutex_);
  LOG(DEBUG) << "get_dependency() for " << object_id.ToString();
  if (!object_dependencies_.count(object_id)) {
    object_dependencies_[object_id] = std::make_shared<ObjectDependency>(
        object_id, [this](const ObjectID &object_id) { handle_object_ready(object_id); });
  }
  return object_dependencies_[object_id];
}

void NotificationServiceImpl::add_object_for_reduce(const ObjectID &object_id, int64_t object_size,
                                                    const std::string &owner_ip, const std::string &inband_data) {
  TIMELINE("[add_object_for_reduce]");
  std::lock_guard<std::mutex> lock(reduce_manager_mutex_);
  if (inband_data.empty()) {
    auto results = reduce_manager_.AddObject(object_id, object_size, owner_ip);
    for (auto &r : results) {
      Node *n = r.first;
      ObjectID &reduction_id = r.second;
      // check if we have a child dependency
      if (n->left_child && n->left_child->location_known()) {
        InvokePullAndReduceObject(n, n->left_child, reduction_id, object_size);
      }
      if (n->right_child && n->right_child->location_known()) {
        LOG(FATAL) << "This case should not exist";
      }
      // check if we have a parent dependency
      if (n->parent && n->parent->location_known()) {
        InvokePullAndReduceObject(n->parent, n, reduction_id, object_size);
        // now we can publish the reduction id
        if (!n->parent->is_root()) {
          auto dep = get_dependency(reduction_id);
          // the root could be registered twice, so we check the availability first
          if (!dep->Available()) {
            dep->HandleCompletion(n->parent->owner_ip, object_size);
          }
        }
      }
    }
  } else {
    auto results = reduce_manager_.AddInbandObject(object_id, std::move(inband_data));
    for (auto &r : results) {
      InbandDataNode *n = r.first;
      if (n->finished) {
        ObjectID reduction_id = r.second;
        std::string receiver_ip = n->owner_ip;
        // n->reduced_inband_data
        std::string reduced_inband_data = n->get_inband_data();
        thread_pool_.push(
            [this, receiver_ip, reduction_id](int id, std::string reduced_inband_data) {
              InvokeReduceInbandObject(receiver_ip, reduction_id, reduced_inband_data);
            },
            std::move(reduced_inband_data));
      }
    }
  }
}

void NotificationServiceImpl::handle_object_ready(const ObjectID &object_id) {
  TIMELINE("[object directory server] handle_object_ready");
  std::shared_ptr<ObjectDependency> dep = get_dependency(object_id);
  std::string inband_data = dep->GetInbandData();
  std::queue<ReceiverQueueElement> q = pending_queue_.PopQueue(object_id);
  while (!q.empty()) {
    ReceiverQueueElement &receiver = q.front();
    std::string receiver_ip = receiver.receiver_ip;
    int64_t object_size;
    std::string inband_data;
    std::string sender_ip;
    if (inband_data.empty()) {
      dep->Get(receiver_ip, receiver.occupying, &object_size, &sender_ip, &inband_data,
               []() { LOG(FATAL) << "Not expect to fail when there are objects ready"; });
    }
    switch (receiver.type) {
    case ReceiverQueueElement::SYNC: {
      // Reply to synchronous get_location call
      LOG(DEBUG) << "The location of " << object_id.ToString() << " is informed now. "
                 << "sender_ip = " << sender_ip << ", object_size = " << object_size;
      receiver.reply->set_sender_ip(std::move(sender_ip));
      receiver.reply->set_object_size(object_size);
      receiver.reply->set_inband_data(std::move(inband_data));
      DCHECK(!receiver.sync_mutex->try_lock()) << "sync_mutex should be locked";
      receiver.sync_mutex->unlock();
    } break;
    case ReceiverQueueElement::REDUCE: {
      add_object_for_reduce(object_id, object_size, /*owner_ip=*/sender_ip, inband_data);
    } break;
    }
    q.pop();
  }
}

grpc::Status NotificationServiceImpl::WriteLocation(grpc::ServerContext *context, const WriteLocationRequest *request,
                                                    WriteLocationReply *reply) {
  TIMELINE("notification WriteLocation");
  ObjectID object_id = ObjectID::FromBinary(request->object_id());
  std::string sender_ip = request->sender_ip();
  bool finished = request->finished();
  // TODO(siyuan): deal with 'finished' property
  std::shared_ptr<ObjectDependency> dep = get_dependency(object_id);
  if (request->has_inband_data_case() == WriteLocationRequest::kInbandData) {
    dep->HandleInbandCompletion(request->inband_data());
  } else {
    dep->HandleCompletion(sender_ip, request->object_size());
  }
  reply->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::GetLocationSync(grpc::ServerContext *context,
                                                      const GetLocationSyncRequest *request,
                                                      GetLocationSyncReply *reply) {
  TIMELINE("notification GetLocationSync");
  ObjectID object_id = ObjectID::FromBinary(request->object_id());
  std::string receiver_ip = request->receiver_ip();
  // TODO: change this sync_mutex to a condition variable
  // We initiate a locked mutex here. This mutex will be unlocked when
  // we find the sender for this request.
  std::shared_ptr<std::mutex> sync_mutex;
  int64_t object_size;
  std::string inband_data;
  std::string sender_ip;

  std::shared_ptr<ObjectDependency> dep = get_dependency(object_id);
  bool success = dep->Get(receiver_ip, request->occupying(), &object_size, &sender_ip, &inband_data, [&]() {
    // this makes sure that no on completion event will happen before we queued our request
    sync_mutex = std::make_shared<std::mutex>();
    sync_mutex->lock();
    pending_queue_.EnqueueGetLocationSync(object_id, sync_mutex, reply, receiver_ip, request->occupying());
  });
  if (!success) {
    LOG(DEBUG) << "The location of " << object_id.ToString()
               << " is unavailable yet. Waiting for further notification.";
    // we must wait the lock outside so we would not block the dependency manager.
    sync_mutex->lock();
    // TODO: This check seems not working
    // DCHECK(sync_mutex.use_count() == 1) << "sync_mutex memory leak detected";
  } else {
    LOG(DEBUG) << "The location of " << object_id.ToString() << " is already know. "
               << "sender_ip = " << sender_ip << ", object_size = " << object_size;
    reply->set_sender_ip(std::move(sender_ip));
    reply->set_object_size(object_size);
    reply->set_inband_data(std::move(inband_data));
  }
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::HandlePullObjectFailure(grpc::ServerContext *context,
                                                              const HandlePullObjectFailureRequest *request,
                                                              HandlePullObjectFailureReply *reply) {
  auto dep = get_dependency(ObjectID::FromBinary(request->object_id()));
  std::string alternative_sender;
  bool success = dep->HandleFailure(request->receiver_ip(), &alternative_sender);
  reply->set_alternative_sender_ip(std::move(alternative_sender));
  reply->set_success(success);
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::CreateReduceTask(grpc::ServerContext *context,
                                                       const CreateReduceTaskRequest *request,
                                                       CreateReduceTaskReply *reply) {
  TIMELINE("notification CreateReduceTask");
  ObjectID reduction_id = ObjectID::FromBinary(request->reduction_id());
  std::vector<ObjectID> objects_to_reduce;
  for (auto &object_id_it : request->objects_to_reduce()) {
    objects_to_reduce.push_back(ObjectID::FromBinary(object_id_it));
  }
  {
    std::lock_guard<std::mutex> lock(reduce_manager_mutex_);
    reduce_manager_.CreateReduceTask(request->reduce_dst(), objects_to_reduce, reduction_id,
                                     request->num_reduce_objects());
  }

  for (auto &object_id : objects_to_reduce) {
    auto dep = get_dependency(object_id);
    int64_t object_size;
    std::string owner_ip;
    std::string inband_data;
    bool success = dep->Get(request->reduce_dst(), /*occupying=*/false, &object_size, &owner_ip, &inband_data, [&]() {
      // this makes sure that no on completion event will happen before we queued our request
      pending_queue_.EnqueueGetLocationForReduce(object_id);
    });
    // FIXME: there could be some extreme race condition that the object is taking away by someone
    // else after "dep->Get". Not sure if this could be an issue.
    if (success) {
      add_object_for_reduce(object_id, object_size, std::move(owner_ip), std::move(inband_data));
    }
  }
  return grpc::Status::OK;
}

void NotificationServiceImpl::InvokePullAndReduceObject(const Node *receiver_node, const Node *sender_node,
                                                        const ObjectID &reduction_id, int64_t object_size) {
  thread_pool_.push([this, receiver_node, sender_node, reduction_id, object_size](int id) {
    TIMELINE("notification InvokePullAndReduceObject");
    auto remote_address = receiver_node->owner_ip + ":" + std::to_string(notification_listener_port_);
    objectstore::NotificationListener::Stub *stub = create_or_get_notification_listener_stub(remote_address);
    grpc::ClientContext context;
    PullAndReduceObjectRequest request;
    request.set_reduction_id(reduction_id.Binary());
    request.set_is_tree_branch(receiver_node->is_tree_branch());
    request.set_sender_ip(sender_node->owner_ip);
    request.set_from_left_child(receiver_node->left_child == sender_node);
    request.set_object_size(object_size);
    request.set_object_id_to_reduce(receiver_node->object_id.Binary());
    request.set_object_id_to_pull(sender_node->object_id.Binary());
    request.set_is_sender_leaf(sender_node->is_leaf());
    PullAndReduceObjectReply reply;
    auto status = stub->PullAndReduceObject(&context, request, &reply);
    DCHECK(status.ok());
  });
}

void NotificationServiceImpl::InvokeReduceInbandObject(const std::string &receiver_ip, const ObjectID &reduction_id,
                                                       const std::string &inband_data) {
  TIMELINE("notification ReduceInbandObject");
  auto remote_address = receiver_ip + ":" + std::to_string(notification_listener_port_);
  objectstore::NotificationListener::Stub *stub = create_or_get_notification_listener_stub(remote_address);
  grpc::ClientContext context;
  ReduceInbandObjectRequest request;
  request.set_reduction_id(reduction_id.Binary());
  request.set_inband_data(inband_data);
  ReduceInbandObjectReply reply;
  auto status = stub->ReduceInbandObject(&context, request, &reply);
  DCHECK(status.ok());
}

objectstore::NotificationListener::Stub *
NotificationServiceImpl::create_or_get_notification_listener_stub(const std::string &remote_grpc_address) {
  std::lock_guard<std::mutex> lock(channel_pool_mutex_);
  if (channel_pool_.find(remote_grpc_address) == channel_pool_.end()) {
    channel_pool_[remote_grpc_address] = grpc::CreateChannel(remote_grpc_address, grpc::InsecureChannelCredentials());
  }
  if (notification_listener_stub_pool_.find(remote_grpc_address) == notification_listener_stub_pool_.end()) {
    notification_listener_stub_pool_[remote_grpc_address] =
        objectstore::NotificationListener::NewStub(channel_pool_[remote_grpc_address]);
  }
  return notification_listener_stub_pool_[remote_grpc_address].get();
}

NotificationServer::NotificationServer(const std::string &my_address, const int notification_server_port,
                                       const int notification_listener_port)
    : notification_server_port_(notification_server_port), notification_listener_port_(notification_listener_port),
      service_(std::make_shared<NotificationServiceImpl>(notification_listener_port)) {
  std::string grpc_address = my_address + ":" + std::to_string(notification_server_port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
}

void NotificationServer::worker_loop() {
  LOG(INFO) << "[NotificationServer] notification server started";

  grpc_server_->Wait();
}

int main(int argc, char **argv) {
  std::string my_address = std::string(argv[1]);

  std::unique_ptr<NotificationServer> notification_server;
  std::thread notification_server_thread;
  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);

  notification_server.reset(new NotificationServer(my_address, 7777, 8888));
  notification_server_thread = notification_server->Run();
  notification_server_thread.join();
}
