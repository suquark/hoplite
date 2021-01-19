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

#include "dependency.h"
#include "logging.h"
#include "notification.h"
#include "object_store.grpc.pb.h"
#include "util/ctpl_stl.h"

using objectstore::BarrierReply;
using objectstore::BarrierRequest;
using objectstore::ConnectListenerReply;
using objectstore::ConnectListenerRequest;
using objectstore::ConnectReply;
using objectstore::ConnectRequest;
using objectstore::ExitReply;
using objectstore::ExitRequest;
using objectstore::GetLocationAsyncAnswerReply;
using objectstore::GetLocationAsyncAnswerRequest;
using objectstore::GetLocationAsyncReply;
using objectstore::GetLocationAsyncRequest;
using objectstore::GetLocationSyncReply;
using objectstore::GetLocationSyncRequest;
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

  grpc::Status GetLocationAsync(grpc::ServerContext *context, const GetLocationAsyncRequest *request,
                                GetLocationAsyncReply *reply);

private:
  bool send_notification(const std::string &receiver_ip, const GetLocationAsyncAnswerRequest &request);

  objectstore::NotificationListener::Stub *
  create_or_get_notification_listener_stub(const std::string &remote_grpc_address);

  void handle_object_ready(const ObjectID &object_id);

  ObjectDependency &get_dependency(const ObjectID &object_id);

  std::mutex barrier_mutex_;
  std::atomic<int> barrier_arrive_counter_;
  std::atomic<int> barrier_leave_counter_;

  const int notification_listener_port_;
  struct ReceiverQueueElement {
    enum { SYNC, ASYNC } type;
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
  std::unordered_map<ObjectID, std::queue<ReceiverQueueElement>> pending_receiver_ips_;

  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_pool_;
  std::unordered_map<std::string, std::unique_ptr<objectstore::NotificationListener::Stub>>
      notification_listener_stub_pool_;
  std::mutex channel_pool_mutex_;

  std::mutex object_location_mutex_;

  // thread pool for launching tasks
  ctpl::thread_pool thread_pool_;
  std::unordered_map<ObjectID, ObjectDependency> object_dependencies_;
};

NotificationServiceImpl::NotificationServiceImpl(const int notification_listener_port)
    : objectstore::NotificationServer::Service(), notification_listener_port_(notification_listener_port),
      thread_pool_(1), barrier_arrive_counter_(0), barrier_leave_counter_(0) {}

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

ObjectDependency &NotificationServiceImpl::get_dependency(const ObjectID &object_id) {
  if (!object_dependencies_.count(object_id)) {
    object_dependencies_.emplace(
        object_id, ObjectDependency(object_id, std::bind(&NotificationServiceImpl::handle_object_ready, this)));
  }
  return object_dependencies_[object_id];
}

void NotificationServiceImpl::handle_object_ready(const ObjectID &object_id) {
  // object_location_mutex_ should be held before entering here
  std::queue<ReceiverQueueElement> &q = pending_receiver_ips_[object_id].second;
  ObjectDependency &dep = get_dependency(object_id);
  std::string inband_data = dep.GetInbandData();

  while (!q.empty()) {
    ReceiverQueueElement &receiver = q.front();
    std::string receiver_ip = receiver.receiver_ip;
    int64_t object_size;
    std::string inband_data;
    std::string sender_ip;
    if (inband_data.empty()) {
      dep.Get(receiver_ip, receiver.occupying, &object_size, &sender_ip, &inband_data,
              []() { LOG(FATAL) << "Not expect to fail when there are objects ready"; });
    }
    switch (receiver.type) {
    case ReceiverQueueElement::SYNC: {
      // Reply to synchronous get_location call
      receiver.reply->set_sender_ip(std::move(sender_ip));
      receiver.reply->set_object_size(object_size);
      receiver.reply->set_inband_data(std::move(inband_data));
      DCHECK(!receiver.sync_mutex->try_lock()) << "sync_mutex should be locked";
      receiver.sync_mutex->unlock();
    } break;
    case ReceiverQueueElement::ASYNC: {
      GetLocationAsyncAnswerRequest req;
      // Batching replies to asynchronous get_location call
      GetLocationAsyncAnswerRequest::ObjectInfo *object = req.add_objects();
      object->set_object_id(object_id.Binary());
      object->set_sender_ip(std::move(sender_ip));
      object->set_query_id(receiver.query_id);
      object->set_object_size(object_size);
      object->set_inband_data(std::move(inband_data));
      thread_pool_.push([this, receiver_ip](int id, GetLocationAsyncAnswerRequest r) {
        send_notification(receiver_ip, r);
      })(std::move(req));
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
  std::lock_guard<std::mutex> l(object_location_mutex_);
  auto &dep = get_dependency(object_id);
  if (request->has_inband_data_case() == WriteLocationRequest::kInbandData) {
    dep.HandleInbandCompletion(request->inband_data());
  } else {
    dep.HandleCompletion(sender_ip, request->object_size());
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

  std::unique_lock<std::mutex> l(object_location_mutex_);
  bool success =
      get_dependency(object_id).Get(receiver_ip, request->occupying(), &object_size, &sender_ip, &inband_data, [&]() {
        // this makes sure that no on completion event will happen before we queued our request
        sync_mutex = std::make_shared<std::mutex>();
        sync_mutex->lock();
        pending_receiver_ips_[object_id].emplace(
            ReceiverQueueElement{ReceiverQueueElement::SYNC, sync_mutex, reply, receiver_ip, {}, request->occupying()});
      });
  // unlock so that object dependency can get processed
  l.unlock();

  if (!inband_data.empty()) {
    reply->set_inband_data(inband_data);
    reply->set_object_size(object_size);
    return grpc::Status::OK;
  }
  if (!success) {
    sync_mutex->lock();
    DCHECK(sync_mutex.use_count() == 1) << "sync_mutex memory leak detected";
  }
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::GetLocationAsync(grpc::ServerContext *context,
                                                       const GetLocationAsyncRequest *request,
                                                       GetLocationAsyncReply *reply) {
  TIMELINE("notification GetLocationAsync");
  std::string receiver_ip = request->receiver_ip();
  GetLocationAsyncAnswerRequest req;

  std::unique_lock<std::mutex> l(object_location_mutex_);
  // TODO: pass in repeated object ids will send twice.
  for (auto object_id_it : request->object_ids()) {
    ObjectID object_id = ObjectID::FromBinary(object_id_it);
    int64_t object_size;
    std::string sender_ip;
    std::string inband_data;
    bool success =
        get_dependency(object_id).Get(receiver_ip, request->occupying(), &object_size, &sender_ip, &inband_data, [&]() {
          // this makes sure that no on completion event will happen before we queued our request
          pending_receiver_ips_[object_id].emplace(ReceiverQueueElement{
              ReceiverQueueElement::ASYNC, {}, NULL, receiver_ip, request->query_id(), request->occupying()});
        });
    if (success) {
      // Batching replies to asynchronous get_location call
      GetLocationAsyncAnswerRequest::ObjectInfo *object = req.add_objects();
      object->set_object_id(object_id.Binary());
      object->set_sender_ip(std::move(sender_ip));
      object->set_query_id(request->query_id());
      object->set_object_size(object_size);
      object->set_inband_data(std::move(inband_data));
    }
  }
  l.unlock();

  if (req.objects_size() > 0) {
    thread_pool_.push(
        [this, receiver_ip, req](int id, GetLocationAsyncAnswerRequest r) { send_notification(receiver_ip, r); },
        std::move(req));
  }
  reply->set_ok(true);
  return grpc::Status::OK;
}

bool NotificationServiceImpl::send_notification(const std::string &receiver_ip,
                                                const GetLocationAsyncAnswerRequest &request) {
  TIMELINE("notification send_notification");
  auto remote_address = receiver_ip + ":" + std::to_string(notification_listener_port_);
  objectstore::NotificationListener::Stub *stub = create_or_get_notification_listener_stub(remote_address);
  grpc::ClientContext context;
  GetLocationAsyncAnswerReply reply;
  stub->GetLocationAsyncAnswer(&context, request, &reply);
  return reply.ok();
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
