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

#include "logging.h"
#include "notification.h"
#include "object_store.grpc.pb.h"
#include "util/ctpl_stl.h"

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
using objectstore::IsReadyReply;
using objectstore::IsReadyRequest;
using objectstore::RegisterReply;
using objectstore::RegisterRequest;
using objectstore::BarrierReply;
using objectstore::BarrierRequest;
using objectstore::WriteLocationReply;
using objectstore::WriteLocationRequest;

class NotificationServiceImpl final
    : public objectstore::NotificationServer::Service {
public:
  NotificationServiceImpl(const int notification_listener_port);

  grpc::Status Register(grpc::ServerContext *context,
                        const RegisterRequest *request, RegisterReply *reply);

  grpc::Status IsReady(grpc::ServerContext *context,
                       const IsReadyRequest *request, IsReadyReply *reply);

  grpc::Status Barrier(grpc::ServerContext *context,
                       const IsReadyRequest *request, IsReadyReply *reply);

  grpc::Status Exit(grpc::ServerContext *context, const ExitRequest *request,
                    ExitReply *reply);

  grpc::Status Connect(grpc::ServerContext *context,
                       const ConnectRequest *request, ConnectReply *reply);

  grpc::Status WriteLocation(grpc::ServerContext *context,
                             const WriteLocationRequest *request,
                             WriteLocationReply *reply);

  grpc::Status GetLocationSync(grpc::ServerContext *context,
                               const GetLocationSyncRequest *request,
                               GetLocationSyncReply *reply);

  grpc::Status GetLocationAsync(grpc::ServerContext *context,
                                const GetLocationAsyncRequest *request,
                                GetLocationAsyncReply *reply);

private:
  void put_inband_data(const ObjectID &key, const std::string &value);

  bool has_inband_data(const ObjectID &key);

  std::string get_inband_data(const ObjectID &key);

  void try_send_notification(std::vector<ObjectID> object_ids);

  void push_async_request_into_queue(GetLocationAsyncRequest request);

  bool send_notification(const std::string &receiver_ip,
                         const GetLocationAsyncAnswerRequest &request);

  void
  create_notification_listener_stub(const std::string &remote_grpc_address);

  // Inband data directory and its atomic lock.
  // TODO: We should implement LRU gabage collection for the inband data
  // storage. But it doesn't matter now because these data take too few
  // space.
  std::unordered_map<ObjectID, std::string> inband_data_directory_;
  std::atomic_flag directory_lock_ = ATOMIC_FLAG_INIT;

  std::mutex barrier_mutex_;
  int number_of_nodes_;
  std::unordered_set<std::string> participants_;
  std::atomic<int> barrier_arrive_counter_;
  std::atomic<int> barrier_leave_counter_;
  int barrier_flag_;
  const int notification_listener_port_;
  struct ReceiverQueueElement {
    enum { SYNC, ASYNC } type;
    // For synchronous recevier
    std::shared_ptr<std::mutex> sync_mutex;
    std::shared_ptr<std::string> result_sender_ip;
    // For asynchronous receiver
    std::string receiver_ip;
    std::string query_id;
    // For both synchronous and asynchronous receivers
    // Whether to delete the reference to the object or not
    bool occupying;
  };
  std::unordered_map<ObjectID, std::queue<ReceiverQueueElement>>
      pending_receiver_ips_;
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_pool_;
  std::unordered_map<std::string,
                     std::unique_ptr<objectstore::NotificationListener::Stub>>
      notification_listener_stub_pool_;
  std::mutex object_location_mutex_;
  std::unordered_map<ObjectID, std::priority_queue<std::pair<int, std::string>>>
      object_location_store_ready_; // (weight, ip) in priority queue, weight=1
                                    // means finished
  std::unordered_map<ObjectID, size_t> object_size_;
  // thread pool for launching tasks
  ctpl::thread_pool thread_pool_;
};

NotificationServiceImpl::NotificationServiceImpl(
    const int notification_listener_port)
    : objectstore::NotificationServer::Service(),
      notification_listener_port_(notification_listener_port), thread_pool_(1) {
}

grpc::Status NotificationServiceImpl::Register(grpc::ServerContext *context,
                                               const RegisterRequest *request,
                                               RegisterReply *reply) {
  std::lock_guard<std::mutex> guard(barrier_mutex_);
  number_of_nodes_ = request->num_of_nodes();
  participants_.clear();
  reply->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::IsReady(grpc::ServerContext *context,
                                              const IsReadyRequest *request,
                                              IsReadyReply *reply) {
  {
    std::lock_guard<std::mutex> guard(barrier_mutex_);
    participants_.insert(request->ip());
    LOG(INFO) << "Number of participants = " << participants_.size();
    for (auto &node : participants_) {
      LOG(INFO) << "participants " << node;
    }
  }
  while (true) {
    {
      std::lock_guard<std::mutex> guard(barrier_mutex_);
      if (participants_.size() == number_of_nodes_) {
        break;
      }
    }
    usleep(1);
  }

  reply->set_ok(true);
  LOG(ERROR) << "barrier exits";
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::Barrier(grpc::ServerContext *context,
                                               const BarrierRequest *request,
                                               BarrierReply *reply) {
  int n_nodes = request->num_of_nodes();
  std::lock_guard<std::mutex> l(barrier_mutex_);
  if (barier_leave_counter_ == n_nodes) {
    if (barrier_arrive_counter_ == 0) {
      barrier_flag_ = 0;
    } else {
      l.unlock();
      while (barrier_leave_counter_ != p);
      l.lock();
      barrier_flag_ = 0;
    }
  }
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::Exit(grpc::ServerContext *context,
                                           const ExitRequest *request,
                                           ExitReply *reply) {
  {
    std::lock_guard<std::mutex> guard(barrier_mutex_);
    participants_.erase(request->ip());
    LOG(INFO) << "Participant " << request->ip() << " wants to exit! "
              << participants_.size() << " nodes remaining!";
  }

  while (true) {
    {
      std::lock_guard<std::mutex> guard(barrier_mutex_);
      if (participants_.empty()) {
        break;
      }
    }
    usleep(1);
  }
  LOG(INFO) << "Participant " << request->ip() << " exited!";
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::Connect(grpc::ServerContext *context,
                                              const ConnectRequest *request,
                                              ConnectReply *reply) {
  // Create reverse stub
  std::string sender_address =
      request->sender_ip() + ":" + std::to_string(notification_listener_port_);
  create_notification_listener_stub(sender_address);
  grpc::ClientContext client_context;
  ConnectListenerRequest connect_request;
  ConnectListenerReply connect_reply;
  auto status =
      notification_listener_stub_pool_[sender_address]->ConnectListener(
          &client_context, connect_request, &connect_reply);
  DCHECK(status.ok()) << "Connect to " << sender_address
                      << " failed: " << status.error_message();

  LOG(INFO) << "Create succeeds on the notification server";
  return grpc::Status::OK;
}

grpc::Status
NotificationServiceImpl::WriteLocation(grpc::ServerContext *context,
                                       const WriteLocationRequest *request,
                                       WriteLocationReply *reply) {
  TIMELINE("notification WriteLocation");
  std::unique_lock<std::mutex> l(object_location_mutex_);
  ObjectID object_id = ObjectID::FromBinary(request->object_id());
  std::string sender_ip = request->sender_ip();
  bool finished = request->finished();
  size_t object_size = request->object_size();
  if (request->has_inband_data_case() == WriteLocationRequest::kInbandData) {
    put_inband_data(object_id, request->inband_data());
  }
  // Weights of finished objects will be always larger than the weights of
  // unfinished objects. All finished objects as well as unfinished objects
  // will have random weights.
  int weight = (rand() % 100) + (finished ? 100 : 0);
  if (object_size_.find(object_id) == object_size_.end()) {
    object_size_[object_id] = object_size;
  } else {
    DCHECK(object_size_[object_id] == object_size)
        << "Size of object " << object_id.Hex() << " has changed.";
  }
  object_location_store_ready_[object_id].push(
      std::make_pair(weight, sender_ip));
  l.unlock();
  thread_pool_.push(
      [this, object_id](int id) { try_send_notification({object_id}); });
  reply->set_ok(true);
  return grpc::Status::OK;
}

grpc::Status
NotificationServiceImpl::GetLocationSync(grpc::ServerContext *context,
                                         const GetLocationSyncRequest *request,
                                         GetLocationSyncReply *reply) {
  TIMELINE("notification GetLocationSync");
  std::unique_lock<std::mutex> l(object_location_mutex_);
  ObjectID object_id = ObjectID::FromBinary(request->object_id());
  std::shared_ptr<std::mutex> sync_mutex = std::make_shared<std::mutex>();
  // TODO: change this sync_mutex to a condition variable
  // We initiate a locked mutex here. This mutex will be unlocked when
  // we find the sender for this request.
  sync_mutex->lock();
  std::shared_ptr<std::string> result_sender_ip =
      std::make_shared<std::string>();
  pending_receiver_ips_[object_id].emplace(
      ReceiverQueueElement{ReceiverQueueElement::SYNC,
                           sync_mutex,
                           result_sender_ip,
                           {},
                           {},
                           request->occupying()});
  l.unlock();
  thread_pool_.push(
      [this, object_id](int id) { try_send_notification({object_id}); });
  sync_mutex->lock();
  l.lock();
  DCHECK(sync_mutex.use_count() == 1) << "sync_mutex memory leak detected";
  DCHECK(result_sender_ip.use_count() == 1)
      << "result_sender_ip memory leak detected";
  reply->set_sender_ip(*result_sender_ip);
  reply->set_object_size(object_size_[object_id]);
  reply->set_inband_data(get_inband_data(object_id));
  return grpc::Status::OK;
}

grpc::Status NotificationServiceImpl::GetLocationAsync(
    grpc::ServerContext *context, const GetLocationAsyncRequest *request,
    GetLocationAsyncReply *reply) {
  TIMELINE("notification GetLocationAsync");
  thread_pool_.push(
      [this](int id, GetLocationAsyncRequest request) {
        push_async_request_into_queue(request);
      },
      std::move(*request));
  reply->set_ok(true);
  return grpc::Status::OK;
}

void NotificationServiceImpl::put_inband_data(const ObjectID &key,
                                              const std::string &value) {
  while (directory_lock_.test_and_set(std::memory_order_acquire))
    ;
  inband_data_directory_[key] = value;
  directory_lock_.clear(std::memory_order_release);
}

bool NotificationServiceImpl::has_inband_data(const ObjectID &key) {
  while (directory_lock_.test_and_set(std::memory_order_acquire))
    ;
  bool exist = inband_data_directory_.count(key) > 0;
  directory_lock_.clear(std::memory_order_release);
  return exist;
}

std::string NotificationServiceImpl::get_inband_data(const ObjectID &key) {
  while (directory_lock_.test_and_set(std::memory_order_acquire))
    ;
  // return an empty string if the object ID does not exist
  std::string data;
  auto search = inband_data_directory_.find(key);
  if (search != inband_data_directory_.end()) {
    data = search->second;
  }
  directory_lock_.clear(std::memory_order_release);
  // likely that return copy will be avoided by the compiler
  return data;
}

void NotificationServiceImpl::try_send_notification(
    std::vector<ObjectID> object_ids) {
  TIMELINE("notification try_send_notification");
  std::unique_lock<std::mutex> l(object_location_mutex_);
  std::unordered_map<std::string, GetLocationAsyncAnswerRequest> request_pool;
  for (auto &object_id : object_ids) {
    if (pending_receiver_ips_.find(object_id) != pending_receiver_ips_.end() &&
        object_location_store_ready_.find(object_id) !=
            object_location_store_ready_.end()) {
      // if both the pending receivers queue and pending senders queue are
      // not empty, we can pair the receiver and senders.
      while (!pending_receiver_ips_[object_id].empty() &&
             !object_location_store_ready_[object_id].empty()) {
        std::string sender_ip =
            object_location_store_ready_[object_id].top().second;
        ReceiverQueueElement receiver =
            pending_receiver_ips_[object_id].front();
        if (!has_inband_data(object_id) && receiver.occupying) {
          // In this case, the client will take the ownership
          // of the object transfer. Just pop it here so later
          // requests of this object ID will be pending.
          object_location_store_ready_[object_id].pop();
        }
        pending_receiver_ips_[object_id].pop();
        switch (receiver.type) {
        case ReceiverQueueElement::SYNC: {
          // Reply to synchronous get_lcoation call
          *receiver.result_sender_ip = sender_ip;
          DCHECK(!receiver.sync_mutex->try_lock())
              << "sync_mutex should be locked";
          receiver.sync_mutex->unlock();
        } break;
        case ReceiverQueueElement::ASYNC: {
          // Batching replies to asynchronous get_lcoation call
          GetLocationAsyncAnswerRequest::ObjectInfo *object =
              request_pool[receiver.receiver_ip].add_objects();
          object->set_object_id(object_id.Binary());
          object->set_sender_ip(sender_ip);
          object->set_query_id(receiver.query_id);
          object->set_object_size(object_size_[object_id]);
          object->set_inband_data(get_inband_data(object_id));
        } break;
        }
      }
    }
  }
  for (auto &request : request_pool) {
    DCHECK(send_notification(request.first, request.second))
        << "Failed to send notification";
  }
}

void NotificationServiceImpl::push_async_request_into_queue(
    GetLocationAsyncRequest request) {
  TIMELINE("notification push_async_request_into_queue");
  std::unique_lock<std::mutex> l(object_location_mutex_);
  std::string receiver_ip = request.receiver_ip();
  std::string query_id = request.query_id();
  std::vector<ObjectID> object_ids;
  // TODO: pass in repeated object ids will send twice.
  for (auto object_id_it : request.object_ids()) {
    ObjectID object_id = ObjectID::FromBinary(object_id_it);
    pending_receiver_ips_[object_id].emplace(
        ReceiverQueueElement{ReceiverQueueElement::ASYNC,
                             {},
                             {},
                             receiver_ip,
                             query_id,
                             request.occupying()});
    object_ids.push_back(object_id);
  }
  l.unlock();
  thread_pool_.push(
      [this, object_ids](int id) { try_send_notification(object_ids); });
}

bool NotificationServiceImpl::send_notification(
    const std::string &receiver_ip,
    const GetLocationAsyncAnswerRequest &request) {
  TIMELINE("notification send_notification");
  auto remote_address =
      receiver_ip + ":" + std::to_string(notification_listener_port_);
  create_notification_listener_stub(remote_address);
  grpc::ClientContext context;
  GetLocationAsyncAnswerReply reply;
  notification_listener_stub_pool_[remote_address]->GetLocationAsyncAnswer(
      &context, request, &reply);
  return reply.ok();
}

void NotificationServiceImpl::create_notification_listener_stub(
    const std::string &remote_grpc_address) {
  if (channel_pool_.find(remote_grpc_address) == channel_pool_.end()) {
    channel_pool_[remote_grpc_address] = grpc::CreateChannel(
        remote_grpc_address, grpc::InsecureChannelCredentials());
  }
  if (notification_listener_stub_pool_.find(remote_grpc_address) ==
      notification_listener_stub_pool_.end()) {
    notification_listener_stub_pool_[remote_grpc_address] =
        objectstore::NotificationListener::NewStub(
            channel_pool_[remote_grpc_address]);
  }
}

NotificationServer::NotificationServer(const std::string &my_address,
                                       const int notification_server_port,
                                       const int notification_listener_port)
    : notification_server_port_(notification_server_port),
      notification_listener_port_(notification_listener_port),
      service_(std::make_shared<NotificationServiceImpl>(
          notification_listener_port)) {
  std::string grpc_address =
      my_address + ":" + std::to_string(notification_server_port);
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
