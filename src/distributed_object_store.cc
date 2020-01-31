#include <cmath>
#include <unordered_set>

// gRPC headers
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "distributed_object_store.h"
#include "logging.h"

using objectstore::ObjectStore;
using objectstore::PullReply;
using objectstore::PullRequest;
using objectstore::RedirectReduceReply;
using objectstore::RedirectReduceRequest;
using objectstore::ReduceToReply;
using objectstore::ReduceToRequest;

////////////////////////////////////////////////////////////////
// The gRPC server side of the object store
////////////////////////////////////////////////////////////////

class ObjectStoreServiceImpl final : public ObjectStore::Service {
public:
  ObjectStoreServiceImpl(ObjectSender &object_sender,
                         DistributedObjectStore &store)
      : ObjectStore::Service(), object_sender_(object_sender), store_(store) {}

  grpc::Status Pull(grpc::ServerContext *context, const PullRequest *request,
                    PullReply *reply) {
    TIMELINE("ObjectStoreServiceImpl::Pull()");
    ObjectID object_id = ObjectID::FromBinary(request->object_id());

    LOG(DEBUG) << ": Received a pull request from " << request->puller_ip()
               << " for object " << object_id.ToString();

    object_sender_.send_object(request);
    LOG(DEBUG) << ": Finished a pull request from " << request->puller_ip()
               << " for object " << object_id.ToString();

    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status ReduceTo(grpc::ServerContext *context,
                        const ReduceToRequest *request, ReduceToReply *reply) {
    TIMELINE("ObjectStoreServiceImpl::ReduceTo()");
    object_sender_.AppendTask(request);
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status RedirectReduce(grpc::ServerContext *context,
                              const RedirectReduceRequest *request,
                              RedirectReduceReply *reply) {
    TIMELINE("ObjectStoreServiceImpl::RedirectReduce()");
    ObjectID reduction_id = ObjectID::FromBinary(request->reduction_id());
    std::vector<ObjectID> object_ids;
    for (const auto &object_id_str : request->object_ids()) {
      ObjectID object_id = ObjectID::FromBinary(object_id_str);
      object_ids.push_back(object_id);
    }
    store_.Reduce(object_ids, reduction_id);
    reply->set_ok(true);
    return grpc::Status::OK;
  }

private:
  ObjectSender &object_sender_;
  DistributedObjectStore &store_;
};

////////////////////////////////////////////////////////////////
// The gRPC client side of the object store
////////////////////////////////////////////////////////////////

bool DistributedObjectStore::PullObject(const std::string &remote_address,
                                        const ObjectID &object_id) {
  TIMELINE("DistributedObjectStore::PullObject");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  create_stub(remote_grpc_address);
  grpc::ClientContext context;
  PullRequest request;
  PullReply reply;
  request.set_object_id(object_id.Binary());
  request.set_puller_ip(my_address_);
  auto stub = get_stub(remote_grpc_address);
  // TODO: make sure that grpc stub is thread-safe.
  auto status = stub->Pull(&context, request, &reply);
  return reply.ok();
}

bool DistributedObjectStore::InvokeReduceTo(
    const std::string &remote_address, const ObjectID &reduction_id,
    const std::vector<ObjectID> &dst_object_ids, const std::string &dst_address,
    bool is_endpoint, const ObjectID *src_object_id) {
  TIMELINE("GrpcServer::InvokeReduceTo");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  create_stub(remote_grpc_address);
  grpc::ClientContext context;
  ReduceToRequest request;
  ReduceToReply reply;

  request.set_reduction_id(reduction_id.Binary());
  for (auto &object_id : dst_object_ids) {
    request.add_dst_object_ids(object_id.Binary());
  }
  request.set_dst_address(dst_address);
  request.set_is_endpoint(is_endpoint);
  if (src_object_id != nullptr) {
    request.set_src_object_id(src_object_id->Binary());
  }
  auto stub = get_stub(remote_grpc_address);
  // TODO: make sure that grpc stub is thread-safe.
  auto status = stub->ReduceTo(&context, request, &reply);
  DCHECK(status.ok()) << "[GrpcServer] ReduceTo failed at remote address:"
                      << remote_grpc_address
                      << ", message: " << status.error_message()
                      << ", details = " << status.error_code();

  return reply.ok();
}

bool DistributedObjectStore::InvokeRedirectReduce(
    const std::string &remote_address, const std::vector<ObjectID> &object_ids,
    const ObjectID &reduction_id) {
  TIMELINE("GrpcServer::InvokeRedirectReduce");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  create_stub(remote_grpc_address);
  grpc::ClientContext context;
  RedirectReduceRequest request;
  RedirectReduceReply reply;
  request.set_reduction_id(reduction_id.Binary());
  for (const auto &object_id : object_ids) {
    request.add_object_ids(object_id.Binary());
  }
  auto stub = get_stub(remote_grpc_address);
  // TODO: make sure that grpc stub is thread-safe.
  auto status = stub->RedirectReduce(&context, request, &reply);
  DCHECK(status.ok()) << "[GrpcServer] ReduceTo failed at remote address:"
                      << remote_grpc_address
                      << ", message: " << status.error_message()
                      << ", details = " << status.error_code();
  return reply.ok();
}

////////////////////////////////////////////////////////////////
// The object store API
////////////////////////////////////////////////////////////////

DistributedObjectStore::DistributedObjectStore(
    const std::string &notification_server_address, int redis_port,
    int notification_server_port, int notification_listen_port,
    const std::string &plasma_socket, const std::string &my_address,
    int object_writer_port, int grpc_port)
    : my_address_(my_address), gcs_client_{notification_server_address,
                                           my_address_,
                                           notification_server_port,
                                           notification_listen_port},
      local_store_client_{false, plasma_socket},
      object_writer_{state_, gcs_client_, local_store_client_, my_address_,
                     object_writer_port},
      object_sender_{state_, gcs_client_, local_store_client_, my_address_},
      grpc_port_(grpc_port),
      grpc_address_(my_address_ + ":" + std::to_string(grpc_port_)) {
  TIMELINE("DistributedObjectStore construction function");
  // create a thread to receive remote object
  object_writer_.Run();
  // create a thread to send object
  object_sender_thread_ = object_sender_.Run();

  // initialize the object store
  service_.reset(new ObjectStoreServiceImpl(object_sender_, *this));
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(service_.get());
  grpc_server_ = builder.BuildAndStart();
  object_control_thread_ =
      std::thread(&DistributedObjectStore::worker_loop, this);
  // create a thread to process notifications
  notification_thread_ = gcs_client_.Run();

  gcs_client_.ConnectNotificationServer();
}

DistributedObjectStore::~DistributedObjectStore() {
  TIMELINE("~DistributedObjectStore");
  object_writer_.Shutdown();
  object_sender_.Shutdown();
  object_sender_thread_.join();
  Shutdown();
  gcs_client_.Shutdown();
  notification_thread_.join();
  LOG(INFO) << "Object store has been shutdown.";
}

bool DistributedObjectStore::IsLocalObject(const ObjectID &object_id,
                                           int64_t *size) {
  if (local_store_client_.ObjectExists(object_id)) {
    ObjectBuffer object_buffer;
    local_store_client_.Get(object_id, &object_buffer);
    if (size != nullptr) {
      *size = object_buffer.data->Size();
    }
    return true;
  }
  auto stream = state_.get_progressive_stream(object_id);
  if (stream) {
    if (size != nullptr) {
      *size = stream->size();
    }
    return true;
  }
  return false;
}

void DistributedObjectStore::Put(const std::shared_ptr<Buffer> &buffer,
                                 const ObjectID &object_id) {
  TIMELINE(std::string("DistributedObjectStore Put single object ") +
           object_id.Hex());
  // put object into Plasma
  std::shared_ptr<Buffer> ptr;
  auto pstatus = local_store_client_.Create(object_id, buffer->Size(), &ptr);
  DCHECK(pstatus.ok()) << "Plasma failed to create object_id = "
                       << object_id.Hex() << " size = " << buffer->Size()
                       << ", status = " << pstatus.ToString();
  ptr->CopyFrom(*buffer);
  local_store_client_.Seal(object_id);
  gcs_client_.WriteLocation(object_id, my_address_, true, buffer->Size(),
                            buffer->Data());
}

ObjectID DistributedObjectStore::Put(const std::shared_ptr<Buffer> &buffer) {
  TIMELINE("DistributedObjectStore Put without object_id");
  // generate a random object id
  auto object_id = ObjectID::FromRandom();
  Put(buffer, object_id);
  return object_id;
}

void DistributedObjectStore::Reduce(const std::vector<ObjectID> &object_ids,
                                    ObjectID *created_reduction_id) {
  const auto reduction_id = ObjectID::FromRandom();
  *created_reduction_id = reduction_id;
  Reduce(object_ids, reduction_id);
}

void DistributedObjectStore::Reduce(const std::vector<ObjectID> &object_ids,
                                    const ObjectID &reduction_id) {
  // TODO: support different reduce op and types.
  TIMELINE("DistributedObjectStore Async Reduce");
  DCHECK(object_ids.size() > 0);
  std::thread reduction_thread;
  // starting a thread
  // FIXME: this is an ad-hoc condition
  if (object_ids.size() > 100) {
    reduction_thread = std::thread(&DistributedObjectStore::poll_and_reduce_2d,
                                   this, object_ids, reduction_id);
  } else {
    reduction_thread = std::thread(&DistributedObjectStore::poll_and_reduce,
                                   this, object_ids, reduction_id);
  }

  {
    std::lock_guard<std::mutex> l(reduction_tasks_mutex_);
    DCHECK(reduction_tasks_.find(reduction_id) == reduction_tasks_.end())
        << "Reduction task with " << reduction_id.ToString()
        << " already exists.";
    reduction_tasks_[reduction_id] = {nullptr, std::move(reduction_thread)};
  }
}

void DistributedObjectStore::Get(const ObjectID &object_id,
                                 std::shared_ptr<Buffer> *result) {
  TIMELINE(std::string("DistributedObjectStore Get single object ") +
           object_id.ToString());

  // FIXME: currently the object store will assume that the object
  // exists even before 'Seal' is called. This will cause the problem
  // that an on-going reduction task could be skipped. Here we just
  // reorder the checking process as a workaround.
  std::unique_lock<std::mutex> l(reduction_tasks_mutex_);
  auto search = reduction_tasks_.find(object_id);
  if (search != reduction_tasks_.end()) {
    l.unlock();
    // ==> This ObjectID belongs to a reduction task.
    auto &reduction_task_pair = search->second;
    // we must join the thread first, because the stream
    // pointer could still be nullptr at creation.
    reduction_task_pair.reduction_thread.join();
    auto &stream = reduction_task_pair.stream;
    if (stream) {
      LOG(DEBUG) << "waiting the reduction stream";
      // wait until the object is fully reduced
      stream->wait();
      local_store_client_.Seal(object_id);
    }
    // TODO: should we add this line?
    // gcs_client_.WriteLocation(object_id, my_address_, true, stream->size(),
    //                           stream->data());
    l.lock();
    reduction_tasks_.erase(object_id);
    l.unlock();
  } else {
    l.unlock();
    if (!local_store_client_.ObjectExists(object_id)) {
      // ==> This ObjectID refers to a remote object.
      SyncReply reply = gcs_client_.GetLocationSync(object_id, true);
      if (!check_and_store_inband_data(object_id, reply.object_size,
                                       reply.inband_data)) {
        // send pull request to one of the location
        DCHECK(PullObject(reply.sender_ip, object_id))
            << "Failed to pull object";
      }
    }
  }

  // get object from local store
  ObjectBuffer object_buffer;
  local_store_client_.Get(object_id, &object_buffer);
  *result = object_buffer.data;
}

bool DistributedObjectStore::check_and_store_inband_data(
    const ObjectID &object_id, int64_t object_size,
    const std::string &inband_data) {
  TIMELINE("DistributedObjectStore::check_and_store_inband_data");
  if (inband_data.size() > 0) {
    LOG(DEBUG) << "fetching object directly from inband data";
    DCHECK(inband_data.size() <= inband_data_size_limit)
        << "unexpected inband data size";
    std::shared_ptr<Buffer> data;
    local_store_client_.Create(object_id, object_size, &data);
    data->CopyFrom(inband_data);
    local_store_client_.Seal(object_id);
    return true;
  }
  return false;
}

////////////////////////////////////////////////////////////////
// The object store internal functions
////////////////////////////////////////////////////////////////

void DistributedObjectStore::poll_and_reduce(
    const std::vector<ObjectID> object_ids, const ObjectID reduction_id) {
  TIMELINE("DistributedObjectStore Reduce Thread");
  // the buffer for reduction results
  std::shared_ptr<Buffer> buffer;
  std::vector<ObjectID> notification_candidates;
  std::vector<ObjectID> local_object_ids;

  // iterate over object ids to see if they are local objects
  for (const auto &object_id : object_ids) {
    TIMELINE(std::string("Check local object for ") + object_id.Hex())
    int64_t object_size;
    if (IsLocalObject(object_id, &object_size)) {
      local_object_ids.push_back(object_id);
      if (!buffer) {
        // create the endpoint buffer
        auto pstatus =
            local_store_client_.Create(reduction_id, object_size, &buffer);
        DCHECK(pstatus.ok())
            << "Plasma failed to create reduction_id = " << reduction_id.Hex()
            << " size = " << object_size << ", status = " << pstatus.ToString();
      }
    } else {
      notification_candidates.push_back(object_id);
    }
  }

  std::shared_ptr<ObjectNotifications> notifications =
      gcs_client_.GetLocationAsync(notification_candidates,
                                   reduction_id.Binary(), false);
  // states for enumerating the chain
  std::unordered_set<ObjectID> remaining_ids(notification_candidates.begin(),
                                             notification_candidates.end());

  int node_index = 0;
  ObjectID tail_objectid;
  std::string tail_address;

  // main loop for constructing the reduction chain.
  while (remaining_ids.size() > 0) {
    std::vector<NotificationMessage> ready_ids =
        notifications->GetNotifications();
    // TODO: we should group ready ids by their node address.
    for (auto &ready_id_message : ready_ids) {
      ObjectID ready_id = ready_id_message.object_id;
      std::string address = ready_id_message.sender_ip;
      size_t object_size = ready_id_message.object_size;
      const std::string &inband_data = ready_id_message.inband_data;
      if (check_and_store_inband_data(ready_id, object_size, inband_data)) {
        // mark this object as local
        address = my_address_;
      }
      DCHECK(address != "")
          << ready_id.ToString()
          << " location is not ready, but notification is received!";
      LOG(INFO) << "Received notification, address = " << address
                << ", object_id = " << ready_id.ToString();

      if (!buffer) {
        TIMELINE("Create endpoint buffer")
        // create the endpoint buffer
        auto pstatus =
            local_store_client_.Create(reduction_id, object_size, &buffer);
        DCHECK(pstatus.ok())
            << "Plasma failed to create reduction_id = " << reduction_id.Hex()
            << " size = " << object_size << ", status = " << pstatus.ToString();
      }

      if (address == my_address_) {
        // move local objects to another address, because there's no
        // necessary to transfer them through the network.
        local_object_ids.push_back(ready_id);
      } else {
        // wait until at lease 2 objects in nodes except the master node are
        // ready.
        if (node_index == 0) {
          auto reduction_endpoint =
              state_.create_progressive_stream(reduction_id, buffer);
          {
            std::lock_guard<std::mutex> l(reduction_tasks_mutex_);
            reduction_tasks_[reduction_id].stream = reduction_endpoint;
          }
        } else if (node_index == 1) {
          // Send 'ReduceTo' command to the first node in the chain.
          bool reply_ok = InvokeReduceTo(tail_address, reduction_id, {ready_id},
                                         address, false, &tail_objectid);
          DCHECK(reply_ok);
        } else if (node_index > 1) {
          // Send 'ReduceTo' command to the other node in the chain.
          bool reply_ok = InvokeReduceTo(tail_address, reduction_id, {ready_id},
                                         address, false);
          DCHECK(reply_ok);
        }
        tail_objectid = ready_id;
        tail_address = address;
        node_index++;
      }
      // mark it as done
      remaining_ids.erase(ready_id);
    }
  }

  // send the reduced object back to the master node.
  bool reply_ok = false;
  if (node_index == 0) {
    // all the objects are local, we just reduce them locally
    LOG(INFO) << "All the objects to be reduced are local";
    // TODO: support more reduction types & ops
    reduce_local_objects<float>(local_object_ids, buffer.get());
    local_store_client_.Seal(reduction_id);
    // write the location just like in 'Put()'
    gcs_client_.WriteLocation(reduction_id, my_address_, true, buffer->Size(),
                              buffer->Data());
  } else if (node_index == 1) {
    // only two nodes, no streaming needed
    reply_ok = InvokeReduceTo(tail_address, reduction_id, local_object_ids,
                              my_address_, true, &tail_objectid);
    DCHECK(reply_ok);
  } else {
    // more than 2 nodes
    reply_ok = InvokeReduceTo(tail_address, reduction_id, local_object_ids,
                              my_address_, true);
    DCHECK(reply_ok);
  }
}

void DistributedObjectStore::poll_and_reduce_2d(
    const std::vector<ObjectID> object_ids, const ObjectID reduction_id) {
  TIMELINE("DistributedObjectStore Reduce Thread 2D");
  // we do not use reference for its parameters because it will be executed
  // in a thread.

  // TODO: separate local objects first
  size_t n_objects = object_ids.size();
  int rows = floor(sqrt(n_objects));
  LOG(INFO) << "number of rows: " << rows;

  std::vector<std::pair<std::string, ObjectID>> lines;

  std::shared_ptr<ObjectNotifications> notifications =
      gcs_client_.GetLocationAsync(object_ids, reduction_id.Binary(), false);

  // states for enumerating the chain
  std::unordered_set<ObjectID> remaining_ids(object_ids.begin(),
                                             object_ids.end());
  std::vector<ObjectID> local_object_ids;

  // main loop for constructing the reduction chain.
  while (remaining_ids.size() > 0) {
    std::vector<NotificationMessage> ready_ids =
        notifications->GetNotifications();
    // TODO: we should group ready ids by their node address.
    for (auto &ready_id_message : ready_ids) {
      ObjectID ready_id = ready_id_message.object_id;
      std::string address = ready_id_message.sender_ip;
      size_t object_size = ready_id_message.object_size;
      const std::string &inband_data = ready_id_message.inband_data;
      if (check_and_store_inband_data(ready_id, object_size, inband_data)) {
        // mark this object as local
        address = my_address_;
      }
      DCHECK(address != "")
          << ready_id.ToString()
          << " location is not ready, but notification is received!";
      LOG(INFO) << "Received notification, address = " << address
                << ", object_id = " << ready_id.ToString();

      if (address == my_address_) {
        // move local objects to another address, because there's no
        // necessary to transfer them through the network.
        local_object_ids.push_back(ready_id);
      } else {
        lines.emplace_back(address, ready_id);
      }
      // mark it as done
      remaining_ids.erase(ready_id);
      if (lines.size() >= rows) {
        break;
      }
    }
    if (lines.size() >= rows) {
      // TODO: unsubscribe objects
      break;
    }
  }

  if (lines.size() < rows || remaining_ids.size() < rows) {
    // TODO: This is inefficient. There should be a pathway that all
    // objects are ready.
    poll_and_reduce(object_ids, reduction_id);
  } else {
    std::vector<ObjectID> edge(local_object_ids.begin(),
                               local_object_ids.end());
    int remaining_size = remaining_ids.size();
    std::vector<ObjectID> remaining_ids_list(remaining_ids.begin(),
                                             remaining_ids.end());
    int processed_count = 0;
    for (int i = 0; i < rows; i++) {
      std::vector<ObjectID> redirect_object_ids{lines[i].second};
      int share_count = (remaining_size / rows) + (i < remaining_size % rows);
      for (int j = 0; j < share_count; j++, processed_count++) {
        redirect_object_ids.push_back(remaining_ids_list[processed_count]);
      }
      auto line_reduction_id = ObjectID::FromRandom();
      edge.push_back(line_reduction_id);
      InvokeRedirectReduce(lines[i].first, redirect_object_ids,
                           line_reduction_id);
    }
    poll_and_reduce(edge, reduction_id);
  }
}

void DistributedObjectStore::worker_loop() {
  LOG(INFO) << "[GprcServer] grpc server " << my_address_ << " started";
  grpc_server_->Wait();
}

objectstore::ObjectStore::Stub *
DistributedObjectStore::get_stub(const std::string &remote_grpc_address) {
  std::lock_guard<std::mutex> lock(grpc_stub_map_mutex_);
  return object_store_stub_pool_[remote_grpc_address].get();
}

void DistributedObjectStore::create_stub(
    const std::string &remote_grpc_address) {
  std::lock_guard<std::mutex> lock(grpc_stub_map_mutex_);
  if (channel_pool_.find(remote_grpc_address) == channel_pool_.end()) {
    channel_pool_[remote_grpc_address] = grpc::CreateChannel(
        remote_grpc_address, grpc::InsecureChannelCredentials());
  }
  if (object_store_stub_pool_.find(remote_grpc_address) ==
      object_store_stub_pool_.end()) {
    object_store_stub_pool_[remote_grpc_address] =
        ObjectStore::NewStub(channel_pool_[remote_grpc_address]);
  }
}