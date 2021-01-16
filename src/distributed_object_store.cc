#include <cmath>
#include <unordered_set>

// gRPC headers
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "common/config.h"
#include "distributed_object_store.h"
#include "logging.h"

using objectstore::ObjectStore;
using objectstore::PullReply;
using objectstore::PullRequest;
using objectstore::RedirectReduceReply;
using objectstore::RedirectReduceRequest;
using objectstore::ReduceToReply;
using objectstore::ReduceToRequest;
using objectstore::RemoteGetReducedObjectsReply;
using objectstore::RemoteGetReducedObjectsRequest;

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
    // TODO(siyuan): This function may need some refactoring.
    TIMELINE("ObjectStoreServiceImpl::Pull()");
    ObjectID object_id = ObjectID::FromBinary(request->object_id());

    LOG(DEBUG) << ": Received a pull request from " << request->puller_ip()
               << " for object " << object_id.ToString() << " offset=" request->offset();

    int status = object_sender_.send_object(request);
    if (status) {
      LOG(DEBUG) << ": Finished a pull request from " << request->puller_ip()
                << " for object " << object_id.ToString();
    } else {
      LOG(ERROR) << "Failed to send object " << object_id.ToString() << " to " << request->puller_ip();
    }
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
    store_.Reduce(object_ids, reduction_id, request->num_reduce_objects());
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status
  RemoteGetReducedObjects(grpc::ServerContext *context,
                          const RemoteGetReducedObjectsRequest *request,
                          RemoteGetReducedObjectsReply *reply) {
    TIMELINE("ObjectStoreServiceImpl::RemoteGetReducedObjects()");
    ObjectID reduction_id = ObjectID::FromBinary(request->reduction_id());
    std::unordered_set<ObjectID> reduced_objects =
        store_.GetReducedObjects(reduction_id);
    for (const auto &object_id : reduced_objects) {
      reply->add_object_ids(object_id.Binary());
    }
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
                                        const ObjectID &object_id,
                                        int64_t offset) {
  TIMELINE("DistributedObjectStore::PullObject");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  create_stub(remote_grpc_address);
  grpc::ClientContext context;
  PullRequest request;
  PullReply reply;
  request.set_object_id(object_id.Binary());
  request.set_puller_ip(my_address_);
  request.set_offset(offset);
  auto stub = get_stub(remote_grpc_address);
  auto status = stub->Pull(&context, request, &reply);
  return status.ok() && reply.ok();
}

bool DistributedObjectStore::InvokeReduceTo(
    const std::string &remote_address, const ObjectID &reduction_id,
    const std::vector<ObjectID> &dst_object_ids, const std::string &dst_address,
    bool is_endpoint, const ObjectID *src_object_id) {
  TIMELINE("GrpcServer::InvokeReduceTo");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  create_stub(remote_grpc_address);
  ReduceToRequest request;
  request.set_reduction_id(reduction_id.Binary());
  for (auto &object_id : dst_object_ids) {
    request.add_dst_object_ids(object_id.Binary());
  }
  request.set_dst_address(dst_address);
  request.set_is_endpoint(is_endpoint);
  if (src_object_id != nullptr) {
    request.set_src_object_id(src_object_id->Binary());
  }
  (void)pool_.push(
      [this](int id, std::string remote_grpc_address, ReduceToRequest request) {
        grpc::ClientContext context;
        ReduceToReply reply;
        auto stub = get_stub(remote_grpc_address);
        TIMELINE("GrpcServer::InvokeReduceTo[stub->ReduceTo]");
        auto status = stub->ReduceTo(&context, request, &reply);
        DCHECK(status.ok())
            << "[GrpcServer] ReduceTo failed at remote address:"
            << remote_grpc_address << ", message: " << status.error_message()
            << ", details = " << status.error_code();
        DCHECK(reply.ok());
      },
      std::move(remote_grpc_address), std::move(request));
  return true;
}

bool DistributedObjectStore::InvokeRedirectReduce(
    const std::string &remote_address, const std::vector<ObjectID> &object_ids,
    const ObjectID &reduction_id, ssize_t num_reduce_objects) {
  TIMELINE("GrpcServer::InvokeRedirectReduce");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  create_stub(remote_grpc_address);
  RedirectReduceRequest request;
  request.set_reduction_id(reduction_id.Binary());
  for (const auto &object_id : object_ids) {
    request.add_object_ids(object_id.Binary());
  }
  request.set_num_reduce_objects(num_reduce_objects);
  (void)pool_.push(
      [this](int id, std::string remote_grpc_address,
             RedirectReduceRequest request) {
        grpc::ClientContext context;
        RedirectReduceReply reply;
        auto stub = get_stub(remote_grpc_address);
        TIMELINE("GrpcServer::InvokeRedirectReduce[stub->RedirectReduce]");
        auto status = stub->RedirectReduce(&context, request, &reply);
        DCHECK(status.ok())
            << "[GrpcServer] ReduceTo failed at remote address:"
            << remote_grpc_address << ", message: " << status.error_message()
            << ", details = " << status.error_code();
        DCHECK(reply.ok());
      },
      std::move(remote_grpc_address), std::move(request));
  return true;
}

std::unordered_set<ObjectID> DistributedObjectStore::RemoteGetReducedObjects(
    const std::string &remote_address, const ObjectID &reduction_id) {
  TIMELINE("GrpcServer::RemoteGetReducedObjects");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  create_stub(remote_grpc_address);
  grpc::ClientContext context;
  RemoteGetReducedObjectsRequest request;
  RemoteGetReducedObjectsReply reply;
  request.set_reduction_id(reduction_id.Binary());
  auto stub = get_stub(remote_grpc_address);
  auto status = stub->RemoteGetReducedObjects(&context, request, &reply);
  std::unordered_set<ObjectID> reduced_objects;
  for (const auto &object_id_str : reply.object_ids()) {
    ObjectID object_id = ObjectID::FromBinary(object_id_str);
    reduced_objects.insert(object_id);
  }
  return reduced_objects;
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
      grpc_address_(my_address_ + ":" + std::to_string(grpc_port_)),
      pool_(HOPLITE_THREADPOOL_SIZE_FOR_RPC) {
  TIMELINE("DistributedObjectStore construction function");
  // Creating the first random ObjectID will initialize the random number
  // generator, which is pretty slow. So we generate one first, and it
  // will not surprise us later.
  (void)ObjectID::FromRandom();
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
  LOG(DEBUG) << "Object store has been shutdown.";
}

bool DistributedObjectStore::IsLocalObject(const ObjectID &object_id,
                                           int64_t *size) {
  if (local_store_client_.ObjectExists(object_id, false)) {
    if (size != nullptr) {
      ObjectBuffer object_buffer;
      local_store_client_.Get(object_id, &object_buffer);
      *size = object_buffer.data->Size();
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
  if (buffer->Size() <= inband_data_size_limit) {
    LOG(DEBUG) << "Put a small object, copy without streaming";
    ptr->CopyFrom(*buffer);
    local_store_client_.Seal(object_id);
    gcs_client_.WriteLocation(object_id, my_address_, true, buffer->Size(),
                              buffer->Data(),
                              /*blocking=*/HOPLITE_PUT_BLOCKING);
  } else {
    LOG(DEBUG) << "Put with streaming";
    gcs_client_.WriteLocation(object_id, my_address_, false, buffer->Size(),
                              buffer->Data(),
                              /*blocking=*/HOPLITE_PUT_BLOCKING);
    ptr->StreamCopy(*buffer);
    local_store_client_.Seal(object_id);
  }
}

ObjectID DistributedObjectStore::Put(const std::shared_ptr<Buffer> &buffer) {
  TIMELINE("DistributedObjectStore Put without object_id");
  // generate a random object id
  auto object_id = ObjectID::FromRandom();
  Put(buffer, object_id);
  return object_id;
}

void DistributedObjectStore::Reduce(const std::vector<ObjectID> &object_ids,
                                    ObjectID *created_reduction_id,
                                    ssize_t num_reduce_objects) {
  const auto reduction_id = ObjectID::FromRandom();
  *created_reduction_id = reduction_id;
  Reduce(object_ids, reduction_id, num_reduce_objects);
}

void DistributedObjectStore::Reduce(const std::vector<ObjectID> &object_ids,
                                    const ObjectID &reduction_id,
                                    ssize_t num_reduce_objects) {
  // TODO: support different reduce op and types.
  TIMELINE("DistributedObjectStore Async Reduce");
  DCHECK(object_ids.size() > 0);
  // starting a thread
  std::thread reduction_thread(&DistributedObjectStore::poll_and_reduce, this,
                               object_ids, reduction_id, num_reduce_objects);
  {
    std::lock_guard<std::mutex> l(reduction_tasks_mutex_);
    DCHECK(reduction_tasks_.find(reduction_id) == reduction_tasks_.end())
        << "Reduction task with " << reduction_id.ToString()
        << " already exists.";
    reduction_tasks_[reduction_id] = std::move(reduction_thread);
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
    LOG(DEBUG) << "Reduction task " << object_id.ToString() << " found.";
    // ==> This ObjectID belongs to a reduction task.
    // we must join the thread first, because the stream
    // pointer could still be nullptr at creation.
    search->second.join();
    local_store_client_.Wait(object_id);
    // wait until the object is fully reduced
    local_store_client_.Seal(object_id);
    // Location is written in 'object_writer.cc'. So we skip writing the
    // location here.
    l.lock();
    reduction_tasks_.erase(object_id);
    l.unlock();
  } else {
    l.unlock();
    LOG(DEBUG) << "Try to fetch " << object_id.ToString()
               << " from local store.";
    if (!local_store_client_.ObjectExists(object_id)) {
      LOG(DEBUG) << "Cannot find " << object_id.ToString()
                 << " in local store. Try to pull it from remote";
      // ==> This ObjectID refers to a remote object.
      SyncReply reply = gcs_client_.GetLocationSync(object_id, true);
      if (!check_and_store_inband_data(object_id, reply.object_size,
                                       reply.inband_data)) {
        // send pull request to one of the location
        // TODO(siyuan): implement fault-tolerant multicast.
        bool status = PullObject(reply.sender_ip, object_id, 0);
        DCHECK(status) << "Failed to pull object";
      }
    }
  }

  // get object from local store
  ObjectBuffer object_buffer;
  local_store_client_.Get(object_id, &object_buffer);
  *result = object_buffer.data;
}

std::unordered_set<ObjectID>
DistributedObjectStore::GetReducedObjects(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduced_objects_mutex_);
  reduced_objects_cv_.wait(l, [this, &reduction_id] {
    return reduced_objects_.find(reduction_id) != reduced_objects_.end();
  });
  return reduced_objects_[reduction_id];
}

std::unordered_set<ObjectID>
DistributedObjectStore::GetUnreducedObjects(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduced_objects_mutex_);
  reduced_objects_cv_.wait(l, [this, &reduction_id] {
    return unreduced_objects_.find(reduction_id) != unreduced_objects_.end();
  });
  return unreduced_objects_[reduction_id];
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
    const std::vector<ObjectID> object_ids, const ObjectID reduction_id,
    ssize_t num_reduce_objects) {
  TIMELINE("DistributedObjectStore Reduce Thread");

  // By default, reduce all the objects.
  if (num_reduce_objects < 0) {
    num_reduce_objects = object_ids.size();
  }

  // In this method, we will do until we get the size of the first object.
  std::vector<ObjectID> notification_candidates;
  std::vector<ObjectID> local_object_ids;

  // The object size for reduction. If it is less than 0,
  // it means that we haven't get the size.
  int64_t object_size = -1;

  // iterate over object ids to see if they are local objects
  for (const auto &object_id : object_ids) {
    if (IsLocalObject(object_id, &object_size)) {
      local_object_ids.push_back(object_id);
    } else {
      notification_candidates.push_back(object_id);
    }
  }

  ssize_t num_remote_reduce_objects = std::max(
      num_reduce_objects - (ssize_t)local_object_ids.size(), (ssize_t)0);

  std::shared_ptr<ObjectNotifications> notifications =
      gcs_client_.GetLocationAsync(notification_candidates,
                                   reduction_id.Binary(), false);
  std::vector<NotificationMessage> ready_ids;
  if (object_size < 0) {
    // we haven't got the object size yet, so we have to subscribe to the
    // notifications and get the message of the first completed object.
    ready_ids = notifications->GetNotifications(false);
    object_size = ready_ids[0].object_size;
  }

  // the buffer for reduction results
  std::shared_ptr<Buffer> buffer;
  // Since we have got the object size, we can create the endpoint buffer now.
  auto pstatus = local_store_client_.Create(reduction_id, object_size, &buffer);
  DCHECK(pstatus.ok()) << "Plasma failed to create reduction_id = "
                       << reduction_id.Hex() << " size = " << object_size
                       << ", status = " << pstatus.ToString();

  // Here we will calculate which reduction algorithm will be used.
  // P: number of nodes
  // L: latency (in second)
  // B: bandwidth (in bytes)
  // S: object size (in bytes)
  double L = HOPLITE_RPC_LATENCY;
  double B = HOPLITE_BANDWIDTH;
  double P = (double)num_remote_reduce_objects;
  double S = object_size;
  LOG(DEBUG) << "grid/pipe boundary object size = "
             << pow(sqrt(P) - 1, 2) * B * L;

  std::unordered_set<ObjectID> reduced_objects;
  if (sqrt(P) - 1 > sqrt(S / (B * L)) && P > 3.5) {
    // NOTE: for P = 16 (including one local node), S < 7.67 MB
    LOG(DEBUG) << "Grid reduce algorithm is used.";
    reduced_objects = poll_and_reduce_grid_impl(
        notifications, notification_candidates, local_object_ids, object_size,
        buffer, reduction_id, num_reduce_objects);
  } else {
    LOG(DEBUG) << "Pipe reduce algorithm is used.";
    reduced_objects = poll_and_reduce_pipe_impl(
        notifications, notification_candidates, local_object_ids, object_size,
        buffer, reduction_id, num_reduce_objects);
  }
  // Write down the unreduced objects.
  std::unordered_set<ObjectID> unreduced_objects;
  for (const auto &object_id : object_ids)
    if (reduced_objects.find(object_id) == reduced_objects.end()) {
      unreduced_objects.insert(object_id);
    }
  {
    std::lock_guard<std::mutex> lock(reduced_objects_mutex_);
    reduced_objects_.emplace(reduction_id, std::move(reduced_objects));
    unreduced_objects_.emplace(reduction_id, std::move(unreduced_objects));
    reduced_objects_cv_.notify_all();
  }
}

std::unordered_set<ObjectID> DistributedObjectStore::poll_and_reduce_pipe_impl(
    const std::shared_ptr<ObjectNotifications> &notifications,
    const std::vector<ObjectID> &notification_candidates,
    std::vector<ObjectID> &local_object_ids, const int64_t object_size,
    const std::shared_ptr<Buffer> &buffer, const ObjectID &reduction_id,
    ssize_t num_reduce_objects) {
  TIMELINE("DistributedObjectStore Reduce Thread Pipe");

  // stores the objects that we reduced in this round. Used as return value.
  std::unordered_set<ObjectID> reduced_objects;
  // states for enumerating the chain
  std::unordered_set<ObjectID> remaining_ids(notification_candidates.begin(),
                                             notification_candidates.end());

  int node_index = 0;
  ObjectID tail_objectid;
  std::string tail_address;
  if (local_object_ids.size() > num_reduce_objects) {
    local_object_ids =
        std::vector<ObjectID>(local_object_ids.begin(),
                              local_object_ids.begin() + num_reduce_objects);
  }
  ssize_t num_ready_objects = local_object_ids.size();

  // main loop for constructing the reduction chain.
  while (num_ready_objects < num_reduce_objects) {
    std::vector<NotificationMessage> ready_ids =
        notifications->GetNotifications(true);
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
      LOG(DEBUG) << "Received notification, address = " << address
                 << ", object_id = " << ready_id.ToString();

      if (address == my_address_) {
        // move local objects to another address, because there's no
        // necessary to transfer them through the network.
        local_object_ids.push_back(ready_id);
      } else {
        // wait until at lease 2 objects in nodes except the master node are
        // ready.
        if (node_index == 1) {
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
        reduced_objects.insert(ready_id);
        tail_objectid = ready_id;
        tail_address = address;
        node_index++;
      }
      // mark it as done
      remaining_ids.erase(ready_id);
      if (++num_ready_objects >= num_reduce_objects) {
        break;
      }
    }
  }

  for (const auto &object_id : local_object_ids) {
    reduced_objects.insert(object_id);
  }

  // send the reduced object back to the master node.
  bool reply_ok = false;
  if (node_index == 0) {
    // all the objects are local, we just reduce them locally
    LOG(DEBUG) << "All the objects to be reduced are local";
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
  DCHECK(reduced_objects.size() == num_reduce_objects);
  return reduced_objects;
}

std::unordered_set<ObjectID> DistributedObjectStore::poll_and_reduce_grid_impl(
    const std::shared_ptr<ObjectNotifications> &notifications,
    const std::vector<ObjectID> &notification_candidates,
    std::vector<ObjectID> &local_object_ids, const int64_t object_size,
    const std::shared_ptr<Buffer> &buffer, const ObjectID &reduction_id,
    ssize_t num_reduce_objects) {
  TIMELINE("DistributedObjectStore Reduce Thread Grid");
  // we do not use reference for its parameters because it will be executed
  // in a thread.

  if (local_object_ids.size() > num_reduce_objects) {
    local_object_ids =
        std::vector<ObjectID>(local_object_ids.begin(),
                              local_object_ids.begin() + num_reduce_objects);
  }
  ssize_t num_ready_objects = local_object_ids.size();

  int rows = round(sqrt(num_reduce_objects - num_ready_objects));
  LOG(DEBUG) << "number of rows: " << rows;
  DCHECK(rows > 0) << "number of rows should be greater than 0";

  std::vector<std::pair<std::string, ObjectID>> lines;

  // states for enumerating the chain
  std::unordered_set<ObjectID> remaining_ids(notification_candidates.begin(),
                                             notification_candidates.end());

  // main loop for constructing the reduction chain.
  while (num_ready_objects < num_reduce_objects) {
    std::vector<NotificationMessage> ready_ids =
        notifications->GetNotifications(true);
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
      LOG(DEBUG) << "Received notification, address = " << address
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
      if (++num_ready_objects >= num_reduce_objects || lines.size() >= rows) {
        break;
      }
    }
    if (lines.size() >= rows) {
      // TODO: unsubscribe objects
      break;
    }
  }

  ssize_t remaining_num_objects_to_reduce =
      num_reduce_objects - num_ready_objects;
  if (lines.size() < rows || remaining_num_objects_to_reduce < rows) {
    // This is a quite unexpected pathway. This is caused by too many
    // objects were discovered to be local objects.
    LOG(WARNING) << "too many objects are found local for grid reduction";
    // restore previous notifications
    notifications->Rewind();
    size_t n_records_erased =
        notifications->EraseRecords(std::unordered_set<ObjectID>(
            local_object_ids.begin(), local_object_ids.end()));
    LOG(DEBUG) << "Notification messages: " << n_records_erased
               << " records removed";
    auto ready_messages = notifications->GetNotifications(false, true);
    std::vector<ObjectID> remaining_candidates;
    for (const auto &msg : ready_messages) {
      remaining_candidates.push_back(msg.object_id);
    }
    return poll_and_reduce_pipe_impl(notifications, remaining_candidates,
                                     local_object_ids, object_size, buffer,
                                     reduction_id, num_reduce_objects);
  }

  std::vector<ObjectID> edge;
  ssize_t remaining_size = remaining_ids.size();
  std::vector<ObjectID> remaining_ids_list(remaining_ids.begin(),
                                           remaining_ids.end());
  int processed_count = 0;
  for (int i = 0; i < rows; i++) {
    // put the master node of each chain in the first place.
    std::vector<ObjectID> redirect_object_ids{lines[i].second};
    // distributing objects into each chain as evenly as possible
    ssize_t share_count = (remaining_size / rows) + (i < remaining_size % rows);
    ssize_t num_reduce_objects_chain =
        (remaining_num_objects_to_reduce / rows) +
        (i < remaining_num_objects_to_reduce % rows) + 1;
    for (int j = 0; j < share_count; j++, processed_count++) {
      redirect_object_ids.push_back(remaining_ids_list[processed_count]);
    }
    auto line_reduction_id = ObjectID::FromRandom();
    edge.push_back(line_reduction_id);
    InvokeRedirectReduce(lines[i].first, redirect_object_ids, line_reduction_id,
                         num_reduce_objects_chain);
  }
  ObjectID _monk_subscription_id = ObjectID::FromRandom();
  std::shared_ptr<ObjectNotifications> new_notifications =
      gcs_client_.GetLocationAsync(edge, _monk_subscription_id.Binary(), false);
  // stores the objects that we reduced in this round. Used as return value.
  std::unordered_set<ObjectID> reduced_objects;
  for (const auto &object_id : local_object_ids) {
    reduced_objects.insert(object_id);
  }
  poll_and_reduce_pipe_impl(new_notifications, edge, local_object_ids,
                            object_size, buffer, reduction_id,
                            edge.size() + local_object_ids.size());
  // FIXME(zhuohan): Get the actual reduced objects on remote nodes and return
  // them back.
  for (int i = 0; i < rows; i++) {
    std::unordered_set<ObjectID> reduced_objects_row =
        RemoteGetReducedObjects(lines[i].first, edge[i]);
    reduced_objects.insert(reduced_objects_row.begin(),
                           reduced_objects_row.end());
  }

  DCHECK(reduced_objects.size() == num_reduce_objects);
  return reduced_objects;
}

void DistributedObjectStore::worker_loop() {
  LOG(DEBUG) << "[GprcServer] grpc server " << my_address_ << " started";
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
