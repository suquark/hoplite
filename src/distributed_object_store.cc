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
using objectstore::RemoteGetReducedObjectsReply;
using objectstore::RemoteGetReducedObjectsRequest;

////////////////////////////////////////////////////////////////
// The gRPC server side of the object store
////////////////////////////////////////////////////////////////

class ObjectStoreServiceImpl final : public ObjectStore::Service {
public:
  ObjectStoreServiceImpl(ObjectSender &object_sender, DistributedObjectStore &store)
      : ObjectStore::Service(), object_sender_(object_sender), store_(store) {}
  grpc::Status RemoteGetReducedObjects(grpc::ServerContext *context, const RemoteGetReducedObjectsRequest *request,
                                       RemoteGetReducedObjectsReply *reply) {
    TIMELINE("ObjectStoreServiceImpl::RemoteGetReducedObjects()");
    ObjectID reduction_id = ObjectID::FromBinary(request->reduction_id());
    std::unordered_set<ObjectID> reduced_objects = store_.GetReducedObjects(reduction_id);
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

std::unordered_set<ObjectID> DistributedObjectStore::RemoteGetReducedObjects(const std::string &remote_address,
                                                                             const ObjectID &reduction_id) {
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

DistributedObjectStore::DistributedObjectStore(const std::string &notification_server_address, int redis_port,
                                               int notification_server_port, int notification_listen_port,
                                               const std::string &plasma_socket, const std::string &my_address,
                                               int object_writer_port, int grpc_port)
    : my_address_(my_address), gcs_client_{notification_server_address, my_address_, notification_server_port},
      local_store_client_{false, plasma_socket}, object_writer_{state_, gcs_client_, local_store_client_, my_address_,
                                                                object_writer_port},
      object_sender_{state_, gcs_client_, local_store_client_, my_address_}, receiver_{state_, gcs_client_,
                                                                                       local_store_client_, my_address_,
                                                                                       object_writer_port},
      notification_listener_(my_address_, notification_listen_port, receiver_), grpc_port_(grpc_port),
      grpc_address_(my_address_ + ":" + std::to_string(grpc_port_)), pool_(HOPLITE_THREADPOOL_SIZE_FOR_RPC) {
  TIMELINE("DistributedObjectStore construction function");
  // Creating the first random ObjectID will initialize the random number
  // generator, which is pretty slow. So we generate one first, and it
  // will not surprise us later.
  (void)ObjectID::FromRandom();
  // create a thread to send object
  object_sender_.Run();

  // initialize the object store
  service_.reset(new ObjectStoreServiceImpl(object_sender_, *this));
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address_, grpc::InsecureServerCredentials());
  builder.RegisterService(service_.get());
  grpc_server_ = builder.BuildAndStart();
  object_control_thread_ = std::thread(&DistributedObjectStore::worker_loop, this);
  notification_listener_.Run();

  gcs_client_.ConnectNotificationServer();
}

DistributedObjectStore::~DistributedObjectStore() {
  TIMELINE("~DistributedObjectStore");
  object_sender_.Shutdown();
  Shutdown();
  notification_listener_.Shutdown();
  gcs_client_.Shutdown();
  LOG(DEBUG) << "Object store has been shutdown.";
}

bool DistributedObjectStore::IsLocalObject(const ObjectID &object_id, int64_t *size) {
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

void DistributedObjectStore::Put(const std::shared_ptr<Buffer> &buffer, const ObjectID &object_id) {
  TIMELINE(std::string("DistributedObjectStore Put single object ") + object_id.Hex());
  // put object into Plasma
  std::shared_ptr<Buffer> ptr;
  auto pstatus = local_store_client_.Create(object_id, buffer->Size(), &ptr);
  DCHECK(pstatus.ok()) << "Plasma failed to create object_id = " << object_id.Hex() << " size = " << buffer->Size()
                       << ", status = " << pstatus.ToString();
  if (buffer->Size() <= inband_data_size_limit) {
    LOG(DEBUG) << "Put a small object, copy without streaming";
    ptr->CopyFrom(*buffer);
    local_store_client_.Seal(object_id);
    gcs_client_.WriteLocation(object_id, my_address_, true, buffer->Size(), buffer->Data(),
                              /*blocking=*/HOPLITE_PUT_BLOCKING);
  } else {
    LOG(DEBUG) << "Put with streaming";
    gcs_client_.WriteLocation(object_id, my_address_, false, buffer->Size(), buffer->Data(),
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

void DistributedObjectStore::Reduce(const std::vector<ObjectID> &object_ids, ObjectID *created_reduction_id,
                                    ssize_t num_reduce_objects) {
  const auto reduction_id = ObjectID::FromRandom();
  *created_reduction_id = reduction_id;
  Reduce(object_ids, reduction_id, num_reduce_objects);
}

void DistributedObjectStore::Reduce(const std::vector<ObjectID> &object_ids, const ObjectID &reduction_id,
                                    ssize_t num_reduce_objects) {
  // TODO: support different reduce op and types.
  TIMELINE("DistributedObjectStore Async Reduce");
  DCHECK(object_ids.size() > 0);

  // only include remote objects
  std::vector<ObjectID> objects_to_reduce;
  std::vector<ObjectID> local_objects;

  for (auto &object_id : object_ids) {
    if (local_store_client_.ObjectExists(object_id, false)) {
      local_objects.push_back(object_id);
    } else {
      objects_to_reduce.push_back(object_id);
    }
  }
  DCHECK(local_objects.size() <= 1);

  auto status = gcs_client_.CreateReduceTask(objects_to_reduce, reduction_id, num_reduce_objects);
  DCHECK(status.ok());

  state_.create_local_reduce_task(reduction_id, local_objects);
  // this is not necessary, but we can create the reduction object ahead of time
  if (local_objects.size() > 0) {
    int64_t size = local_store_client_.GetBufferNoExcept(local_objects[0])->Size();
    std::shared_ptr<Buffer> r;
    auto status = local_store_client_.Create(reduction_id, size, &r);
    DCHECK(status.ok());
  }
}

void DistributedObjectStore::Get(const ObjectID &object_id, std::shared_ptr<Buffer> *result) {
  TIMELINE(std::string("DistributedObjectStore Get single object ") + object_id.ToString());
  // FIXME: currently the object store will assume that the object
  // exists even before 'Seal' is called. This will cause the problem
  // that an on-going reduction task could be skipped. Here we just
  // reorder the checking process as a workaround.
  if (state_.local_reduce_task_exists(object_id)) {
    // ==> This ObjectID belongs to a reduction task.
    LOG(DEBUG) << "Reduction task " << object_id.ToString() << " found.";
    auto task = state_.get_local_reduce_task(object_id);
    // wait until the object is fully reduced
    task->Wait();
    state_.remove_local_reduce_task(object_id);
    // seal the object
    local_store_client_.Seal(object_id);
    // Location is written in 'object_writer.cc'. So we skip writing the
    // location here.
  } else {
    LOG(DEBUG) << "Try to fetch " << object_id.ToString() << " from local store.";
    if (!local_store_client_.ObjectExists(object_id)) {
      LOG(DEBUG) << "Cannot find " << object_id.ToString() << " in local store. Try to pull it from remote";
      receiver_.pull_object(object_id);
    }
  }

  // get object from local store
  ObjectBuffer object_buffer;
  local_store_client_.Get(object_id, &object_buffer);
  *result = object_buffer.data;
}

std::unordered_set<ObjectID> DistributedObjectStore::GetReducedObjects(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduced_objects_mutex_);
  reduced_objects_cv_.wait(
      l, [this, &reduction_id] { return reduced_objects_.find(reduction_id) != reduced_objects_.end(); });
  return reduced_objects_[reduction_id];
}

std::unordered_set<ObjectID> DistributedObjectStore::GetUnreducedObjects(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduced_objects_mutex_);
  reduced_objects_cv_.wait(
      l, [this, &reduction_id] { return unreduced_objects_.find(reduction_id) != unreduced_objects_.end(); });
  return unreduced_objects_[reduction_id];
}

////////////////////////////////////////////////////////////////
// The object store internal functions
////////////////////////////////////////////////////////////////

void DistributedObjectStore::worker_loop() {
  LOG(DEBUG) << "[GprcServer] grpc server " << my_address_ << " started";
  grpc_server_->Wait();
}

objectstore::ObjectStore::Stub *DistributedObjectStore::get_stub(const std::string &remote_grpc_address) {
  std::lock_guard<std::mutex> lock(grpc_stub_map_mutex_);
  return object_store_stub_pool_[remote_grpc_address].get();
}

void DistributedObjectStore::create_stub(const std::string &remote_grpc_address) {
  std::lock_guard<std::mutex> lock(grpc_stub_map_mutex_);
  if (channel_pool_.find(remote_grpc_address) == channel_pool_.end()) {
    channel_pool_[remote_grpc_address] = grpc::CreateChannel(remote_grpc_address, grpc::InsecureChannelCredentials());
  }
  if (object_store_stub_pool_.find(remote_grpc_address) == object_store_stub_pool_.end()) {
    object_store_stub_pool_[remote_grpc_address] = ObjectStore::NewStub(channel_pool_[remote_grpc_address]);
  }
}
