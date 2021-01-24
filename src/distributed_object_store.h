#ifndef DISTRIBUTED_OBJECT_STORE_H
#define DISTRIBUTED_OBJECT_STORE_H

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
// gRPC headers
#include "object_store.grpc.pb.h"
#include <grpcpp/server.h>
// common headers
#include "common/buffer.h"
#include "common/id.h"
// components headers
#include "global_control_store.h"
#include "local_store_client.h"
#include "notification_listener.h"
#include "object_sender.h"
#include "object_store_state.h"
#include "receiver.h"
#include "util/ctpl_stl.h"

class ObjectStoreServiceImpl;

class DistributedObjectStore {
public:
  DistributedObjectStore(const std::string &notification_server_address, int redis_port, int notification_server_port,
                         int notification_listen_port, const std::string &plasma_socket, const std::string &my_address,
                         int object_writer_port, int grpc_port);

  ~DistributedObjectStore();

  void Put(const std::shared_ptr<Buffer> &buffer, const ObjectID &object_id);

  ObjectID Put(const std::shared_ptr<Buffer> &buffer);

  void Reduce(const std::vector<ObjectID> &object_ids, ObjectID *created_reduction_id, ssize_t num_reduce_objects = -1);

  void Reduce(const std::vector<ObjectID> &object_ids, const ObjectID &reduction_id, ssize_t num_reduce_objects = -1);

  void Get(const ObjectID &object_id, std::shared_ptr<Buffer> *result);

  bool IsLocalObject(const ObjectID &object_id, int64_t *size);

  std::unordered_set<ObjectID> GetReducedObjects(const ObjectID &reduction_id);

  std::unordered_set<ObjectID> GetUnreducedObjects(const ObjectID &reduction_id);

  void join_tasks() {
    object_sender_thread_.join();
    object_control_thread_.join();
  }

private:
  void worker_loop();

  template <typename T> void reduce_local_objects(const std::vector<ObjectID> &object_ids, Buffer *output) {
    DCHECK(output->Size() % sizeof(T) == 0) << "Buffer size cannot be divide whole by the element size";
    auto num_elements = output->Size() / sizeof(T);
    T *target = (T *)output->MutableData();
    bool first = true;
    // TODO: implement parallel reducing
    for (const auto &object_id : object_ids) {
      // TODO: those object_ids could also be local streams.
      ObjectBuffer object_buffer;
      DCHECK(local_store_client_.ObjectExists(object_id)) << "ObjectID not in local store";
      local_store_client_.Get(object_id, &object_buffer);
      std::shared_ptr<Buffer> buf = object_buffer.data;
      const T *data_ptr = (const T *)buf->Data();
      if (!first) {
        for (int64_t i = 0; i < num_elements; i++)
          target[i] += data_ptr[i];
      } else {
        for (int64_t i = 0; i < num_elements; i++)
          target[i] = data_ptr[i];
        first = false;
      }
    }
    // TODO: try to pipeline this
    output->progress = output->Size();
  }

  // order of fields should be kept for proper initialization order
  std::string my_address_;
  std::string redis_address_;
  ObjectStoreState state_;
  GlobalControlStoreClient gcs_client_;
  LocalStoreClient local_store_client_;
  ObjectSender object_sender_;
  Receiver receiver_;
  NotificationListener notification_listener_;

  ////////////////////////////////////////////////////////////////////////////////
  // Object Control
  ////////////////////////////////////////////////////////////////////////////////

  // Helper function for getting reduced objects from a remote node. This is
  // used by the grid implementation.
  std::unordered_set<ObjectID> RemoteGetReducedObjects(const std::string &remote_address, const ObjectID &reduction_id);

  void Shutdown() {
    grpc_server_->Shutdown();
    object_control_thread_.join();
  }

  // port for the gRPC service of the object store
  const int grpc_port_;
  // the IP adddress of the gRPC server including the port number
  std::string grpc_address_;
  std::unique_ptr<grpc::Server> grpc_server_;
  std::unique_ptr<ObjectStoreServiceImpl> service_;
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_pool_;
  std::unordered_map<std::string, std::unique_ptr<objectstore::ObjectStore::Stub>> object_store_stub_pool_;
  std::mutex grpc_stub_map_mutex_;
  objectstore::ObjectStore::Stub *get_stub(const std::string &remote_grpc_address);
  void create_stub(const std::string &remote_grpc_address);
  // the thread running the gRPC service
  std::thread object_control_thread_;
  // a thread pool for submitting gRPC calls
  ctpl::thread_pool pool_;

  ////////////////////////////////////////////////////////////////////////////////
  // Own data fields of the object store
  ////////////////////////////////////////////////////////////////////////////////

  // A map for currently working reduction tasks.
  std::mutex reduced_objects_mutex_;
  std::condition_variable reduced_objects_cv_;
  std::unordered_map<ObjectID, std::unordered_set<ObjectID>> reduced_objects_;
  std::unordered_map<ObjectID, std::unordered_set<ObjectID>> unreduced_objects_;
};

#endif // DISTRIBUTED_OBJECT_STORE_H
