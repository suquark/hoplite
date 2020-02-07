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
#include "object_sender.h"
#include "object_store_state.h"
#include "object_writer.h"

class ObjectStoreServiceImpl;

class DistributedObjectStore {
public:
  DistributedObjectStore(const std::string &notification_server_address,
                         int redis_port, int notification_server_port,
                         int notification_listen_port,
                         const std::string &plasma_socket,
                         const std::string &my_address, int object_writer_port,
                         int grpc_port);

  ~DistributedObjectStore();

  void Put(const std::shared_ptr<Buffer> &buffer, const ObjectID &object_id);

  ObjectID Put(const std::shared_ptr<Buffer> &buffer);

  void Reduce(const std::vector<ObjectID> &object_ids,
              ObjectID *created_reduction_id);

  void Reduce(const std::vector<ObjectID> &object_ids,
              const ObjectID &reduction_id);

  void Get(const ObjectID &object_id, std::shared_ptr<Buffer> *result);

  bool IsLocalObject(const ObjectID &object_id, int64_t *size);

  inline void join_tasks() {
    object_sender_thread_.join();
    object_control_thread_.join();
    notification_thread_.join();
  }

private:
  void worker_loop();

  // we do not use reference for its parameters because it will be executed
  // in a thread.
  void poll_and_reduce(const std::vector<ObjectID> object_ids,
                       const ObjectID reduction_id);

  void poll_and_reduce_pipe_impl(
      const std::shared_ptr<ObjectNotifications> &notifications,
      const std::vector<ObjectID> &notification_candidates,
      std::vector<ObjectID> &local_object_ids, const int64_t object_size,
      const std::shared_ptr<Buffer> &buffer, const ObjectID &reduction_id);

  void poll_and_reduce_grid_impl(
      const std::shared_ptr<ObjectNotifications> &notifications,
      const std::vector<ObjectID> &notification_candidates,
      std::vector<ObjectID> &local_object_ids, const int64_t object_size,
      const std::shared_ptr<Buffer> &buffer, const ObjectID &reduction_id);

  bool check_and_store_inband_data(const ObjectID &object_id,
                                   int64_t object_size,
                                   const std::string &inband_data);

  template <typename T>
  void reduce_local_objects(const std::vector<ObjectID> &object_ids,
                            Buffer *output) {
    DCHECK(output->Size() % sizeof(T) == 0)
        << "Buffer size cannot be divide whole by the element size";
    auto num_elements = output->Size() / sizeof(T);
    T *target = (T *)output->MutableData();
    bool first = true;
    // TODO: implement parallel reducing
    for (const auto &object_id : object_ids) {
      // TODO: those object_ids could also be local streams.
      ObjectBuffer object_buffer;
      DCHECK(local_store_client_.ObjectExists(object_id))
          << "ObjectID not in local store";
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
  TCPServer object_writer_;
  ObjectSender object_sender_;

  ////////////////////////////////////////////////////////////////////////////////
  // Object Control
  ////////////////////////////////////////////////////////////////////////////////
  bool PullObject(const std::string &remote_grpc_address,
                  const ObjectID &object_id);

  bool InvokeReduceTo(const std::string &remote_address,
                      const ObjectID &reduction_id,
                      const std::vector<ObjectID> &dst_object_ids,
                      const std::string &dst_address, bool is_endpoint,
                      const ObjectID *src_object_id = nullptr);

  bool InvokeRedirectReduce(const std::string &remote_address,
                            const std::vector<ObjectID> &object_ids,
                            const ObjectID &reduction_id);

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
  std::unordered_map<std::string,
                     std::unique_ptr<objectstore::ObjectStore::Stub>>
      object_store_stub_pool_;
  std::mutex grpc_stub_map_mutex_;
  objectstore::ObjectStore::Stub *
  get_stub(const std::string &remote_grpc_address);
  void create_stub(const std::string &remote_grpc_address);
  // the thread running the gRPC service
  std::thread object_control_thread_;

  ////////////////////////////////////////////////////////////////////////////////
  // Own data fields of the object store
  ////////////////////////////////////////////////////////////////////////////////

  // A map for currently working reduction tasks.
  std::mutex reduction_tasks_mutex_;
  std::unordered_map<ObjectID, std::thread> reduction_tasks_;
  std::thread object_writer_thread_;
  std::thread object_sender_thread_;
  std::thread notification_thread_;
};

#endif // DISTRIBUTED_OBJECT_STORE_H
