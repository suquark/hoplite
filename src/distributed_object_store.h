#ifndef DISTRIBUTED_OBJECT_STORE_H
#define DISTRIBUTED_OBJECT_STORE_H

#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/buffer.h"
#include "common/id.h"

#include "global_control_store.h"
#include "local_store_client.h"
#include "object_control.h"
#include "object_sender.h"
#include "object_store_state.h"
#include "object_writer.h"

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

  inline void join_tasks() {
    object_sender_thread_.join();
    object_control_thread_.join();
    notification_thread_.join();
  }

private:
  void poll_and_reduce(const std::vector<ObjectID> object_ids,
                       const ObjectID reduction_id);

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
      ObjectBuffer object_buffer;
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
  }

  // order of fields should be kept for proper initialization order
  std::string my_address_;
  std::string redis_address_;
  ObjectStoreState state_;
  GlobalControlStoreClient gcs_client_;
  LocalStoreClient local_store_client_;
  TCPServer object_writer_;
  ObjectSender object_sender_;
  GrpcServer object_control_;
  // A map for currently working reduction tasks.
  std::mutex reduction_tasks_mutex_;
  struct reduction_task {
    std::shared_ptr<ProgressiveStream> stream;
    std::thread reduction_thread;
  };
  std::unordered_map<ObjectID, reduction_task> reduction_tasks_;
  std::thread object_writer_thread_;
  std::thread object_sender_thread_;
  std::thread object_control_thread_;
  std::thread notification_thread_;
};

#endif // DISTRIBUTED_OBJECT_STORE_H
