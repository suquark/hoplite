#ifndef DISTRIBUTED_OBJECT_STORE_H
#define DISTRIBUTED_OBJECT_STORE_H

#include <cstdint>
#include <ctime>
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
  DistributedObjectStore(const std::string &redis_address, int redis_port,
                         int notification_port, int notification_listening_port,
                         const std::string &plasma_socket,
                         const std::string &my_address, int object_writer_port,
                         int grpc_port);

  void Put(const std::shared_ptr<Buffer> &buffer, const ObjectID &object_id);

  ObjectID Put(const std::shared_ptr<Buffer> &buffer);

  void Get(const std::vector<ObjectID> &object_ids, size_t _expected_size,
           ObjectID *created_reduction_id, std::shared_ptr<Buffer> *result);

  void Get(const std::vector<ObjectID> &object_ids, size_t _expected_size,
           const ObjectID &reduction_id, std::shared_ptr<Buffer> *result);

  void Get(const ObjectID &object_id, std::shared_ptr<Buffer> *result);

  inline void join_tasks() {
    object_writer_thread_.join();
    object_sender_thread_.join();
    object_control_thread_.join();
  }

private:
  const std::string my_address_;
  ObjectStoreState state_;
  GlobalControlStoreClient gcs_client_;
  LocalStoreClient local_store_client_;
  TCPServer object_writer_;
  ObjectSender object_sender_;
  GrpcServer object_control_;
  std::thread object_writer_thread_;
  std::thread object_sender_thread_;
  std::thread object_control_thread_;
  std::thread notification_thread_;
};

#endif // DISTRIBUTED_OBJECT_STORE_H
