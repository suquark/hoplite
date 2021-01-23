#ifndef OBJECT_STORE_STATE_H
#define OBJECT_STORE_STATE_H

#include <atomic>
#include <condition_variable>
#include <mutex>

#include <unordered_map>
#include <vector>

#include "common/buffer.h"
#include "common/id.h"

class ReduceTaskRoot {};

class ReduceTaskMiddlePoint {
public:
  int left_child_recv_fd = -1;
  int right_child_recv_fd = -1;
  std::thread left_recv_thread_;
  std::thread right_recv_thread_;
};

class ClientReduceManager {};

class ObjectStoreState {

public:
  std::shared_ptr<Buffer> create_reduction_stream(const ObjectID &reduction_id, size_t size);

  std::shared_ptr<Buffer> get_reduction_stream(const ObjectID &reduction_id);

  std::shared_ptr<Buffer> get_or_create_reduction_stream(const ObjectID &reduction_id, size_t size);

  void release_reduction_stream(const ObjectID &reduction_id);

private:
  std::mutex reduction_stream_mutex_;
  std::condition_variable reduction_stream_cv_;
  std::unordered_map<ObjectID, std::shared_ptr<Buffer>> reduction_stream_;
};

#endif // OBJECT_STORE_STATE_H
