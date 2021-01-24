#ifndef OBJECT_STORE_STATE_H
#define OBJECT_STORE_STATE_H

#include <atomic>
#include <condition_variable>
#include <mutex>

#include <unordered_map>
#include <vector>

#include "common/buffer.h"
#include "common/id.h"

class LocalReduceTask {
public:
  LocalReduceTask() {}

  ObjectID local_object;

  void Wait() {
    std::unique_lock<std::mutex> l(notification_mutex_);
    notification_cv_.wait(l, [this]() { return is_finished_; });
  }

  void NotifyFinished() {
    std::unique_lock<std::mutex> l(notification_mutex_);
    is_finished_ = true;
    notification_cv_.notify_all();
  }

private:
  std::atomic<bool> is_finished_ = false;
  std::mutex notification_mutex_;
  std::condition_variable notification_cv_;
};

class ObjectStoreState {

public:
  std::shared_ptr<Buffer> create_reduction_stream(const ObjectID &reduction_id, size_t size);

  std::shared_ptr<Buffer> get_reduction_stream(const ObjectID &reduction_id);

  std::shared_ptr<Buffer> get_or_create_reduction_stream(const ObjectID &reduction_id, size_t size);

  void release_reduction_stream(const ObjectID &reduction_id);

  void create_local_reduce_task(const ObjectID &reduction_id, const std::vector<ObjectID> &local_objects);

  std::shared_ptr<LocalReduceTask> get_local_reduce_task(const ObjectID &reduction_id);

  void remove_local_reduce_task(const ObjectID &reduction_id);

  bool local_reduce_task_exists(const ObjectID &reduction_id);

private:
  std::mutex reduction_stream_mutex_;
  std::condition_variable reduction_stream_cv_;
  std::unordered_map<ObjectID, std::shared_ptr<Buffer>> reduction_stream_;

  std::mutex reduce_tasks_mutex_;
  std::unordered_map<ObjectID, std::shared_ptr<LocalReduceTask>> reduce_tasks_;
};

#endif // OBJECT_STORE_STATE_H
