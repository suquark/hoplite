#include "object_store_state.h"
#include "util/logging.h"
#include <cstring>

std::shared_ptr<Buffer> ObjectStoreState::create_reduction_stream(const ObjectID &reduction_id, size_t size) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  DCHECK(reduction_stream_.find(reduction_id) == reduction_stream_.end());
  auto stream = std::make_shared<Buffer>(size);
  reduction_stream_[reduction_id] = stream;
  l.unlock();
  reduction_stream_cv_.notify_all();
  return stream;
}

std::shared_ptr<Buffer> ObjectStoreState::get_reduction_stream(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  reduction_stream_cv_.wait(
      l, [this, &reduction_id]() { return reduction_stream_.find(reduction_id) != reduction_stream_.end(); });
  return reduction_stream_[reduction_id];
}

std::shared_ptr<Buffer> ObjectStoreState::get_or_create_reduction_stream(const ObjectID &reduction_id, size_t size) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  auto search = reduction_stream_.find(reduction_id);
  if (search == reduction_stream_.end()) {
    auto stream = std::make_shared<Buffer>(size);
    reduction_stream_[reduction_id] = stream;
    l.unlock();
    reduction_stream_cv_.notify_all();
    return stream;
  } else {
    return search->second;
  }
}

void ObjectStoreState::release_reduction_stream(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  // release the memory
  reduction_stream_.erase(reduction_id);
}

void ObjectStoreState::create_local_reduce_task(const ObjectID &reduction_id,
                                                const std::vector<ObjectID> &local_objects) {
  DCHECK(local_objects.size() <= 1);
  auto t = std::make_shared<LocalReduceTask>();
  if (local_objects.size() > 0) {
    t->local_object = local_objects[0];
  }
  {
    std::lock_guard<std::mutex> lock(reduce_tasks_mutex_);
    reduce_tasks_[reduction_id] = t;
  }
}

std::shared_ptr<LocalReduceTask> ObjectStoreState::get_local_reduce_task(const ObjectID &reduction_id) {
  std::lock_guard<std::mutex> lock(reduce_tasks_mutex_);
  DCHECK(reduce_tasks_.count(reduction_id));
  return reduce_tasks_[reduction_id];
}

void ObjectStoreState::remove_local_reduce_task(const ObjectID &reduction_id) {
  std::lock_guard<std::mutex> lock(reduce_tasks_mutex_);
  DCHECK(reduce_tasks_.count(reduction_id));
  reduce_tasks_.erase(reduction_id);
}

bool ObjectStoreState::local_reduce_task_exists(const ObjectID &reduction_id) {
  std::lock_guard<std::mutex> lock(reduce_tasks_mutex_);
  return reduce_tasks_.count(reduction_id) > 0;
}
