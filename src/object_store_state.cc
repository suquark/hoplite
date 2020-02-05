#include "object_store_state.h"
#include "logging.h"
#include <cstring>

std::shared_ptr<Buffer>
ObjectStoreState::create_reduction_stream(const ObjectID &reduction_id,
                                          size_t size) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  DCHECK(reduction_stream_.find(reduction_id) == reduction_stream_.end());
  auto stream = std::make_shared<Buffer>(size);
  reduction_stream_[reduction_id] = stream;
  l.unlock();
  reduction_stream_cv_.notify_all();
  return stream;
}

std::shared_ptr<Buffer>
ObjectStoreState::get_reduction_stream(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  reduction_stream_cv_.wait(l, [this, &reduction_id]() {
    return reduction_stream_.find(reduction_id) != reduction_stream_.end();
  });
  return reduction_stream_[reduction_id];
}

void ObjectStoreState::release_reduction_stream(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  // release the memory
  reduction_stream_.erase(reduction_id);
}
