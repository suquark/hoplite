#include "object_store_state.h"
#include "logging.h"

std::shared_ptr<ReductionStream>
ObjectStoreState::create_reduction_stream(const ObjectID &reduction_id,
                                          size_t size) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  DCHECK(reduction_stream_.find(reduction_id) == reduction_stream_.end());
  auto stream = std::make_shared<ReductionStream>(size);
  reduction_stream_[reduction_id] = stream;
  l.unlock();
  reduction_stream_cv_.notify_all();
  return stream;
}

std::shared_ptr<ReductionStream>
ObjectStoreState::get_reduction_stream(const ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  reduction_stream_cv_.wait(l, [this, &reduction_id]() {
    return reduction_stream_.find(reduction_id) != reduction_stream_.end();
  });
  return reduction_stream_[reduction_id];
}

std::shared_ptr<ProgressiveStream> ObjectStoreState::create_progressive_stream(
    const ObjectID &reduction_id, const std::shared_ptr<Buffer> &buffer) {
  while (progressive_stream_lock_.test_and_set(std::memory_order_acquire))
    ; // spin
  DCHECK(progressive_stream_.find(reduction_id) == progressive_stream_.end());
  progressive_stream_[reduction_id] =
      std::make_shared<ProgressiveStream>(buffer);
  auto stream = progressive_stream_[reduction_id];
  progressive_stream_lock_.clear(std::memory_order_release); // release lock
  return stream;
}

std::shared_ptr<ProgressiveStream>
ObjectStoreState::get_progressive_stream(const ObjectID &reduction_id) {
  while (progressive_stream_lock_.test_and_set(std::memory_order_acquire))
    ; // spin
  std::shared_ptr<ProgressiveStream> stream;
  if (progressive_stream_.find(reduction_id) == progressive_stream_.end()) {
    LOG(DEBUG) << "reduction endpoint id = " << reduction_id.Hex()
               << " not found";
  } else {
    stream = progressive_stream_[reduction_id];
  }
  progressive_stream_lock_.clear(std::memory_order_release); // release lock
  return stream;
}
