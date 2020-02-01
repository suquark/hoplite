#include "object_store_state.h"
#include "logging.h"
#include <cstring>

void ProgressiveStream::stream_copy(const std::shared_ptr<Buffer> &src) {
  const uint8_t *data = src->Data();
  int64_t size = src->Size();
  DCHECK(size == buf_ptr_->Size()) << "Size mismatch for copying.";
  size_t copy_size = size / 1024;
  // trade off 'copy_size' between performance and latency
  if (copy_size < 4096) {
    copy_size = 4096;
  } else if (copy_size > 2 << 20) {
    copy_size = 2 << 20;
  } else {
    // align to 64
    copy_size = (copy_size >> 6) << 6;
  }
  uint8_t *dst = buf_ptr_->MutableData();
  size_t cursor = 0;
  while (copy_size + cursor <= size) {
    memcpy(dst + cursor, data + cursor, copy_size);
    progress += copy_size;
    cursor += copy_size;
  }
  memcpy(dst + cursor, data + cursor, size - cursor);
  progress = cursor;
}

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
