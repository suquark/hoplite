#include "object_store_state.h"
#include "logging.h"

// Return true if we are able to transfer an object.
bool ObjectStoreState::transfer_available(const plasma::ObjectID &object_id) {
  std::lock_guard<std::mutex> guard(transfer_mutex_);
  if (current_transfer_.find(object_id.hex()) == current_transfer_.end()) {
    current_transfer_[object_id.hex()] = 0;
  }

  if (current_transfer_[object_id.hex()] < 1) {
    current_transfer_[object_id.hex()]++;
    return true;
  } else {
    return false;
  }
}

void ObjectStoreState::transfer_complete(const plasma::ObjectID &object_id) {
  std::lock_guard<std::mutex> guard(transfer_mutex_);
  current_transfer_[object_id.hex()]--;
}

std::shared_ptr<ReductionStream>
ObjectStoreState::create_reduction_stream(const plasma::ObjectID &reduction_id,
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
ObjectStoreState::get_reduction_stream(const plasma::ObjectID &reduction_id) {
  std::unique_lock<std::mutex> l(reduction_stream_mutex_);
  reduction_stream_cv_.wait(l, [this](){return reduction_stream_.find(reduction_id) != reduction_stream_.end();});
  return reduction_stream_[reduction_id];
}

std::shared_ptr<ProgressiveStream> ObjectStoreState::create_progressive_stream(
    const plasma::ObjectID &reduction_id,
    const std::shared_ptr<arrow::Buffer> &buffer) {
  DCHECK(progressive_stream_.find(reduction_id) == progressive_stream_.end());
  progressive_stream_[reduction_id] =
      std::make_shared<ProgressiveStream>(buffer);
  return progressive_stream_[reduction_id];
}

std::shared_ptr<ProgressiveStream>
ObjectStoreState::get_progressive_stream(const plasma::ObjectID &reduction_id) {
  if (progressive_stream_.find(reduction_id) == progressive_stream_.end()) {
    LOG(DEBUG) << "reduction endpoint id = " << reduction_id.hex()
               << " not found";
    return nullptr;
  } else {
    return progressive_stream_[reduction_id];
  }
}
