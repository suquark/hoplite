#include "logging.h"
#include "object_store_state.h"
ObjectStoreState::ObjectStoreState() : progress(0){};

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

  std::shared_ptr<ReductionStream> ObjectStoreState::create_reduction_stream(const plasma::ObjectID &reduction_id, size_t size) {
     DCHECK(reduction_stream_.find(reduction_id.hex()) == reduction_stream_.end());
     auto stream = std::make_shared<ReductionStream>(size);
     reduction_stream_[reduction_id.hex()] = stream;
     return stream;
  }

  std::shared_ptr<ReductionStream> ObjectStoreState::get_reduction_stream(const plasma::ObjectID &reduction_id) {
     if (reduction_stream_.find(reduction_id.hex()) == reduction_stream_.end()) {
       return std::shared_ptr<ReductionStream>();
     } else {
       return reduction_stream_[reduction_id.hex()];
     }
  }
