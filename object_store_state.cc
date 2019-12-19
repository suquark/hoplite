#include "object_store_state.h"

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

void bjectStoreState::transfer_complete(const plasma::ObjectID &object_id) {
  std::lock_guard<std::mutex> guard(transfer_mutex_);
  current_transfer_[object_id.hex()]--;
}
