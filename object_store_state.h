#ifndef OBJECT_STORE_STATE_H
#define OBJECT_STORE_STATE_H

#include <atomic>
#include <mutex>
#include <plasma/common.h>
#include <unordered_map>

class ObjectStoreState {

public:
  ObjectStoreState();

  // FIXME: here we assume we are downloading only 1 object
  // need to fix this later
  std::atomic_int64_t progress;
  size_t pending_size;
  void *pending_write = NULL;

  // Return true if we are able to transfer an object.
  bool transfer_available(const plasma::ObjectID &object_id);

  void transfer_complete(const plasma::ObjectID &object_id);

private:
  std::mutex transfer_mutex_;
  std::unordered_map<std::string, int> current_transfer_;
};

#endif // OBJECT_STORE_STATE_H
