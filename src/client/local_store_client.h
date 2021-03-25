#ifndef LOCAL_STORE_H
#define LOCAL_STORE_H

#include "common/buffer.h"
#include "common/id.h"
#include "common/status.h"
#include <mutex>
#include <queue>
#include <unordered_map>

class LocalStoreClient {
public:
  LocalStoreClient();

  Status Create(const ObjectID &object_id, int64_t data_size, std::shared_ptr<Buffer> *data);

  Status Seal(const ObjectID &object_id);

  // Check if an object exists in the store.
  // We assume this function will never fail.
  bool ObjectExists(const ObjectID &object_id, bool require_finished = true);

  Status Get(const std::vector<ObjectID> &object_ids, std::vector<ObjectBuffer> *object_buffers);

  // Get single object from the store.
  Status Get(const ObjectID &object_id, ObjectBuffer *object_buffer);

  std::shared_ptr<Buffer> GetBufferNoExcept(const ObjectID &object_id);

  Status GetBufferOrCreate(const ObjectID &object_id, int64_t size, std::shared_ptr<Buffer> *data);

  Status Delete(const ObjectID &object_id);

  Status Wait(const ObjectID &object_id);

private:
  Status create_internal(const ObjectID &object_id, int64_t data_size, std::shared_ptr<Buffer> *data);
  bool object_exists_unsafe(const ObjectID &object_id, bool require_finished);
  std::mutex local_store_mutex_;
  std::unordered_map<ObjectID, std::shared_ptr<Buffer>> buffers_;
  size_t total_store_size_;
  const size_t lru_bound_size_ = (16LL << 30);
  std::queue<ObjectID> lru_queue_;
};

#endif // LOCAL_STORE_H
