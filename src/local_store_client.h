#ifndef LOCAL_STORE_H
#define LOCAL_STORE_H

#include "common/buffer.h"
#include "common/id.h"
#include "common/status.h"
#include <mutex>
#include <unordered_map>

class LocalStoreClient {
public:
  LocalStoreClient(const bool use_plasma, const std::string &plasma_socket);

  Status Create(const ObjectID &object_id, int64_t data_size,
                std::shared_ptr<Buffer> *data);

  Status Seal(const ObjectID &object_id);

  // Check if an object exists in the store.
  // We assume this function will never fail.
  bool ObjectExists(const ObjectID &object_id);

  Status Get(const std::vector<ObjectID> &object_ids,
             std::vector<ObjectBuffer> *object_buffers);

  // Get single object from the store.
  Status Get(const ObjectID &object_id, ObjectBuffer *object_buffer);

  Status Delete(const ObjectID &object_id);

private:
  const bool use_plasma_;
  std::mutex local_store_mutex_;
  std::unordered_map<ObjectID, std::shared_ptr<Buffer>> buffers_;
};

#endif // LOCAL_STORE_H
