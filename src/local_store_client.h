#ifndef STORE_H
#define STORE_H

#include <unordered_map>
#include <plasma/client.h>
#include <plasma/common.h>

class LocalStoreClient {
public:
  LocalStoreClient(const std::string &plasma_socket);

  plasma::Status Create(const plasma::ObjectID &object_id, int64_t data_size, std::shared_ptr<Buffer>* data);

  plasma::Status Seal(const ObjectID& object_id);

  plasma::Status Get(const std::vector<ObjectID>& object_ids, std::vector<ObjectBuffer>* object_buffers);

  plasma::Status Delete(const ObjectID& object_id);

private:
  std::mutex local_store_mutex_;
  plasma::PlasmaClient plasma_client_;
}

#endif // NOTIFICATION_H
