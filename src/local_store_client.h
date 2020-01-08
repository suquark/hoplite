#ifndef STORE_H
#define STORE_H

#include <mutex>
#include <plasma/client.h>
#include <plasma/common.h>
#include <unordered_map>

class LocalStoreClient {
public:
  LocalStoreClient(const bool use_plasma, const std::string &plasma_socket);

  arrow::Status Create(const plasma::ObjectID &object_id, int64_t data_size,
                       std::shared_ptr<arrow::Buffer> *data);

  arrow::Status Seal(const plasma::ObjectID &object_id);

  arrow::Status Get(const std::vector<plasma::ObjectID> &object_ids,
                    std::vector<plasma::ObjectBuffer> *object_buffers);

  arrow::Status Delete(const plasma::ObjectID &object_id);

private:
  const bool use_plasma_;
  std::mutex local_store_mutex_;
  plasma::PlasmaClient plasma_client_;
  std::unordered_map<plasma::ObjectID, std::shared_ptr<arrow::MutableBuffer>> buffers_;
};

#endif // NOTIFICATION_H
