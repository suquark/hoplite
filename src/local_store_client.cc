#include "local_store_client.h"

using namespace plasma;

LocalStoreClient::LocalStoreClient(const std::string &plasma_socket) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  plasma_client_.Connect(plasma_socket, "");
}

arrow::Status LocalStoreClient::Create(const plasma::ObjectID &object_id,
                                       int64_t data_size,
                                       std::shared_ptr<Buffer> *data) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  return plasma_client_.Create(object_id, data_size, NULL, 0, data);
}

arrow::Status LocalStoreClient::Seal(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  return plasma_client_.Seal(object_id);
}

arrow::Status LocalStoreClient::Get(const std::vector<ObjectID> &object_ids,
                                    std::vector<ObjectBuffer> *object_buffers) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  return plasma_client_.Get(object_ids, -1, object_buffers);
}

arrow::Status LocalStoreClient::Delete(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  return plasma_client_.Delete(object_id);
}
