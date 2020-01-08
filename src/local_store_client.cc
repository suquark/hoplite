#include "local_store_client.h"

LocalStoreClient::LocalStoreClient(const std::string &plasma_socket) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  plasma_client_.Connect(plasma_socket, "");
}

plasma::Status LocalStoreClient::Create(const plasma::ObjectID &object_id, int64_t data_size, std::shared_ptr<Buffer>* data) {
    std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
    return plasma_client.Create(object_id, data_size, NULL, 0, data);
}

plasma::Status LocalStoreClient::Seal(const ObjectID& object_id) {
    std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
    return plasma_client.Seal(object_id);
}

plasma::Status LocalStoreClient::Get(const std::vector<ObjectID>& object_ids, std::vector<ObjectBuffer>* object_buffers) {
    std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  return plasma_client.Get(object_ids, -1, object_buffers);
}

plasma::Status LocalStoreClient::Delete(const ObjectID& object_id) {
    std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  return plasma_client.Delete(object_id);
}