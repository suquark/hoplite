#include "local_store_client.h"
#include "logging.h"

using namespace plasma;

LocalStoreClient::LocalStoreClient(const bool use_plasma,
                                   const std::string &plasma_socket)
    : use_plasma_(use_plasma) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  if (use_plasma) {
    plasma_client_.Connect(plasma_socket, "");
  }
}

arrow::Status LocalStoreClient::Create(const plasma::ObjectID &object_id,
                                       int64_t data_size,
                                       std::shared_ptr<arrow::Buffer> *data) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  if (use_plasma_) {
    return plasma_client_.Create(object_id, data_size, NULL, 0, data);
  }

  uint8_t *b = new uint8_t[data_size];
  buffers_[object_id] = std::make_shared<arrow::MutableBuffer>(b, data_size);

  *data = buffers_[object_id];

  return arrow::Status::OK();
}

arrow::Status LocalStoreClient::Seal(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  if (use_plasma_) {
    return plasma_client_.Seal(object_id);
  }

  return arrow::Status::OK();
}

arrow::Status LocalStoreClient::Get(const std::vector<ObjectID> &object_ids,
                                    std::vector<ObjectBuffer> *object_buffers) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  if (use_plasma_) {
    return plasma_client_.Get(object_ids, -1, object_buffers);
  }

  for (auto &object_id : object_ids) {
    ObjectBuffer buf;
    buf.data = buffers_[object_id];
    buf.metadata = nullptr;
    buf.device_num = 0;
    object_buffers->push_back(buf);
  }

  return arrow::Status::OK();
}

arrow::Status LocalStoreClient::Delete(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  if (use_plasma_) {
    return plasma_client_.Delete(object_id);
  }
  return arrow::Status::OK();
}
