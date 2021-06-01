#include "local_store_client.h"
#include "util/logging.h"

LocalStoreClient::LocalStoreClient() : total_store_size_(0) {
  // std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  // if (use_plasma) {
  //   plasma_client_.Connect(plasma_socket, "");
  // }
}

Status LocalStoreClient::create_internal(const ObjectID &object_id, int64_t data_size, std::shared_ptr<Buffer> *data) {
  buffers_[object_id] = std::make_shared<Buffer>(data_size);
  *data = buffers_[object_id];
  total_store_size_ += data_size;
  lru_queue_.push(object_id);
  while (total_store_size_ > lru_bound_size_) {
    ObjectID front_id = lru_queue_.front();
    lru_queue_.pop();
    std::shared_ptr<Buffer> buffer_ptr = buffers_[front_id];
    buffers_.erase(front_id);
    total_store_size_ -= buffer_ptr->Size();
    buffer_ptr->ShrinkForLRU();
  }
  return Status::OK();
}

Status LocalStoreClient::Create(const ObjectID &object_id, int64_t data_size, std::shared_ptr<Buffer> *data) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  return create_internal(object_id, data_size, data);
}

Status LocalStoreClient::Seal(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  // if (use_plasma_) {
  //   return plasma_client_.Seal(object_id);
  // }

  auto search = buffers_.find(object_id);
  DCHECK(search != buffers_.end()) << "Sealing an object that does not exist.";
  if (!search->second->IsFinished()) {
    // TODO: See GitHub Issue #153. Disable it now.
    LOG(DEBUG) << "Sealing an unfinished buffer.";
  }
  search->second->Seal();
  return Status::OK();
}

bool LocalStoreClient::ObjectExists(const ObjectID &object_id, bool require_finished) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  return object_exists_unsafe(object_id, require_finished);
}

Status LocalStoreClient::Get(const std::vector<ObjectID> &object_ids, std::vector<ObjectBuffer> *object_buffers) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  // if (use_plasma_) {
  //   return plasma_client_.Get(object_ids, -1, object_buffers);
  // }

  for (auto &object_id : object_ids) {
    ObjectBuffer buf;
    buf.data = buffers_[object_id];
    buf.metadata = nullptr;
    buf.device_num = 0;
    object_buffers->push_back(buf);
  }
  return Status::OK();
}

Status LocalStoreClient::Get(const ObjectID &object_id, ObjectBuffer *object_buffer) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  object_buffer->data = buffers_[object_id];
  object_buffer->metadata = nullptr;
  object_buffer->device_num = 0;
  return Status::OK();
}

std::shared_ptr<Buffer> LocalStoreClient::GetBufferNoExcept(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  DCHECK(object_exists_unsafe(object_id, false));
  return buffers_[object_id];
}

Status LocalStoreClient::GetBufferOrCreate(const ObjectID &object_id, int64_t size, std::shared_ptr<Buffer> *data) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  if (!buffers_.count(object_id)) {
    return create_internal(object_id, size, data);
  }
  DCHECK(object_exists_unsafe(object_id, false));
  *data = buffers_[object_id];
  return Status::OK();
}

Status LocalStoreClient::Delete(const ObjectID &object_id) {
  std::lock_guard<std::mutex> lock_guard(local_store_mutex_);
  // if (use_plasma_) {
  //   return plasma_client_.Delete(object_id);
  // }
  return Status::OK();
}

Status LocalStoreClient::Wait(const ObjectID &object_id) {
  LOG(DEBUG) << "waiting the stream with " << object_id.ToString();
  auto buffer = GetBufferNoExcept(object_id);
  buffer->Wait();
  return Status::OK();
}

bool LocalStoreClient::object_exists_unsafe(const ObjectID &object_id, bool require_finished) {
  auto search = buffers_.find(object_id);
  return search != buffers_.end() && (!require_finished || search->second->IsFinished());
}
