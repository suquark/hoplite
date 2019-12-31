#ifndef OBJECT_STORE_STATE_H
#define OBJECT_STORE_STATE_H

#include <arrow/buffer.h>
#include <atomic>
#include <mutex>
#include <plasma/common.h>

#include <unordered_map>
#include <vector>

class ReductionStream {
public:
  ReductionStream(size_t size)
      : buf_(size), receive_progress(0), reduce_progress(0){};

  inline void *data() { return (void *)buf_.data(); }
  inline size_t size() { return buf_.size(); }

  int64_t receive_progress;
  std::atomic_int64_t reduce_progress;

private:
  std::vector<uint8_t> buf_;
};

class ReductionEndpointStream {
public:
  ReductionEndpointStream(std::shared_ptr<arrow::Buffer> buf_ptr)
      : buf_ptr_(buf_ptr){
        finished.lock();
      };
  std::mutex finished;
  inline void *mutable_data() { return (void *)buf_ptr_->mutable_data(); }
  inline size_t size() { return buf_ptr_->size(); }

private:
  std::shared_ptr<arrow::Buffer> buf_ptr_;
};

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

  std::shared_ptr<ReductionStream>
  create_reduction_stream(const plasma::ObjectID &reduction_id, size_t size);

  std::shared_ptr<ReductionStream>
  get_reduction_stream(const plasma::ObjectID &reduction_id);

  void create_reduction_endpoint(const plasma::ObjectID &reduction_id,
                                 const std::shared_ptr<arrow::Buffer> &buffer);

  std::shared_ptr<ReductionEndpointStream>
  get_reduction_endpoint(const plasma::ObjectID &reduction_id);

private:
  std::mutex transfer_mutex_;
  std::unordered_map<std::string, int> current_transfer_;
  std::unordered_map<plasma::ObjectID, std::shared_ptr<ReductionStream>>
      reduction_stream_;
  std::unordered_map<plasma::ObjectID, std::shared_ptr<ReductionEndpointStream>>
      reduction_endpoint_;
};

#endif // OBJECT_STORE_STATE_H
