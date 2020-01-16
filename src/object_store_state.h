#ifndef OBJECT_STORE_STATE_H
#define OBJECT_STORE_STATE_H

#include <atomic>
#include <condition_variable>
#include <mutex>

#include <unordered_map>
#include <vector>

#include "common/buffer.h"
#include "common/id.h"

class ReductionStream {
public:
  ReductionStream(size_t size) : receive_progress(0), progress(0), buf_(size){};

  inline const uint8_t *data() { return buf_.Data(); }
  inline uint8_t *mutable_data() { return buf_.MutableData(); }
  inline size_t size() { return buf_.Size(); }

  int64_t receive_progress;
  std::atomic_int64_t progress;

private:
  Buffer buf_;
};

class ReadOnlyStream {
public:
  ReadOnlyStream(std::shared_ptr<Buffer> buf_ptr)
      : progress(buf_ptr_->Size()), buf_ptr_(buf_ptr) {}
  inline const uint8_t *data() { return buf_ptr_->Data(); }
  inline size_t size() { return buf_ptr_->Size(); }
  const int64_t progress;

private:
  const std::shared_ptr<Buffer> buf_ptr_;
};

class ProgressiveStream {
public:
  ProgressiveStream(std::shared_ptr<Buffer> buf_ptr)
      : receive_progress(0), progress(0), buf_ptr_(buf_ptr) {
    finished_mutex_.lock();
  }
  inline const uint8_t *data() { return buf_ptr_->Data(); }
  inline uint8_t *mutable_data() { return buf_ptr_->MutableData(); }
  inline int64_t size() { return buf_ptr_->Size(); }
  inline void finish() { finished_mutex_.unlock(); }
  inline void wait() {
    finished_mutex_.lock();
    finished_mutex_.unlock();
  }

  int64_t receive_progress;
  std::atomic_int64_t progress;

private:
  const std::shared_ptr<Buffer> buf_ptr_;
  std::mutex finished_mutex_;
};

class ObjectStoreState {

public:
  // Return true if we are able to transfer an object.
  bool transfer_available(const ObjectID &object_id);

  void transfer_complete(const ObjectID &object_id);

  std::shared_ptr<ReductionStream>
  create_reduction_stream(const ObjectID &reduction_id, size_t size);

  std::shared_ptr<ReductionStream>
  get_reduction_stream(const ObjectID &reduction_id);

  std::shared_ptr<ProgressiveStream>
  create_progressive_stream(const ObjectID &object_id,
                            const std::shared_ptr<Buffer> &buffer);

  std::shared_ptr<ProgressiveStream>
  get_progressive_stream(const ObjectID &object_id);

private:
  std::mutex transfer_mutex_;
  std::unordered_map<std::string, int> current_transfer_;
  std::mutex reduction_stream_mutex_;
  std::condition_variable reduction_stream_cv_;
  std::unordered_map<ObjectID, std::shared_ptr<ReductionStream>>
      reduction_stream_;
  std::unordered_map<ObjectID, std::shared_ptr<ProgressiveStream>>
      progressive_stream_;
};

#endif // OBJECT_STORE_STATE_H
