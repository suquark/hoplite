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
      : buf_(size), receive_progress(0), progress(0){};

  inline const uint8_t *data() { return buf_.data(); }
  inline uint8_t *mutable_data() { return buf_.data(); }
  inline size_t size() { return buf_.size(); }

  int64_t receive_progress;
  std::atomic_int64_t progress;

private:
  std::vector<uint8_t> buf_;
};


class ReadOnlyStream {
public:
  ReadOnlyStream(std::shared_ptr<arrow::Buffer> buf_ptr)
      : buf_ptr_(buf_ptr), progress(buf_ptr_->size()) {}
  inline const uint8_t *data() { return buf_ptr_->data(); }
  inline size_t size() { return buf_ptr_->size(); }
  const int64_t progress;

private:
  const std::shared_ptr<arrow::Buffer> buf_ptr_;
};


class ProgressiveStream {
public:
  ProgressiveStream(std::shared_ptr<arrow::Buffer> buf_ptr)
      : buf_ptr_(buf_ptr), progress(0), receive_progress(0) {
    finished_mutex_.lock();
  }
  inline const uint8_t *data() { return buf_ptr_->data(); }
  inline uint8_t *mutable_data() {
    auto data_ptr = buf_ptr_->mutable_data();
    DCHECK(data_ptr != nullptr) << "The object has been sealed";
  }
  inline int64_t size() { return buf_ptr_->size(); }
  inline void finish() { finished_mutex_.unlock(); }
  inline void wait() {
    finished_mutex_.lock();
    finished_mutex_.unlock();
  }

  int64_t receive_progress;
  std::atomic_int64_t progress;

private:
  const std::shared_ptr<arrow::Buffer> buf_ptr_;
  std::mutex finished_mutex_;
};


class ObjectStoreState {

public:
  ObjectStoreState();

  // Return true if we are able to transfer an object.
  bool transfer_available(const plasma::ObjectID &object_id);

  void transfer_complete(const plasma::ObjectID &object_id);

  std::shared_ptr<ReductionStream>
  create_reduction_stream(const plasma::ObjectID &reduction_id, size_t size);

  std::shared_ptr<ReductionStream>
  get_reduction_stream(const plasma::ObjectID &reduction_id);

  std::shared_ptr<ProgressiveStream>
  create_progressive_stream(const plasma::ObjectID &object_id,
                            const std::shared_ptr<arrow::Buffer> &buffer);

  std::shared_ptr<ProgressiveStream>
  get_progressive_stream(const plasma::ObjectID &object_id);

private:
  std::mutex transfer_mutex_;
  std::unordered_map<std::string, int> current_transfer_;
  std::unordered_map<plasma::ObjectID, std::shared_ptr<ReductionStream>>
      reduction_stream_;
  std::unordered_map<plasma::ObjectID, std::shared_ptr<ProgressiveStream>>
      progressive_stream_;
};

#endif // OBJECT_STORE_STATE_H
