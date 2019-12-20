#ifndef OBJECT_STORE_STATE_H
#define OBJECT_STORE_STATE_H

#include <atomic>
#include <mutex>
#include <plasma/common.h>
#include <unordered_map>


class ReductionStream {
public:
  ReductionStream(size_t size): buf_(size), progress(0);

  void* data() { return (void *)buf_.data(); }
  size_t size() { return (void *)buf_.size(); }

  std::vector<uint8_t> buf_;
  std::atomic_int64_t progress;
}


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

  std::shared_ptr<ReductionStream> create_reduction_stream(const plasma::ObjectID &reduction_id, size_t size) {
     DCHECK(reduction_stream_.find(reduction_id.hex()) == reduction_stream_.end());
     auto stream = std::make_shared<ReductionStream>(size);
     reduction_stream_[reduction_id.hex()] = stream;
     return stream;
  }

private:
  std::mutex transfer_mutex_;
  std::unordered_map<std::string, int> current_transfer_;
  std::unordered_map<std::string, std::shared_ptr<ReductionStream>> reduction_stream_;

};

#endif // OBJECT_STORE_STATE_H
