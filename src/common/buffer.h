#ifndef BUFFER_H
#define BUFFER_H

#include <cinttypes>
#include <memory>
#include <atomic>
#include <vector>
#include <mutex>
#include <condition_variable>
#include "common/config.h"
#include "util/hash.h"

class Buffer {
  public:
    Buffer(uint8_t* data_ptr, int64_t size);
    Buffer(int64_t size);

    void CopyFrom(const std::vector<uint8_t> &data);
    void CopyFrom(const uint8_t *data, size_t size);
    void CopyFrom(const Buffer &buffer);
    void CopyFrom(const std::string &data);
    void StreamCopy(const Buffer &buffer);

    uint8_t* MutableData();
    const uint8_t* Data() const;
    int64_t Size() const;
    uint64_t Hash() const;
    void ShrinkForLRU();
    void Seal() { progress = size_; }
    bool IsFinished() const { return progress >= size_; }
    ~Buffer();

    void Wait();
    void NotifyFinished();
#ifdef HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS
    std::atomic_int64_t progress;
#else
    volatile int64_t progress;
#endif
    volatile bool reset = false;
  private:
    uint8_t* data_ptr_;
    int64_t size_;
    bool is_data_owner_;
    std::mutex notification_mutex_;
    std::condition_variable notification_cv_;
};

struct ObjectBuffer {
  std::shared_ptr<Buffer> data;
  uint8_t* metadata;
  int32_t device_num = 0;
};

#endif // BUFFER_H