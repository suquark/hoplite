#include <algorithm>
#include <cstring>
#include "logging.h"
#include "common/buffer.h"
#include <zlib.h>

Buffer::Buffer(uint8_t* data_ptr, int64_t size): data_ptr_(data_ptr), size_(size), is_data_owner_(false) {}

Buffer::Buffer(int64_t size): size_(size), is_data_owner_(true) {
  data_ptr_ = new uint8_t[size];
}

uint8_t* Buffer::MutableData() { return data_ptr_; }
const uint8_t* Buffer::Data() const { return data_ptr_; }
int64_t Buffer::Size() const { return size_; }
uint32_t Buffer::CRC32() const {
  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, data_ptr_, size_);
  return crc;
}

void Buffer::CopyFrom(const std::vector<uint8_t> &data) {
  DCHECK(data.size() == size_) << "input size mismatch";
  std::copy(data.begin(), data.end(), data_ptr_);
}

void Buffer::CopyFrom(const uint8_t *data, size_t size) {
  DCHECK(size == size_) << "input size mismatch";
  std::memcpy(data_ptr_, data, size);
}

void Buffer::CopyFrom(const Buffer &buffer) {
  DCHECK(buffer.Size() == size_) << "input size mismatch";
  std::memcpy(data_ptr_, buffer.Data(), buffer.Size());
}

void Buffer::CopyFrom(const std::string &data) {
  CopyFrom((const uint8_t *)data.data(), data.size());
}

void Buffer::StreamCopy(const Buffer &src) {
  DCHECK(src.IsFinished()) << "Copy from a unfinished buffer";
  const uint8_t *data = src.Data();
  int64_t size = src.Size();
  DCHECK(size == Size()) << "Size mismatch for copying.";
  size_t copy_size = size / 1024;
  // trade off 'copy_size' between performance and latency
  if (copy_size < 4096) {
    copy_size = 4096;
  } else if (copy_size > 2 << 20) {
    copy_size = 2 << 20;
  } else {
    // align to 64
    copy_size = (copy_size >> 6) << 6;
  }
  uint8_t *dst = MutableData();
  size_t cursor = 0;
  while (copy_size + cursor <= size) {
    memcpy(dst + cursor, data + cursor, copy_size);
    progress += copy_size;
    cursor += copy_size;
  }
  memcpy(dst + cursor, data + cursor, size - cursor);
  progress = cursor;
}

void Buffer::Wait() {
  std::unique_lock<std::mutex> l(notification_mutex_);
  notification_cv_.wait(l, [this]() { return IsFinished(); });
}

void Buffer::NotifyFinished() {
  std::unique_lock<std::mutex> l(notification_mutex_);
  DCHECK(IsFinished()) << "The buffer has not been finished";
  notification_cv_.notify_all();
}

void Buffer::ShrinkForLRU() {
  delete[] data_ptr_;
  data_ptr_ = new uint8_t[4];
  size_ = 4;
}

Buffer::~Buffer() {
  if (is_data_owner_) {
    delete[] data_ptr_;
  }
}
