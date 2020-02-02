#include <algorithm>
#include <cstring>
#include "logging.h"
#include "common/buffer.h"
#include <zlib.h>
#include <iostream>
#include <fstream>

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
