#ifndef BUFFER_H
#define BUFFER_H

#include <cinttypes>
#include <memory>
#include <vector>

class Buffer {
  public:
    Buffer(uint8_t* data_ptr, int64_t size);
    Buffer(int64_t size);

    void CopyFrom(const std::vector<uint8_t> &data);
    void CopyFrom(const uint8_t *data, size_t size);
    void CopyFrom(const Buffer &buffer);

    uint8_t* MutableData();
    const uint8_t* Data() const;
    int64_t Size() const;
    uint32_t CRC32() const;
    ~Buffer();
  private:
    uint8_t* data_ptr_;
    int64_t size_;
    bool is_data_owner_;
};

struct ObjectBuffer {
  std::shared_ptr<Buffer> data;
  uint8_t* metadata;
  int32_t device_num = 0;
};

#endif // BUFFER_H