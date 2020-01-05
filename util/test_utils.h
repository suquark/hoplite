// Note: This header should only be included in the target soruce file.
#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include <string>
#include <random>

#include <plasma/common.h>
#include <zlib.h>
#include <logging.h>
#include "distributed_object_store.h"

using namespace plasma;

uint32_t checksum_crc32(const std::shared_ptr<Buffer> &buffer) {
  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, buffer->data(), buffer->size());
  return crc;
}

float get_uniform_random_float(const std::string &seed_str) {
  std::seed_seq seed(seed_str.begin(), seed_str.end());
  std::default_random_engine eng{seed};
  std::uniform_real_distribution<float> dis(0, 1);
  return dis(eng);
}

std::unique_ptr<std::vector<float>>
get_random_float_buffer(size_t size, const std::string &seed_str) {
  std::unique_ptr<std::vector<float>> retval;
  auto buf = new std::vector<float>(size);
  float random_number = get_uniform_random_float(seed_str);
  for (int i = 0; i < size; i++) {
    (*buf)[i] = i * random_number;
  }
  retval.reset(buf);
  return retval;
}

template<typename T>
void put_random_buffer(DistributedObjectStore &store, const ObjectID &object_id,
                       int64_t object_size) {
  DCHECK(object_size % sizeof(T) == 0);
  std::unique_ptr<std::vector<T>> buffer =
      get_random_float_buffer(object_size / sizeof(T), object_id.hex());
  store.Put(buffer->data(), object_size, object_id);
  LOG(INFO) << "Object is created! object_id = " << object_id.hex()
            << ", size = " << object_size
            << ", the chosen random value: " << (*buffer)[1];
}

template<typename T>
void print_reduction_result(
    const ObjectID &object_id,
    const std::shared_ptr<Buffer> &result, T expected_sum) {
  const T* buffer = (const T*)result->data();
  int64_t num_elements = result->size() / sizeof(T);

  LOG(INFO) << "ObjectID(" << object_id.hex() <<  "), "
            << "CRC32 = " << checksum_crc32(result) << "\n"
            << "Results: [" << buffer[0] << ", " << buffer[1] << ", "
            << buffer[2] << ", " << buffer[3] << ", " << buffer[4]
            << ", ... , " << buffer[num_elements - 1] << "] \n"
            << "Result errors: first item = " << buffer[1] - expected_sum
            << ", last item = "
            << buffer[num_elements - 1] / (num_elements - 1) - expected_sum;
}

#endif // TEST_UTILS_H