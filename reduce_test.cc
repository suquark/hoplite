#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <plasma/common.h>
#include <zlib.h>

#include "distributed_object_store.h"
#include "logging.h"
#include "plasma_utils.h"

using namespace plasma;

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

void test_server(DistributedObjectStore &store, int object_size,
                 const std::vector<ObjectID> &object_ids) {
  DCHECK(object_size % sizeof(float) == 0);
  float *buffer;
  size_t size;

  auto start = std::chrono::system_clock::now();
  store.Get(object_ids, (const void **)&buffer, &size, object_size);
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> duration = end - start;

  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const unsigned char *)buffer, size);
  size_t num_elements = object_size / sizeof(float);
  LOG(INFO) << "Object is reduced using " << duration.count()
            << " seconds. CRC32 = " << crc << "; Results: [" << buffer[0]
            << ", " << buffer[1] << ", " << buffer[2] << ", " << buffer[3]
            << ", " << buffer[4] << ", ... , " << buffer[num_elements - 2]
            << ", " << buffer[num_elements - 1] << "]";
}

void test_client(DistributedObjectStore &store, int object_size,
                 ObjectID object_id) {
  DCHECK(object_size % sizeof(float) == 0);
  std::unique_ptr<std::vector<float>> buffer =
      get_random_float_buffer(object_size / sizeof(float), object_id.hex());
  store.Put(buffer->data(), object_size, object_id);
  LOG(INFO) << "The chosen random float value: " << (*buffer)[1];
  LOG(INFO) << "Object is created! object_id = " << object_id.hex()
            << ", size = " << object_size;
}

int main(int argc, char **argv) {
  // argv: *, redis_address, my_address, s/c, object_size, [object_ids]
  std::string redis_address = std::string(argv[1]);
  std::string my_address = std::string(argv[2]);

  ::ray::RayLog::StartRayLog(my_address + ": ");

  DistributedObjectStore store(redis_address, 6380, 6381,
                               "/tmp/multicast_plasma", my_address, 6666,
                               50055);

  if (argv[3][0] == 's') {
    store.flushall();
    std::vector<ObjectID> object_ids;
    for (int i = 5; i < argc; i++) {
      object_ids.push_back(from_hex(argv[i]));
    }
    test_server(store, atoi(argv[4]), object_ids);
  } else {
    test_client(store, atoi(argv[4]), from_hex(argv[5]));
  }

  store.join_tasks();
  return 0;
}