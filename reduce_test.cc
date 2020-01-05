#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <plasma/common.h>
#include <zlib.h>

#include "distributed_object_store.h"
#include "logging.h"
#include "notification.h"
#include "plasma_utils.h"
#include "test_utils.h"

using namespace plasma;

void put_random_buffer(DistributedObjectStore &store, const ObjectID &object_id,
                       int64_t object_size) {
  DCHECK(object_size % sizeof(float) == 0);
  std::unique_ptr<std::vector<float>> buffer =
      get_random_float_buffer(object_size / sizeof(float), object_id.hex());
  store.Put(buffer->data(), object_size, object_id);
  LOG(INFO) << "Object is created! object_id = " << object_id.hex()
            << ", size = " << object_size
            << ", the chosen random float value: " << (*buffer)[1];
}

void test_server(DistributedObjectStore &store, int64_t object_size,
                 const std::vector<ObjectID> &object_ids, float expected_sum) {
  DCHECK(object_size % sizeof(float) == 0);
  std::shared_ptr<Buffer> reduction_result;
  ObjectID reduction_id;

  auto start = std::chrono::system_clock::now();
  store.Get(object_ids, object_size, &reduction_id, &reduction_result);
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> duration = end - start;

  LOG(INFO) << "ObjectID(" << reduction_id.hex() << ") is reduced using "
            << duration.count();
  print_reduction_result<float>(reduction_id, reduction_result, expected_sum);
}

std::thread timed_exit(int seconds) {
  usleep(seconds * 1000000);
  exit(0);
}

int main(int argc, char **argv) {
  // argv: *, redis_address, my_address, s/c, object_size, [object_ids]
  std::string redis_address = std::string(argv[1]);
  std::string my_address = std::string(argv[2]);

  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);

  TIMELINE("main");

  DistributedObjectStore store(redis_address, 6380, 7777, 8888,
                               "/tmp/multicast_plasma", my_address, 6666,
                               50055);

  std::thread exit_thread(timed_exit, 30);

  int64_t object_size = std::strtoll(argv[4], NULL, 10);

  if (argv[3][0] == 's') {
    store.flushall();

    NotificationServer notification_server(my_address, 7777, 8888);
    std::thread notification_server_thread = notification_server.Run();
    ObjectID local_object_id =
        from_hex("0000000000000000000000000000000000000000");
    put_random_buffer<float>(store, local_object_id, object_size);

    std::vector<ObjectID> object_ids{local_object_id};
    for (int i = 5; i < argc; i++) {
      object_ids.push_back(from_hex(argv[i]));
    }

    float sum = 0;
    for (auto &oid : object_ids) {
      sum += get_uniform_random_float(oid.hex());
    }
    LOG(INFO) << "expected sum: " << sum;

    test_server(store, object_size, object_ids, sum);
    notification_server_thread.join();
  } else {
    ObjectID object_id = from_hex(argv[5]);
    put_random_buffer<float>(store, object_id, object_size);
  }

  exit_thread.join();
  store.join_tasks();
  return 0;
}
