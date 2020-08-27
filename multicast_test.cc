#include <chrono>
#include <string>
#include <vector>

#include "common/buffer.h"
#include "common/id.h"
#include "distributed_object_store.h"
#include "logging.h"
#include "test_utils.h"

int main(int argc, char **argv) {
  // signal(SIGPIPE, SIG_IGN);
  // argv: *, redis_address, my_address, #nodes, current_index, object_size
  std::string redis_address = std::string(argv[1]);
  std::string my_address = std::string(argv[2]);
  int64_t world_size = std::strtoll(argv[3], NULL, 10);
  int64_t rank = std::strtoll(argv[4], NULL, 10);
  int64_t object_size = std::strtoll(argv[5], NULL, 10);
  int64_t n_trials = std::strtoll(argv[6], NULL, 10);

  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);

  TIMELINE("main");

  DistributedObjectStore store(redis_address, 6380, 7777, 8888,
                               "/tmp/multicast_plasma", my_address, 6666,
                               50055);


  for (int trial = 0; trial < n_trials; trial++) {
    ObjectID object_id = object_id_from_integer(trial);
    std::shared_ptr<Buffer> result;

    if (rank == 0) {
      result = std::make_shared<Buffer>(object_size);
      uint8_t *buf = result->MutableData();
      for (int64_t i = 0; i < object_size; i++) {
        buf[i] = i % 256;
      }
      result->Seal();
      store.Put(result, object_id);

      LOG(INFO) << object_id.ToString() << " is created!"
                << " CRC32 = " << result->CRC32();

      LOG(INFO) << "entering barrier";
      barrier(redis_address, 7777, world_size);
    } else {

      LOG(INFO) << "entering barrier";
      barrier(redis_address, 7777, world_size);
      auto start = std::chrono::system_clock::now();
      store.Get(object_id, &result);
      auto end = std::chrono::system_clock::now();
      std::chrono::duration<double> duration = end - start;

      LOG(INFO) << object_id.ToString() << " is retrieved using "
                << duration.count() << " seconds. CRC32 = " << result->CRC32();
    }
  }
  barrier(redis_address, 7777, world_size);
  return 0;
}
