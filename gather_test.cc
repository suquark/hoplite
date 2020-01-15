#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "distributed_object_store.h"
#include "logging.h"
#include "test_utils.h"

int main(int argc, char **argv) {
  // argv: *, redis_address, my_address, #nodes, current_index, object_size
  std::string redis_address = std::string(argv[1]);
  std::string my_address = std::string(argv[2]);
  int64_t world_size = std::strtoll(argv[3], NULL, 10);
  int64_t rank = std::strtoll(argv[4], NULL, 10);
  int64_t object_size = std::strtoll(argv[5], NULL, 10);

  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);

  TIMELINE("main");

  DistributedObjectStore store(redis_address, 6380, 7777, 8888,
                               "/tmp/multicast_plasma", my_address, 6666,
                               50055);

  std::thread exit_thread(timed_exit, 20);

  std::vector<ObjectID> object_ids;
  float sum = 0;
  for (int i = 0; i < world_size; i++) {
    auto oid = object_id_from_integer(i);
    object_ids.push_back(oid);
    auto rnum = get_uniform_random_float(oid.Hex());
    sum += rnum;
  }
  DCHECK(object_size % sizeof(float) == 0);

  ObjectID rank_object_id = object_ids[rank];
  std::shared_ptr<Buffer> gather_result;

  put_random_buffer<float>(store, rank_object_id, object_size);

  barrier(rank, redis_address, 7777, world_size, my_address);

  if (rank == 0) {
    auto start = std::chrono::system_clock::now();
    for (auto &object_id : object_ids) {
      store.Get(object_id, &gather_result);
    }
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = end - start;
    LOG(INFO) << "gathered using " << duration.count() << " seconds";
  }

  exit_thread.join();
  store.join_tasks();
  return 0;
}
