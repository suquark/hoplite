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
  int64_t n_trials = std::strtoll(argv[6], NULL, 10);

  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);

  TIMELINE("main");

  DistributedObjectStore store(redis_address, 6380, 7777, 8888,
                               "/tmp/multicast_plasma", my_address, 6666,
                               50055);

  for (int trial = 0; trial < n_trials; trial++) {
    ObjectID reduction_id = object_id_from_integer(trial * 1000000 + 99999);
    std::vector<ObjectID> object_ids;
    for (int i = 0; i < world_size; i++) {
      auto oid = object_id_from_integer(trial * 1000000 + i);
      object_ids.push_back(oid);
    }
    DCHECK(object_size % sizeof(float) == 0);

    ObjectID rank_object_id = object_ids[rank];
    std::shared_ptr<Buffer> reduction_result;
    int num_reduce_objects = world_size / 2;

    put_fixed_buffer(store, rank_object_id, object_size, (float)rank);

    barrier(redis_address, 7777, world_size);

    if (rank == 0) {
      auto start = std::chrono::system_clock::now();
      store.Reduce(object_ids, reduction_id, num_reduce_objects);
      store.Get(reduction_id, &reduction_result);
      auto end = std::chrono::system_clock::now();
      std::chrono::duration<double> duration = end - start;
      LOG(INFO) << "Reduce " << num_reduce_objects << " objects";
      LOG(INFO) << reduction_id.ToString() << " is reduced using "
                << duration.count();
      std::unordered_set<ObjectID> reduced_objects;
      reduced_objects = store.GetReducedObjects(reduction_id);
      for (const auto& reduced_object : reduced_objects) {
        LOG(INFO) << "Reduced object: " << reduced_object.ToString();
      }
      print_reduction_result<float>(reduction_id, reduction_result, 0.0);
    }
  }
  barrier(redis_address, 7777, world_size);
  return 0;
}
