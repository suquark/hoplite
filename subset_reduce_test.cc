#include <chrono>
#include <memory>
#include <mpi.h>
#include <string>
#include <vector>

#include "distributed_object_store.h"
#include "logging.h"
#include "socket_utils.h"
#include "test_utils.h"

int main(int argc, char **argv) {
  // argv: *, redis_address, object_size, n_trials
  std::string redis_address = std::string(argv[1]);
  int64_t object_size = std::strtoll(argv[2], NULL, 10);
  int64_t n_trials = std::strtoll(argv[3], NULL, 10);
  std::string my_address = get_host_ipaddress();
  MPI_Init(NULL, NULL);
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  ::hoplite::RayLog::StartRayLog(my_address, ::hoplite::RayLogLevel::DEBUG);

  TIMELINE("main");

  DistributedObjectStore store(redis_address, 6380, 7777, 8888, "/tmp/multicast_plasma", my_address, 6666, 50055);

  for (int trial = 0; trial < n_trials; trial++) {
    if (world_rank == 0) {
      LOG(INFO) << "\n\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Trail #" << trial << "/" << n_trials << "\n\n\n";
    }
    ObjectID reduction_id = object_id_from_integer(trial * 1000000 + 99999);
    std::vector<ObjectID> object_ids;
    for (int i = 0; i < world_size; i++) {
      auto oid = object_id_from_integer(trial * 1000000 + i);
      object_ids.push_back(oid);
    }
    DCHECK(object_size % sizeof(float) == 0);

    ObjectID rank_object_id = object_ids[world_rank];
    std::shared_ptr<Buffer> reduction_result;
    int num_reduce_objects = world_size / 2;

    put_fixed_buffer(store, rank_object_id, object_size, (float)world_rank);

    MPI_Barrier(MPI_COMM_WORLD);

    if (world_rank == 0) {
      auto start = std::chrono::system_clock::now();
      store.Reduce(object_ids, reduction_id, num_reduce_objects);
      store.Get(reduction_id, &reduction_result);
      auto end = std::chrono::system_clock::now();
      std::chrono::duration<double> duration = end - start;
      LOG(INFO) << "Reduce " << num_reduce_objects << " objects";
      LOG(INFO) << reduction_id.ToString() << " is reduced using " << duration.count();
      std::unordered_set<ObjectID> reduced_objects;
      reduced_objects = store.GetReducedObjects(reduction_id);
      for (const auto &reduced_object : reduced_objects) {
        LOG(INFO) << "Reduced object: " << reduced_object.ToString();
      }
      print_reduction_result<float>(reduction_id, reduction_result, 0.0);
    }
    MPI_Barrier(MPI_COMM_WORLD);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}
