#include <chrono>
#include <memory>
#include <mpi.h>
#include <string>
#include <thread>
#include <vector>

// Make the Put() call blocking for more precise timing.
#define HOPLITE_PUT_BLOCKING true

#include "common/buffer.h"
#include "common/id.h"
#include "distributed_object_store.h"
#include "util/logging.h"
#include "util/socket_utils.h"
#include "util/test_utils.h"

int main(int argc, char **argv) {
  // argv: *, object_directory_address, object_size, n_trials
  std::string object_directory_address = std::string(argv[1]);
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

  DistributedObjectStore store(object_directory_address);

  for (int trial = 0; trial < n_trials; trial++) {
    ObjectID reduction_id = object_id_from_integer(trial * 1000000 + 99999);
    std::vector<ObjectID> object_ids;
    float sum = 0;
    for (int i = 0; i < world_size; i++) {
      auto oid = object_id_from_integer(trial * 1000000 + i);
      object_ids.push_back(oid);
      auto rnum = get_uniform_random_float(oid.Hex());
      sum += rnum;
    }
    DCHECK(object_size % sizeof(float) == 0);

    ObjectID rank_object_id = object_ids[world_rank];
    std::shared_ptr<Buffer> reduction_result;

    put_random_buffer<float>(store, rank_object_id, object_size);
    MPI_Barrier(MPI_COMM_WORLD);

    auto start = std::chrono::system_clock::now();
    if (world_rank == 0) {
      store.Reduce(object_ids, reduction_id);
    }
    store.Get(reduction_id, &reduction_result);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = end - start;
    LOG(INFO) << reduction_id.ToString() << " is reduced using " << duration.count();
    print_reduction_result<float>(reduction_id, reduction_result, sum);
    MPI_Barrier(MPI_COMM_WORLD);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}
