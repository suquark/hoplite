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
  // argv: *, redis_address, my_address, object_size
  std::string redis_address = std::string(argv[1]);
  int64_t object_size = std::strtoll(argv[2], NULL, 10);
  int64_t n_trials = std::strtoll(argv[3], NULL, 10);

  std::string my_address = get_host_ipaddress();

  MPI_Init(NULL, NULL);
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);

  TIMELINE("main");

  DistributedObjectStore store(redis_address, 6380, 7777, 8888,
                               "/tmp/multicast_plasma", my_address, 6666,
                               50055);

  for (int trial = 0; trial < n_trials; trial++) {
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
    std::unordered_map<ObjectID, std::shared_ptr<Buffer>> gather_result;

    put_random_buffer<float>(store, rank_object_id, object_size);

    MPI_Barrier(MPI_COMM_WORLD);

    auto start = std::chrono::system_clock::now();
    for (auto &object_id : object_ids) {
      store.Get(object_id, &gather_result[object_id]);
    }
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = end - start;
    LOG(INFO) << "allgathered using " << duration.count() << " seconds";
    uint32_t sum_crc = 0;
    for (auto &object_id : object_ids) {
      sum_crc += gather_result[object_id]->CRC32();
    }
    LOG(INFO) << "CRC32 for objects is " << sum_crc;
    MPI_Barrier(MPI_COMM_WORLD);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  return 0;
}
