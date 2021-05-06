#include <chrono>
#include <mpi.h>
#include <string>
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
    ObjectID object_id = object_id_from_integer(trial);
    std::shared_ptr<Buffer> result;

    if (world_rank == 0) {
      result = std::make_shared<Buffer>(object_size);
      uint8_t *buf = result->MutableData();
      for (int64_t i = 0; i < object_size; i++) {
        buf[i] = i % 256;
      }
      result->Seal();
      store.Put(result, object_id);

      LOG(INFO) << object_id.ToString() << " is created!"
                << " Hash = " << result->Hash();

      LOG(INFO) << "entering barrier";
      MPI_Barrier(MPI_COMM_WORLD);
    } else {

      LOG(INFO) << "entering barrier";
      MPI_Barrier(MPI_COMM_WORLD);
      auto start = std::chrono::system_clock::now();
      store.Get(object_id, &result);
      auto end = std::chrono::system_clock::now();
      std::chrono::duration<double> duration = end - start;

      LOG(INFO) << object_id.ToString() << " is retrieved. Hash = " << result->Hash();
      LOG(INFO) << "Retrieving duration = " << duration.count();
    }
    MPI_Barrier(MPI_COMM_WORLD);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}
