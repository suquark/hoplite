#include <chrono>
#include <string>
#include <vector>

#include <plasma/common.h>
#include <zlib.h>

#include "distributed_object_store.h"
#include "logging.h"
#include "notification.h"
#include "test_utils.h"

using namespace plasma;

std::thread timed_exit(int seconds) {
  usleep(seconds * 1000000);
  exit(0);
}

int main(int argc, char **argv) {
  // signal(SIGPIPE, SIG_IGN);
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

  std::unique_ptr<NotificationServer> notification_server;
  std::thread notification_server_thread;
  if (rank == 0) {
    store.flushall();
    notification_server.reset(new NotificationServer(my_address, 7777, 8888));
    notification_server_thread = notification_server->Run();
    // TODO: make notification server a standalone process
    // notification_server_thread.join();
  }

  ObjectID object_id = object_id_from_integer(0);
  std::shared_ptr<Buffer> result;

  if (rank == 0) {
    char *buffer = new char[object_size];
    for (int i = 0; i < object_size; i++) {
      buffer[i] = i % 256;
    }
    store.Put(buffer, object_size, object_id);
    result = std::make_shared<Buffer>((const uint8_t *)buffer, object_size);

    LOG(INFO) << "Object(" << object_id.hex() << ") is created!"
              << ", CRC32 = " << checksum_crc32(result);

    LOG(INFO) << "entering barrier";
    barrier(rank, redis_address, 7777, world_size);
  } else {

    LOG(INFO) << "entering barrier";
    barrier(rank, redis_address, 7777, world_size);
    auto start = std::chrono::system_clock::now();
    store.Get(object_id, &result);
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> duration = end - start;

    LOG(INFO) << "Object(" << object_id.hex() << ") is retrieved using "
              << duration.count()
              << " seconds. CRC32 = " << checksum_crc32(result);
  }

  exit_thread.join();
  store.join_tasks();
  return 0;
}
