#include <chrono>
#include <string>
#include <vector>

#include <plasma/common.h>
#include <zlib.h>

#include "distributed_object_store.h"
#include "logging.h"
#include "notification.h"
#include "plasma_utils.h"

using namespace plasma;

std::chrono::high_resolution_clock::time_point start_time;

double get_time() {
  auto now = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> time_span = now - start_time;
  return time_span.count();
}

void test_server(DistributedObjectStore &store, int object_size) {
  char *buffer = new char[1024 * 1024 * 1024];
  for (int i = 0; i < object_size; i++) {
    buffer[i] = i % 256;
  }

  ObjectID object_id = store.Put(buffer, object_size);
  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const unsigned char *)buffer, object_size);
  LOG(INFO) << "Object is created! object_id = " << object_id.hex()
            << ", CRC32 = " << crc;
}

void test_client(DistributedObjectStore &store, ObjectID object_id) {
  const char *buffer;
  size_t size;
  auto start = std::chrono::system_clock::now();
  store.Get(object_id, (const void **)&buffer, &size);
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> duration = end - start;

  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const unsigned char *)buffer, size);
  LOG(INFO) << "Object is retrieved using " << duration.count()
            << " seconds. CRC32 = " << crc;
}

std::thread timed_exit(int seconds) {
  usleep(seconds * 1000000);
  exit(0);
}

int main(int argc, char **argv) {
  // signal(SIGPIPE, SIG_IGN);
  LOGFUNC(__func__);
  start_time = std::chrono::high_resolution_clock::now();
  std::string redis_address = std::string(argv[1]);
  std::string my_address = std::string(argv[2]);

  ::ray::RayLog::StartRayLog(my_address, ::ray::RayLogLevel::DEBUG);

  DistributedObjectStore store(redis_address, 6380, 7777, 8888,
                               "/tmp/multicast_plasma", my_address, 6666,
                               50055);

  std::thread exit_thread(timed_exit, 20);

  if (argv[3][0] == 's') {
    NotificationServer notification_server(my_address, 7777, 8888);
    std::thread notification_server_thread = notification_server.Run();
    store.flushall();
    test_server(store, atoi(argv[4]));
    notification_server_thread.join();
  } else {
    test_client(store, from_hex(argv[4]));
  }

  exit_thread.join();
  store.join_tasks();
  return 0;
}
