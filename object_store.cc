#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <errno.h>

#include "object_store.grpc.pb.h"
#include <iostream>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <plasma/client.h>
#include <plasma/common.h>
#include <plasma/test_util.h>
#include <signal.h>
#include <stdlib.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <zlib.h>

#include "logging.h"

#include "global_control_store.h"
#include "object_control.h"
#include "object_store_state.h"
#include "object_writer.h"
#include "plasma_utils.h"
#include "socket_utils.h"

using namespace plasma;

std::chrono::high_resolution_clock::time_point start_time;

double get_time() {
  auto now = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> time_span = now - start_time;
  return time_span.count();
}

class DistributedObjectStore {
public:
  DistributedObjectStore(const std::string &redis_address, int redis_port,
                         const std::string &plasma_socket,
                         const std::string &my_address, int object_writer_port,
                         int grpc_port)
      : my_address_(my_address),
        gcs_client_(GlobalControlStoreClient(redis_address, redis_port)),
        object_control_(GrpcServer(state_, my_address, grpc_port)),
        object_writer_(TCPServer(state_, gcs_client_, plasma_client_,
                                 my_address, object_writer_port)) {
    // connect to the plasma store
    plasma_client_.Connect(plasma_socket, "");
    // create a thread to receive remote object
    object_writer_thread_ = object_writer_.run();
    // create a thread to process pull requests
    object_control_thread_ = object_control_.Run();
  }

  ObjectID Put(const void *data, size_t size) {
    // generate a random object id
    ObjectID object_id = random_object_id();
    // put object into Plasma
    std::shared_ptr<Buffer> ptr;
    plasma_client_.Create(object_id, size, NULL, 0, &ptr);
    memcpy(ptr->mutable_data(), data, size);
    plasma_client_.Seal(object_id);
    gcs_client_->write_object_location(object_id.hex(), my_address_);
    return object_id;
  }

  void Get(ObjectID object_id, const void **data, size_t *size) {
    // get object location from redis
    while (true) {
      std::string address = gcs_client_->get_object_location(object_id.hex());

      // send pull request to one of the location
      bool reply_ok = object_control_.PullObject(address, object_id);

      if (reply_ok) {
        break;
      }
      // if the sender is busy, wait for 1 millisecond and try again
      usleep(1000);
    }

    // get object from Plasma
    std::vector<ObjectBuffer> object_buffers;
    plasma_client_.Get({object_id}, -1, &object_buffers);

    *data = object_buffers[0].data->data();
    *size = object_buffers[0].data->size();
  }

  void join_tasks() {
    object_writer_thread_.join();
    object_control_thread_.join();
  }

  void flushall() { gcs_client_.flushall(); }

private:
  const std::string my_address_;
  ObjectStoreState state_;
  GlobalControlStoreClient gcs_client_;
  PlasmaClient plasma_client_;
  TCPServer object_writer_;
  GrpcServer object_control_;
  std::thread object_writer_thread_;
  std::thread object_control_thread_;
};

void test_server(DistributedObjectStore &store, int object_size) {
  char *buffer = new char[1024 * 1024 * 1024];
  for (int i = 0; i < object_size; i++) {
    buffer[i] = i % 256;
  }

  ObjectID object_id = store.Put(buffer, object_size);
  LOG(INFO) << "Object is created! object_id = " << object_id.hex();
  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const unsigned char *)buffer, object_size);

  LOG(INFO) << "Object CRC = " << crc;
}

void test_client(DistributedObjectStore &store, ObjectID object_id) {
  const char *buffer;
  size_t size;
  auto start = std::chrono::system_clock::now();
  store.Get(object_id, (const void **)&buffer, &size);
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> duration = end - start;
  LOG(INFO) << "Object is retrieved using " << duration.count() << " seconds";

  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const unsigned char *)buffer, size);
  LOG(INFO) << "Object retrieved CRC = " << crc;
}

int main(int argc, char **argv) {
  // signal(SIGPIPE, SIG_IGN);
  start_time = std::chrono::high_resolution_clock::now();
  std::string redis_address = std::string(argv[1]);
  std::string my_address = std::string(argv[2]);

  ::ray::RayLog::StartRayLog(my_address + ": ");

  DistributedObjectStore store(redis_address, 6380, "/tmp/multicast_plasma",
                               my_address, 6666, 50055);

  if (argv[3][0] == 's') {
    store.flushall();
    test_server(store, atoi(argv[4]));
  } else {
    test_client(store, from_hex(argv[4]));
  }

  store.join_tasks();
  return 0;
}
