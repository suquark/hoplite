#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <errno.h>

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

#include "global_control_store.h"
#include "logging.h"
#include "object_store.grpc.pb.h"
#include "object_writer.h"
#include "plasma_utils.h"
#include "socket_utils.h"

using namespace plasma;

using objectstore::ObjectStore;
using objectstore::PullReply;
using objectstore::PullRequest;

std::string redis_address;
std::string my_address;

std::unique_ptr<GlobalControlStoreClient> gcs_client;
std::unique_ptr<TCPServer> tcp_server;

std::chrono::high_resolution_clock::time_point start_time;

std::map<std::string, int> current_transfer;

double get_time() {
  auto now = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> time_span = now - start_time;
  return time_span.count();
}

class ObjectStoreServiceImpl final : public ObjectStore::Service {
public:
  grpc::Status Pull(grpc::ServerContext *context, const PullRequest *request,
                    PullReply *reply) {

    ObjectID object_id = ObjectID::from_binary(request->object_id());

    {
      std::lock_guard<std::mutex> guard(transfer_mutex_);
      if (current_transfer.find(object_id.hex()) == current_transfer.end()) {
        current_transfer[object_id.hex()] = 0;
      }

      if (current_transfer[object_id.hex()] < 1) {
        current_transfer[object_id.hex()]++;
      } else {
        reply->set_ok(false);
        return grpc::Status::OK;
      }
    }

    LOG(DEBUG) << get_time() << ": Received a pull request from "
               << request->puller_ip() << " for object " << object_id.hex();

    // create a TCP connection, send the object through the TCP connection
    int conn_fd;
    auto status = tcp_connect(request->puller_ip(), 6666, &conn_fd);
    DCHECK(!status) << "socket connect error";

    void *object_buffer = NULL;
    size_t object_size = 0;
    // TODO: support multiple object.
    if (tcp_server->get_pending_write() == NULL) {
      // fetch object from Plasma
      LOG(DEBUG) << "[GrpcServer] fetching a complete object from plasma";
      std::vector<ObjectBuffer> object_buffers;
      plasma_client.Get({object_id}, -1, &object_buffers);
      object_buffer = (void *)object_buffers[0].data->data();
      object_size = object_buffers[0].data->size();
      tcp_server->set_progress(object_size);
    } else {
      // fetch partial object in memory
      LOG(DEBUG) << "[GrpcServer] fetching a partial object";
      object_buffer = tcp_server->get_pending_write();
      object_size = tcp_server->get_pending_size();
    }

    // send object_id
    status = send_all(conn_fd, (void *)object_id.data(), kUniqueIDSize);
    DCHECK(!status) << "socket send error: object_id";

    // send object size
    status = send_all(conn_fd, (void *)&object_size, sizeof(object_size));
    DCHECK(!status) << "socket send error: object size";

    // send object
    int64_t cursor = 0;
    while (cursor < object_size) {
      int64_t current_progress = tcp_server->get_progress();
      if (cursor < current_progress) {
        int bytes_sent =
            send(conn_fd, object_buffer + cursor, current_progress - cursor, 0);
        DCHECK(bytes_sent > 0) << "socket send error: object content";
        cursor += bytes_sent;
      }
    }

    // receive ack
    char ack[5];
    status = recv_all(conn_fd, ack, 3);
    DCHECK(!status) << "socket recv error: ack, error code = " << errno;

    if (strcmp(ack, "OK") != 0)
      LOG(FATAL) << "ack is wrong";

    close(conn_fd);
    LOG(DEBUG) << get_time() << ": Finished a pull request from "
               << request->puller_ip() << " for object " << object_id.hex();

    {
      std::lock_guard<std::mutex> guard(transfer_mutex_);
      current_transfer[object_id.hex()]--;
    }

    reply->set_ok(true);
    return grpc::Status::OK;
  }

private:
  std::mutex transfer_mutex_;
};

void RunGRPCServer(std::string ip, int port) {
  std::string grpc_address = ip + ":" + std::to_string(port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  ObjectStoreServiceImpl service;
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> grpc_server = builder.BuildAndStart();
  LOG(INFO) << "[GprcServer] grpc server " << grpc_address << " started";
  grpc_server->Wait();
}

class DistributedObjectStore {
public:
  DistributedObjectStore(const std::string &redis_address, int redis_port,
                         const std::string &plasma_socket,
                         const std::string &my_address, int object_writer_port,
                         int grpc_port)
      : gcs_client_(GlobalControlStoreClient(redis_address, redis_port)),
        object_writer_(TCPServer(gcs_client_, plasma_client_, my_address,
                                 object_writer_port)) {
    // connect to the plasma store
    plasma_client_.Connect(plasma_socket, "");
    // create a thread to receive remote object
    object_writer_thread_ = tcp_server.run();
    // create a thread to process pull requests
    grpc_thread_ = std::thread(RunGRPCServer, my_address, grpc_port);
  }

  ObjectID put(const void *data, size_t size) {
    // generate a random object id
    ObjectID object_id = random_object_id();
    // put object into Plasma
    std::shared_ptr<Buffer> ptr;
    plasma_client_.Create(object_id, size, NULL, 0, &ptr);
    memcpy(ptr->mutable_data(), data, size);
    plasma_client_.Seal(object_id);
    gcs_client_->write_object_location(object_id.hex(), my_address);
    return object_id;
  }

  void get(ObjectID object_id, const void **data, size_t *size) {
    // get object location from redis
    while (true) {
      std::string address = gcs_client_->get_object_location(object_id.hex());

      // send pull request to one of the location
      std::string remote_grpc_address = address + ":" + std::to_string(50055);
      auto channel = grpc::CreateChannel(remote_grpc_address,
                                         grpc::InsecureChannelCredentials());
      std::unique_ptr<ObjectStore::Stub> stub(ObjectStore::NewStub(channel));
      grpc::ClientContext context;
      PullRequest request;
      PullReply reply;
      request.set_object_id(object_id.binary());
      request.set_puller_ip(my_address);
      stub->Pull(&context, request, &reply);
      if (reply.ok()) {
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
    grpc_thread_.join();
  }

  void flushall() { gcs_client_.flushall(); }

private:
  GlobalControlStoreClient gcs_client_;
  PlasmaClient plasma_client_;
  TCPServer object_writer_;
  std::thread object_writer_thread_;
  std::thread grpc_thread_;
}


void test_server(DistributedObjectStore& store, int object_size) {
  char *buffer = new char[1024 * 1024 * 1024];
  for (int i = 0; i < object_size; i++) {
    buffer[i] = i % 256;
  }

  ObjectID object_id = store.put(buffer, object_size);
  LOG(INFO) << "Object is created! object_id = " << object_id.hex();
  unsigned long crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const unsigned char *)buffer, object_size);

  LOG(INFO) << "Object CRC = " << crc;
}

void test_client(DistributedObjectStore &store, ObjectID object_id) {
  const char *buffer;
  size_t size;
  auto start = std::chrono::system_clock::now();
  store.get(object_id, (const void **)&buffer, &size);
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
  redis_address = std::string(argv[1]);
  my_address = std::string(argv[2]);

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
