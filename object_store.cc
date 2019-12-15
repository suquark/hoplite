#include <arpa/inet.h>
#include <chrono>
#include <ctime>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <hiredis.h>
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
#include <errno.h>

#include "logging.h"
#include "object_store.grpc.pb.h"

using namespace plasma;
#define LOG(level) RAY_LOG(level) << my_address << ": "


using objectstore::ObjectStore;
using objectstore::PullReply;
using objectstore::PullRequest;

std::string redis_address;
std::string my_address;

PlasmaClient plasma_client;
redisContext *redis_client;

std::chrono::high_resolution_clock::time_point start_time;

std::map<std::string, int> current_transfer;
std::mutex transfer_mutex;


double get_time() {
  auto now = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> time_span = now - start_time;
  return time_span.count();
}


int send_all(int conn_fd, const void *buf, const size_t size) {
  size_t cursor = 0;
  while (cursor < size) {
    int bytes_sent = send(conn_fd, buf + cursor, size - cursor, 0);
    if (bytes_sent < 0) {
      LOG(ERROR) << "Socket send error (code=" << errno << ")";
      return bytes_sent;
    }
    cursor += bytes_sent;
  }
  return 0;
}


int recv_all(int conn_fd, void *buf, const size_t size) {
  size_t cursor = 0;
  while (cursor < size) {
    int bytes_recv = recv(conn_fd, buf + cursor, size - cursor, 0);
    if (bytes_recv < 0) {
      LOG(ERROR) << "Socket recv error (code=" << errno << ")";
      return bytes_recv;
    }
    cursor += bytes_recv;
  }
  return 0;
}


void write_object_location(const std::string &hex) {
  redisReply *redis_reply = (redisReply *)redisCommand(
      redis_client, "LPUSH %s %s", hex.c_str(), my_address.c_str());
  freeReplyObject(redis_reply);
}

std::string get_object_location(const std::string &hex) {
  redisReply *redis_reply =
      (redisReply *)redisCommand(redis_client, "LRANGE %s 0 -1", hex.c_str());

  int num_of_copies = redis_reply->elements;

  if (num_of_copies == 0) {
    std::cout << "cannot find object " << hex << " in Redis" << std::endl;
    exit(-1);
  }

  std::string address =
      std::string(redis_reply->element[rand() % num_of_copies]->str);

  freeReplyObject(redis_reply);

  return address;
}

ObjectID put(const void *data, size_t size) {
  // generate a random object id
  ObjectID object_id = random_object_id();
  // put object into Plasma
  std::shared_ptr<Buffer> ptr;
  plasma_client.Create(object_id, size, NULL, 0, &ptr);
  memcpy(ptr->mutable_data(), data, size);
  plasma_client.Seal(object_id);

  write_object_location(object_id.hex());
  return object_id;
}

void get(ObjectID object_id, const void **data, size_t *size) {
  // get object location from redis
  while (true) {
    std::string address = get_object_location(object_id.hex());

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
  plasma_client.Get({object_id}, -1, &object_buffers);

  *data = object_buffers[0].data->data();
  *size = object_buffers[0].data->size();

  write_object_location(object_id.hex());
}

class ObjectStoreServiceImpl final : public ObjectStore::Service {
public:
  grpc::Status Pull(grpc::ServerContext *context, const PullRequest *request,
                    PullReply *reply) {

    ObjectID object_id = ObjectID::from_binary(request->object_id());

    {
      std::lock_guard<std::mutex> guard(transfer_mutex);
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

    std::cout << get_time() << ": Received a pull request from "
              << request->puller_ip() << " for object " << object_id.hex()
              << std::endl;

    // create a TCP connection, send the object through the TCP connection
    struct sockaddr_in push_addr;
    int conn_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (conn_fd < 0) {
      std::cout << "socket creation error" << std::endl;
      exit(-1);
    }
    std::string puller_ip = request->puller_ip();
    push_addr.sin_family = AF_INET;
    push_addr.sin_addr.s_addr = inet_addr(puller_ip.c_str());
    push_addr.sin_port = htons(6666);
    int success =
        connect(conn_fd, (struct sockaddr *)&push_addr, sizeof(push_addr));
    if (success < 0) {
      std::cout << "socket connect error" << std::endl;
      exit(-1);
    }
    // fetech object from Plasma
    std::vector<ObjectBuffer> object_buffers;
    plasma_client.Get({object_id}, -1, &object_buffers);
    // send object_id
    success = send_all(conn_fd, (void*)object_id.data(), kUniqueIDSize);
    if (success < 0) {
      LOG(FATAL) << "socket send error: object_id";
    }

    // send object size
    long object_size = object_buffers[0].data->size();
    success = send_all(conn_fd, (void*)&object_size, sizeof(object_size));
    if (success < 0) {
      LOG(FATAL) << "socket send error: object size";
    }

    // send object
    success = send_all(conn_fd, (void*)object_buffers[0].data->data(), object_size);
    if (success < 0) {
      LOG(FATAL) << "socket send error: object content";
    }

    char ack[5];
    int status = recv_all(conn_fd, ack, 3);
    if (status) {
      LOG(FATAL) << "socket recv error: ack, error code = " << errno;
    }

    if (strcmp(ack, "OK") != 0) {
      LOG(FATAL) << "ack is wrong";
    }

    close(conn_fd);
    std::cout << get_time() << ": Finished a pull request from "
              << request->puller_ip() << " for object " << object_id.hex()
              << std::endl;

    {
      std::lock_guard<std::mutex> guard(transfer_mutex);
      current_transfer[object_id.hex()]--;
    }

    reply->set_ok(true);
    return grpc::Status::OK;
  }
};

void RunTCPServer(std::string ip, int port) {
  // data format:
  // [object_id (160bit), size (64bit), object]
  int server_fd, conn_fd;
  struct sockaddr_in address;
  socklen_t addrlen = sizeof(address);
  int opt = 1;

  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  bind(server_fd, (struct sockaddr *)&address, sizeof(address));
  listen(server_fd, 10);

  std::cout << "tcp server is ready at " << ip << ":" << port << std::endl;

  while (true) {
    char obj_id[kUniqueIDSize];
    long object_size;
    conn_fd = accept(server_fd, (struct sockaddr *)&address, &addrlen);
    if (conn_fd < 0) {
      std::cout << "socket accept error" << std::endl;
      exit(-1);
    }

    auto status = recv_all(conn_fd, obj_id, kUniqueIDSize);
    if (status) {
      LOG(FATAL) << "socket recv error: object id";
    }
    ObjectID object_id = ObjectID::from_binary(obj_id);
    status = recv_all(conn_fd, &object_size, sizeof(object_size));
    if (status) {
      LOG(FATAL) << "socket recv error: object size";
    }
    std::shared_ptr<Buffer> ptr;
    plasma_client.Create(object_id, object_size, NULL, 0, &ptr);

    status = recv_all(conn_fd, ptr->mutable_data(), object_size);
    if (status) {
      LOG(FATAL) << "socker recv error: object content";
    }

    status = send_all(conn_fd, "OK", 3);
    if (status) {
      LOG(FATAL) << "socket send error: object ack";
    }
    plasma_client.Seal(object_id);
    close(conn_fd);
    LOG(INFO) << "sending object completes";
  }
}

void RunGRPCServer(std::string ip, int port) {
  std::string grpc_address = ip + ":" + std::to_string(port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  ObjectStoreServiceImpl service;
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> grpc_server = builder.BuildAndStart();
  LOG(INFO) << "grpc server " << grpc_address << " started";
  grpc_server->Wait();
}

void test_server(int object_size) {
  char *buffer = new char[1024 * 1024 * 1024];
  for (int i = 0; i < object_size; i++) {
    buffer[i] = 'r';
  }

  ObjectID object_id = put(buffer, object_size);
  std::cout << "Object is created!" << std::endl;
  std::cout << object_id.hex() << std::endl;
}

void test_client(ObjectID object_id) {
  const char *buffer;
  size_t size;
  auto start = std::chrono::system_clock::now();
  get(object_id, (const void **)&buffer, &size);
  auto end = std::chrono::system_clock::now();
  std::chrono::duration<double> duration = end - start;
  std::cout << "Object is retrieved using " << duration.count() << " seconds"
            << std::endl;
}

unsigned char hex_to_dec(char a) {
  if (a <= '9') {
    return a - '0';
  } else {
    return a - 'a' + 10;
  }
}

ObjectID from_hex(char *hex) {
  unsigned char id[kUniqueIDSize];
  for (int i = 0; i < kUniqueIDSize; i++) {
    id[i] = hex_to_dec(hex[2 * i]) * 16 + hex_to_dec(hex[2 * i + 1]);
  }
  std::string binary = std::string((char *)id, kUniqueIDSize);

  ObjectID object_id = ObjectID::from_binary(binary);
  if (object_id.hex().compare(hex) != 0) {
    std::cout << object_id.hex() << std::endl;
    std::cout << "error in decoding object id" << std::endl;
    exit(-1);
  }

  return object_id;
}

int main(int argc, char **argv) {
  // signal(SIGPIPE, SIG_IGN);
  start_time = std::chrono::high_resolution_clock::now();
  redis_address = std::string(argv[1]);
  my_address = std::string(argv[2]);
  // create a thread to receive remote object
  std::thread tcp_thread(RunTCPServer, my_address, 6666);
  // create a thread to process pull requests
  std::thread grpc_thread(RunGRPCServer, my_address, 50055);
  // create a redis client
  redis_client = redisConnect(redis_address.c_str(), 6380);
  std::cout << "Connected to Redis server running at " << redis_address
            << std::endl;

  // create a plasma client
  plasma_client.Connect("/tmp/multicast_plasma", "");

  if (argv[3][0] == 's') {
    redisReply *reply = (redisReply *)redisCommand(redis_client, "FLUSHALL");
    freeReplyObject(reply);

    test_server(atoi(argv[4]));
  } else {
    test_client(from_hex(argv[4]));
  }

  tcp_thread.join();
  grpc_thread.join();

  return 0;
}
