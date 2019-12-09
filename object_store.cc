#include <arpa/inet.h>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <hiredis.h>
#include <iostream>
#include <netinet/in.h>
#include <plasma/client.h>
#include <plasma/common.h>
#include <plasma/test_util.h>
#include <stdlib.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "object_store.grpc.pb.h"

using namespace plasma;

using objectstore::ObjectStore;
using objectstore::PullReply;
using objectstore::PullRequest;

std::string redis_address;
std::string my_address;

PlasmaClient plasma_client;
redisContext *redis_client;

ObjectID put(const void *data, size_t size) {
  // generate a random object id
  ObjectID object_id = random_object_id();
  // put object into Plasma
  std::shared_ptr<Buffer> ptr;
  plasma_client.Create(object_id, size, NULL, 0, &ptr);
  memcpy(ptr->mutable_data(), data, size);
  plasma_client.Seal(object_id);
  // put object location information into redis
  redisReply *redis_reply = (redisReply *)redisCommand(
      redis_client, "SET %s %s", object_id.hex(), my_address.c_str());
  freeReplyObject(redis_reply);

  return object_id;
}

void get(ObjectID object_id, const void **data, size_t *size) {
  // get object location from redis
  redisReply *redis_reply =
      (redisReply *)redisCommand(redis_client, "GET %s", object_id.hex());
  std::string address = std::string(redis_reply->str);
  freeReplyObject(redis_reply);

  // send pull request to one of the location
  std::string remote_grpc_address = address + ":" + std::to_string(50051);
  auto channel = grpc::CreateChannel(remote_grpc_address,
                                     grpc::InsecureChannelCredentials());
  std::unique_ptr<ObjectStore::Stub> stub(ObjectStore::NewStub(channel));
  grpc::ClientContext context;
  PullRequest request;
  PullReply reply;
  request.set_object_id(object_id.binary());
  request.set_puller_ip(my_address);
  stub->Pull(&context, request, &reply);

  // get object from Plasma
  std::vector<ObjectBuffer> object_buffers;
  plasma_client.Get({object_id}, -1, &object_buffers);

  *data = object_buffers[0].data->data();
  *size = object_buffers[0].data->size();
}

class ObjectStoreServiceImpl final : public ObjectStore::Service {
public:
  grpc::Status Pull(grpc::ServerContext *context, const PullRequest *request,
                    PullReply *reply) {
    ObjectID object_id = ObjectID::from_binary(request->object_id());
    // create a TCP connection, send the object through the TCP connection
    struct sockaddr_in push_addr;
    push_addr.sin_port = 6666;
    int conn_fd = socket(AF_INET, SOCK_STREAM, 0);
    inet_pton(AF_INET, request->puller_ip().c_str(), &push_addr.sin_addr);
    connect(conn_fd, (struct sockaddr *)&push_addr, sizeof(push_addr));
    // fetech object from Plasma
    std::vector<ObjectBuffer> object_buffers;
    plasma_client.Get({object_id}, -1, &object_buffers);
    // send object_id
    send(conn_fd, object_id.data(), kUniqueIDSize, 0);
    // send object size
    long object_size = object_buffers[0].data->size();
    send(conn_fd, &object_size, sizeof(long), 0);
    // send object
    send(conn_fd, object_buffers[0].data->data(), object_size, 0);
    plasma_client.Release(object_id);
    close(conn_fd);
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
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt,
             sizeof(opt));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  bind(server_fd, (struct sockaddr *)&address, sizeof(address));
  listen(server_fd, 10);

  while (true) {
    char obj_id[kUniqueIDSize];
    long object_size;
    conn_fd = accept(server_fd, (struct sockaddr *)&address, &addrlen);
    recv(conn_fd, obj_id, kUniqueIDSize, 0);
    ObjectID object_id = ObjectID::from_binary(obj_id);
    recv(conn_fd, &object_size, sizeof(long), 0);
    std::shared_ptr<Buffer> ptr;
    plasma_client.Create(object_id, object_size, NULL, 0, &ptr);
    recv(conn_fd, ptr->mutable_data(), object_size, 0);
    plasma_client.Seal(object_id);
    close(conn_fd);
  }
}

void RunGRPCServer(std::string ip, int port) {
  std::string grpc_address = ip + ":" + std::to_string(port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  ObjectStoreServiceImpl service;
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> grpc_server = builder.BuildAndStart();
  grpc_server->Wait();
}

void test_server() {
  ObjectID object_id = put("abc", 3);
  std::cout << "Object is created!" << std::endl;
  std::cout << object_id.hex() << std::endl;
  while (true) {
  }
}

void test_client(ObjectID object_id) {
  const char *buffer;
  size_t size;
  get(object_id, (const void **)&buffer, &size);
  std::string a;
  a.assign(buffer, size);
  std::cout << "Object is retrieved:" << std::endl;
  std::cout << a << std::endl;
  while (true) {
  }
}

unsigned char hex_to_dec(char a) {
  if (a < '9') {
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
  return ObjectID::from_binary(binary);
}

int main(int argc, char **argv) {
  redis_address = std::string(argv[1]);
  my_address = std::string(argv[2]);
  // create a thread to receive remote object
  std::thread tcp_thread(RunTCPServer, my_address, 6666);
  // create a thread to process pull requests
  std::thread grpc_thread(RunGRPCServer, my_address, 50051);
  // create a redis client
  redis_client = redisConnect(redis_address.c_str(), 6379);
  // create a plasma client
  plasma_client.Connect("/tmp/plasma", "");

  if (argv[3][0] == 's') {
    test_server();
  } else {
    test_client(from_hex(argv[4]));
  }

  return 0;
}
