#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <hiredis.h>
#include <iostream>
#include <plasma/client.h>
#include <plasma/common.h>
#include <plasma/test_util.h>
#include <string>
#include <thread>
#include <unistd.h>

#include "object_store.grpc.pb.h"

using namespace plasma;

using objectstore::ObjectStore;
using objectstore::PullReply;
using objectstore::PullRequest;

std::string redis_address;
std::string my_address;

ObjectID put(redisContext *redis, PlasmaClient *client, void *data,
             size_t size) {
  // generate a random object id
  ObjectID object_id = random_object_id();
  // put object into Plasma
  // put object location information into redis
  return object_id;
}

void get(redisContext *redis, PlasmaClient *client, ObjectID object_id,
         void **data, size_t *size) {
  // get object location from redis
  // send pull request to one of the location
  // put object into Plasma
}

class ObjectStoreServiceImpl final : public ObjectStore::Service {
public:
  grpc::Status Pull(grpc::ServerContext *context, const PullRequest *request,
                    PullReply *reply) {
    return grpc::Status::OK;
  }
};

int main(int argc, char **argv) {
  redis_address = std::string(argv[1]);
  my_address = std::string(argv[2]);
  // create a polling thread to process pull requests

  // create a redis client
  redisContext *redis;
  redis = redisConnect(redis_address.c_str(), 6379);
  // create a plasma client
  PlasmaClient client;
  client.Connect("/tmp/plasma", "");
  client.Disconnect();

  return 0;
}
