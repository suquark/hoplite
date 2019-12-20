#include <hiredis.h>

#include "global_control_store.h"
#include "logging.h"

GlobalControlStoreClient::GlobalControlStoreClient(
    const std::string &redis_address, int port) {
  // create a redis client
  redis_client_ = redisConnect(redis_address.c_str(), port);
  LOG(INFO) << "[RedisClient] Connected to Redis server running at "
            << redis_address << ":" << port << ".";
}

void GlobalControlStoreClient::write_object_location(
    const std::string &object_id_hex, const std::string &my_address) {
  LOG(INFO) << "[RedisClient] Adding object " << object_id_hex
            << " to Redis with address = " << my_address << ".";
  redisReply *redis_reply = (redisReply *)redisCommand(
      redis_client_, "LPUSH %s %s", object_id_hex.c_str(), my_address.c_str());
  freeReplyObject(redis_reply);
}

void GlobalControlStoreClient::flushall() {
  redisReply *reply = (redisReply *)redisCommand(redis_client_, "FLUSHALL");
  freeReplyObject(reply);
}

std::string
GlobalControlStoreClient::get_object_location(const std::string &hex) {
  redisReply *redis_reply =
      (redisReply *)redisCommand(redis_client_, "LRANGE %s 0 -1", hex.c_str());

  int num_of_copies = redis_reply->elements;
  DCHECK(num_of_copies > 0) << "cannot find object " << hex << " in Redis";

  std::string address =
      std::string(redis_reply->element[rand() % num_of_copies]->str);

  freeReplyObject(redis_reply);
  return address;
}
