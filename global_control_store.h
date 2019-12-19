#pragma once

struct redisContext;

class GlobalControlStoreClient {
  public:
    GlobalControlStoreClient(const string& redis_address, int port);

    // Write object location to Redis server.
    void write_object_location(const std::string &object_id_hex, const std::string &my_address);

    // Get object location from Redis server.
    std::string get_object_location(const std::string &hex);

  private:
    redisContext *redis_client_;
}