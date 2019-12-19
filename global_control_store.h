#ifndef GLOBAL_CONTROL_STORE_H
#define GLOBAL_CONTROL_STORE_H

#include <string>

struct redisContext;

class GlobalControlStoreClient {
  public:
    GlobalControlStoreClient(const std::string& redis_address, int port);

    // Write object location to Redis server.
    void write_object_location(const std::string &object_id_hex, const std::string &my_address);

    // Get object location from Redis server.
    std::string get_object_location(const std::string &hex);

    // Clean up Redis store
    void flushall();

  private:
    redisContext *redis_client_;
};


#endif // GLOBAL_CONTROL_STORE_H
