#ifndef GLOBAL_CONTROL_STORE_H
#define GLOBAL_CONTROL_STORE_H

#include "common/id.h"
#include "object_store.grpc.pb.h"
#include "util/ctpl_stl.h"
#include <condition_variable>
#include <grpcpp/channel.h>
#include <grpcpp/server.h>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

constexpr int64_t inband_data_size_limit = 65536;

struct SyncReply {
  std::string sender_ip;
  size_t object_size;
  std::string inband_data;
};

class GlobalControlStoreClient {
public:
  GlobalControlStoreClient(const std::string &notification_server_address, const std::string &my_address,
                           int notification_server_port);

  void ConnectNotificationServer();

  // Write object location to the notification server.
  void WriteLocation(const ObjectID &object_id, const std::string &my_address, bool finished, size_t object_size,
                     const uint8_t *inband_data = nullptr, bool blocking = false);

  // Get object location from the notification server.
  SyncReply GetLocationSync(const ObjectID &object_id, bool occupying, const std::string &receiver_ip);

  bool HandlePullObjectFailure(const ObjectID &object_id, const std::string &receiver_ip,
                               std::string *alternative_sender_ip);

  /// Create reduce task
  /// \param reduce_dst The IP address of the node that holds the final reduced object.
  void CreateReduceTask(const std::vector<ObjectID> &objects_to_reduce, const ObjectID &reduction_id,
                        int num_reduce_objects);

private:
  const std::string &notification_server_address_;
  const std::string &my_address_;
  const int notification_server_port_;
  std::shared_ptr<grpc::Channel> notification_channel_;
  std::unique_ptr<objectstore::NotificationServer::Stub> notification_stub_;
  ctpl::thread_pool pool_;
};

#endif // GLOBAL_CONTROL_STORE_H
