#ifndef OBJECT_CONTROL_H
#define OBJECT_CONTROL_H

#include <grpcpp/server.h>
#include <thread>
#include <unordered_map>

#include "local_store_client.h"
#include "object_sender.h"
#include "object_store.grpc.pb.h"
#include "object_store_state.h"

class ObjectStoreServiceImpl;

class GrpcServer {
public:
  GrpcServer(ObjectSender &object_sender,
             LocalStoreClient &local_store_client, 
             GlobalControlStoreClient &gcs_client,
             ObjectStoreState &state, const std::string &my_address,
             int port);

  inline std::thread Run() {
    return std::thread(&GrpcServer::worker_loop, this);
  }

  bool PullObject(const std::string &remote_grpc_address,
                  const ObjectID &object_id);

  bool InvokeReduceTo(const std::string &remote_address,
                      const ObjectID &reduction_id,
                      const std::vector<ObjectID> &dst_object_ids,
                      const std::string &dst_address, bool is_endpoint,
                      const ObjectID *src_object_id = nullptr);

private:
  void worker_loop();
  const int grpc_port_;
  const std::string &my_address_;
  ObjectStoreState &state_;
  std::unique_ptr<grpc::Server> grpc_server_;
  std::shared_ptr<ObjectStoreServiceImpl> service_;
  std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channel_pool_;
  std::unordered_map<std::string,
                     std::unique_ptr<objectstore::ObjectStore::Stub>>
      object_store_stub_pool_;
  void create_stub(const std::string &remote_grpc_address);
};

#endif // OBJECT_CONTROL_H
