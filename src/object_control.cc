#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <sys/socket.h>
#include <unistd.h>

#include "logging.h"
#include "object_control.h"
#include "object_store.grpc.pb.h"
#include "socket_utils.h"

using objectstore::ObjectStore;
using objectstore::PullReply;
using objectstore::PullRequest;
using objectstore::ReduceToReply;
using objectstore::ReduceToRequest;

using namespace plasma;

class ObjectStoreServiceImpl final : public ObjectStore::Service {
public:
  ObjectStoreServiceImpl(ObjectSender &object_sender,
                         LocalStoreClient &local_store_client,
                         ObjectStoreState &state)
      : ObjectStore::Service(), object_sender_(object_sender),
        local_store_client_(local_store_client), state_(state) {}

  grpc::Status Pull(grpc::ServerContext *context, const PullRequest *request,
                    PullReply *reply) {
    TIMELINE("ObjectStoreServiceImpl::Pull()");
    ObjectID object_id = ObjectID::from_binary(request->object_id());
    bool transfer_available = state_.transfer_available(object_id);
    if (!transfer_available) {
      reply->set_ok(false);
      return grpc::Status::OK;
    }

    LOG(DEBUG) << ": Received a pull request from " << request->puller_ip()
               << " for object " << object_id.hex();

    object_sender_.send_object(request);
    LOG(DEBUG) << ": Finished a pull request from " << request->puller_ip()
               << " for object " << object_id.hex();

    state_.transfer_complete(object_id);
    reply->set_ok(true);
    return grpc::Status::OK;
  }

  grpc::Status ReduceTo(grpc::ServerContext *context,
                        const ReduceToRequest *request, ReduceToReply *reply) {
    TIMELINE("ObjectStoreServiceImpl::ReduceTo()");
    object_sender_.AppendTask(request);
    reply->set_ok(true);
    return grpc::Status::OK;
  }

private:
  ObjectSender &object_sender_;
  ObjectStoreState &state_;
  LocalStoreClient &local_store_client_;
};

GrpcServer::GrpcServer(ObjectSender &object_sender,
                       LocalStoreClient &local_store_client,
                       ObjectStoreState &state, const std::string &my_address,
                       int port)
    : my_address_(my_address), grpc_port_(port), state_(state),
      service_(std::make_shared<ObjectStoreServiceImpl>(
          object_sender, local_store_client, state)) {
  TIMELINE("GrpcServer construction function");
  std::string grpc_address = my_address + ":" + std::to_string(port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
}

bool GrpcServer::PullObject(const std::string &remote_address,
                            const plasma::ObjectID &object_id) {
  TIMELINE("GrpcServer::PullObject");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  auto channel = grpc::CreateChannel(remote_grpc_address,
                                     grpc::InsecureChannelCredentials());
  std::unique_ptr<ObjectStore::Stub> stub(ObjectStore::NewStub(channel));
  grpc::ClientContext context;
  PullRequest request;
  PullReply reply;
  request.set_object_id(object_id.binary());
  request.set_puller_ip(my_address_);
  stub->Pull(&context, request, &reply);
  return reply.ok();
}

bool GrpcServer::InvokeReduceTo(
    const std::string &remote_address, const ObjectID &reduction_id,
    const std::vector<plasma::ObjectID> &dst_object_ids,
    const std::string &dst_address, bool is_endpoint,
    const ObjectID *src_object_id) {
  TIMELINE("GrpcServer::InvokeReduceTo");
  auto remote_grpc_address = remote_address + ":" + std::to_string(grpc_port_);
  auto channel = grpc::CreateChannel(remote_grpc_address,
                                     grpc::InsecureChannelCredentials());
  std::unique_ptr<ObjectStore::Stub> stub(ObjectStore::NewStub(channel));
  grpc::ClientContext context;
  ReduceToRequest request;
  ReduceToReply reply;

  request.set_reduction_id(reduction_id.binary());
  for (auto &object_id : dst_object_ids) {
    request.add_dst_object_ids(object_id.binary());
  }
  request.set_dst_address(dst_address);
  request.set_is_endpoint(is_endpoint);
  if (src_object_id != nullptr) {
    request.set_src_object_id(src_object_id->binary());
  }
  auto status = stub->ReduceTo(&context, request, &reply);
  DCHECK(status.ok()) << "[GrpcServer] ReduceTo failed at remote address:"
                      << remote_grpc_address
                      << ", message: " << status.error_message()
                      << ", details = " << status.error_code();

  return reply.ok();
}

void GrpcServer::worker_loop() {
  LOG(INFO) << "[GprcServer] grpc server " << my_address_ << " started";

  grpc_server_->Wait();
}
