#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "global_control_store.h"
#include "logging.h"

using objectstore::ConnectReply;
using objectstore::ConnectRequest;
using objectstore::CreateReduceTaskReply;
using objectstore::CreateReduceTaskRequest;
using objectstore::GetLocationSyncReply;
using objectstore::GetLocationSyncRequest;
using objectstore::HandlePullObjectFailureReply;
using objectstore::HandlePullObjectFailureRequest;
using objectstore::HandleReceiveReducedObjectFailureReply;
using objectstore::HandleReceiveReducedObjectFailureRequest;
using objectstore::WriteLocationReply;
using objectstore::WriteLocationRequest;

////////////////////////////////////////////////////////////////
// The object notification structure
////////////////////////////////////////////////////////////////

GlobalControlStoreClient::GlobalControlStoreClient(const std::string &notification_server_address,
                                                   const std::string &my_address, int notification_server_port)
    : notification_server_address_(notification_server_address), my_address_(my_address),
      notification_server_port_(notification_server_port), pool_(2) {
  TIMELINE("GlobalControlStoreClient");
  auto remote_notification_server_address =
      notification_server_address_ + ":" + std::to_string(notification_server_port_);
  LOG(DEBUG) << "remote_notification_server_address " << remote_notification_server_address;
  notification_channel_ = grpc::CreateChannel(remote_notification_server_address, grpc::InsecureChannelCredentials());
  notification_stub_ = objectstore::NotificationServer::NewStub(notification_channel_);
  LOG(DEBUG) << "notification_stub_ created";
}

void GlobalControlStoreClient::ConnectNotificationServer() {
  grpc::ClientContext context;
  ConnectRequest request;
  request.set_sender_ip(my_address_);
  ConnectReply reply;
  auto status = notification_stub_->Connect(&context, request, &reply);
  DCHECK(status.ok()) << status.error_message();
}

void GlobalControlStoreClient::WriteLocation(const ObjectID &object_id, const std::string &sender_ip, bool finished,
                                             size_t object_size, const uint8_t *inband_data, bool blocking) {
  TIMELINE("GlobalControlStoreClient::WriteLocation");
  LOG(DEBUG) << "[GlobalControlStoreClient] Adding object " << object_id.Hex()
             << " to notification server with address = " << sender_ip << ".";
  // FIXME(suquark): Figure out why running `WriteLocation` in a thread pool
  // would fail when we are running multicast repeatly with certain object sizes
  // (65K~300K).
  WriteLocationRequest request;
  request.set_object_id(object_id.Binary());
  request.set_sender_ip(sender_ip);
  request.set_finished(finished);
  request.set_object_size(object_size);
  // inline small data buffer into the message
  if (finished && object_size <= inband_data_size_limit && inband_data != nullptr) {
    request.set_inband_data(inband_data, object_size);
  }
  if (blocking) {
    grpc::ClientContext context;
    WriteLocationReply reply;
    auto status = notification_stub_->WriteLocation(&context, request, &reply);
    DCHECK(status.ok()) << status.error_message();
    DCHECK(reply.ok()) << "WriteLocation for " << object_id.ToString() << " failed.";
  } else {
    pool_.push(
        [this, object_id](int id, WriteLocationRequest request) {
          grpc::ClientContext context;
          WriteLocationReply reply;
          auto status = notification_stub_->WriteLocation(&context, request, &reply);
          DCHECK(status.ok()) << status.error_message();
          DCHECK(reply.ok()) << "WriteLocation for " << object_id.ToString() << " failed.";
        },
        std::move(request));
  }
}

SyncReply GlobalControlStoreClient::GetLocationSync(const ObjectID &object_id, bool occupying,
                                                    const std::string &receiver_ip) {
  TIMELINE("GetLocationSync");
  grpc::ClientContext context;
  GetLocationSyncRequest request;
  GetLocationSyncReply reply;
  request.set_object_id(object_id.Binary());
  request.set_occupying(occupying);
  request.set_receiver_ip(receiver_ip);
  notification_stub_->GetLocationSync(&context, request, &reply);
  return {std::string(reply.sender_ip()), reply.object_size(), reply.inband_data()};
}

bool GlobalControlStoreClient::HandlePullObjectFailure(const ObjectID &object_id, const std::string &receiver_ip,
                                                       std::string *alternative_sender_ip) {
  TIMELINE("HandlePullObjectFailure");
  grpc::ClientContext context;
  HandlePullObjectFailureRequest request;
  HandlePullObjectFailureReply reply;
  request.set_object_id(object_id.Binary());
  request.set_receiver_ip(receiver_ip);
  auto status = notification_stub_->HandlePullObjectFailure(&context, request, &reply);
  DCHECK(status.ok()) << status.error_message();
  *alternative_sender_ip = reply.alternative_sender_ip();
  return reply.success();
}

void GlobalControlStoreClient::HandleReceiveReducedObjectFailure(const ObjectID &reduction_id,
                                                                 const std::string &receiver_ip,
                                                                 const std::string &sender_ip) {
  TIMELINE("HandleReceiveReducedObjectFailure");
  grpc::ClientContext context;
  HandleReceiveReducedObjectFailureRequest request;
  HandleReceiveReducedObjectFailureReply reply;
  request.set_reduction_id(reduction_id.Binary());
  request.set_receiver_ip(receiver_ip);
  request.set_sender_ip(sender_ip);
  auto status = notification_stub_->HandleReceiveReducedObjectFailure(&context, request, &reply);
  DCHECK(status.ok()) << status.error_message();
}

void GlobalControlStoreClient::CreateReduceTask(const std::vector<ObjectID> &objects_to_reduce,
                                                const ObjectID &reduction_id, int num_reduce_objects) {
  TIMELINE("CreateReduceTask");
  grpc::ClientContext context;
  CreateReduceTaskRequest request;
  CreateReduceTaskReply reply;
  request.set_reduce_dst(my_address_);
  request.set_reduction_id(reduction_id.Binary());
  request.set_num_reduce_objects(num_reduce_objects);
  for (auto &object_id : objects_to_reduce) {
    request.add_objects_to_reduce(object_id.Binary());
  }
  auto status = notification_stub_->CreateReduceTask(&context, request, &reply);
  DCHECK(status.ok()) << status.error_message();
}
