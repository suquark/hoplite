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

using namespace plasma;

class ObjectStoreServiceImpl final : public ObjectStore::Service {
public:
  ObjectStoreServiceImpl(PlasmaClient &plasma_client, ObjectStoreState &state)
      : ObjectStore::Service(), plasma_client_(plasma_client), state_(state) {}

  grpc::Status Pull(grpc::ServerContext *context, const PullRequest *request,
                    PullReply *reply) {
    ObjectID object_id = ObjectID::from_binary(request->object_id());
    bool transfer_available = state_.transfer_available(object_id);
    if (!transfer_available) {
      reply->set_ok(false);
      return grpc::Status::OK;
    }

    LOG(DEBUG) << ": Received a pull request from " << request->puller_ip()
               << " for object " << object_id.hex();

    // create a TCP connection, send the object through the TCP connection
    int conn_fd;
    auto status = tcp_connect(request->puller_ip(), 6666, &conn_fd);
    DCHECK(!status) << "socket connect error";

    void *object_buffer = NULL;
    size_t object_size = 0;
    // TODO: support multiple object.
    if (state_.pending_write == NULL) {
      // fetch object from Plasma
      LOG(DEBUG) << "[GrpcServer] fetching a complete object from plasma";
      std::vector<ObjectBuffer> object_buffers;
      plasma_client_.Get({object_id}, -1, &object_buffers);
      object_buffer = (void *)object_buffers[0].data->data();
      object_size = object_buffers[0].data->size();
      state_.progress = object_size;
    } else {
      // fetch partial object in memory
      LOG(DEBUG) << "[GrpcServer] fetching a partial object";
      object_buffer = state_.pending_write;
      object_size = state_.pending_size;
    }

    // send object_id
    status = send_all(conn_fd, (void *)object_id.data(), kUniqueIDSize);
    DCHECK(!status) << "socket send error: object_id";

    // send object size
    status = send_all(conn_fd, (void *)&object_size, sizeof(object_size));
    DCHECK(!status) << "socket send error: object size";

    // send object
    int64_t cursor = 0;
    while (cursor < object_size) {
      int64_t current_progress = state_.progress;
      if (cursor < current_progress) {
        int bytes_sent =
            send(conn_fd, object_buffer + cursor, current_progress - cursor, 0);
        DCHECK(bytes_sent > 0) << "socket send error: object content";
        cursor += bytes_sent;
      }
    }

    // receive ack
    char ack[5];
    status = recv_all(conn_fd, ack, 3);
    DCHECK(!status) << "socket recv error: ack, error code = " << errno;
    if (strcmp(ack, "OK") != 0)
      LOG(FATAL) << "ack is wrong";

    close(conn_fd);
    LOG(DEBUG) << ": Finished a pull request from " << request->puller_ip()
               << " for object " << object_id.hex();

    state_.transfer_complete(object_id);
    reply->set_ok(true);
    return grpc::Status::OK;
  }


  grpc::Status ReduceTo(grpc::ServerContext *context, const ReduceToRequest *request,
                        ReduceToReply *reply) {
    int conn_fd;
    auto status = tcp_connect(request->dst_address(), 6666, &conn_fd);
    DCHECK(!status) << "socket connect error";
    ObjectID reduction_id = ObjectID::from_binary(request->reduction_id());
    ObjectID dst_object_id = ObjectID::from_binary(request->dst_object_id());
    status = send_all(conn_fd, (void *)reduction_id.data(), reduction_id.size());
    DCHECK(!status) << "socket send error: reduction_id";
    status = send_all(conn_fd, (void *)dst_object_id.data(), dst_object_id.size());
    DCHECK(!status) << "socket send error: dst_object_id";

    void *object_buffer = NULL;
    size_t object_size = 0;
    if (request->has_src_object_id()) {
      LOG(DEBUG) << "[GrpcServer] fetching a complete object from plasma";
      ObjectID src_object_id = ObjectID::from_binary(request->src_object_id());
      std::vector<ObjectBuffer> object_buffers;
      plasma_client_.Get({src_object_id}, -1, &object_buffers);
      object_buffer = (void *)object_buffers[0].data->data();
      object_size = object_buffers[0].data->size();
    } else {
      auto stream = state_.get_reduction_stream(reduction_id);
      object_buffer = stream->data();
      object_size = stream->size();
    }

    // send object
    int64_t cursor = 0;
    while (cursor < object_size) {
      int64_t current_progress = stream->reduce_progress;
      if (cursor < current_progress) {
        int bytes_sent =
            send(conn_fd, object_buffer + cursor, current_progress - cursor, 0);
        DCHECK(bytes_sent > 0) << "socket send error: object content";
        cursor += bytes_sent;
      }
    }

    // receive ack
    char ack[5];
    status = recv_all(conn_fd, ack, 3);
    DCHECK(!status) << "socket recv error: ack, error code = " << errno;
    if (strcmp(ack, "OK") != 0)
      LOG(FATAL) << "ack is wrong";

    close(conn_fd);
    LOG(DEBUG) << ": Finished a pull request from " << request->puller_ip()
               << " for object " << object_id.hex();

    state_.transfer_complete(object_id);
    reply->set_ok(true);
    return grpc::Status::OK;
  }

private:
  ObjectStoreState &state_;
  PlasmaClient &plasma_client_;
};

GrpcServer::GrpcServer(PlasmaClient &plasma_client, ObjectStoreState &state,
                       const std::string &my_address, int port)
    : my_address_(my_address), grpc_port_(port), state_(state),
      service_(std::make_shared<ObjectStoreServiceImpl>(plasma_client, state)) {
  std::string grpc_address = my_address + ":" + std::to_string(port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&*service_);
  grpc_server_ = builder.BuildAndStart();
}

bool GrpcServer::PullObject(const std::string &remote_address,
                            const plasma::ObjectID &object_id) {
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

void GrpcServer::worker_loop() {
  LOG(INFO) << "[GprcServer] grpc server " << my_address_ << " started";

  grpc_server_->Wait();
}
