#include <cstdint>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <plasma/client.h>
#include <plasma/common.h>

#include "global_control_store.h"
#include "logging.h"
#include "object_writer.h"
#include "socket_utils.h"
#include "protocol.h"

using namespace plasma;

TCPServer::TCPServer(ObjectStoreState &state,
                     GlobalControlStoreClient &gcs_client,
                     PlasmaClient &plasma_client,
                     const std::string &server_ipaddr, int port)
    : state_(state), gcs_client_(gcs_client), server_ipaddr_(server_ipaddr),
      plasma_client_(plasma_client) {
  tcp_bind_and_listen(port, &address_, &server_fd_);
  LOG(INFO) << "[TCPServer] tcp server is ready at " << server_ipaddr << ":"
            << port;
}


void TCPServer::worker_loop() {
  while (true) {
    LOG(DEBUG) << "waiting for a connection";
    socklen_t addrlen = sizeof(address_);
    int conn_fd = accept(server_fd_, (struct sockaddr *)&address_, &addrlen);
    DCHECK(conn_fd >= 0) << "socket accept error";
    char *incoming_ip = inet_ntoa(address_.sin_addr);
    LOG(DEBUG) << "recieve a TCP connection from " << incoming_ip;

    MessageType msg_type = ReadMessageType(conn_fd);
    switch (msg_type) {
      case MessageType::ReceiveObject:
        receive_object(conn_fd);
        LOG(INFO) << "[TCPServer] receiving object from " << incoming_ip
              << " completes";
        break;
      case MessageType::ReceiveAndReduceObject:
        receive_and_reduce_object(conn_fd);
        LOG(INFO) << "[TCPServer] reducing object from " << incoming_ip
              << " completes";
        break;
      default:
        LOG(FATAL) << "unrecognized message type " << msg_type;
    }
    close(conn_fd);
  }
}

void TCPServer::receive_and_reduce_object(int conn_fd) {
  ObjectID reduce_id = ReadObjectID(conn_fd);
  LOG(DEBUG) << "reduce id = " << reduce_id.hex();
  ObjectID object_id = ReadObjectID(conn_fd);
  LOG(DEBUG) << "targeted object id = " << reduce_id.hex();

  // Get object from Plasma Store
  std::vector<ObjectBuffer> object_buffers;
  plasma_client_.Get({object_id}, -1, &object_buffers);
  void *object_buffer = (void *)object_buffers[0].data->data();
  size_t object_size = object_buffers[0].data->size();
  std::shared_ptr<ReductionStream> stream = state_.create_reduction_stream(object_size);
  while (stream.progress < object_size) {
    int bytes_recv = recv(conn_fd, stream->data() + stream->progress,
                          object_size - stream->progress, 0);
    // TODO: reduce received data
    DCHECK(bytes_recv > 0) << "socket recv error: object content";
    stream->progress += bytes_recv;
  }

  // reply message
  status = send_all(conn_fd, "OK", 3);
  DCHECK(!status) << "socket send error: object ack";
}

void TCPServer::receive_object(int conn_fd) {
  // Protocol: [object_id(kUniqueIDSize), object_size(8B), buffer(*)]
  // receive object ID
  ObjectID object_id = ReadObjectID(conn_fd);
  LOG(DEBUG) << "start receiving object " << object_id.hex();

  // receive object size
  int64_t object_size = ReadObjectSize(conn_fd);
  LOG(DEBUG) << "Received object size = " << object_size;

  // receive object buffer
  std::shared_ptr<Buffer> ptr;
  plasma_client_.Create(object_id, object_size, NULL, 0, &ptr);
  state_.progress = 0;
  state_.pending_size = object_size;
  state_.pending_write = ptr->mutable_data();
  gcs_client_.write_object_location(object_id.hex(), server_ipaddr_);
  while (state_.progress < object_size) {
    int bytes_recv = recv(conn_fd, ptr->mutable_data() + state_.progress,
                          object_size - state_.progress, 0);
    DCHECK(bytes_recv > 0) << "socket recv error: object content";
    state_.progress += bytes_recv;
  }
  plasma_client_.Seal(object_id);
  gcs_client_.PublishObjectCompletionEvent(object_id.hex());

  // reply message
  status = send_all(conn_fd, "OK", 3);
  DCHECK(!status) << "socket send error: object ack";
}
