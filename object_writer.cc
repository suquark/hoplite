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
#include "protocol.h"
#include "socket_utils.h"

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
      LOG(FATAL) << "unrecognized message type " << (int)msg_type;
    }
    close(conn_fd);
  }
}

void TCPServer::receive_and_reduce_object(int conn_fd) {
  // Protocol: [object_id(kUniqueIDSize), object_size(8B), buffer(*)]
  ObjectID reduce_id = ReadObjectID(conn_fd);
  LOG(DEBUG) << "reduce id = " << reduce_id.hex();
  ObjectID object_id = ReadObjectID(conn_fd);
  LOG(DEBUG) << "targeted object id = " << object_id.hex();

  if (object_id == reduce_id) {
    // this is the endpoint
    auto buffer = state_.get_reduction_endpoint(reduce_id);
    int status = recv_all(conn_fd, buffer->mutable_data(), buffer->size());
  } else {
    // Get object from Plasma Store
    std::vector<ObjectBuffer> object_buffers;
    plasma_client_.Get({reduce_id}, -1, &object_buffers);
    void *object_buffer = (void *)object_buffers[0].data->data();
    size_t object_size = object_buffers[0].data->size();
    std::shared_ptr<ReductionStream> stream =
        state_.create_reduction_stream(reduce_id, object_size);

    // TODO: implement support for general element types.
    size_t element_size = sizeof(float);
    while (stream->receive_progress < object_size) {
      int bytes_recv = recv(conn_fd, stream->data() + stream->receive_progress,
                            object_size - stream->receive_progress, 0);
      stream->receive_progress += bytes_recv;
      DCHECK(bytes_recv > 0) << "socket recv error: object content";
      int64_t n_reduce_elements =
          (stream->receive_progress - stream->reduce_progress) / element_size;
      float *cursor =
          (float *)(stream->data() + (int64_t)stream->reduce_progress);
      float *own_data_cursor =
          (float *)(object_buffer + (int64_t)stream->reduce_progress);
      for (int i = 0; i < n_reduce_elements; i++) {
        cursor[i] += own_data_cursor[i];
      }
      stream->reduce_progress += n_reduce_elements * element_size;
    }
  }

  // reply message
  auto status = send_all(conn_fd, "OK", 3);
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
  auto status = send_all(conn_fd, "OK", 3);
  DCHECK(!status) << "socket send error: object ack";
}
