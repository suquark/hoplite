#include <csignal>
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

using namespace plasma;
constexpr int64_t STREAM_MAX_BLOCK_SIZE = 4 * (2 << 20); // 4MB

using objectstore::ObjectWriterRequest;

void ReceiveMessage(int conn_fd, ObjectWriterRequest *request) {
  size_t message_len;
  int status = recv_all(conn_fd, &message_len, sizeof(message_len));
  DCHECK(!status) << "receive message_len failed";

  std::vector<uint8_t> message(message_len);
  status = recv_all(conn_fd, message.data(), message_len);
  DCHECK(!status) << "receive message failed";

  request->ParseFromArray(message.data(), message.size());
}

template <typename T, typename DT>
void stream_reduce_add(int conn_fd, T &stream,
                       std::vector<uint8_t *> reduce_buffers,
                       size_t object_size) {
  // TODO: implement support for general element types.
  const size_t element_size = sizeof(DT);
  while (stream->receive_progress < object_size) {
    int64_t remaining_size = object_size - stream->receive_progress;
    int64_t recv_block_size = remaining_size > STREAM_MAX_BLOCK_SIZE
                                  ? STREAM_MAX_BLOCK_SIZE
                                  : remaining_size;
    int bytes_recv = recv(conn_fd, stream->data() + stream->receive_progress,
                          recv_block_size, 0);
    DCHECK(bytes_recv > 0) << "socket recv error: object content";
    auto reduce_progress = stream->reduce_progress.load();

    stream->receive_progress += bytes_recv;
    int64_t n_reduce_elements =
        (stream->receive_progress - reduce_progress) / element_size;
    DT *cursor = (DT *)(stream->data() + reduce_progress);
    for (auto &buffer : reduce_buffers) {
      DT *own_data_cursor = (DT *)(buffer + reduce_progress);
      for (int i = 0; i < n_reduce_elements; i++) {
        cursor[i] += own_data_cursor[i];
      }
    }
    stream->reduce_progress += n_reduce_elements * element_size;
  }
}

TCPServer::TCPServer(ObjectStoreState &state,
                     GlobalControlStoreClient &gcs_client,
                     PlasmaClient &plasma_client,
                     const std::string &server_ipaddr, int port)
    : state_(state), gcs_client_(gcs_client), server_ipaddr_(server_ipaddr),
      plasma_client_(plasma_client) {
  TIMELINE(std::string("TCPServer construction function ") + server_ipaddr + ":" + std::to_string(port));
  tcp_bind_and_listen(port, &address_, &server_fd_);
  LOG(INFO) << "[TCPServer] tcp server is ready at " << server_ipaddr << ":"
            << port;
}

void TCPServer::worker_loop() {
  while (true) {
    TIMELINE("TCPServer::worker_loop() step");
    LOG(DEBUG) << "waiting for a connection";
    socklen_t addrlen = sizeof(address_);
    int conn_fd = accept(server_fd_, (struct sockaddr *)&address_, &addrlen);
    DCHECK(conn_fd >= 0) << "socket accept error";
    char *incoming_ip = inet_ntoa(address_.sin_addr);
    LOG(INFO) << "recieve a TCP connection from " << incoming_ip;

    ObjectWriterRequest message;
    ReceiveMessage(conn_fd, &message);
    switch (message.message_type_case()) {
    case ObjectWriterRequest::kReceiveObject: {
      auto request = message.receive_object();
      ObjectID object_id = ObjectID::from_binary(request.object_id());
      int64_t object_size = request.object_size();
      receive_object(conn_fd, object_id, object_size);
      break;
    }
    case ObjectWriterRequest::kReceiveAndReduceObject: {
      auto request = message.receive_and_reduce_object();
      ObjectID reduction_id = ObjectID::from_binary(request.reduction_id());
      LOG(DEBUG) << "reduction id = " << reduction_id.hex();

      std::vector<ObjectID> object_ids;
      for (auto &object_id_str : request.object_ids()) {
        ObjectID object_id = ObjectID::from_binary(object_id_str);
        object_ids.push_back(object_id);
        LOG(DEBUG) << "targeted object id = " << object_id.hex();
      }
      bool is_endpoint = request.is_endpoint();
      receive_and_reduce_object(conn_fd, reduction_id, object_ids, is_endpoint);
      break;
    }
    default:
      LOG(FATAL) << "unrecognized message type " << message.message_type_case();
    }
    close(conn_fd);
  }
}

// TODO: implement support for general element types.
void TCPServer::receive_and_reduce_object(
    int conn_fd, const ObjectID &reduction_id,
    const std::vector<ObjectID> &object_ids, bool is_endpoint) {
  TIMELINE(std::string("TCPServer::receive_and_reduce_objectt() ") + object_id.hex() + " " + std::to_string(object_size));

  // The endpoint can have no objects to reduce.
  DCHECK(object_ids.size() > 0 || is_endpoint)
      << "At least one object should be reduced.";

  // Get object buffers from Plasma Store
  std::vector<ObjectBuffer> object_buffers;
  plasma_client_.Get(object_ids, -1, &object_buffers);
  size_t object_size;
  // TODO: should we include the reduce size in the message?
  if (is_endpoint) {
    object_size = state_.get_reduction_endpoint(reduction_id)->size();
  } else {
    object_size = object_buffers[0].data->size();
  }

  std::vector<uint8_t *> buffers;
  for (auto &buf_info : object_buffers) {
    buffers.push_back(buf_info.data->mutable_data());
    DCHECK(buf_info.data->size() == object_size)
        << "reduction object size mismatch";
  }

  if (is_endpoint) {
    std::shared_ptr<ReductionEndpointStream> stream =
        state_.get_reduction_endpoint(reduction_id);
    stream_reduce_add<std::shared_ptr<ReductionEndpointStream>, float>(
        conn_fd, stream, buffers, object_size);
    stream->finish();
  } else {
    std::shared_ptr<ReductionStream> stream =
        state_.create_reduction_stream(reduction_id, object_size);
    stream_reduce_add<std::shared_ptr<ReductionStream>, float>(
        conn_fd, stream, buffers, object_size);
  }

  // reply message
  auto status = send_all(conn_fd, "OK", 3);
  DCHECK(!status) << "socket send error: object ack";
}

void TCPServer::receive_object(int conn_fd, const ObjectID &object_id,
                               int64_t object_size) {
  TIMELINE(std::string("TCPServer::receive_object() ") + object_id.hex() + " " + std::to_string(object_size));
  LOG(DEBUG) << "start receiving object " << object_id.hex()
             << ", size = " << object_size;

  // receive object buffer
  std::shared_ptr<Buffer> ptr;
  auto pstatus = plasma_client_.Create(object_id, object_size, NULL, 0, &ptr);
  DCHECK(pstatus.ok()) << "Plasma failed to allocate object id = "
                       << object_id.hex() << " size = " << object_size
                       << ", status = " << pstatus.ToString();

  state_.progress = 0;
  state_.pending_size = object_size;
  state_.pending_write = ptr->mutable_data();
  gcs_client_.write_object_location(object_id, server_ipaddr_);
  while (state_.progress < object_size) {
    int remaining_size = object_size - state_.progress;
    int recv_block_size = remaining_size > STREAM_MAX_BLOCK_SIZE
                              ? STREAM_MAX_BLOCK_SIZE
                              : remaining_size;
    int bytes_recv = recv(conn_fd, ptr->mutable_data() + state_.progress,
                          recv_block_size, 0);
    DCHECK(bytes_recv > 0) << "socket recv error: object content";
    state_.progress += bytes_recv;
  }
  plasma_client_.Seal(object_id);
  gcs_client_.PublishObjectCompletionEvent(object_id);

  // reply message
  auto status = send_all(conn_fd, "OK", 3);
  DCHECK(!status) << "socket send error: object ack";
  LOG(DEBUG) << "object " << object_id.hex() << " received";
}
