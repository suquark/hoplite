#include <arpa/inet.h>
#include <logging.h>
#include <netinet/in.h>
#include <socket_utils.h>
#include <sys/socket.h>
#include <unistd.h>

#include <plasma/common.h>

#include "object_sender.h"
#include "protocol.h"

using namespace plasma;
using objectstore::ReduceToRequest;

ObjectSender::ObjectSender(ObjectStoreState &state, PlasmaClient &plasma_client)
    : state_(state), plasma_client_(plasma_client) {
  LOG(INFO) << "[ObjectSender] object sender is ready.";
}

void ObjectSender::worker_loop() {
  while (true) {
    if (pending_tasks_.empty()) {
      usleep(1000);
      continue;
    }

    auto request = pending_tasks_.front();
    pending_tasks_.pop_front();

    send_object(request);

    delete request;
  }
}

void ObjectSender::AppendTask(const ReduceToRequest *request) {
  auto new_request = new ReduceToRequest(*request);
  pending_tasks_.push_back(new_request);
}

void ObjectSender::send_object(const ReduceToRequest *request) {
  int conn_fd;
  auto status = tcp_connect(request->dst_address(), 6666, &conn_fd);
  DCHECK(!status) << "socket connect error";

  SendMessageType(conn_fd, MessageType::ReceiveAndReduceObject);

  ObjectID reduction_id = ObjectID::from_binary(request->reduction_id());
  ObjectID dst_object_id = ObjectID::from_binary(request->dst_object_id());
  status = send_all(conn_fd, (void *)reduction_id.data(), reduction_id.size());
  DCHECK(!status) << "socket send error: reduction_id";
  status =
      send_all(conn_fd, (void *)dst_object_id.data(), dst_object_id.size());
  DCHECK(!status) << "socket send error: dst_object_id";

  void *object_buffer = NULL;
  size_t object_size = 0;

  if (request->reduction_source_case() == ReduceToRequest::kSrcObjectId) {
    LOG(DEBUG) << "[GrpcServer] fetching a complete object from plasma";
    ObjectID src_object_id = ObjectID::from_binary(request->src_object_id());
    std::vector<ObjectBuffer> object_buffers;
    plasma_client_.Get({src_object_id}, -1, &object_buffers);
    object_buffer = (void *)object_buffers[0].data->data();
    object_size = object_buffers[0].data->size();
    int status = send_all(conn_fd, object_buffer, object_size);
    DCHECK(!status) << "Failed to send object";
  } else {
    auto stream = state_.get_reduction_stream(reduction_id);
    object_buffer = stream->data();
    object_size = stream->size();
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
  }

  // receive ack
  char ack[5];
  status = recv_all(conn_fd, ack, 3);
  DCHECK(!status) << "socket recv error: ack, error code = " << errno;
  if (strcmp(ack, "OK") != 0)
    LOG(FATAL) << "ack is wrong";

  close(conn_fd);
}
