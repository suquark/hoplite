#include <arpa/inet.h>
#include <logging.h>
#include <netinet/in.h>
#include <socket_utils.h>
#include <sys/socket.h>
#include <unistd.h>

#include "object_sender.h"

using objectstore::ObjectWriterRequest;
using objectstore::PullRequest;
using objectstore::ReceiveAndReduceObjectRequest;
using objectstore::ReceiveObjectRequest;
using objectstore::ReduceToRequest;

void SendMessage(int conn_fd, const ObjectWriterRequest &message) {
  size_t message_size = message.ByteSizeLong();
  auto status = send_all(conn_fd, (void *)&message_size, sizeof(message_size));
  DCHECK(!status) << "socket send error: message_size";

  std::vector<uint8_t> message_buf(message_size);
  message.SerializeWithCachedSizesToArray(message_buf.data());

  status = send_all(conn_fd, (void *)message_buf.data(), message_buf.size());
  DCHECK(!status) << "socket send error: message";
}

template <typename T> void stream_send(int conn_fd, T *stream) {
  const uint8_t *data_ptr = stream->data();
  const int64_t object_size = stream->size();

  // send object
  int64_t cursor = 0;
  while (cursor < object_size) {
    int64_t current_progress = stream->progress;
    if (cursor < current_progress) {
      int bytes_sent =
          send(conn_fd, data_ptr + cursor, current_progress - cursor, 0);
      DCHECK(bytes_sent > 0) << "socket send error: object content";
      cursor += bytes_sent;
    }
  }
}

ObjectSender::ObjectSender(ObjectStoreState &state,
                           GlobalControlStoreClient &gcs_client,
                           LocalStoreClient &local_store_client,
                           const std::string &my_address)
    : state_(state), gcs_client_(gcs_client),
      local_store_client_(local_store_client), my_address_(my_address),
      exit_(false) {
  TIMELINE("ObjectSender construction function");
  LOG(INFO) << "[ObjectSender] object sender is ready.";
}

void ObjectSender::worker_loop() {
  while (true) {
    objectstore::ReduceToRequest *request;
    {
      std::unique_lock<std::mutex> l(queue_mutex_);
      queue_cv_.wait_for(l, std::chrono::seconds(1),
                         [this]() { return !pending_tasks_.empty(); });
      {
        if (exit_) {
          return;
        }
      }
      if (pending_tasks_.empty()) {
        continue;
      }
      request = pending_tasks_.front();
      pending_tasks_.pop();
    }
    send_object_for_reduce(request);

    delete request;
  }
}

void ObjectSender::AppendTask(const ReduceToRequest *request) {
  auto new_request = new ReduceToRequest(*request);
  std::unique_lock<std::mutex> l(queue_mutex_);
  pending_tasks_.push(new_request);
  l.unlock();
  queue_cv_.notify_one();
}

void ObjectSender::send_object(const PullRequest *request) {
  TIMELINE("ObjectSender::send_object()");
  // create a TCP connection, send the object through the TCP connection
  int conn_fd;
  auto status = tcp_connect(request->puller_ip(), 6666, &conn_fd);
  DCHECK(!status) << "socket connect error";

  const uint8_t *object_buffer = NULL;
  size_t object_size = 0;
  ObjectID object_id = ObjectID::FromBinary(request->object_id());
  auto stream = state_.get_progressive_stream(object_id);
  if (stream) {
    // fetch partial object in memory
    LOG(DEBUG) << "[GrpcServer] fetching a partial object";
    object_size = stream->size();
  } else {
    // fetch object from Plasma
    LOG(DEBUG) << "[GrpcServer] fetching a complete object from local store";
    std::vector<ObjectBuffer> object_buffers;
    local_store_client_.Get({object_id}, &object_buffers);
    LOG(DEBUG) << "[GrpcServer] fetched a completed object from local store, "
               << object_id.ToString();
    object_buffer = object_buffers[0].data->Data();
    object_size = object_buffers[0].data->Size();
  }

  ObjectWriterRequest ow_request;
  auto ro_request = new ReceiveObjectRequest();
  ro_request->set_object_id(request->object_id());
  ro_request->set_object_size(object_size);
  ow_request.set_allocated_receive_object(ro_request);
  SendMessage(conn_fd, ow_request);

  if (stream) {
    stream_send<ProgressiveStream>(conn_fd, stream.get());
  } else {
    int status = send_all(conn_fd, object_buffer, object_size);
    DCHECK(!status) << "Failed to send object";
  }

  LOG(DEBUG) << "send " << object_id.ToString() << " done";

  // receive ack
  char ack[5];
  status = recv_all(conn_fd, ack, 3);
  DCHECK(!status) << "socket recv error: ack, error code = " << errno;
  if (strcmp(ack, "OK") != 0)
    LOG(FATAL) << "ack is wrong";

  close(conn_fd);

  // TODO: this is for reference counting in the notification service. 
  // When we get an object's location from the server for broadcast, we 
  // reduce the reference count. This line is used to increase the ref count
  // back after we finish the sending. It is better to move this line to the
  // receiver side since the decrease is done by the receiver.
  gcs_client_.WriteLocation(object_id, my_address_, true, object_size);

  LOG(DEBUG) << "function returned";
}

void ObjectSender::send_object_for_reduce(const ReduceToRequest *request) {
  TIMELINE("ObjectSender::send_object_for_reduce()");
  int conn_fd;
  auto status = tcp_connect(request->dst_address(), 6666, &conn_fd);
  DCHECK(!status) << "socket connect error";

  ObjectWriterRequest ow_request;
  auto ro_request = new ReceiveAndReduceObjectRequest();
  ro_request->set_reduction_id(request->reduction_id());
  for (auto &oid_str : request->dst_object_ids()) {
    ro_request->add_object_ids(oid_str);
  }
  ro_request->set_is_endpoint(request->is_endpoint());
  ow_request.set_allocated_receive_and_reduce_object(ro_request);
  SendMessage(conn_fd, ow_request);

  size_t object_size = 0;
  if (request->reduction_source_case() == ReduceToRequest::kSrcObjectId) {
    LOG(INFO) << "[GrpcServer] fetching a complete object from local store";
    // TODO: there could be multiple source objects.
    ObjectID src_object_id = ObjectID::FromBinary(request->src_object_id());
    std::vector<ObjectBuffer> object_buffers;
    local_store_client_.Get({src_object_id}, &object_buffers);
    auto &buffer_ptr = object_buffers[0].data;
    int status = send_all(conn_fd, buffer_ptr->Data(), buffer_ptr->Size());
    DCHECK(!status) << "Failed to send object";
    object_size = buffer_ptr->Size();
  } else {
    LOG(INFO)
        << "[GrpcServer] fetching an incomplete object from reduction stream";
    ObjectID reduction_id = ObjectID::FromBinary(request->reduction_id());
    auto stream = state_.get_reduction_stream(reduction_id);
    DCHECK(stream != nullptr) << "Stream should not be nullptr";
    stream_send<ReductionStream>(conn_fd, stream.get());
    object_size = stream->size();
  }

  // receive ack
  char ack[5];
  status = recv_all(conn_fd, ack, 3);
  DCHECK(!status) << "socket recv error: ack, error code = " << errno;
  if (strcmp(ack, "OK") != 0)
    LOG(FATAL) << "ack is wrong";

  close(conn_fd);
}
