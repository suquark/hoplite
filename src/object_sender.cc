#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <logging.h>
#include <netinet/in.h>
#include <socket_utils.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/config.h"
#include "finegrained_pipelining.h"
#include "object_sender.h"
#include "util/protobuf_utils.h"

using objectstore::ObjectWriterRequest;
using objectstore::ReceiveAndReduceObjectRequest;
using objectstore::ReceiveObjectRequest;
using objectstore::ReduceToRequest;

ObjectSender::ObjectSender(ObjectStoreState &state, GlobalControlStoreClient &gcs_client,
                           LocalStoreClient &local_store_client, const std::string &my_address)
    : state_(state), gcs_client_(gcs_client), local_store_client_(local_store_client), my_address_(my_address),
      exit_(false), pool_(1) {
  TIMELINE(std::string("ObjectSender construction function ") + my_address + ":" + std::to_string(HOPLITE_SENDER_PORT));
  tcp_bind_and_listen(HOPLITE_SENDER_PORT, &address_, &server_fd_);
  LOG(DEBUG) << "[ObjectSender] object sender is ready.";
}

void sender_handle_signal(int sig) {
  LOG(DEBUG) << "Signal received on object sender";
  pthread_exit(NULL);
}

std::thread ObjectSender::Run() {
  server_thread_ = std::thread(&ObjectSender::listener_loop, this);

  std::thread sender_thread(&ObjectSender::worker_loop, this);
  return sender_thread;
}

void ObjectSender::Shutdown() {
  exit_ = true;

  close(server_fd_);
  server_fd_ = -1;
  // we still send a signal here because the thread may be
  // processing a task
  pthread_kill(server_thread_.native_handle(), SIGUSR1);
  server_thread_.join();
}

void ObjectSender::listener_loop() {
  signal(SIGUSR1, sender_handle_signal);
  while (true) {
    LOG(DEBUG) << "waiting for a connection";
    socklen_t addrlen = sizeof(address_);
    int conn_fd = accept(server_fd_, (struct sockaddr *)&address_, &addrlen);
    if (conn_fd < 0) {
      LOG(ERROR) << "Socket accept error, maybe it has been closed by the user. "
                 << "Shutting down the object sender ...";
      return;
    }
    DCHECK(conn_fd >= 0) << "socket accept error";
    char *incoming_ip = inet_ntoa(address_.sin_addr);
    LOG(DEBUG) << "recieve a TCP connection from " << incoming_ip;
    TIMELINE(std::string("Sender::worker_loop(), requester_ip = ") + incoming_ip);

    ObjectWriterRequest message;
    ReceiveProtobufMessage(conn_fd, &message);
    switch (message.message_type_case()) {
    case ObjectWriterRequest::kReceiveObject: {
      auto request = message.receive_object();
      ObjectID object_id = ObjectID::FromBinary(request.object_id());
      int ec = send_object(conn_fd, object_id, request.offset());
      if (ec) {
        LOG(ERROR) << "[Sender] Failed to send object. " << strerror(errno) << ", error_code=" << errno << ")";
      }
    } break;
    default:
      LOG(FATAL) << "unrecognized message type " << message.message_type_case();
    }
  }
}

int ObjectSender::send_object(int conn_fd, const ObjectID &object_id, int64_t offset) {
  // fetch object from local store
  std::shared_ptr<Buffer> stream = local_store_client_.GetBufferNoExcept(object_id);
  LOG(DEBUG) << object_id.ToString() << " find in local store. Ready to send.";
  if (stream->IsFinished()) {
    LOG(DEBUG) << "[Sender] fetched a completed object from local store: " << object_id.ToString();
  } else {
    LOG(DEBUG) << "[Sender] fetching a partial object: " << object_id.ToString();
  }
  int ec = stream_send<Buffer>(conn_fd, stream.get(), offset);
  LOG(DEBUG) << "send " << object_id.ToString() << " done, error_code=" << ec;
  close(conn_fd);
  // TODO: this is for reference counting in the notification service.
  // When we get an object's location from the server for broadcast, we
  // reduce the reference count. This line is used to increase the ref count
  // back after we finish the sending. It is better to move this line to the
  // receiver side since the decrease is done by the receiver.
  gcs_client_.WriteLocation(object_id, my_address_, true, stream->Size(), stream->Data());
  return ec;
}

void ObjectSender::worker_loop() {
  while (true) {
    objectstore::ReduceToRequest *request;
    {
      std::unique_lock<std::mutex> l(queue_mutex_);
      queue_cv_.wait_for(l, std::chrono::seconds(1), [this]() { return !pending_tasks_.empty(); });
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

void ObjectSender::send_object_for_reduce(const ReduceToRequest *request) {
  TIMELINE("ObjectSender::send_object_for_reduce(), dst_address = " + request->dst_address());
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
  SendProtobufMessage(conn_fd, ow_request);

  if (request->reduction_source_case() == ReduceToRequest::kSrcObjectId) {
    LOG(DEBUG) << "[GrpcServer] fetching a complete object from local store";
    // TODO: there could be multiple source objects.
    ObjectID src_object_id = ObjectID::FromBinary(request->src_object_id());
    std::vector<ObjectBuffer> object_buffers;
    local_store_client_.Get({src_object_id}, &object_buffers);
    auto &stream = object_buffers[0].data;
    stream_send<Buffer>(conn_fd, stream.get());
  } else {
    LOG(DEBUG) << "[GrpcServer] fetching an incomplete object from reduction stream";
    ObjectID reduction_id = ObjectID::FromBinary(request->reduction_id());
    auto stream = state_.get_reduction_stream(reduction_id);
    DCHECK(stream != nullptr) << "Stream should not be nullptr";
    stream_send<Buffer>(conn_fd, stream.get());
    state_.release_reduction_stream(reduction_id);
  }

#ifdef HOPLITE_ENABLE_ACK
  recv_ack(conn_fd);
#endif
  close(conn_fd);
}
