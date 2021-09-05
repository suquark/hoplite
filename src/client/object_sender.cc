#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <util/logging.h>
#include <netinet/in.h>
#include <util/socket_utils.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>

#include "common/config.h"
#include "object_sender.h"
#include "util/protobuf_utils.h"

using objectstore::ObjectWriterRequest;
using objectstore::ReceiveObjectRequest;
using objectstore::ReceiveReducedObjectRequest;

template <typename T> inline int stream_send(int conn_fd, T *stream, int64_t offset = 0) {
  TIMELINE("ObjectSender::stream_send()");
  LOG(DEBUG) << "ObjectSender::stream_send(), offset=" << offset;
  const uint8_t *data_ptr = stream->Data();
  const int64_t object_size = stream->Size();

  if (stream->IsFinished()) {
    int status = send_all(conn_fd, data_ptr + offset, object_size - offset);
    if (status) {
      LOG(ERROR) << "Failed to send object.";
      return status;
    }
    return 0;
  }
  int64_t cursor = offset;
  while (cursor < object_size) {
    int64_t current_progress = stream->progress;
    if (cursor < current_progress) {
      int bytes_sent = send(conn_fd, data_ptr + cursor, current_progress - cursor, 0);
      if (bytes_sent < 0) {
        LOG(ERROR) << "[stream_send] socket send error (" << strerror(errno) << ", code=" << errno
                   << ", cursor=" << cursor << ", stream_progress=" << current_progress << ")";
        if (errno == EAGAIN) {
          continue;
        }
        return errno;
      }
      cursor += bytes_sent;
    }
  }
  return 0;
}

ObjectSender::ObjectSender(ObjectStoreState &state, GlobalControlStoreClient &gcs_client,
                           LocalStoreClient &local_store_client, const std::string &my_address)
    : state_(state), gcs_client_(gcs_client), local_store_client_(local_store_client), my_address_(my_address),
      pool_(HOPLITE_MAX_OUTLOW_CONCURRENCY) {
  TIMELINE(std::string("ObjectSender construction function ") + my_address + ":" + std::to_string(HOPLITE_SENDER_PORT));
  tcp_bind_and_listen(HOPLITE_SENDER_PORT, &address_, &server_fd_);
  LOG(DEBUG) << "[ObjectSender] object sender is ready.";
}

void sender_handle_signal(int sig) {
  LOG(DEBUG) << "Signal received on object sender";
  pthread_exit(nullptr);
}

void ObjectSender::Run() { server_thread_ = std::thread(&ObjectSender::listener_loop, this); }

void ObjectSender::Shutdown() {
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
    char *incoming_ip = inet_ntoa(address_.sin_addr);
    LOG(DEBUG) << "recieve a TCP connection from " << incoming_ip;
    TIMELINE(std::string("Sender::worker_loop(), requester_ip = ") + incoming_ip);

    ObjectWriterRequest message;
    ReceiveProtobufMessage(conn_fd, &message);
    switch (message.message_type_case()) {
    case ObjectWriterRequest::kReceiveObject: {
      auto request = message.receive_object();
      pool_.push(
          [this, conn_fd](int tid, ReceiveObjectRequest request) {
            ObjectID object_id = ObjectID::FromBinary(request.object_id());
            int ec = send_object(conn_fd, object_id, request.object_size(), request.offset());
            if (ec) {
              LOG(ERROR) << "[Sender] Failed to send object. " << strerror(errno) << ", error_code=" << errno << ")";
            } else {
              LOG(DEBUG) << "[Sender] Send finished successfully.";
            }
          },
          std::move(request));
    } break;
    case ObjectWriterRequest::kReceiveReducedObject: {
      auto request = message.receive_reduced_object();
      pool_.push(
          [this, conn_fd](int tid, ReceiveReducedObjectRequest request) {
            ObjectID reduction_id = ObjectID::FromBinary(request.reduction_id());
            int ec = send_reduced_object(conn_fd, reduction_id, request.object_size(), request.offset());
            if (ec) {
              LOG(ERROR) << "[Sender] Failed to send reduced object. " << strerror(errno) << ", error_code=" << errno
                         << ")";
            } else {
              LOG(DEBUG) << "[Sender] Send finished successfully.";
            }
          },
          std::move(request));
    } break;
    default:
      LOG(FATAL) << "unrecognized message type " << message.message_type_case();
    }
  }
}

int ObjectSender::send_object(int conn_fd, const ObjectID &object_id, int64_t object_size, int64_t offset) {
  // fetch object from local store
  std::shared_ptr<Buffer> stream;
  local_store_client_.GetBufferOrCreate(object_id, object_size, &stream);
  LOG(DEBUG) << object_id.ToString() << " find in local store. Ready to send.";
  if (stream->IsFinished()) {
    LOG(DEBUG) << "[Sender] fetched a completed object from local store: " << object_id.ToString();
  } else {
    LOG(DEBUG) << "[Sender] fetching a partial object: " << object_id.ToString();
  }
  int ec = stream_send<Buffer>(conn_fd, stream.get(), offset);
  LOG(DEBUG) << "send " << object_id.ToString() << " done, error_code=" << ec;
  close(conn_fd);
  return ec;
}

int ObjectSender::send_reduced_object(int conn_fd, const ObjectID &reduction_id, int64_t object_size, int64_t offset) {
  TIMELINE("ObjectSender::send_reduced_object");
  // fetch object from object_store_state
  std::shared_ptr<Buffer> stream = state_.get_or_create_reduction_stream(reduction_id, object_size);
  if (stream->IsFinished()) {
    LOG(DEBUG) << "[Sender] fetched a completed object from local reduction_stream: " << reduction_id.ToString();
  } else {
    LOG(DEBUG) << "[Sender] fetched a partial object from reduction_stream: " << reduction_id.ToString();
  }
  int ec = stream_send<Buffer>(conn_fd, stream.get(), offset);
  LOG(DEBUG) << "send " << reduction_id.ToString() << " done, error_code=" << ec;
  close(conn_fd);
  return ec;
}
