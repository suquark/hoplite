#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h> // for non-blocking socket
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/config.h"
#include "global_control_store.h"
#include "logging.h"
#include "object_writer.h"
#include "socket_utils.h"
#include "finegrained_pipelining.h"

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
int stream_reduce_add(int conn_fd, T *stream,
                      std::vector<uint8_t *> reduce_buffers) {
  TIMELINE("stream_reduce_add");
  int64_t receive_progress = 0;
  const size_t element_size = sizeof(DT);
  uint8_t *data_ptr = stream->MutableData();
  const int64_t object_size = stream->Size();
  while (receive_progress < object_size) {
    int status = stream_write_next<T>(conn_fd, stream, &receive_progress);
    if (status) {
      // return the error
      return status;
    }
    // reduce related objects
#ifdef HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS
    auto progress = stream->progress.load();
#else
    auto progress = stream->progress;
#endif
    int64_t n_reduce_elements = (receive_progress - progress) / element_size;
    DT *cursor = (DT *)(data_ptr + progress);
    for (auto &buffer : reduce_buffers) {
      DT *own_data_cursor = (DT *)(buffer + progress);
      for (int i = 0; i < n_reduce_elements; i++) {
        cursor[i] += own_data_cursor[i];
      }
    }
    stream->progress += n_reduce_elements * element_size;
  }
  return 0;
}

TCPServer::TCPServer(ObjectStoreState &state,
                     GlobalControlStoreClient &gcs_client,
                     LocalStoreClient &local_store_client,
                     const std::string &server_ipaddr, int port)
    : state_(state), gcs_client_(gcs_client), server_ipaddr_(server_ipaddr),
      local_store_client_(local_store_client),
      pool_(HOPLITE_MAX_INFLOW_CONCURRENCY) {
  TIMELINE(std::string("TCPServer construction function ") + server_ipaddr +
           ":" + std::to_string(port));
  tcp_bind_and_listen(port, &address_, &server_fd_);
  LOG(DEBUG) << "[TCPServer] tcp server is ready at " << server_ipaddr << ":"
             << port;
}

void TCPServer::Shutdown() {
  close(server_fd_);
  server_fd_ = -1;
  // we still send a signal here because the thread may be
  // processing a task
  pthread_kill(server_thread_.native_handle(), SIGUSR1);
  server_thread_.join();
}

void handle_signal(int sig) {
  LOG(DEBUG) << "Signal received on object writer";
  pthread_exit(NULL);
}

void TCPServer::worker_loop() {
  signal(SIGUSR1, handle_signal);
  while (true) {
    LOG(DEBUG) << "waiting for a connection";
    socklen_t addrlen = sizeof(address_);
    int conn_fd = accept(server_fd_, (struct sockaddr *)&address_, &addrlen);
    if (conn_fd < 0) {
      LOG(ERROR)
          << "Socket accept error, maybe it has been closed by the user. "
          << "Shutting down the object writer ...";
      return;
    }
    DCHECK(conn_fd >= 0) << "socket accept error";
#ifdef HOPLITE_ENABLE_NONBLOCKING_SOCKET_RECV
    DCHECK(fcntl(conn_fd, F_SETFL, fcntl(conn_fd, F_GETFL) | O_NONBLOCK) >= 0)
        << "Cannot enable non-blocking for the socket (errno = " << errno
        << ").";
#endif
    char *incoming_ip = inet_ntoa(address_.sin_addr);
    LOG(DEBUG) << "recieve a TCP connection from " << incoming_ip;
    TIMELINE(std::string("TCPServer::worker_loop(), requester_ip = ") +
             incoming_ip);

    ObjectWriterRequest message;
    ReceiveMessage(conn_fd, &message);
    switch (message.message_type_case()) {
    case ObjectWriterRequest::kReceiveObject: {
      auto request = message.receive_object();
      ObjectID object_id = ObjectID::FromBinary(request.object_id());
      int64_t object_size = request.object_size();
      (void)pool_.push([=](int fd) {
        int status = receive_object(conn_fd, object_id, object_size);
        if (status) {
          LOG(FATAL) << "[receive_object] receive object failed. " << strerror(errno)
              << ", code=" << errno << ")";
        }
      });
    } break;
    case ObjectWriterRequest::kReceiveAndReduceObject: {
      auto request = message.receive_and_reduce_object();
      ObjectID reduction_id = ObjectID::FromBinary(request.reduction_id());
      LOG(DEBUG) << "reduction id = " << reduction_id.ToString();

      std::vector<ObjectID> object_ids;
      for (auto &object_id_str : request.object_ids()) {
        ObjectID object_id = ObjectID::FromBinary(object_id_str);
        object_ids.push_back(object_id);
        LOG(DEBUG) << "targeted object id = " << object_id.ToString();
      }
      bool is_endpoint = request.is_endpoint();
      (void)pool_.push([=](int fd) {
        int status = receive_and_reduce_object(conn_fd, reduction_id, object_ids,
                                               is_endpoint);
        if (status) {
          LOG(FATAL) << "[receive_and_reduce_object] receive object failed. " << strerror(errno)
              << ", code=" << errno << ")";
        }
      });
    } break;
    default:
      LOG(FATAL) << "unrecognized message type " << message.message_type_case();
    }
  }
}

// TODO: implement support for general element types.
int TCPServer::receive_and_reduce_object(
    int conn_fd, const ObjectID &reduction_id,
    const std::vector<ObjectID> &object_ids, bool is_endpoint) {
  TIMELINE(std::string("TCPServer::receive_and_reduce_object() ") +
           reduction_id.ToString() + " " + std::to_string(is_endpoint));

  // The endpoint can have no objects to reduce.
  DCHECK(object_ids.size() > 0 || is_endpoint)
      << "At least one object should be reduced.";

  // Get object buffers from Plasma Store
  std::vector<ObjectBuffer> object_buffers;
  auto pstatus = local_store_client_.Get(object_ids, &object_buffers);
  DCHECK(pstatus.ok()) << "Plasma failed to get objects";

  int64_t object_size;
  // TODO: should we include the reduce size in the message?
  if (is_endpoint) {
    object_size = local_store_client_.GetBufferNoExcept(reduction_id)->Size();
  } else {
    object_size = object_buffers[0].data->Size();
  }

  std::vector<uint8_t *> buffers;
  for (auto &buf_info : object_buffers) {
    uint8_t *buf_ptr = buf_info.data->MutableData();
    DCHECK(buf_ptr) << "object buffer is nullptr";
    buffers.push_back(buf_ptr);
    DCHECK(buf_info.data->Size() == object_size)
        << "reduction object size mismatch";
  }

  if (is_endpoint) {
    // notify other nodes that our stream is on progress
    gcs_client_.WriteLocation(reduction_id, server_ipaddr_, false, object_size);
    auto stream = local_store_client_.GetBufferNoExcept(reduction_id);
    int status = stream_reduce_add<Buffer, float>(conn_fd, stream.get(), buffers);
    if (status) {
      return status;
    }
    // notify other threads that we have finished
    stream->NotifyFinished();
  } else {
    std::shared_ptr<Buffer> stream =
        state_.create_reduction_stream(reduction_id, object_size);
    int status = stream_reduce_add<Buffer, float>(conn_fd, stream.get(), buffers);
    if (status) {
      return status;
    }
  }
#ifdef HOPLITE_ENABLE_ACK
  // TODO: handle errors here.
  send_ack(conn_fd);
#endif
  close(conn_fd);
  return 0;
}

int TCPServer::receive_object(int conn_fd, const ObjectID &object_id,
                              int64_t object_size) {
  TIMELINE(std::string("TCPServer::receive_object() ") + object_id.ToString() +
           " " + std::to_string(object_size));
  LOG(DEBUG) << "start receiving object " << object_id.ToString()
             << ", size = " << object_size;

  // receive object buffer
  std::shared_ptr<Buffer> stream;
  auto pstatus = local_store_client_.Create(object_id, object_size, &stream);
  DCHECK(pstatus.ok()) << "Plasma failed to allocate " << object_id.ToString()
                       << " size = " << object_size
                       << ", status = " << pstatus.ToString();

  // notify other nodes that our stream is on progress
  gcs_client_.WriteLocation(object_id, server_ipaddr_, false, object_size);
  int status = stream_write<Buffer>(conn_fd, stream.get());
  if (!status) {
    local_store_client_.Seal(object_id);
#ifdef HOPLITE_ENABLE_ACK
    // TODO: handle error here.
    send_ack(conn_fd);
#endif
    LOG(DEBUG) << object_id.ToString() << " received";
  }
  close(conn_fd);
  return status;
}
