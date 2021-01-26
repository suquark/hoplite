#include "receiver.h"

#include <fcntl.h> // for non-blocking socket
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include "common/config.h"

#include "object_store.pb.h"
#include "util/protobuf_utils.h"

using objectstore::ObjectWriterRequest;
using objectstore::ReceiveObjectRequest;
using objectstore::ReceiveReducedObjectRequest;

template <typename T> inline int stream_receive_next(int conn_fd, T *stream, int64_t *receive_progress) {
  int remaining_size = stream->Size() - *receive_progress;
  // here we receive no more than STREAM_MAX_BLOCK_SIZE for streaming
  int recv_block_size = remaining_size > STREAM_MAX_BLOCK_SIZE ? STREAM_MAX_BLOCK_SIZE : remaining_size;
  while (true) {
    int bytes_recv = recv(conn_fd, stream->MutableData() + *receive_progress, recv_block_size, 0);
    if (bytes_recv < 0) {
      if (errno == EAGAIN) {
#ifndef HOPLITE_ENABLE_NONBLOCKING_SOCKET_RECV
        LOG(WARNING) << "[stream_receive_next] socket recv error (EAGAIN). Ignored.";
#endif
        continue;
      }
      LOG(ERROR) << "[stream_receive_next] socket recv error (" << strerror(errno) << ", code=" << errno << ")";
      return -1;
    } else if (bytes_recv == 0) {
      LOG(ERROR) << "[stream_receive_next] 0 bytes received (" << strerror(errno) << ", code=" << errno << ")";
      return -1;
    }
    *receive_progress += bytes_recv;
    return 0;
  }
}

template <typename T> inline int stream_receive(int conn_fd, T *stream, int64_t offset = 0) {
  TIMELINE("stream_receive");
  int64_t receive_progress = offset;
  while (receive_progress < stream->Size()) {
    int ec = stream_receive_next<T>(conn_fd, stream, &receive_progress);
    if (ec) {
      // return the error
      LOG(ERROR) << "[stream_receive] socket receive error (" << strerror(errno) << ", code=" << errno
                 << ", receive_progress=" << receive_progress << ")";
      return ec;
    }
    // update the progress
#ifdef HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS
    stream->progress.store(receive_progress);
#else
    stream->progress = receive_progress;
#endif
  }
  return 0;
}

/// reduce(conn, dep_stream) -> stream
template <typename T, typename DT>
int stream_reduce_add_single_thread(int conn_fd, T *stream, T &dep_stream, int64_t offset) {
  TIMELINE("stream_reduce_add_single_thread");
  LOG(DEBUG) << "stream_reduce_add_single_thread(), offset=" << offset;
  int64_t receive_progress = offset;
  const size_t element_size = sizeof(DT);
  uint8_t *data_ptr = stream->MutableData();
  uint8_t *dep_data_ptr = dep_stream.MutableData();
  const int64_t object_size = stream->Size();
  while (receive_progress < object_size) {
    int status = stream_receive_next<T>(conn_fd, stream, &receive_progress);
    if (status) {
      // return the error
      return status;
    }
    // reduce related objects
#ifdef HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS
    auto progress = stream->progress.load();
    auto dep_stream_progress = dep_stream.progress.load();
#else
    auto progress = stream->progress;
    auto dep_stream_progress = dep_stream.progress;
#endif
    if (dep_stream_progress > progress) {
      int64_t n_reduce_elements = (std::min(dep_stream_progress, receive_progress) - progress) / element_size;
      DT *cursor = (DT *)(data_ptr + progress);
      const DT *own_data_cursor = (DT *)(dep_data_ptr + progress);
      for (size_t i = 0; i < n_reduce_elements; i++) {
        cursor[i] += own_data_cursor[i];
      }
      stream->progress += n_reduce_elements * element_size;
    }
  }
  while (!stream->IsFinished()) {
#ifdef HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS
    auto progress = stream->progress.load();
    auto dep_stream_progress = dep_stream.progress.load();
#else
    auto progress = stream->progress;
    auto dep_stream_progress = dep_stream.progress;
#endif
    int64_t n_reduce_elements = (dep_stream_progress - progress) / element_size;
    DT *cursor = (DT *)(data_ptr + progress);
    const DT *own_data_cursor = (DT *)(dep_data_ptr + progress);
    for (size_t i = 0; i < n_reduce_elements; i++) {
      cursor[i] += own_data_cursor[i];
    }
    stream->progress += n_reduce_elements * element_size;
  }
  return 0;
}

/// reduce(conn, dep_stream) -> stream
template <typename T, typename DT>
int stream_reduce_add_multi_thread(int conn_fd, T *stream, T &dep_stream, int64_t offset) {
  // FIXME: these debug print causes memory leak and fail fault tolerance test
  // TIMELINE("stream_reduce_add_multi_thread");
  // LOG(DEBUG) << "stream_reduce_add_multi_thread(), offset=" << offset;
  int64_t receive_progress = offset;
  const size_t element_size = sizeof(DT);
  uint8_t *data_ptr = stream->MutableData();
  uint8_t *dep_data_ptr = dep_stream.MutableData();

  std::thread t([&]() {
    while (!stream->IsFinished()) {
#ifdef HOPLITE_ENABLE_ATOMIC_BUFFER_PROGRESS
      auto progress = stream->progress.load();
      auto dep_stream_progress = dep_stream.progress.load();
#else
      auto progress = stream->progress;
      auto dep_stream_progress = dep_stream.progress;
#endif
      int64_t n_reduce_elements = (std::min(dep_stream_progress, receive_progress) - progress) / element_size;
      DT *cursor = (DT *)(data_ptr + progress);
      const DT *own_data_cursor = (DT *)(dep_data_ptr + progress);
      for (size_t i = 0; i < n_reduce_elements; i++) {
        cursor[i] += own_data_cursor[i];
      }
      stream->progress += n_reduce_elements * element_size;
    }
  });

  const int64_t object_size = stream->Size();
  while (receive_progress < object_size) {
    int status = stream_receive_next<T>(conn_fd, stream, &receive_progress);
    if (status) {
      // return the error
      return status;
    }
  }
  t.join();
  return 0;
}

/// reduce(conn, dep_stream) -> stream
template <typename T, typename DT> int stream_reduce_add(int conn_fd, T *stream, T &dep_stream, int64_t offset) {
  // TIMELINE("stream_reduce_add");
  return stream_reduce_add_single_thread<T, DT>(conn_fd, stream, dep_stream, offset);
  // FIXME: "stream_reduce_add_multi_thread" is buggy because the thread would not be joined if we have an error.

  // int64_t left = stream->Size() - stream->progress;
  // if (left >= HOPLITE_MULTITHREAD_REDUCE_SIZE) {
  //   return stream_reduce_add_multi_thread<T, DT>(conn_fd, stream, dep_stream, offset);
  // } else {
  //   return stream_reduce_add_single_thread<T, DT>(conn_fd, stream, dep_stream, offset);
  // }
}

Receiver::Receiver(ObjectStoreState &state, GlobalControlStoreClient &gcs_client, LocalStoreClient &local_store_client,
                   const std::string &my_address, int port)
    : state_(state), gcs_client_(gcs_client), my_address_(my_address), local_store_client_(local_store_client),
      pool_(HOPLITE_MAX_INFLOW_CONCURRENCY) {}

bool Receiver::check_and_store_inband_data(const ObjectID &object_id, int64_t object_size,
                                           const std::string &inband_data) {
  TIMELINE("Receiver::check_and_store_inband_data");
  if (inband_data.size() > 0) {
    LOG(DEBUG) << "fetching object directly from inband data";
    DCHECK(inband_data.size() <= inband_data_size_limit) << "unexpected inband data size";
    std::shared_ptr<Buffer> data;
    local_store_client_.Create(object_id, object_size, &data);
    data->CopyFrom(inband_data);
    local_store_client_.Seal(object_id);
    return true;
  }
  return false;
}

int Receiver::receive_object(const std::string &sender_ip, int sender_port, const ObjectID &object_id, Buffer *stream) {
  TIMELINE(std::string("Receiver::receive_object() ") + object_id.ToString());
  LOG(DEBUG) << "start receiving object " << object_id.ToString() << " from " << sender_ip
             << ", size = " << stream->Size() << ", intial_progress=" << stream->progress;
  int conn_fd;
  int ec = tcp_connect(sender_ip, sender_port, &conn_fd);
  if (ec) {
    LOG(ERROR) << "Failed to connect to sender (ip=" << sender_ip << ", port=" << sender_port << ").";
    return ec;
  }

  // send request
  ObjectWriterRequest req;
  auto ro_request = new ReceiveObjectRequest();
  ro_request->set_object_id(object_id.Binary());
  ro_request->set_object_size(stream->Size());
  ro_request->set_offset(stream->progress);
  req.set_allocated_receive_object(ro_request);
  SendProtobufMessage(conn_fd, req);

  // start receiving object
#ifdef HOPLITE_ENABLE_NONBLOCKING_SOCKET_RECV
  DCHECK(fcntl(conn_fd, F_SETFL, fcntl(conn_fd, F_GETFL) | O_NONBLOCK) >= 0)
      << "Cannot enable non-blocking for the socket (errno = " << errno << ").";
#endif
  ec = stream_receive<Buffer>(conn_fd, stream, stream->progress);
  LOG(DEBUG) << "receive " << object_id.ToString() << " done, error_code=" << ec;
  close(conn_fd);
  if (stream->IsFinished()) {
    gcs_client_.WriteLocation(object_id, my_address_, true, stream->Size(), stream->Data());
  }
  return ec;
}

void Receiver::pull_object(const ObjectID &object_id) {
  SyncReply reply = gcs_client_.GetLocationSync(object_id, true, my_address_);
  if (!check_and_store_inband_data(object_id, reply.object_size, reply.inband_data)) {
    // prepare object buffer for receiving.
    std::shared_ptr<Buffer> stream;
    auto pstatus = local_store_client_.GetBufferOrCreate(object_id, reply.object_size, &stream);
    DCHECK(pstatus.ok()) << "Plasma failed to allocate " << object_id.ToString() << " size = " << reply.object_size
                         << ", status = " << pstatus.ToString();
    LOG(DEBUG) << "Created buffer for " << object_id.ToString() << ", size=" << reply.object_size;
    std::string sender_ip = reply.sender_ip;
    // ---------------------------------------------------------------------------------------------
    // Here is our fault tolerance logic for multicast.
    // If the sender failed during sending the object, we retry with another sender that already has
    // the buffer ready before we declare that our stream is on progress. Retry with a random sender
    // could cause deadlock since the sender could also depend on us. In most cases we should get
    // the sender that was sending data to the failed sender, and we can continue transmission
    // immediately.
    // ---------------------------------------------------------------------------------------------
    while (!stream->IsFinished()) {
      LOG(DEBUG) << "Try receiving " << object_id.ToString() << " from " << sender_ip << ", size=" << reply.object_size;
      int ec = receive_object(sender_ip, HOPLITE_SENDER_PORT, object_id, stream.get());
      if (ec) {
        LOG(ERROR) << "Failed to receive " << object_id.ToString() << " from sender " << sender_ip;
        bool success = gcs_client_.HandlePullObjectFailure(object_id, my_address_, &sender_ip);
        if (!success) {
          LOG(ERROR) << "Cannot immediately recover from error. Retrying get location again...";
          reply = gcs_client_.GetLocationSync(object_id, true, my_address_);
          sender_ip = reply.sender_ip;
        }
        LOG(INFO) << "Retry receiving object from " << sender_ip;
      }
    }
    local_store_client_.Seal(object_id);
  }
}

int ReduceReceiverTask::receive_reduced_object(const std::string &sender_ip, int sender_port, bool is_left_child) {
  TIMELINE(std::string("Receiver::receive_reduced_object() ") + reduction_id_.ToString());
  Buffer *stream;
  bool work_on_target_stream = true;
  bool is_sender_leaf;
  if (is_left_child) {
    is_sender_leaf = this->is_left_sender_leaf;
    if (!is_tree_branch_) {
      // directly reduced to the target stream
      stream = target_stream.get();
    } else {
      // reduced to the left stream temporarily, and the right child thread will reduce it to the target stream
      stream = left_stream.get();
      work_on_target_stream = false;
    }
  } else {
    is_sender_leaf = this->is_right_sender_leaf;
    // directly reduced to the target stream
    stream = target_stream.get();
  }
  LOG(DEBUG) << "start receiving object " << reduction_id_.ToString() << " from " << sender_ip
             << ", size = " << stream->Size() << ", intial_progress=" << stream->progress;
  int conn_fd;
  int ec = tcp_connect(sender_ip, sender_port, &conn_fd);
  if (ec) {
    LOG(ERROR) << "Failed to connect to sender (ip=" << sender_ip << ", port=" << sender_port << ").";
    return ec;
  }
  // send request
  ObjectWriterRequest req;
  if (is_sender_leaf) {
    auto ro_request = new ReceiveObjectRequest();
    ro_request->set_object_id(is_left_child ? this->left_sender_object.Binary() : this->right_sender_object.Binary());
    ro_request->set_object_size(stream->Size());
    ro_request->set_offset(stream->progress);
    req.set_allocated_receive_object(ro_request);
  } else {
    auto ro_request = new ReceiveReducedObjectRequest();
    ro_request->set_reduction_id(reduction_id_.Binary());
    ro_request->set_object_size(stream->Size());
    ro_request->set_offset(stream->progress);
    req.set_allocated_receive_reduced_object(ro_request);
  }
  SendProtobufMessage(conn_fd, req);
  // start receiving object
#ifdef HOPLITE_ENABLE_NONBLOCKING_SOCKET_RECV
  DCHECK(fcntl(conn_fd, F_SETFL, fcntl(conn_fd, F_GETFL) | O_NONBLOCK) >= 0)
      << "Cannot enable non-blocking for the socket (errno = " << errno << ").";
#endif
  if (is_left_child) {
    if (!local_object) {
      // no local object, so we only need to receive from the sender
      ec = stream_receive<Buffer>(conn_fd, stream, stream->progress);
    } else {
      ec = stream_reduce_add<Buffer, float>(conn_fd, stream, *local_object, stream->progress);
    }
  } else {
    ec = stream_reduce_add<Buffer, float>(conn_fd, stream, *left_stream, stream->progress);
  }
  LOG(DEBUG) << "receive " << reduction_id_.ToString() << " from " << sender_ip << " done, error_code=" << ec;
  close(conn_fd);
  if (!ec && work_on_target_stream && target_stream->IsFinished() && local_task_) {
    LOG(DEBUG) << "Notify " << reduction_id_.ToString() << " is finished.";
    local_task_->NotifyFinished();
  }
  return ec;
}

void receiver_handle_signal(int sig) {
  pthread_exit(NULL);
}

void ReduceReceiverTask::start_recv(bool is_left_child) {
  auto func = [this, is_left_child](std::string sender_ip) {
    signal(SIGUSR1, receiver_handle_signal);
    int ec = receive_reduced_object(sender_ip, HOPLITE_SENDER_PORT, /*is_left_child=*/is_left_child);
    if (ec) {
      LOG(ERROR) << "Failed to receive object for reduce from sender " << sender_ip;
      gcs_client_.HandleReceiveReducedObjectFailure(reduction_id_, my_address_, sender_ip);
      // later this receiver would be reset
    }
  };
  if (is_left_child) {
    DCHECK(!left_recv_thread_.joinable());
    left_recv_thread_ = std::thread(func, left_sender_ip);
  } else {
    DCHECK(!right_recv_thread_.joinable());
    right_recv_thread_ = std::thread(func, right_sender_ip);
  }
}

void ReduceReceiverTask::reset_progress(bool is_left_child) {
  // clean up previous threads
  if (right_recv_thread_.joinable()) {
    pthread_kill(right_recv_thread_.native_handle(), SIGUSR1);
    right_recv_thread_.join();
  }
  if (left_recv_thread_.joinable()) {
    pthread_kill(left_recv_thread_.native_handle(), SIGUSR1);
    left_recv_thread_.join();
  }
  // target stream is required to reset anyway
  target_stream->progress = 0;
  if (is_left_child && is_tree_branch_) {
    // the left sender first reduces it to the left stream, so both stream needs to be reset
    left_stream->progress = 0;
  }
}

void Receiver::receive_and_reduce_object(const ObjectID &reduction_id, bool is_tree_branch,
                                         const std::string &sender_ip, bool from_left_child, int64_t object_size,
                                         const ObjectID &object_id_to_reduce, const ObjectID &object_id_to_pull,
                                         bool is_sender_leaf, bool reset_progress,
                                         const std::shared_ptr<LocalReduceTask> &local_task) {
  TIMELINE("Receiver::receive_and_reduce_object() ");
  std::lock_guard<std::mutex> lock(reduce_receiver_tasks_mutex_);
  std::shared_ptr<ReduceReceiverTask> task;
  if (!reduce_receiver_tasks_.count(reduction_id)) {
    task = std::make_shared<ReduceReceiverTask>(reduction_id, is_tree_branch, local_task, gcs_client_, my_address_);
    reduce_receiver_tasks_[reduction_id] = task;
  } else {
    task = reduce_receiver_tasks_[reduction_id];
  }
  if (!task->local_object && !object_id_to_reduce.IsNil()) {
    Status s = local_store_client_.GetBufferOrCreate(object_id_to_reduce, object_size, &task->local_object);
    DCHECK(s.ok());
  }
  if (!task->target_stream) {
    if (local_task) {
      Status s = local_store_client_.GetBufferOrCreate(reduction_id, object_size, &task->target_stream);
      DCHECK(s.ok());
    } else {
      task->target_stream = state_.get_or_create_reduction_stream(reduction_id, object_size);
    }
  }
  if (is_tree_branch && !task->left_stream) {
    task->left_stream = std::make_shared<Buffer>(task->target_stream->Size());
  }
  if (from_left_child) {
    task->is_left_sender_leaf = is_sender_leaf;
    task->left_sender_object = object_id_to_pull;
  } else {
    task->is_right_sender_leaf = is_sender_leaf;
    task->right_sender_object = object_id_to_pull;
  }

  if (!reset_progress) {
    if ((from_left_child && !task->left_sender_ip.empty()) || (!from_left_child && !task->right_sender_ip.empty())) {
      return; // the task is running. prevent overriding.
    }
  }

  // override ip address
  if (from_left_child) {
    task->left_sender_ip = sender_ip;
  } else {
    task->right_sender_ip = sender_ip;
  }

  if (!reset_progress) {
    task->start_recv(from_left_child);
  } else {
    // clean up previous threads
    task->reset_progress(from_left_child);
    // restart all tasks
    if (!task->left_sender_ip.empty()) {
      task->start_recv(/*is_left_child=*/true);
    }
    if (!task->right_sender_ip.empty()) {
      task->start_recv(/*is_left_child=*/false);
    }
  }
}
