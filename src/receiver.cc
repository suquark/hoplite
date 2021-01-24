#include "receiver.h"
#include "common/config.h"
#include "finegrained_pipelining.h"

#include "object_store.pb.h"
#include "util/protobuf_utils.h"

using objectstore::ObjectWriterRequest;
using objectstore::ReceiveObjectRequest;
using objectstore::ReceiveReducedObjectRequest;

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
  TIMELINE(std::string("Receiver::receive_object() ") + reduction_id_.ToString());
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
  if (is_left_child) {
    left_recv_conn_fd_ = conn_fd;
  } else {
    right_recv_conn_fd_ = conn_fd;
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

  if (intended_reset_) {
    // when the outside code close the fd, the fd may not have been updated. double check and handle it here
    close(conn_fd);
    return -1;
  }

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
    local_task_->NotifyFinished();
  }
  return ec;
}

void ReduceReceiverTask::start_recv(const std::string &sender_ip, bool is_left_child) {
  auto func = [&, sender_ip, is_left_child]() {
    int ec = receive_reduced_object(sender_ip, HOPLITE_SENDER_PORT, /*is_left_child=*/is_left_child);
    if (ec) {
      if (!intended_reset_) {
        LOG(FATAL) << "Failed to receive object for reduce from sender " << sender_ip;
        // TODO(siyuan): handle failure.
      } else {
        LOG(INFO) << "Intended reset receiving object for reduce from sender " << sender_ip;
      }
    }
  };
  if (is_left_child) {
    DCHECK(!left_recv_thread_.joinable());
    left_sender_ip_ = sender_ip;
    left_recv_thread_ = std::thread(func);
  } else {
    DCHECK(!right_recv_thread_.joinable());
    right_recv_thread_ = std::thread(func);
    right_sender_ip_ = sender_ip;
  }
}

void ReduceReceiverTask::reset_recv(const std::string &new_sender_ip, bool is_left_child) {
  intended_reset_ = true;
  close(left_recv_conn_fd_);
  close(right_recv_conn_fd_);
  if (left_recv_thread_.joinable()) {
    left_recv_thread_.join();
  }
  if (right_recv_thread_.joinable()) {
    right_recv_thread_.join();
  }
  intended_reset_ = false;
  // target stream is required to reset anyway
  target_stream->progress = 0;
  if (is_left_child) {
    if (is_tree_branch_) {
      // the left sender first reduces it to the left stream, so both stream needs to be reset
      left_stream->progress = 0;
    }
    start_recv(new_sender_ip, /*is_left_child=*/true);
    start_recv(right_sender_ip_, /*is_left_child=*/false);
  } else {
    DCHECK(is_tree_branch_);
    start_recv(left_sender_ip_, /*is_left_child=*/true);
    start_recv(new_sender_ip, /*is_left_child=*/false);
  }
}

void Receiver::receive_and_reduce_object(const ObjectID &reduction_id, bool is_tree_branch,
                                         const std::string &sender_ip, bool from_left_child, int64_t object_size,
                                         const ObjectID &object_id_to_reduce, const ObjectID &object_id_to_pull,
                                         bool is_sender_leaf, const std::shared_ptr<LocalReduceTask> &local_task) {
  TIMELINE("Receiver::receive_and_reduce_object() ");
  std::shared_ptr<ReduceReceiverTask> task;
  {
    std::lock_guard<std::mutex> lock(reduce_receiver_tasks_mutex_);
    if (!reduce_receiver_tasks_.count(reduction_id)) {
      task = std::make_shared<ReduceReceiverTask>(reduction_id, is_tree_branch, local_task);
      reduce_receiver_tasks_[reduction_id] = task;
    } else {
      task = reduce_receiver_tasks_[reduction_id];
    }
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
  task->start_recv(sender_ip, from_left_child);
}
