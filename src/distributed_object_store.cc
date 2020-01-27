#include <cmath>

#include <unistd.h> // usleep
#include <unordered_set>

#include "common/buffer.h"
#include "common/id.h"
#include "distributed_object_store.h"
#include "logging.h"

DistributedObjectStore::DistributedObjectStore(
    const std::string &notification_server_address, int redis_port,
    int notification_server_port, int notification_listen_port,
    const std::string &plasma_socket, const std::string &my_address,
    int object_writer_port, int grpc_port)
    : my_address_(my_address), gcs_client_{notification_server_address,
                                           my_address_,
                                           notification_server_port,
                                           notification_listen_port},
      local_store_client_{false, plasma_socket},
      object_control_{object_sender_, local_store_client_, state_, my_address_,
                      grpc_port},
      object_writer_{state_, gcs_client_, local_store_client_, my_address_,
                     object_writer_port},
      object_sender_{state_, gcs_client_, local_store_client_, my_address_} {
  TIMELINE("DistributedObjectStore construction function");
  // create a thread to receive remote object
  object_writer_.Run();
  // create a thread to send object
  object_sender_thread_ = object_sender_.Run();
  // create a thread to process pull requests
  object_control_thread_ = object_control_.Run();
  // create a thread to process notifications
  notification_thread_ = gcs_client_.Run();

  gcs_client_.ConnectNotificationServer();
}

DistributedObjectStore::~DistributedObjectStore() {
  TIMELINE("~DistributedObjectStore");
  object_writer_.Shutdown();
  object_sender_.Shutdown();
  object_sender_thread_.join();
  object_control_.Shutdown();
  object_control_thread_.join();
  gcs_client_.Shutdown();
  notification_thread_.join();
  LOG(INFO) << "Object store has been shutdown.";
}

void DistributedObjectStore::IsLocalObject(const ObjectID &object_id) {
  return local_store_client_.ObjectExists(object_id) ||
         state_.get_progressive_stream(object_id);
}

void DistributedObjectStore::Put(const std::shared_ptr<Buffer> &buffer,
                                 const ObjectID &object_id) {
  TIMELINE(std::string("DistributedObjectStore Put single object ") +
           object_id.Hex());
  // put object into Plasma
  std::shared_ptr<Buffer> ptr;
  auto pstatus = local_store_client_.Create(object_id, buffer->Size(), &ptr);
  DCHECK(pstatus.ok()) << "Plasma failed to create object_id = "
                       << object_id.Hex() << " size = " << buffer->Size()
                       << ", status = " << pstatus.ToString();
  ptr->CopyFrom(*buffer);
  local_store_client_.Seal(object_id);
  gcs_client_.WriteLocation(object_id, my_address_, true, buffer->Size(),
                            buffer->Data());
}

ObjectID DistributedObjectStore::Put(const std::shared_ptr<Buffer> &buffer) {
  TIMELINE("DistributedObjectStore Put without object_id");
  // generate a random object id
  auto object_id = ObjectID::FromRandom();
  Put(buffer, object_id);
  return object_id;
}

void DistributedObjectStore::Reduce(const std::vector<ObjectID> &object_ids,
                                    ObjectID *created_reduction_id) {
  const auto reduction_id = ObjectID::FromRandom();
  *created_reduction_id = reduction_id;
  Reduce(object_ids, reduction_id);
}

void DistributedObjectStore::Reduce(const std::vector<ObjectID> &object_ids,
                                    const ObjectID &reduction_id) {
  // TODO: support different reduce op and types.
  TIMELINE("DistributedObjectStore Async Reduce");
  DCHECK(object_ids.size() > 0);
  // starting a thread
  std::thread reduction_thread(&DistributedObjectStore::poll_and_reduce, this,
                               object_ids, reduction_id);
  {
    std::lock_guard<std::mutex> l(reduction_tasks_mutex_);
    reduction_tasks_[reduction_id] = {nullptr, std::move(reduction_thread)};
  }
}

void DistributedObjectStore::poll_and_reduce(
    const std::vector<ObjectID> object_ids, const ObjectID reduction_id) {
  TIMELINE("DistributedObjectStore Reduce Thread");
  // we do not use reference for its parameters because it will be executed
  // in a thread.

  std::shared_ptr<ObjectNotifications> notifications =
      gcs_client_.GetLocationAsync(object_ids, reduction_id.Binary(), false);

  // states for enumerating the chain
  std::unordered_set<ObjectID> remaining_ids(object_ids.begin(),
                                             object_ids.end());
  std::vector<ObjectID> local_object_ids;
  int node_index = 0;
  ObjectID tail_objectid;
  std::string tail_address;

  // the buffer for reduction results
  std::shared_ptr<Buffer> buffer;

  // main loop for constructing the reduction chain.
  while (remaining_ids.size() > 0) {
    std::vector<NotificationMessage> ready_ids =
        notifications->GetNotifications();
    // TODO: we should group ready ids by their node address.
    for (auto &ready_id_message : ready_ids) {
      ObjectID ready_id = ready_id_message.object_id;
      std::string address = ready_id_message.sender_ip;
      size_t object_size = ready_id_message.object_size;
      const std::string &inband_data = ready_id_message.inband_data;
      if (check_and_store_inband_data(ready_id, object_size, inband_data)) {
        // mark this object as local
        address = my_address_;
      }
      DCHECK(address != "")
          << ready_id.ToString()
          << " location is not ready, but notification is received!";
      LOG(INFO) << "Received notification, address = " << address
                << ", object_id = " << ready_id.ToString();

      if (!buffer) {
        // create the endpoint buffer
        auto pstatus =
            local_store_client_.Create(reduction_id, object_size, &buffer);
        DCHECK(pstatus.ok())
            << "Plasma failed to create reduction_id = " << reduction_id.Hex()
            << " size = " << object_size << ", status = " << pstatus.ToString();
      }

      if (address == my_address_) {
        // move local objects to another address, because there's no
        // necessary to transfer them through the network.
        local_object_ids.push_back(ready_id);
      } else {
        // wait until at lease 2 objects in nodes except the master node are
        // ready.
        if (node_index == 0) {
          auto reduction_endpoint =
              state_.create_progressive_stream(reduction_id, buffer);
          {
            std::lock_guard<std::mutex> l(reduction_tasks_mutex_);
            reduction_tasks_[reduction_id].stream = reduction_endpoint;
          }
        } else if (node_index == 1) {
          // Send 'ReduceTo' command to the first node in the chain.
          bool reply_ok = object_control_.InvokeReduceTo(
              tail_address, reduction_id, {ready_id}, address, false,
              &tail_objectid);
          DCHECK(reply_ok);
        } else if (node_index > 1) {
          // Send 'ReduceTo' command to the other node in the chain.
          bool reply_ok = object_control_.InvokeReduceTo(
              tail_address, reduction_id, {ready_id}, address, false);
          DCHECK(reply_ok);
        }
        tail_objectid = ready_id;
        tail_address = address;
        node_index++;
      }
      // mark it as done
      remaining_ids.erase(ready_id);
    }
  }

  // send the reduced object back to the master node.
  bool reply_ok = false;
  if (node_index == 0) {
    // all the objects are local, we just reduce them locally
    LOG(INFO) << "All the objects to be reduced are local";
    // TODO: support more reduction types & ops
    reduce_local_objects<float>(local_object_ids, buffer.get());
    local_store_client_.Seal(reduction_id);
    // write the location just like in 'Put()'
    gcs_client_.WriteLocation(reduction_id, my_address_, true, buffer->Size(),
                              buffer->Data());
  } else if (node_index == 1) {
    // only two nodes, no streaming needed
    reply_ok = object_control_.InvokeReduceTo(tail_address, reduction_id,
                                              local_object_ids, my_address_,
                                              true, &tail_objectid);
    DCHECK(reply_ok);
  } else {
    // more than 2 nodes
    reply_ok = object_control_.InvokeReduceTo(
        tail_address, reduction_id, local_object_ids, my_address_, true);
    DCHECK(reply_ok);
  }
}

void DistributedObjectStore::poll_and_reduce_k_dimension(
    const std::vector<ObjectID> object_ids, const ObjectID reduction_id,
    int k_dimension) {
  TIMELINE("DistributedObjectStore Reduce Thread 2D");
  // we do not use reference for its parameters because it will be executed
  // in a thread.

  // TODO: separate local objects first
  size_t n_objects = object_ids.size();
  int max_dimension = floor(log2(n_objects));
  if (k_dimension > max_dimension) {
    LOG(WARNING) << "The specified dimension is greater than the maximum dimension. "
                 << "Use the maximum dimension instead.";
    k_dimension = max_dimension;
  }
  double avg_edge_length = pow(n_objects, 1. / max_dimension);
  std::vector<int> edge_lengths;
  edge_lengths.push_back()

  std::shared_ptr<ObjectNotifications> notifications =
      gcs_client_.GetLocationAsync(object_ids, reduction_id.Binary(), false);

  // states for enumerating the chain
  std::unordered_set<ObjectID> remaining_ids(object_ids.begin(),
                                             object_ids.end());
  std::vector<ObjectID> local_object_ids;
  int node_index = 0;
  ObjectID tail_objectid;
  std::string tail_address;

  // the buffer for reduction results
  std::shared_ptr<Buffer> buffer;

  // main loop for constructing the reduction chain.
  while (remaining_ids.size() > 0) {
    std::vector<NotificationMessage> ready_ids =
        notifications->GetNotifications();
    // TODO: we should group ready ids by their node address.
    for (auto &ready_id_message : ready_ids) {
      ObjectID ready_id = ready_id_message.object_id;
      std::string address = ready_id_message.sender_ip;
      size_t object_size = ready_id_message.object_size;
      const std::string &inband_data = ready_id_message.inband_data;
      if (check_and_store_inband_data(ready_id, object_size, inband_data)) {
        // mark this object as local
        address = my_address_;
      }
      DCHECK(address != "")
          << ready_id.ToString()
          << " location is not ready, but notification is received!";
      LOG(INFO) << "Received notification, address = " << address
                << ", object_id = " << ready_id.ToString();

      if (!buffer) {
        // create the endpoint buffer
        auto pstatus =
            local_store_client_.Create(reduction_id, object_size, &buffer);
        DCHECK(pstatus.ok())
            << "Plasma failed to create reduction_id = " << reduction_id.Hex()
            << " size = " << object_size << ", status = " << pstatus.ToString();
      }

      if (address == my_address_) {
        // move local objects to another address, because there's no
        // necessary to transfer them through the network.
        local_object_ids.push_back(ready_id);
      } else {
        // wait until at lease 2 objects in nodes except the master node are
        // ready.
        if (node_index == 0) {
          auto reduction_endpoint =
              state_.create_progressive_stream(reduction_id, buffer);
          {
            std::lock_guard<std::mutex> l(reduction_tasks_mutex_);
            reduction_tasks_[reduction_id].stream = reduction_endpoint;
          }
        } else if (node_index == 1) {
          // Send 'ReduceTo' command to the first node in the chain.
          bool reply_ok = object_control_.InvokeReduceTo(
              tail_address, reduction_id, {ready_id}, address, false,
              &tail_objectid);
          DCHECK(reply_ok);
        } else if (node_index > 1) {
          // Send 'ReduceTo' command to the other node in the chain.
          bool reply_ok = object_control_.InvokeReduceTo(
              tail_address, reduction_id, {ready_id}, address, false);
          DCHECK(reply_ok);
        }
        tail_objectid = ready_id;
        tail_address = address;
        node_index++;
      }
      // mark it as done
      remaining_ids.erase(ready_id);
    }
  }

  // send the reduced object back to the master node.
  bool reply_ok = false;
  if (node_index == 0) {
    // all the objects are local, we just reduce them locally
    LOG(INFO) << "All the objects to be reduced are local";
    // TODO: support more reduction types & ops
    reduce_local_objects<float>(local_object_ids, buffer.get());
    local_store_client_.Seal(reduction_id);
    // write the location just like in 'Put()'
    gcs_client_.WriteLocation(reduction_id, my_address_, true, buffer->Size(),
                              buffer->Data());
  } else if (node_index == 1) {
    // only two nodes, no streaming needed
    reply_ok = object_control_.InvokeReduceTo(tail_address, reduction_id,
                                              local_object_ids, my_address_,
                                              true, &tail_objectid);
    DCHECK(reply_ok);
  } else {
    // more than 2 nodes
    reply_ok = object_control_.InvokeReduceTo(
        tail_address, reduction_id, local_object_ids, my_address_, true);
    DCHECK(reply_ok);
  }
}

void DistributedObjectStore::Get(const ObjectID &object_id,
                                 std::shared_ptr<Buffer> *result) {
  TIMELINE(std::string("DistributedObjectStore Get single object ") +
           object_id.ToString());

  // FIXME: currently the object store will assume that the object
  // exists even before 'Seal' is called. This will cause the problem
  // that an on-going reduction task could be skipped. Here we just
  // reorder the checking process as a workaround.
  std::unique_lock<std::mutex> l(reduction_tasks_mutex_);
  auto search = reduction_tasks_.find(object_id);
  if (search != reduction_tasks_.end()) {
    l.unlock();
    // ==> This ObjectID belongs to a reduction task.
    auto &reduction_task_pair = search->second;
    // we must join the thread first, because the stream
    // pointer could still be nullptr at creation.
    reduction_task_pair.reduction_thread.join();
    auto &stream = reduction_task_pair.stream;
    if (stream) {
      LOG(DEBUG) << "waiting the reduction stream";
      // wait until the object is fully reduced
      stream->wait();
      local_store_client_.Seal(object_id);
    }
    // TODO: should we add this line?
    // gcs_client_.WriteLocation(object_id, my_address_, true, stream->size(),
    //                           stream->data());
    l.lock();
    reduction_tasks_.erase(object_id);
    l.unlock();
  } else {
    l.unlock();
    if (!local_store_client_.ObjectExists(object_id)) {
      // ==> This ObjectID refers to a remote object.
      SyncReply reply = gcs_client_.GetLocationSync(object_id, true);
      if (!check_and_store_inband_data(object_id, reply.object_size,
                                       reply.inband_data)) {
        // send pull request to one of the location
        DCHECK(object_control_.PullObject(reply.sender_ip, object_id))
            << "Failed to pull object";
      }
    }
  }

  // get object from local store
  ObjectBuffer object_buffer;
  local_store_client_.Get(object_id, &object_buffer);
  *result = object_buffer.data;
}

bool DistributedObjectStore::check_and_store_inband_data(
    const ObjectID &object_id, int64_t object_size,
    const std::string &inband_data) {
  TIMELINE("DistributedObjectStore::check_and_store_inband_data");
  if (inband_data.size() > 0) {
    LOG(DEBUG) << "fetching object directly from inband data";
    DCHECK(inband_data.size() <= inband_data_size_limit)
        << "unexpected inband data size";
    std::shared_ptr<Buffer> data;
    local_store_client_.Create(object_id, object_size, &data);
    data->CopyFrom(inband_data);
    local_store_client_.Seal(object_id);
    return true;
  }
  return false;
}
