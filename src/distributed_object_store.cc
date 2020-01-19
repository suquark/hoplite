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
                                           my_address, notification_server_port,
                                           notification_listen_port},
      object_control_{object_sender_, local_store_client_, state_, my_address,
                      grpc_port},
      object_writer_{state_, gcs_client_, local_store_client_, my_address,
                     object_writer_port},
      object_sender_{state_, gcs_client_, local_store_client_, my_address},
      local_store_client_{false, plasma_socket} {
  TIMELINE("DistributedObjectStore construction function");
  // create a thread to receive remote object
  object_writer_thread_ = object_writer_.Run();
  // create a thread to send object
  object_sender_thread_ = object_sender_.Run();
  // create a thread to process pull requests
  object_control_thread_ = object_control_.Run();
  // create a thread to process notifications
  notification_thread_ = gcs_client_.Run();
}

void DistributedObjectStore::Put(const void *data, size_t size,
                                 const ObjectID &object_id) {
  TIMELINE(std::string("DistributedObjectStore Put single object ") +
           object_id.Hex());
  // put object into Plasma
  std::shared_ptr<Buffer> ptr;
  auto pstatus = local_store_client_.Create(object_id, size, &ptr);
  DCHECK(pstatus.ok()) << "Plasma failed to create object_id = "
                       << object_id.Hex() << " size = " << size
                       << ", status = " << pstatus.ToString();
  ptr->CopyFrom((const uint8_t *)data, size);
  local_store_client_.Seal(object_id);
  gcs_client_.WriteLocation(object_id, my_address_, true);
}

ObjectID DistributedObjectStore::Put(const void *data, size_t size) {
  TIMELINE("DistributedObjectStore Put without object_id");
  // generate a random object id
  auto object_id = ObjectID::FromRandom();
  Put(data, size, object_id);
  return object_id;
}

void DistributedObjectStore::Get(const std::vector<ObjectID> &object_ids,
                                 size_t _expected_size,
                                 ObjectID *created_reduction_id,
                                 std::shared_ptr<Buffer> *result) {
  const auto reduction_id = ObjectID::FromRandom();
  *created_reduction_id = reduction_id;
  Get(object_ids, _expected_size, reduction_id, result);
}

void DistributedObjectStore::Get(const std::vector<ObjectID> &object_ids,
                                 size_t _expected_size,
                                 const ObjectID &reduction_id,
                                 std::shared_ptr<Buffer> *result) {
  TIMELINE("DistributedObjectStore Get multiple objects");

  DCHECK(object_ids.size() > 0);
  // TODO: get size by checking the size of ObjectIDs
  std::unordered_set<ObjectID> remaining_ids(object_ids.begin(),
                                             object_ids.end());
  std::vector<ObjectID> local_object_ids;
  // create the endpoint buffer
  std::shared_ptr<Buffer> buffer;
  auto pstatus =
      local_store_client_.Create(reduction_id, _expected_size, &buffer);
  DCHECK(pstatus.ok()) << "Plasma failed to create reduction_id = "
                       << reduction_id.Hex() << " size = " << _expected_size
                       << ", status = " << pstatus.ToString();

  auto reduction_endpoint =
      state_.create_progressive_stream(reduction_id, buffer);

  int node_index = 0;
  ObjectID tail_objectid;
  std::string tail_address;
  // TODO: support different reduce op and types.
  std::shared_ptr<ObjectNotifications> notifications =
      gcs_client_.GetLocationAsync(object_ids, reduction_id.Binary());
  while (remaining_ids.size() > 0) {
    std::vector<std::pair<ObjectID, std::string>> ready_ids =
        notifications->GetNotifications();
    // TODO: we can sort the ready ids by its node address.
    for (auto &ready_id_pair : ready_ids) {
      // FIXME: Somehow the location of the object is not written to Redis.
      ObjectID ready_id = ready_id_pair.first;
      std::string address = ready_id_pair.second;
      DCHECK(address != "")
          << ready_id.ToString()
          << " location is not ready, but notification is received!";
      LOG(INFO) << "Received notification, address = " << address
                << ready_id.ToString();
      if (address == my_address_) {
        // move local objects to another address, because there's no
        // necessary to transfer them through the network.
        local_object_ids.push_back(ready_id);
      } else {
        // wait until at lease 2 objects in nodes except the master node are
        // ready.
        if (node_index == 1) {
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
    // all the objects are local
    // TODO: add support when all the objects are local
    LOG(FATAL) << "All the objects are local";
  } else if (node_index == 1) {
    // only two nodes, no streaming needed
    reply_ok = object_control_.InvokeReduceTo(tail_address, reduction_id,
                                              local_object_ids, my_address_,
                                              true, &tail_objectid);
  } else {
    // more than 2 nodes
    reply_ok = object_control_.InvokeReduceTo(
        tail_address, reduction_id, local_object_ids, my_address_, true);
  }

  DCHECK(reply_ok);
  // wait until the object is fully reduced.
  reduction_endpoint->wait();

  // reduce remaining objects.
  local_store_client_.Seal(reduction_id);

  // get object from Plasma
  *result = buffer;
}

void DistributedObjectStore::Get(const ObjectID &object_id,
                                 std::shared_ptr<Buffer> *result) {
  TIMELINE(std::string("DistributedObjectStore Get single object ") +
           object_id.ToString());
  // get object location from notification server
  std::string address = gcs_client_.GetLocationSync(object_id);

  // send pull request to one of the location
  DCHECK(object_control_.PullObject(address, object_id))
      << "Failed to pull object";

  // get object from Plasma
  std::vector<ObjectBuffer> object_buffers;
  local_store_client_.Get({object_id}, &object_buffers);
  *result = object_buffers[0].data;
}
