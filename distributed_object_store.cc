#include <unistd.h> // usleep
#include <unordered_set>

#include "distributed_object_store.h"
#include "logging.h"
#include <plasma/test_util.h>

using namespace plasma;

DistributedObjectStore::DistributedObjectStore(
    const std::string &redis_address, int redis_port,
    int redis_notification_port, const std::string &plasma_socket,
    const std::string &my_address, int object_writer_port, int grpc_port)
    : my_address_(my_address), gcs_client_{redis_address, redis_port, my_address,
                                           redis_notification_port},
      object_control_{object_sender_, plasma_client_, state_, my_address,
                      grpc_port},
      object_writer_{state_, gcs_client_, plasma_client_, my_address,
                     object_writer_port},
      object_sender_{state_, plasma_client_} {
  // connect to the plasma store
  plasma_client_.Connect(plasma_socket, "");
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
                                 ObjectID object_id) {
  // put object into Plasma
  std::shared_ptr<Buffer> ptr;
  plasma_client_.Create(object_id, size, NULL, 0, &ptr);
  memcpy(ptr->mutable_data(), data, size);
  plasma_client_.Seal(object_id);
  gcs_client_.write_object_location(object_id, my_address_);
  gcs_client_.PublishObjectCompletionEvent(object_id);
}

ObjectID DistributedObjectStore::Put(const void *data, size_t size) {
  // generate a random object id
  ObjectID object_id = random_object_id();
  Put(data, size, object_id);
  return object_id;
}

void DistributedObjectStore::Get(const std::vector<ObjectID> &object_ids,
                                 const void **data, size_t *size,
                                 size_t _expected_size) {
  DCHECK(object_ids.size() > 0);
  // TODO: get size by checking the size of ObjectIDs
  std::unordered_set<ObjectID> remaining_ids(object_ids.begin(),
                                             object_ids.end());
  ObjectID reduction_id = random_object_id();
  // create the endpoint buffer
  std::shared_ptr<Buffer> buffer;
  plasma_client_.Create(reduction_id, _expected_size, NULL, 0, &buffer);
  auto reduction_endpoint =
      state_.create_reduction_endpoint(reduction_id, buffer);

  int node_index = 0;
  ObjectID tail_objectid;
  std::string tail_address;
  // TODO: support different reduce op and types
  ObjectNotifications *notifications =
      gcs_client_.subscribe_object_locations(object_ids, true);
  while (remaining_ids.size() > 0) {
    std::vector<ObjectID> ready_ids = notifications->GetNotifications();
    for (auto &ready_id : ready_ids) {
      // FIXME: Somehow the location of the object is not written to Redis.
      std::string address = gcs_client_.get_object_location(ready_id);
      DCHECK(address != "")
          << "object (" << ready_id.hex()
          << ") location is not ready, but notification is received!";
      LOG(INFO) << "Received notification, address = " << address
                << ", object_id = " << ready_id.hex();
      if (node_index == 1) {
        bool reply_ok = object_control_.InvokeReduceTo(
            tail_address, reduction_id, ready_id, address, &tail_objectid);
        DCHECK(reply_ok);
      } else if (node_index > 1) {
        bool reply_ok = object_control_.InvokeReduceTo(
            tail_address, reduction_id, ready_id, address);
        DCHECK(reply_ok);
      }
      tail_objectid = ready_id;
      tail_address = address;
      node_index++;
      // mark it as done
      remaining_ids.erase(ready_id);
    }
    usleep(10);
  }
  // send it back to self
  bool reply_ok = false;
  if (object_ids.size() > 1) {
    reply_ok = object_control_.InvokeReduceTo(tail_address, reduction_id,
                                              reduction_id, my_address_);
  } else {
    reply_ok = object_control_.InvokeReduceTo(
        tail_address, reduction_id, reduction_id, my_address_, &tail_objectid);
  }

  DCHECK(reply_ok);
  reduction_endpoint->finished.lock();
  reduction_endpoint->finished.unlock();
  plasma_client_.Seal(reduction_id);
  gcs_client_.unsubscribe_object_locations(notifications);

  // get object from Plasma
  *data = buffer->data();
  *size = buffer->size();
}

void DistributedObjectStore::Get(ObjectID object_id, const void **data,
                                 size_t *size) {
  // get object location from redis
  while (true) {
    std::string address = gcs_client_.get_object_location(object_id);

    // send pull request to one of the location
    bool reply_ok = object_control_.PullObject(address, object_id);

    if (reply_ok) {
      break;
    }
    // if the sender is busy, wait for 1 millisecond and try again
    usleep(1000);
  }

  // get object from Plasma
  std::vector<ObjectBuffer> object_buffers;
  plasma_client_.Get({object_id}, -1, &object_buffers);

  *data = object_buffers[0].data->data();
  *size = object_buffers[0].data->size();
}
