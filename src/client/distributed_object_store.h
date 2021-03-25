#ifndef DISTRIBUTED_OBJECT_STORE_H
#define DISTRIBUTED_OBJECT_STORE_H

#include <cstdint>
#include <string>
#include <vector>
// common headers
#include "common/buffer.h"
#include "common/id.h"
// components headers
#include "global_control_store.h"
#include "local_store_client.h"
#include "notification_listener.h"
#include "object_sender.h"
#include "object_store_state.h"
#include "receiver.h"

class DistributedObjectStore {
public:
  explicit DistributedObjectStore(const std::string &object_directory_address);

  ~DistributedObjectStore();

  void Put(const std::shared_ptr<Buffer> &buffer, const ObjectID &object_id);

  ObjectID Put(const std::shared_ptr<Buffer> &buffer);

  void Reduce(const std::vector<ObjectID> &object_ids, ObjectID *created_reduction_id, ssize_t num_reduce_objects = -1);

  void Reduce(const std::vector<ObjectID> &object_ids, const ObjectID &reduction_id, ssize_t num_reduce_objects = -1);

  void Get(const ObjectID &object_id, std::shared_ptr<Buffer> *result);

  bool IsLocalObject(const ObjectID &object_id, int64_t *size);

  std::unordered_set<ObjectID> GetReducedObjects(const ObjectID &reduction_id);

private:
  template <typename T> void reduce_local_objects(const std::vector<ObjectID> &object_ids, Buffer *output) {
    DCHECK(output->Size() % sizeof(T) == 0) << "Buffer size cannot be divide whole by the element size";
    auto num_elements = output->Size() / sizeof(T);
    T *target = (T *)output->MutableData();
    bool first = true;
    // TODO: implement parallel reducing
    for (const auto &object_id : object_ids) {
      // TODO: those object_ids could also be local streams.
      ObjectBuffer object_buffer;
      DCHECK(local_store_client_.ObjectExists(object_id)) << "ObjectID not in local store";
      local_store_client_.Get(object_id, &object_buffer);
      std::shared_ptr<Buffer> buf = object_buffer.data;
      const T *data_ptr = (const T *)buf->Data();
      if (!first) {
        for (int64_t i = 0; i < num_elements; i++)
          target[i] += data_ptr[i];
      } else {
        for (int64_t i = 0; i < num_elements; i++)
          target[i] = data_ptr[i];
        first = false;
      }
    }
    // TODO: try to pipeline this
    output->progress = output->Size();
  }

  // order of fields should be kept for proper initialization order
  std::string my_address_;
  ObjectStoreState state_;
  GlobalControlStoreClient gcs_client_;
  LocalStoreClient local_store_client_;
  ObjectSender object_sender_;
  Receiver receiver_;
  NotificationListener notification_listener_;
};

#endif // DISTRIBUTED_OBJECT_STORE_H
