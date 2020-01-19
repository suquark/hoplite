#ifndef OBJECT_SENDER_H
#define OBJECT_SENDER_H

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include "local_store_client.h"
#include "global_control_store.h"
#include "object_store.pb.h"
#include "object_store_state.h"

class ObjectSender {
public:
  ObjectSender(ObjectStoreState &state, 
               GlobalControlStoreClient &gcs_client,
               LocalStoreClient &local_store_client, 
               const std::string &my_address);

  void AppendTask(const objectstore::ReduceToRequest *request);

  inline std::thread Run() {
    std::thread sender_thread(&ObjectSender::worker_loop, this);
    return sender_thread;
  }

  void send_object(const objectstore::PullRequest *request);

private:
  void worker_loop();

  void send_object_for_reduce(const objectstore::ReduceToRequest *request);

  std::queue<objectstore::ReduceToRequest *> pending_tasks_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  GlobalControlStoreClient &gcs_client_;
  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;
  std::string my_address_;
};

#endif // OBJECT_SENDER_H
