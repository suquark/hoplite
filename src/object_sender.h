#ifndef OBJECT_SENDER_H
#define OBJECT_SENDER_H

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "local_store_client.h"
#include "object_store.pb.h"
#include "object_store_state.h"

class ObjectSender {
public:
  ObjectSender(ObjectStoreState &state, LocalStoreClient &local_store_client);

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
  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;
};

#endif // OBJECT_SENDER_H
