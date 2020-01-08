#ifndef OBJECT_SENDER_H
#define OBJECT_SENDER_H

#include <list>
#include <thread>

#include "object_store.pb.h"
#include "object_store_state.h"
#include "local_store_client.h"

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

  std::list<objectstore::ReduceToRequest *> pending_tasks_;

  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;
};

#endif // OBJECT_SENDER_H
