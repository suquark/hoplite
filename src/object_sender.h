#ifndef OBJECT_SENDER_H
#define OBJECT_SENDER_H

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

#include <netinet/in.h> // struct sockaddr_in

#include "global_control_store.h"
#include "local_store_client.h"
#include "object_store.pb.h"
#include "object_store_state.h"

class ObjectSender {
public:
  ObjectSender(ObjectStoreState &state, GlobalControlStoreClient &gcs_client,
               LocalStoreClient &local_store_client,
               const std::string &my_address);

  void AppendTask(const objectstore::ReduceToRequest *request);

  std::thread Run();

  void Shutdown();

private:
  void worker_loop();

  void listener_loop();

  int send_object(int conn_fd, const ObjectID &object_id, int64_t offset);

  void send_object_for_reduce(const objectstore::ReduceToRequest *request);

  std::queue<objectstore::ReduceToRequest *> pending_tasks_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  GlobalControlStoreClient &gcs_client_;
  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;
  std::string my_address_;

  std::atomic<bool> exit_;

  // for the TCP listener
  int server_fd_;
  std::thread server_thread_;
  struct sockaddr_in address_;
  // thread pool for launching tasks
  ctpl::thread_pool pool_;
};

#endif // OBJECT_SENDER_H
