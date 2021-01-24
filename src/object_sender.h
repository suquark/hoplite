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
  ObjectSender(ObjectStoreState &state, GlobalControlStoreClient &gcs_client, LocalStoreClient &local_store_client,
               const std::string &my_address);

  void Run();

  void Shutdown();

private:
  void listener_loop();

  int send_object(int conn_fd, const ObjectID &object_id, int64_t object_size, int64_t offset);

  int send_reduced_object(int conn_fd, const ObjectID &object_id, int64_t object_size);

  GlobalControlStoreClient &gcs_client_;
  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;
  std::string my_address_;

  // for the TCP listener
  int server_fd_;
  std::thread server_thread_;
  struct sockaddr_in address_;
  // thread pool for launching tasks
  ctpl::thread_pool pool_;
};

#endif // OBJECT_SENDER_H
