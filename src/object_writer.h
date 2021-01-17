#ifndef OBJECT_WRITER_H
#define OBJECT_WRITER_H

#include <atomic>
#include <iostream>
#include <netinet/in.h> // struct sockaddr_in
#include <thread>

#include "common/buffer.h"
#include "common/id.h"

#include "global_control_store.h"
#include "local_store_client.h"
#include "object_store.grpc.pb.h"
#include "object_store_state.h"
#include "util/ctpl_stl.h"

class TCPServer {
public:
  TCPServer(ObjectStoreState &state, GlobalControlStoreClient &gcs_client, LocalStoreClient &local_store_client,
            const std::string &server_ipaddr, int port);

  inline void Run() { server_thread_ = std::thread(&TCPServer::worker_loop, this); }

  void Shutdown();

private:
  void worker_loop();

  int receive_and_reduce_object(int conn_fd, const ObjectID &reduction_id, const std::vector<ObjectID> &object_ids,
                                bool is_endpoint);

  GlobalControlStoreClient &gcs_client_;
  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;

  int server_fd_;
  std::thread server_thread_;
  const std::string &server_ipaddr_;
  struct sockaddr_in address_;
  // thread pool for launching tasks
  ctpl::thread_pool pool_;
};

#endif // OBJECT_WRITER_H
