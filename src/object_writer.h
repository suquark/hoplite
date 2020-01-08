#ifndef OBJECT_WRITER_H
#define OBJECT_WRITER_H

#include <atomic>
#include <iostream>
#include <thread>
#include <netinet/in.h> // struct sockaddr_in
#include <plasma/common.h>

#include "global_control_store.h"
#include "object_store.grpc.pb.h"
#include "object_store_state.h"
#include "local_store_client.h"

class TCPServer {
public:
  TCPServer(ObjectStoreState &state, GlobalControlStoreClient &gcs_client,
            LocalStoreClient &local_store_client,
            const std::string &server_ipaddr, int port);

  inline std::thread Run() {
    std::thread tcp_thread(&TCPServer::worker_loop, this);
    return tcp_thread;
  }

private:
  void worker_loop();

  void receive_object(int conn_fd, const plasma::ObjectID &object_id,
                      int64_t object_size);

  void
  receive_and_reduce_object(int conn_fd, const plasma::ObjectID &reduction_id,
                            const std::vector<plasma::ObjectID> &object_ids,
                            bool is_endpoint);

  GlobalControlStoreClient &gcs_client_;
  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;

  int server_fd_;
  const std::string &server_ipaddr_;
  struct sockaddr_in address_;
};

#endif // OBJECT_WRITER_H
