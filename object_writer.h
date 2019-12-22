#ifndef OBJECT_WRITER_H
#define OBJECT_WRITER_H

#include <atomic>
#include <iostream>
#include <thread>

#include <netinet/in.h> // struct sockaddr_in

#include <plasma/client.h>

#include "global_control_store.h"
#include "object_store_state.h"

class TCPServer {
public:
  TCPServer(ObjectStoreState &state, GlobalControlStoreClient &gcs_client,
            plasma::PlasmaClient &plasma_client,
            const std::string &server_ipaddr, int port);

  inline std::thread Run() {
    std::thread tcp_thread(&TCPServer::worker_loop, this);
    return tcp_thread;
  }

private:
  void worker_loop();

  void receive_object(int conn_fd);
  void receive_and_reduce_object(int conn_fd);

  GlobalControlStoreClient &gcs_client_;
  plasma::PlasmaClient &plasma_client_;
  ObjectStoreState &state_;

  int server_fd_;
  const std::string &server_ipaddr_;
  struct sockaddr_in address_;
};

#endif // OBJECT_WRITER_H
