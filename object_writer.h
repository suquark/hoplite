#ifndef OBJECT_WRITER_H
#define OBJECT_WRITER_H

#include "global_control_store.h"
#include <atomic>
#include <iostream>
#include <plasma/client.h>
#include <thread>

class TCPServer {
public:
  TCPServer(GlobalControlStoreClient &gcs_client, PlasmaClient &plasma_client,
            const std::string &server_ipaddr, int port);

  std::thread run();

  inline int64_t get_progress() { return progress_; }
  inline void set_progress(int64_t progress) { progress_ = progress; }
  inline size_t get_pending_size() { return pending_size_; }
  inline void *get_pending_write() { return pending_write_; }

private:
  void worker_loop_();

  void recv_object_();

  GlobalControlStoreClient &gcs_client_;
  plasma::PlasmaClient &plasma_client_;
  int server_fd_;
  struct sockaddr_in address_;
  // FIXME: here we assume we are downloading only 1 object
  // need to fix this later
  std::atomic_int64_t progress_(0);
  size_t pending_size_;
  void *pending_write_ = NULL;
}

#endif // OBJECT_WRITER_H
