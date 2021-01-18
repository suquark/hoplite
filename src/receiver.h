#pragma once

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

class Receiver {
public:
  Receiver(ObjectStoreState &state, GlobalControlStoreClient &gcs_client, LocalStoreClient &local_store_client,
           const std::string &my_address, int port);

  /// If the inband data exists, store the object with the inband data.
  bool check_and_store_inband_data(const ObjectID &object_id, int64_t object_size, const std::string &inband_data);

  /// Receive object from the sender. This is a low-level function. The object receiving
  /// starts from the initial progress of the stream.
  /// \param sender_ip The IP address of the sender.
  /// \param sender_port The port of the sender.
  /// \param object_id The ID of the object.
  /// \param stream The buffer for receiving the object.
  /// \return The error code. 0 means success.
  int receive_object(const std::string &sender_ip, int sender_port, const ObjectID &object_id, Buffer *stream);

  /// Pull object from remote object store. This function also provides fault tolerance.
  /// \param object_id The object to pull.
  void pull_object(const ObjectID &object_id);

private:
  GlobalControlStoreClient &gcs_client_;
  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;

  const std::string &my_address_;
  struct sockaddr_in address_;
  // thread pool for launching tasks
  ctpl::thread_pool pool_;
};
