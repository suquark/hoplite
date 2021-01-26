#pragma once

#include <atomic>
#include <iostream>
#include <netinet/in.h> // struct sockaddr_in
#include <thread>
#include <unordered_map>

#include "common/buffer.h"
#include "common/id.h"

#include "global_control_store.h"
#include "local_store_client.h"
#include "object_store.grpc.pb.h"
#include "object_store_state.h"
#include "util/ctpl_stl.h"

struct ReduceReceiverTask {
  ReduceReceiverTask(const ObjectID &reduction_id, bool is_tree_branch,
                     const std::shared_ptr<LocalReduceTask> &local_task, GlobalControlStoreClient &gcs_client,
                     const std::string &my_address)
      : reduction_id_(reduction_id), is_tree_branch_(is_tree_branch), local_task_(local_task), gcs_client_(gcs_client),
        my_address_(my_address) {}
  volatile int left_recv_conn_fd_;
  volatile int right_recv_conn_fd_;
  int receive_reduced_object(const std::string &sender_ip, int sender_port, bool is_left_child);

  std::shared_ptr<Buffer> target_stream;
  std::shared_ptr<Buffer> local_object;
  std::shared_ptr<Buffer> left_stream;
  bool is_left_sender_leaf = false;
  ObjectID left_sender_object;
  bool is_right_sender_leaf = false;
  ObjectID right_sender_object;

  void start_recv(const std::string &sender_ip, bool is_left_child);
  void reset_recv(const std::string &new_sender_ip, bool is_left_child);

private:
  ObjectID reduction_id_;
  const bool is_tree_branch_;
  std::string left_sender_ip_;
  std::string right_sender_ip_;
  std::thread left_recv_thread_;
  std::thread right_recv_thread_;
  std::shared_ptr<LocalReduceTask> local_task_;
  GlobalControlStoreClient &gcs_client_;
  const std::string &my_address_;
};

class Receiver {
public:
  Receiver(ObjectStoreState &state, GlobalControlStoreClient &gcs_client, LocalStoreClient &local_store_client,
           const std::string &my_address, int port);

  /// If the inband data exists, store the object with the inband data.
  bool check_and_store_inband_data(const ObjectID &object_id, int64_t object_size, const std::string &inband_data);

  /// Pull object from remote object store. This function also provides fault tolerance.
  /// \param object_id The object to pull.
  void pull_object(const ObjectID &object_id);

  /// \param object_id_to_reduce If IsNil, then we skip reducing the local object. This would happen on
  /// the reduce caller, where the receiver has no object to reduce.
  void receive_and_reduce_object(const ObjectID &reduction_id, bool is_tree_branch, const std::string &sender_ip,
                                 bool from_left_child, int64_t object_size, const ObjectID &object_id_to_reduce,
                                 const ObjectID &object_id_to_pull, bool is_sender_leaf,
                                 const std::shared_ptr<LocalReduceTask> &local_task);

  void reset_reduced_object(const ObjectID &reduction_id, const std::string &new_sender_ip, bool from_left_child);

private:
  /// Receive object from the sender. This is a low-level function. The object receiving
  /// starts from the initial progress of the stream.
  /// \param sender_ip The IP address of the sender.
  /// \param sender_port The port of the sender.
  /// \param object_id The ID of the object.
  /// \param stream The buffer for receiving the object.
  /// \return The error code. 0 means success.
  int receive_object(const std::string &sender_ip, int sender_port, const ObjectID &object_id, Buffer *stream);

  GlobalControlStoreClient &gcs_client_;
  LocalStoreClient &local_store_client_;
  ObjectStoreState &state_;

  const std::string &my_address_;
  struct sockaddr_in address_;
  // thread pool for launching tasks
  ctpl::thread_pool pool_;
  // on going reducing tasks
  std::unordered_map<ObjectID, std::shared_ptr<ReduceReceiverTask>> reduce_receiver_tasks_;
  std::mutex reduce_receiver_tasks_mutex_;
};
