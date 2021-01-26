#pragma once
#include <cstdint>

#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/id.h"

struct Node {
  // assotiated with the reduced object
  ObjectID object_id;
  std::string owner_ip;
  bool finished = false;
  bool is_tree_node = false;

  Node *parent = NULL;
  // In the chain, one node only has left node.
  Node *left_child = NULL;
  Node *right_child = NULL;

  int subtree_size = -1;
  int order = -1;
  // If True, the node is currently in failed state
  bool failed = false;

  bool is_root() const { return parent == NULL; }
  bool is_tree_branch() const { return left_child != NULL && right_child != NULL; }
  bool is_leaf() const { return left_child == NULL && right_child == NULL; }
  bool location_known() const { return !owner_ip.empty(); }

  // set finished recursively
  void set_finished() {
    finished = true;
    if (left_child) {
      left_child->set_finished();
    }
    if (right_child) {
      right_child->set_finished();
    }
  }

  int init_order() {
    if (parent == NULL) {
      order = left_child->subtree_size;
    } else {
      if (parent->left_child == this) {
        if (right_child != NULL) {
          order = parent->order - right_child->subtree_size - 1;
        } else {
          order = parent->order - 1;
        }
      } else {
        if (left_child != NULL) {
          order = parent->order + left_child->subtree_size + 1;
        } else {
          order = parent->order + 1;
        }
      }
    }
    return order;
  }
};

struct InbandDataNode : Node {
  std::vector<float> reduced_inband_data;
  std::string get_inband_data() {
    return std::string((char *)reduced_inband_data.data(), reduced_inband_data.size() * sizeof(float));
  }
};

class ReduceTreeChain {
public:
  /// Constructor
  /// \param[in] object_count includes remote objects and the node that invokes reduction.
  /// \param[in] maximum_chain_length The maximum length of the chain part.
  ReduceTreeChain(int64_t object_count, int64_t maximum_chain_length);

  /// Get the node by the index in the tree.
  Node *GetNode(int index) { return map_[index]; }

  /// Get the root node.
  Node *GetRoot() {
    if (tree_.size()) {
      return &tree_[0];
    } else {
      return &chains_[0].back();
    }
  }

  /// Return a debug string.
  /// \return A string helpful for debugging.
  std::string DebugString();

private:
  std::vector<Node> tree_;
  std::vector<std::vector<Node>> chains_;
  std::vector<Node *> map_;
  int depth_ = 0;

  int64_t object_count_;
  int64_t maximum_chain_length_;
};

// TODO(siyuan): support more reduce types
class ReduceTask {
public:
  ReduceTask(const std::string &reduce_dst, const std::vector<ObjectID> &remote_objects_for_reduce,
             const ObjectID &reduction_id, int num_reduce_objects)
      : reduce_dst_(reduce_dst), remote_objects_for_reduce_(remote_objects_for_reduce), reduction_id_(reduction_id),
        num_reduce_objects_(num_reduce_objects) {}

  Node *AddObject(const ObjectID &object_id, int64_t object_size, const std::string &owner_ip);

  ///
  /// \return The destination node.
  InbandDataNode *AddInbandObject(const ObjectID &object_id, const std::string &inband_data);

  std::vector<float> &GetInbandReducedData() { return reduced_inband_data_; }

  ObjectID GetReductionID() const { return reduction_id_; }

  int64_t GetObjectSize() const { return object_size_; }

  std::string DebugString() {
    if (rtc_) {
      return rtc_->DebugString();
    } else {
      return "Debug string is not available";
    }
  }

  /// Mark the associated node as complete.
  /// \param[in] receiver_ip The address of the receiver reporting this completion.
  void CompleteReduce(const std::string &receiver_ip) { owner_to_node_[receiver_ip]->set_finished(); }

  /// Return the node by IP address.
  /// \param[in] ip_address The address associated to the node.
  /// \return The pointer of the node. If the IP of the node is unknown, return NULL.
  Node *GetNodeByIPAddress(const std::string &ip_address) {
    auto search = owner_to_node_.find(ip_address);
    if (search != owner_to_node_.end()) {
      return search->second;
    }
    return NULL;
  }

  /// \param[out] failed_node
  /// \return True if the node is reassigned.
  bool ReassignFailedNode(Node *failed_node);

private:
  std::string reduce_dst_;
  std::vector<ObjectID> remote_objects_for_reduce_;
  ObjectID reduction_id_;
  int64_t object_size_ = -1;
  int num_reduce_objects_;
  int num_ready_objects_ = 0;
  std::unique_ptr<ReduceTreeChain> rtc_;
  std::unordered_map<std::string, Node *> owner_to_node_;
  std::queue<std::pair<ObjectID, std::string>> backup_objects_;
  std::queue<Node *> suspended_nodes_;
  // for inband data
  std::vector<float> reduced_inband_data_;
  InbandDataNode reduced_inband_dst_;
};

class ReduceManager {
public:
  void CreateReduceTask(const std::string &reduce_dst, const std::vector<ObjectID> &objects_to_reduce,
                        const ObjectID &reduction_id, int num_reduce_objects) {
    auto task = std::make_shared<ReduceTask>(reduce_dst, objects_to_reduce, reduction_id, num_reduce_objects);
    tasks_[reduction_id] = task;
    for (auto &id : objects_to_reduce) {
      object_id_to_tasks_[id].push_back(task);
    }
  }

  /// Mark one object is available for reduce.
  /// \param[in] object_id The ID of the object.
  /// \param[in] object_size The size of the object.
  /// \param[in] owner_ip The address of the owner of the object.
  /// \return All nodes+reduction_id associate with the object.
  std::vector<std::pair<Node *, ObjectID>> AddObject(const ObjectID &object_id, int64_t object_size,
                                                     const std::string &owner_ip);

  std::vector<std::pair<InbandDataNode *, ObjectID>> AddInbandObject(const ObjectID &object_id,
                                                                     const std::string &inband_data);

  /// Return the task by reduction ID.
  std::shared_ptr<ReduceTask> GetReduceTask(const ObjectID &reduction_id) { return tasks_[reduction_id]; }

private:
  // reduction_id -> task
  std::unordered_map<ObjectID, std::shared_ptr<ReduceTask>> tasks_;
  // object_id -> tasks
  std::unordered_map<ObjectID, std::vector<std::shared_ptr<ReduceTask>>> object_id_to_tasks_;
};
