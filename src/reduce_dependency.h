#pragma once
#include "common/id.h"
#include <cstdint>
#include <string>
#include <vector>

struct Node {
  ObjectID object_id;
  std::string owner_ip;
  bool is_tree_node = false;
  Node *parent = NULL;
  // In the chain, one node only has left node.
  Node *left_child = NULL;
  Node *right_child = NULL;
  int subtree_size = -1;
  int order = -1;
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

class ReduceTreeChain {
public:
  /// Constructor
  /// \param[in] object_count includes remote objects and the node that invokes reduction.
  /// \param[in] maximum_chain_length The maximum length of the chain part.
  ReduceTreeChain(int64_t object_count, int64_t maximum_chain_length);

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
