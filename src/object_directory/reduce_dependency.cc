#include "reduce_dependency.h"

#include <cmath>
#include <memory>

#include "common/config.h"
#include "util/logging.h"

ReduceTreeChain::ReduceTreeChain(int64_t object_count, int64_t maximum_chain_length)
    : object_count_(object_count), maximum_chain_length_(maximum_chain_length) {
  int64_t k = maximum_chain_length;
  // 2^d -1 + 2^(d-1)*2k = object_count
  int d = int(ceil(log2(double(object_count + 1) / double(k + 1))));
  int max_depth = int(floor(log2(double(object_count + 1))));
  depth_ = d < max_depth ? d : max_depth;
  if (depth_ <= 0) {
    // In this case we are totally a chain.
    depth_ = 0;
    chains_.emplace_back();
    chains_[0].resize(object_count);
  } else {
    // the tree is always a full binary tree
    tree_.resize((1LL << depth_) - 1);
    int64_t remaining_size = object_count - tree_.size();
    DCHECK(remaining_size >= 0);
    chains_.resize(1LL << depth_);
    for (size_t i = 0; i < chains_.size(); ++i) {
      // at most differ by 1
      chains_[i].resize(remaining_size / chains_.size() + int(i < remaining_size % chains_.size()));
    }
  }

  // initialize chains
  for (auto &chain : chains_) {
    if (!chain.empty()) {
      chain[0].is_tree_node = false;
      chain[0].subtree_size = 1;
      for (int i = 1; i < chain.size(); i++) {
        chain[i].is_tree_node = false;
        chain[i].subtree_size = chain[i - 1].subtree_size + 1;
        chain[i].left_child = &chain[i - 1];
        chain[i - 1].parent = &chain[i];
      }
    }
  }

  // initialize tree
  if (depth_ > 0) {
    // initalize the bottom of the tree
    int w = depth_ - 1;
    int front = (1 << w) - 1;
    int end = (1 << (w + 1)) - 1;
    if (!chains_.empty()) {
      for (int i = front; i < end; i++) {
        auto &t = tree_[i];
        t.is_tree_node = true;
        t.subtree_size = 1;
        auto &left_chain = chains_[(i - front) << 1];
        auto &right_chain = chains_[((i - front) << 1) + 1];
        if (!left_chain.empty()) {
          t.left_child = &left_chain.back();
          t.left_child->parent = &t;
          t.subtree_size += t.left_child->subtree_size;
        }
        if (!right_chain.empty()) {
          t.right_child = &right_chain.back();
          t.right_child->parent = &t;
          t.subtree_size += t.right_child->subtree_size;
        }
      }
    } else {
      for (int i = front; i < end; i++) {
        tree_[i].is_tree_node = true;
        tree_[i].subtree_size = 1;
      }
    }

    if (depth_ > 1) {
      // initalize the upper part
      for (int w = depth_ - 2; w >= 0; w--) {
        int front = (1 << w) - 1;
        int end = (1 << (w + 1)) - 1;
        for (int i = front; i < end; i++) {
          auto &t = tree_[i];
          t.is_tree_node = true;
          t.left_child = &tree_[(i << 1) + 1];
          t.right_child = &tree_[(i << 1) + 2];
          t.left_child->parent = &t;
          t.right_child->parent = &t;
          t.subtree_size = 1 + t.left_child->subtree_size + t.right_child->subtree_size;
        }
      }
    }
  }

  // assign orders by in-order tree traverse
  map_.resize(object_count);
  for (auto &t : tree_) {
    map_[t.init_order()] = &t;
  }
  for (auto &c : chains_) {
    if (!c.empty()) {
      for (Node *s = &c.back(); s != nullptr; s = s->left_child) {
        map_[s->init_order()] = s;
      }
    }
  }
}

std::string ReduceTreeChain::DebugString() {
  std::stringstream s;
  s << std::endl << "==============================================================" << std::endl;
  s << "object_count: " << object_count_ << ", maximum_chain_length: " << maximum_chain_length_ << std::endl;
  for (size_t i = 0; i < map_.size(); i++) {
    Node *n = map_[i];
    if (n->location_known()) {
      s << i << ": " << n->object_id.ToString() << " @ " << n->owner_ip << std::endl;
    }
  }
  s << std::endl;
  if (chains_.empty()) {
    s << "Chain: NULL";
  } else {
    for (int i = 0; i < chains_.size(); i++) {
      auto const &c = chains_[i];
      s << "Chain #" << i << ", length=" << c.size() << ": [ ";
      for (auto const &y : c) {
        if (y.parent) {
          s << y.order << "->" << y.parent->order << " ";
        } else {
          s << y.order << " ";
        }
      }
      s << "]" << std::endl;
    }
  }
  s << std::endl << std::endl;
  if (tree_.empty()) {
    s << "Tree: NULL" << std::endl;
  } else {
    s << "Tree:" << std::endl;
    for (int i = 0; i < tree_.size(); i++) {
      auto &y = tree_[i];
      if (y.parent) {
        s << y.order << "->" << y.parent->order << " ";
      } else {
        s << y.order << " ";
      }
      if (((i + 2) & (i + 1)) == 0) {
        s << std::endl;
      }
    }
  }
  s << "==============================================================" << std::endl;
  return s.str();
}

Node *ReduceTask::AddObject(const ObjectID &object_id, int64_t object_size, const std::string &owner_ip) {
  if (!rtc_) {
    // we intialize it now because previously we do not know the object size
    int64_t maximum_chain_length = round(double(object_size) / double(HOPLITE_BANDWIDTH * HOPLITE_RPC_LATENCY));
    // add one for the reduction result receiver
    rtc_ = std::make_unique<ReduceTreeChain>(num_reduce_objects_ + 1, maximum_chain_length);
    // we initialize the root node here, because it could be skipped later
    Node *root = rtc_->GetRoot();
    DCHECK(!root->parent);
    root->object_id = reduction_id_;
    root->owner_ip = reduce_dst_;
    object_size_ = object_size;
  }
  if (ready_ids_.count(object_id)) {
    // duplicated object. ignore
    return nullptr;
  }
  ready_ids_.insert(object_id);
  if (!suspended_nodes_.empty()) {
    // assign to a suspended node first
    Node *n = suspended_nodes_.front();
    n->object_id = object_id;
    n->owner_ip = owner_ip;
    owner_to_node_[owner_ip] = n;
    suspended_nodes_.pop();
    return n;
  }
  if (num_ready_objects_ >= num_reduce_objects_ + 1) {
    // We already have enough nodes in the tree. Push more into the backup node.
    backup_objects_.emplace_back(object_id, owner_ip);
    return nullptr;
  }
  Node *n = rtc_->GetNode(num_ready_objects_);
  if (n->is_root()) {
    // skip the root node
    ++num_ready_objects_;
    if (num_ready_objects_ >= num_reduce_objects_ + 1) {
      // We already have enough nodes in the tree. Push more into the backup node.
      backup_objects_.emplace_back(object_id, owner_ip);
      return nullptr;
    }
    n = rtc_->GetNode(num_ready_objects_);
  }
  n->object_id = object_id;
  n->owner_ip = owner_ip;
  owner_to_node_[owner_ip] = n;
  num_ready_objects_++;
  return n;
}

InbandDataNode *ReduceTask::AddInbandObject(const ObjectID &object_id, const std::string &inband_data) {
  auto *data = (float *)inband_data.data();
  size_t size = inband_data.size() / sizeof(float);
  if (reduced_inband_dst_.reduced_inband_data.empty()) {
    reduced_inband_dst_.reduced_inband_data = std::vector<float>(data, data + size);
    reduced_inband_dst_.object_id = reduction_id_;
    reduced_inband_dst_.owner_ip = reduce_dst_;
  } else {
    // if we have got enough objects, skip reducing
    if (num_ready_objects_ < num_reduce_objects_) {
      for (size_t i = 0; i < size; i++) {
        reduced_inband_dst_.reduced_inband_data[i] += data[i];
      }
    }
  }
  num_ready_objects_++;
  if (num_ready_objects_ >= num_reduce_objects_) {
    reduced_inband_dst_.finished = true;
  }
  return &reduced_inband_dst_;
}

bool ReduceTask::ReassignFailedNode(Node *failed_node) {
  failed_node->failed = true; // mark it as failed
  if (!backup_objects_.empty()) {
    auto pair = backup_objects_.front();
    failed_node->object_id = pair.first;
    failed_node->owner_ip = pair.second;
    ready_ids_.erase(failed_node->object_id);
    owner_to_node_[failed_node->owner_ip] = failed_node;
    backup_objects_.pop_front();
    return true;
  }
  // reset the node. reassign it later.
  RemoveNode(failed_node);
  suspended_nodes_.push(failed_node);
  return false;
}

std::vector<std::pair<Node *, ObjectID>> ReduceManager::AddObject(const ObjectID &object_id, int64_t object_size,
                                                                  const std::string &owner_ip) {
  std::vector<std::pair<Node *, ObjectID>> nodes;
  if (object_id_to_tasks_.count(object_id)) {
    for (auto &task : object_id_to_tasks_[object_id]) {
      Node *n = task->AddObject(object_id, object_size, owner_ip);
      if (n) {
        nodes.emplace_back(n, task->GetReductionID());
      }
    }
  }
  return nodes;
}

std::vector<std::pair<InbandDataNode *, ObjectID>> ReduceManager::AddInbandObject(const ObjectID &object_id,
                                                                                  const std::string &inband_data) {
  std::vector<std::pair<InbandDataNode *, ObjectID>> nodes;
  if (object_id_to_tasks_.count(object_id)) {
    for (auto &task : object_id_to_tasks_[object_id]) {
      InbandDataNode *n = task->AddInbandObject(object_id, inband_data);
      if (n) {
        nodes.emplace_back(n, task->GetReductionID());
      }
    }
  }
  return nodes;
}
