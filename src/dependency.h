#pragma once
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <atomic>
#include <list>

#include "common/id.h"

using chain_type = std::list<std::string>;

bool compare_priority(const std::pair<int, int64_t> &left, const std::pair<int, int64_t> &right) {
  // smaller number means higher priority for us
  return left.first > right.first;
}

class ObjectDependency {
 public:
  ObjectDependency(const ObjectID& object_id) : object_id_(object_id), index_(0), pq_(compare_priority) {}

  // append the node in the dependency. returns the parent in the dependency chain.
  std::string Append(const std::string &node);

  void HandleCompletion(const std::string &node);

  void HandleFailure(const std::string &failed_node);

 private:
  void register_new_chain(const std::shared_ptr<chain_type> &c);

  void disable_chain(int64_t key);

  void update_chain(int64_t key, const std::shared_ptr<chain_type> &c);

  ObjectID object_id_;
  std::mutex mutex_;
  std::atomic<int64_t> index_;

  std::unordered_map<std::string, std::shared_ptr<chain_type>> node_to_chain_;
  std::unordered_map<int64_t, std::shared_ptr<chain_type>> chains_;
  std::unordered_map<std::shared_ptr<chain_type>, int64_t> reversed_map_;

  std::priority_queue<std::pair<int, int64_t>, decltype(compare_priority)> pq_;
  std::unordered_set<int64_t> available_keys_;
  std::unordered_set<int64_t> removed_keys_;

  // chains that are suspended due to failures
  std::unordered_set<std::shared_ptr<chain_type>> suspended_chains_;
};
