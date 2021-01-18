#include "dependency.h"
#include "util/logging.h"

void ObjectDependency::register_new_chain(const std::shared_ptr<chain_type> &c) {
  int64_t new_key = ++index_;
  available_keys_.insert(new_key);
  reversed_map_[c] = new_key;
  chains_[new_key] = c;
  int new_size = c->size();
  pq_.push(std::make_pair(new_size, new_key));
}

void ObjectDependency::disable_chain(int64_t key) {
  removed_keys_.insert(key);
  available_keys_.erase(key);
  chains_.erase(key);
}

void ObjectDependency::update_chain(int64_t key, const std::shared_ptr<chain_type> &c) {
  disable_chain(key);
  register_new_chain(c);
}

// append the node in the dependency. returns the parent in the dependency chain.
std::string ObjectDependency::Append(const std::string &node) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (available_keys_.size() <= 0) {
    return "";
  }
  std::string parent = "";
  // we are sure we can find one in the priority queue
  while (true) {
    int64_t key = pq_.top().second;
    pq_.pop();
    if (removed_keys_.count(key) <= 0) {
      auto &c = chains_[key];
      parent = c->back();
      if (node_to_chain_.count(node) > 0) {
        auto &old_c = node_to_chain_[node];
        // this should always be suspended chains
        DCHECK(suspended_chains_.count(old_c) > 0);
        DCHECK(old_c->front() == node);
        for (auto n: *old_c) {
          c->push_back(n);
          node_to_chain_[n] = c;
        }
        suspended_chains_.erase(old_c);
      } else {
        c->push_back(node);
        node_to_chain_[node] = c;
      }
      update_chain(key, c);
      break;
    } else {
      // no longer in priority queue. stop tracking it.
      removed_keys_.erase(key);
    }
  }
  return parent;
}

void ObjectDependency::HandleCompletion(const std::string &node) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto &c = node_to_chain_[node];
  DCHECK(c->size() > 0) << "we assume that each chain should have length >= 1";
  if (c->size() == 1) {
    DCHECK(c->front() == node) << "when there is only one node, it should be the node itself.";
    // nothing to handle here
  } else {
    // We also complete all previous nodes.
    // They must have been completed because of the dependency.
    bool new_chain_created = false;
    for (std::string n=""; n!=node; n=c->front()) {
      c->pop_front();
      auto new_chain = std::make_shared<chain_type>(std::initializer_list<std::string>{n});
      register_new_chain(new_chain);
      new_chain_created = true;
    }
    if (new_chain_created) {
      // TODO: use a callback to notify the object is ready.
    }
  }
}

void ObjectDependency::HandleFailure(const std::string &failed_node) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (node_to_chain_.count(failed_node) <= 0) {
    // the error has been handled.
    return;
  }
  // copy out and erase the value
  auto c = node_to_chain_[failed_node];
  node_to_chain_.erase(failed_node);
  int64_t key = reversed_map_[c];
  reversed_map_.erase(c);
  bool active = (suspended_chains_.count(c) == 0);
  // prevent others from joining this failed chain, if the chain is not suspended
  if (active) {
    disable_chain(key);
  }
  // move the part before the failed node into a new chain
  auto new_chain = std::make_shared<chain_type>();
  while (!c->empty()) {
    std::string n = c->front();
    c->pop_front();
    if (n == failed_node) {
      break;
    } else {
      new_chain->push_back(n);
    }
  }
  if (c->empty()) {
    if (!active) {
      // remove empty chain from suspended chains
      suspended_chains_.erase(c);
    }
  } else {
    // we mark it as suspended so we can recover the whole chain later
    suspended_chains_.insert(c);
  }
  if (!new_chain->empty()) {
    register_new_chain(new_chain);
    // TODO: use a callback to notify the object is ready.
  }
}
