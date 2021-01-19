#include "dependency.h"
#include "util/logging.h"

ObjectDependency::ObjectDependency(const ObjectID &object_id,
                                   std::function<void(const ObjectID &)> object_ready_callback)
    : object_id_(object_id), object_ready_callback_(object_ready_callback), index_(0), pq_(compare_priority) {}

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

bool ObjectDependency::Get(const std::string &node, bool occupying, int64_t *object_size, std::string *sender,
                           std::string *inband_data, std::function<void()> on_fail) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!inband_data_.empty()) {
    *inband_data = inband_data_;
    *object_size = object_size_;
    return true;
  }
  if (available_keys_.size() <= 0) {
    if (on_fail != nullptr) {
      on_fail();
    }
    return false;
  }
  std::string parent = "";
  // we are sure we can find one in the priority queue
  while (true) {
    int64_t key = pq_.top().second;
    pq_.pop();
    if (removed_keys_.count(key) <= 0) {
      auto &c = chains_[key];
      *sender = c->back();
      if (occupying) {
        if (node_to_chain_.count(node) <= 0) {
          c->push_back(node);
          node_to_chain_[node] = c;
        } else {
          // this path indicates the node is suspended
          auto &old_c = node_to_chain_[node];
          // this should always be suspended chains
          DCHECK(suspended_chains_.count(old_c) > 0);
          DCHECK(old_c->front() == node);
          for (auto n : *old_c) {
            c->push_back(n);
            node_to_chain_[n] = c;
          }
          suspended_chains_.erase(old_c);
        }
        update_chain(key, c);
      }
      break;
    } else {
      // no longer in priority queue. stop tracking it.
      removed_keys_.erase(key);
    }
  }
  *object_size = object_size_;
  return true;
}

void ObjectDependency::HandleCompletion(const std::string &node, int64_t object_size) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (object_size_ < 0) {
    object_size_ = object_size;
  } else {
    DCHECK(object_size_ == object_size) << "Size of object " << object_id_.Hex() << " has changed.";
  }
  auto &c = node_to_chain_[node];
  DCHECK(c->size() > 0) << "we assume that each chain should have length >= 1";
  if (c->size() == 1) {
    DCHECK(c->front() == node) << "when there is only one node, it should be the node itself.";
    // nothing to handle here
  } else {
    bool notification_required = available_keys_.empty();
    // We also complete all previous nodes.
    // They must have been completed because of the dependency.
    std::string n;
    do {
      n = c->front();
      c->pop_front();
      auto new_chain = std::make_shared<chain_type>(std::initializer_list<std::string>{n});
      register_new_chain(new_chain);
    } while (n != node);
    if (available_keys_.size() > 0 && notification_required) {
      lock.unlock();
      // we must unlock here because the callback may access this lock again.
      object_ready_callback_(object_id_);
    }
  }
}

void ObjectDependency::HandleInbandCompletion(const std::string &inband_data) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (object_size_ < 0) {
      object_size_ = inband_data.size();
    } else {
      DCHECK(object_size_ == inband_data.size()) << "Size of object " << object_id_.Hex() << " has changed.";
    }
    inband_data_ = inband_data;
  }
  object_ready_callback_(object_id_);
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
    bool notification_required = available_keys_.empty();
    register_new_chain(new_chain);
    // no need to notify completion here, because at least the chain of failed node
    // is available before.
  }
}
