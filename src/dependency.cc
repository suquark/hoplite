#include "dependency.h"
#include "util/logging.h"

ObjectDependency::ObjectDependency(const ObjectID &object_id,
                                   std::function<void(const ObjectID &)> object_ready_callback)
    : object_id_(object_id), object_ready_callback_(object_ready_callback), index_(0), pq_(&compare_priority) {}

void ObjectDependency::create_new_chain(const std::string &node) {
  auto new_chain = std::make_shared<chain_type>(std::initializer_list<std::string>{node});
  node_to_chain_[node] = new_chain;
  register_new_chain(new_chain);
}

void ObjectDependency::register_new_chain(const std::shared_ptr<chain_type> &c) {
  int64_t new_key = ++index_;
  available_keys_.insert(new_key);
  // copy out the pointer to maintain its reference count
  std::shared_ptr<chain_type> new_chain = c;
  reversed_map_[new_chain] = new_key;
  chains_[new_key] = new_chain;
  int new_size = new_chain->size();
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

void ObjectDependency::recover_chain(const std::shared_ptr<chain_type> &c, const std::string &sender) {
  DCHECK(suspended_chains_.count(c)) << "Chain to recover is not suspended";
  auto &new_sender_chain = node_to_chain_[sender];
  recevier_to_sender_[c->front()] = sender;
  for (auto &n : *c) {
    new_sender_chain->push_back(n);
    node_to_chain_[n] = new_sender_chain;
  }
  suspended_chains_.erase(c);
  update_chain(reversed_map_[new_sender_chain], new_sender_chain);
}

bool ObjectDependency::Get(const std::string &receiver, bool occupying, int64_t *object_size, std::string *sender,
                           std::string *inband_data, std::function<void()> on_fail) {
  TIMELINE("ObjectDependency::Get");
  std::lock_guard<std::mutex> lock(mutex_);
  return get_impl_(receiver, occupying, object_size, sender, inband_data, on_fail);
}

bool ObjectDependency::Available() const {
  return !inband_data_.empty() || available_keys_.size() > 0;
}

bool ObjectDependency::get_impl_(const std::string &receiver, bool occupying, int64_t *object_size, std::string *sender,
                                 std::string *inband_data, std::function<void()> on_fail) {
  LOG(DEBUG) << "[Dependency] Get for " << receiver;
  if (!inband_data_.empty()) {
    *inband_data = inband_data_;
    *object_size = object_size_;
    return true;
  }
  if (available_keys_.size() <= 0) {
    if (on_fail != nullptr) {
      LOG(DEBUG) << "[Dependency] Get failed for " << receiver;
      on_fail();
    }
    return false;
  }
  std::string parent = "";
  // we are sure we can find one in the priority queue
  while (true) {
    int64_t key = pq_.top().second;
    if (removed_keys_.count(key) <= 0) {
      auto &c = chains_[key];
      *sender = c->back();
      if (occupying) {
        if (!node_to_chain_.count(receiver)) {
          c->push_back(receiver);
          node_to_chain_[receiver] = c;
          recevier_to_sender_[receiver] = *sender;
          update_chain(key, c);
        } else {
          // this path indicates the node is suspended
          recover_chain(node_to_chain_[receiver], *sender);
        }
      }
      break;
    } else {
      // no longer in priority queue. stop tracking it.
      pq_.pop();
      removed_keys_.erase(key);
    }
  }
  *object_size = object_size_;
  return true;
}

void ObjectDependency::HandleCompletion(const std::string &receiver, int64_t object_size) {
  TIMELINE("ObjectDependency::HandleCompletion");
  LOG(DEBUG) << "[Dependency] handles completion for " << object_id_.ToString() << ", size=" << object_size;
  std::unique_lock<std::mutex> lock(mutex_);
  if (object_size_ < 0) {
    object_size_ = object_size;
  } else {
    DCHECK(object_size_ == object_size) << "Size of object " << object_id_.Hex() << " has changed.";
  }
  if (!node_to_chain_.count(receiver)) {
    LOG(DEBUG) << "[Dependency] handles completion for the initial object of " << object_id_.ToString()
               << " created by " << receiver;
    create_new_chain(receiver);
    lock.unlock();
    // we must unlock here because the callback may access this lock again.
    object_ready_callback_(object_id_);
    return;
  }
  if (recevier_to_sender_.count(receiver)) {
    // we no longer need to keep the sender of the receiver.
    recevier_to_sender_.erase(receiver);
  }
  auto &c = node_to_chain_[receiver];
  DCHECK(c->size() > 0) << "We assume that each chain should have length >= 1. (node=" << receiver << ")."
                        << DebugPrint();
  if (c->size() == 1) {
    DCHECK(c->front() == receiver) << "When there is only one node, it should be the node itself (" << receiver << "). "
                                   << "However, \"" << c->front() << "\" is found." << DebugPrint();
    // nothing to handle here
  } else {
    bool notification_required = available_keys_.empty();
    // We complete all previous nodes. They must have been completed because of the dependency.
    // The current node should not be moved out from the chain because:
    // 1. If it is not the last node in the chain, it may still serves as a sender.
    // 2. If it is the last node in the chain, we just need to keep the chain.
    bool updated = false;
    std::string n;
    while ((n = c->front()) != receiver) {
      c->pop_front();
      updated = true;
      create_new_chain(n);
    }
    DCHECK(c->size() > 0) << "We assume that each chain should have length >= 1. (node=" << receiver << ")."
                          << DebugPrint();
    if (updated) {
      // update the chain because its size changed
      update_chain(reversed_map_[c], c);
    }
    // TODO(siyuan): maybe we should remove this since it would never be reached.
    if (available_keys_.size() > 0 && notification_required) {
      lock.unlock();
      // we must unlock here because the callback may access this lock again.
      object_ready_callback_(object_id_);
    }
  }
}

void ObjectDependency::HandleInbandCompletion(const std::string &inband_data) {
  LOG(DEBUG) << "[Dependency] handles inband completion for " << object_id_.ToString();
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

bool ObjectDependency::HandleFailure(const std::string &receiver, std::string *alternative_sender) {
  TIMELINE("ObjectDependency::HandleFailure");
  LOG(DEBUG) << "[Dependency] handles failure for " << receiver;
  std::lock_guard<std::mutex> lock(mutex_);

  std::string sender = recevier_to_sender_[receiver];
  if (!node_to_chain_.count(sender)) {
    // the error has been handled.
    return true;
  }

  // keep the reference of the chain
  std::shared_ptr<chain_type> c = node_to_chain_[receiver];
  int64_t key = reversed_map_[c];
  // erase the sender
  node_to_chain_.erase(sender);
  recevier_to_sender_.erase(receiver);

  // remove the sender in the middle of the chain and connect both side
  if (c->front() != sender) {
    for (auto itr = c->cbegin(); itr != c->cend(); ++itr) {
      if (*itr == sender) {
        itr = c->erase(itr);
        recevier_to_sender_[receiver] = *alternative_sender = *(--itr);
        update_chain(key, c);
        return true;
      }
    }
    LOG(FATAL) << "Unreachable code";
  } else {
    // remove the sender
    c->pop_front();
    // prevent others from joining this failed chain, if the chain is not suspended
    if (!suspended_chains_.count(c)) {
      // we mark it as suspended so we can recover the whole chain later
      suspended_chains_.insert(c);
      disable_chain(key);
      // remove the chain from the map
      reversed_map_.erase(c);
    }
    int64_t object_size;
    std::string sender;
    std::string inband_data;
    bool success = get_impl_(receiver, true, &object_size, &sender, &inband_data, nullptr);
    if (success) {
      *alternative_sender = sender;
      recover_chain(c, sender);
    }
    return success;
  }
}

std::string ObjectDependency::DebugPrint() {
  std::stringstream s;
  s << std::endl << "==============================================================" << std::endl;
  for (auto const &x : chains_) {
    s << "Chain #" << x.first << ", size=" << x.second->size() << ": [";
    for (auto const &y : *x.second) {
      s << y << " ";
    }
    s << "]" << std::endl;
  }

  s << "Available: [";
  for (auto const &x : available_keys_) {
    s << x << " ";
  }
  s << "]" << std::endl;

  s << "Removed: [";
  for (auto const &x : removed_keys_) {
    s << x << " ";
  }
  s << "]" << std::endl;

  s << "Priority Queue: [";
  auto pq_copied = pq_;
  while (!pq_copied.empty()) {
    const auto &pair = pq_copied.top();
    s << "(" << pair.first << ", " << pair.second << "), ";
    pq_copied.pop();
  }
  s << "]" << std::endl;
  s << "==============================================================" << std::endl;
  return s.str();
}
