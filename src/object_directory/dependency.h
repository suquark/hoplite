#pragma once

#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/id.h"

using chain_type = std::list<std::string>;

inline bool compare_priority(const std::pair<int, int64_t> &left, const std::pair<int, int64_t> &right) {
  // smaller number means higher priority for us
  return left.first > right.first;
}

class ObjectDependency {
public:
  ObjectDependency() {}

  /// Constructor for the object dependency manager.
  /// \param[in] object_id The ID of the object this object dependency manager handles.
  /// \param[in] object_ready_callback A callback that will be triggered when we know the object is
  /// ready for pulling somewhere.
  ObjectDependency(const ObjectID &object_id, std::function<void(const ObjectID &)> object_ready_callback);

  /// Get the information for pulling the object. We will return the best plan for the recevier.
  /// Once the information is returned, we append the receiver in the dependency if the receiver wants to occupy
  /// the object.
  /// \param[in] receiver The receiver we are going to provide info for.
  /// \param[in] occupying If True, then the receiver wants to occupy the object.
  /// \param[out] object_size The object size info.
  /// \param[out] sender The sender info.
  /// \param[out] inband_data We provide the object content as inband data if the object is small enough.
  /// \param[in] on_fail An optional function that handles the case that we cannot get the info immediately.
  /// The function call is secured by the internal lock so the info will not get updated inside the function call.
  /// \return True if we successfully returned the information for the receiver.
  bool Get(const std::string &receiver, bool occupying, int64_t *object_size, std::string *sender,
           std::string *inband_data, const std::function<void()>& on_fail = nullptr);

  /// A shortcut check of the availability.
  bool Available() const;

  /// The receiver declares it has got a complete object. This receiver can become the sender for later
  /// receivers.
  /// \param[in] receiver The receiver that has the complete object.
  /// \param[in] object_size The size of the complete object.
  void HandleCompletion(const std::string &receiver, int64_t object_size);

  /// The receiver declares it has got a complete object. The object is so small that we directly keeps it here.
  /// \param[in] inband_data The complete small object.
  void HandleInbandCompletion(const std::string &inband_data);

  /// Handles failure for a node.
  /// \param[in] receiver The receiver we need to take care of. Usually this is caused by the failed of its sender.
  /// \param[out] alternative_sender The alternative sender we suggest for the receiver. This would enable
  /// recovering from error as fast as possible.
  /// \return If True, we find an alternative sender for the receiver. Otherwise we will handle it later when the
  /// receiver pulls for object again.
  bool HandleFailure(const std::string &receiver, std::string *alternative_sender);

  std::string GetInbandData() {
    std::lock_guard<std::mutex> lock(mutex_);
    return inband_data_;
  }

  bool HasInbandData() {
    std::lock_guard<std::mutex> lock(mutex_);
    return !inband_data_.empty();
  }

  std::string DebugPrint();

private:
  void create_new_chain(const std::string &node);

  void register_new_chain(const std::shared_ptr<chain_type> &c);

  void disable_chain(int64_t key);

  void update_chain(int64_t key, const std::shared_ptr<chain_type> &c);

  void recover_chain(const std::shared_ptr<chain_type> &c, const std::string &sender);

  bool get_impl_(const std::string &receiver, bool occupying, int64_t *object_size, std::string *sender,
                 std::string *inband_data, const std::function<void()>& on_fail);

  ObjectID object_id_;
  int64_t object_size_ = -1;
  // TODO: We should implement LRU gabage collection for the inband data
  // storage. But it doesn't matter now because these data take too few
  // space.
  std::string inband_data_;
  std::function<void(const ObjectID &)> object_ready_callback_;

  std::mutex mutex_;
  std::atomic<int64_t> index_;

  std::unordered_map<std::string, std::shared_ptr<chain_type>> node_to_chain_;
  std::unordered_map<int64_t, std::shared_ptr<chain_type>> chains_;
  std::unordered_map<std::shared_ptr<chain_type>, int64_t> reversed_map_;
  std::unordered_map<std::string, std::string> recevier_to_sender_;

  std::priority_queue<std::pair<int, int64_t>, std::vector<std::pair<int, int64_t>>, decltype(&compare_priority)> pq_;
  std::unordered_set<int64_t> available_keys_;
  std::unordered_set<int64_t> removed_keys_;

  // chains that are suspended due to failures
  std::unordered_set<std::shared_ptr<chain_type>> suspended_chains_;
};
