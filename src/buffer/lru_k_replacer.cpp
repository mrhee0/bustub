//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> guard(latch_);
  if (evictables_.empty()) {
    return std::nullopt;
  }
  size_t max_distance = 0;
  size_t earliest_timestamp = current_timestamp_;
  frame_id_t toBeEvicted;
  for (auto fid : evictables_) {
    auto &node = node_store_.at(fid);
    if (max_distance < std::numeric_limits<size_t>::max() && node.history_size() == k_ &&
        (current_timestamp_ - node.get_front()) > max_distance) {
      toBeEvicted = fid;
      max_distance = current_timestamp_ - node.get_front();
    } else if (node.history_size() < k_ && node.get_front() <= earliest_timestamp) {
      toBeEvicted = fid;
      max_distance = std::numeric_limits<size_t>::max();
      earliest_timestamp = node.get_front();
    }
  }
  curr_size_--;
  node_store_.erase(toBeEvicted);
  evictables_.erase(toBeEvicted);
  return toBeEvicted;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::out_of_range("Frame ID is invalid");
  }
  current_timestamp_++;
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode new_node = LRUKNode(frame_id);
    node_store_.emplace(frame_id, new_node);
  }
  node_store_[frame_id].add_timestamp(current_timestamp_);
  if (node_store_[frame_id].history_size() > k_) {
    node_store_[frame_id].remove_oldest_timestamp();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::out_of_range("Frame ID is invalid");
  }
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (set_evictable && evictables_.find(frame_id) == evictables_.end()) {
    evictables_.emplace(frame_id);
    curr_size_++;
  } else if (!set_evictable && evictables_.find(frame_id) != evictables_.end()) {
    evictables_.erase(frame_id);
    curr_size_--;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  } else if (evictables_.find(frame_id) == evictables_.end()) {
    throw std::out_of_range("Frame ID is not evictable");
  }
  evictables_.erase(frame_id);
  node_store_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
