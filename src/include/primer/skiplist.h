//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// skiplist.h
//
// Identification: src/include/primer/skiplist.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <shared_mutex>
#include <vector>
#include "common/macros.h"
#include <fmt/core.h>
#include <fmt/format.h>

namespace bustub {

/** @brief Common template arguments used for the `SkipList` class. */
#define SKIPLIST_TEMPLATE_ARGUMENTS template <typename K, typename Compare, size_t MaxHeight, uint32_t Seed>

/**
 * @brief A probablistic data structure that implements the ordered set abstract data type.
 *
 * The skip list is implemented as a linked list of nodes. Each node has a list of forward links. The number of
 * forward links is determined by a geometric distribution. The skip list maintains a header node that is always at the
 * maximum height of the skip list. The skip list is always sorted by the key using the comparison function provided.
 *
 * @tparam K the type of key.
 * @tparam Compare the comparison function for the key.
 * @tparam MaxHeight the maximum height of the skip list.
 * @tparam Seed the seed for the random number generator.
 */
template <typename K, typename Compare = std::less<K>, size_t MaxHeight = 14, uint32_t Seed = 15445>
class SkipList {
 protected:
  struct SkipNode;

 public:
  /**  @brief Constructs an empty skip list with an optional custom comparison function. */
  explicit SkipList(const Compare &compare = Compare{}) : header_{new SkipList::SkipNode(MaxHeight)}, height_{1} {}

  /**
   * @brief Destructs the skip list.
   *
   * See `Drop()` for how we avoid blowing up the stack.
   */
  ~SkipList() { Drop(); }

  /**
   * We disable both copy & move operators/constructors as the skip list contains a mutex
   * that cannot be safely moved/copied while other threads may be using it.
   */
  SkipList(const SkipList &) = delete;
  auto operator=(const SkipList &) -> SkipList & = delete;
  SkipList(SkipList &&) = delete;
  auto operator=(SkipList &&) -> SkipList & = delete;

  auto Empty() -> bool;
  auto Size() -> size_t;

  void Clear();
  auto Insert(const K &key) -> bool;
  auto Erase(const K &key) -> bool;
  auto Contains(const K &key) -> bool;

  void Print();

 protected:
  auto Header() -> std::shared_ptr<SkipNode> { return header_; }

 private:
  auto RandomHeight() -> size_t;

  void Drop();
  // Students may add any private helper functions that they desire.
  //
  // To give you an idea, the following are some common helper functions that you may want to implement:
  // - Finds the node that is no less than the given key.
  // - Inserts a new node with the given key.
  // - Adjust height and previous pointers.

  // Returns list of previous nodes
  auto search(const K &key) -> std::vector<std::shared_ptr<SkipNode>> {
    std::vector<std::shared_ptr<SkipNode>> result(MaxHeight, nullptr);
		auto curr = header_;
		for (int i = static_cast<int>(height_) - 1; i >= 0; i--) {
		  while (curr && curr->links_[i] && compare_(curr->links_[i]->key_, key)) {
//		    fmt::println("At level {}, visiting node with key: {}", i, curr->links_[i]->key_);
				curr = curr->links_[i];
		  }
      result[i] = curr;
		}
	  return result;
  }

  // Inserts a new node with the given key and adjusts skip list height
  auto insertNode(std::vector<std::shared_ptr<SkipNode>> &chain, const K &key) -> std::shared_ptr<SkipNode> {
    auto node = chain[0]->links_[0];
    if (node && !compare_(node->key_,key) && !compare_(key,node->key_)) {
        // Key already exists in skip list
        return nullptr;
    }

    // Create new node
    auto newHeight = RandomHeight();
    fmt::println("Inserting key {} at height {}", key, newHeight);
    auto newNode = std::make_shared<SkipNode>(newHeight, key);

    // Update new node pointers
    for (size_t i = 0; i < newHeight && i < height_; i++) {
      newNode->links_[i] = chain[i]->links_[i];
    }

    // Adjust height
    while (height_ < newHeight) {
//      fmt::println("NEW HEIGHT {}", height_);
      chain[height_] = header_;
      height_++;
    }
    size_++;
    return newNode;
  }

  // Adjust previous pointers given list of previous nodes, target node
  void updatePrevs(std::vector<std::shared_ptr<SkipNode>> &chain, std::shared_ptr<SkipNode> &node) {
//    fmt::println("CHAIN {} {}", chain.size(), node->Height());
    for (size_t i = 0; i < chain.size() && i < node->Height(); i++) {
      if (chain[i] == nullptr) {
        return;
      }
//      fmt::println("Updating level {} orig node {} {} with node key {}", i, chain[i]==header_, chain[i]->Key(), node->Key());
      chain[i]->links_[i] = node->links_[i];
    }
  }

  /** @brief Lowest level index for the skip list. */
  static constexpr size_t LOWEST_LEVEL = 0;

  /** @brief Comparison function used to check key orders and keep the skip list sorted. */
  Compare compare_;

  /**
   * @brief Header consists of a list of forward links for level 0 to `MaxHeight - 1`. The forward links at
   * level higher than current `height_` are `nullptr`.
   */
  std::shared_ptr<SkipNode> header_;

  /**
   * @brief Current height of the skip list.
   *
   * Invariant: `height_` should never be greater than `MaxHeight`.
   */
  uint32_t height_{1};

  /** @brief Number of elements in the skip list. */
  size_t size_{0};

  /** @brief Random number generator. */
  std::mt19937 rng_{Seed};

  /** @brief A reader-writer latch protecting the skip list. */
  std::shared_mutex rwlock_{};
};

/**
 * Node type for `SkipList`.
 */
SKIPLIST_TEMPLATE_ARGUMENTS struct SkipList<K, Compare, MaxHeight, Seed>::SkipNode {
  /**
   * @brief Constructs a skip list node with height number of links and given key.
   *
   * This constructor is used both for creating regular nodes (with key) and the header node (without key).
   *
   * @param height The number of links the node will have
   * @param key The key to store in the node (default empty for header)
   */
  explicit SkipNode(size_t height, K key = K{}) : links_(height,nullptr), key_{key} {}

  auto Height() const -> size_t;
  auto Next(size_t level) const -> std::shared_ptr<SkipNode>;
  void SetNext(size_t level, const std::shared_ptr<SkipNode> &node);
  auto Key() const -> const K &;

  /**
   * @brief A list of forward links.
   *
   * Note: `links_[0]` is the lowest level link, and `links_[links_.size()-1]` is the highest level link.
   *
   * We use `std::shared_ptr` to manage the memory of the next node. This is because the next node can be shared by
   * multiple links. We also use `std::vector` instead of a flexible array member for simplicity instead of performance.
   */
  std::vector<std::shared_ptr<SkipNode>> links_;
  K key_;
};

}  // namespace bustub
