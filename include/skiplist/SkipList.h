#pragma once

#include <memory>
#include <optional>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <vector>

#ifndef SKIP_LIST_H
#define SKIP_LIST_H
constexpr int MAX_LEVEL = 16;

#define SKIPLISTNODE_TEMPLATE_ARGUMENTS template <typename K, typename V>
#define SKIPLIST_TEMPLATE_ARGUMENTS template <typename K, typename V, typename KeyComparator>
#define SKIPLIST_TYPE SkipList<K, V, KeyComparator>
#define SKIPLIST_ITERATOR_ARGUMENTS template <typename K, typename V, typename KeyComparator>
#define SKIPLIST_ITERATOR_TYPE SkipListIterator<K, V, KeyComparator>
#endif  // SKIP_LIST_H

// SkipListNode
SKIPLISTNODE_TEMPLATE_ARGUMENTS
struct SkipListNode {
  K key_;
  V value_;
  std::vector<std::shared_ptr<SkipListNode>> forward_;
  std::vector<std::weak_ptr<SkipListNode>> backward_;
  explicit SkipListNode(int level = MAX_LEVEL) : forward_(level, nullptr), backward_(level) {}
  void SetForward(int level, std::shared_ptr<SkipListNode> node) { forward_[level] = node; }
  void SetBackward(int level, std::weak_ptr<SkipListNode> node) { backward_[level] = node; }
};

// SkipListIterator
SKIPLIST_ITERATOR_ARGUMENTS
class SkipListIterator {
 private:
  std::shared_ptr<SkipListNode<K, V>> current_;
  // lock_ is hold when the iterators is in use
  std::shared_ptr<std::shared_lock<std::shared_mutex>> lock_;

 public:
  explicit SkipListIterator(std::shared_ptr<SkipListNode<K, V>> node) : current_(node) {}
  ~SkipListIterator() = default;

  std::pair<K, V> operator*() {
    if (current_ == nullptr) {
      throw std::runtime_error("Invalid iterator");
    }
    return {current_->key_, current_->value_};
  }

  SKIPLIST_ITERATOR_TYPE &operator++() {
    current_ = current_->forward_[0];
    return *this;
  }

  bool operator==(const SKIPLIST_ITERATOR_TYPE &other) { return current_ == other.current_; }

  bool operator!=(const SKIPLIST_ITERATOR_TYPE &other) { return current_ != other.current_; }

  K GetKey() { return current_->key_; }
  V GetValue() { return current_->value_; }

  bool IsValid() { return current_ != nullptr; }
  bool IsEnd() { return current_ == nullptr || current_->key == KeyComparator::MaxValue(); }
};

// SkipList
SKIPLIST_TEMPLATE_ARGUMENTS
class SkipList {
 private:
  std::shared_ptr<SkipListNode<K, V>> head_;
  std::shared_ptr<SkipListNode<K, V>> tail_;
  std::vector<std::shared_ptr<SkipListNode<K, V>>> last_;
  int max_level_;
  int level_;
  K tail_key_;
  size_t used_bytes_;

  std::uniform_int_distribution<int> dist_01_;
  std::uniform_int_distribution<int> dist_level_;
  std::mt19937 gen_;

  KeyComparator comp_;

 private:
  int RandomLevel();
  // private version of Search, used by Put and Remove, returns the node before the target node
  // will record the last node at each level in last_
  std::optional<std::shared_ptr<SkipListNode<K, V>>> InternalSearch(const K &key);

 public:
  explicit SkipList(const KeyComparator &comparator, int maxLevel = MAX_LEVEL);
  ~SkipList() = default;

  // returns the value of the key, if the key is not found, returns std::nullopt
  std::optional<V> Get(const K &key);
  bool Put(const K &key, const V &value);
  bool Remove(const K &key);
  size_t UsedBytes() const { return used_bytes_; }
  std::vector<std::pair<K, V>> Dump();
  void Clear();

  // Iterator
  using Iterator = SkipListIterator<K, V, KeyComparator>;
  Iterator Begin() { return Iterator(head_->forward_[0]); };
  Iterator End() { return Iterator(tail_); };

  // find the first element that is not less than key
  template <typename U = K>
  std::enable_if_t<std::is_same_v<U, std::string>, Iterator> BeginPreffix(const K &preffix) {
    auto p = head_;
    for (int i = level_; i >= 0; i--) {
      while (comp_(p->forward_[i]->key_, preffix) < 0) {
        p = p->forward_[i];
      }
    }
    p = p->forward_[0];
    return Iterator(p);
  }

  // find the first element that is greater than key
  template <typename U = K>
  std::enable_if_t<std::is_same_v<U, std::string>, Iterator> EndPreffix(const K &preffix) {
    auto p = head_;
    for (int i = level_; i >= 0; i--) {
      while (comp_(p->forward_[i]->key_, preffix) < 0) {
        p = p->forward_[i];
      }
    }
    p = p->forward_[0];
    while (p != tail_ && p->key_.substr(0, preffix.size()) == preffix) {
      p = p->forward_[0];
    }
    return Iterator(p);
  }
};
