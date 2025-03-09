#pragma once

#include <block/Block.h>

#include <utility>

class BlockIterator {
  using iterator_category = std::forward_iterator_tag;
  using value_type = std::pair<std::string, std::string>;
  using difference_type = std::ptrdiff_t;

 private:
  std::shared_ptr<Block> block_;
  size_t current_idx_{};
  mutable std::optional<value_type> cached_value_;

 private:
  void UpdateCurrent() const;

 public:
  BlockIterator(std::shared_ptr<Block> block, size_t current_idx)
      : block_(std::move(block)), current_idx_(current_idx) {}
  BlockIterator(std::shared_ptr<Block> block, const std::string &key);
  explicit BlockIterator(std::shared_ptr<Block> block) : block_(std::move(block)) {}
  BlockIterator() : block_(nullptr) {}

  value_type *operator->() const;
  BlockIterator &operator++();
  BlockIterator operator++(int) = delete;
  bool operator==(const BlockIterator &other) const;
  bool operator!=(const BlockIterator &other) const;
  value_type &operator*() const;
  bool IsEnd();
};