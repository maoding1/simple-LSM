#pragma once

#include <memory>
#include <queue>
#include <string>
struct SearchItem {
  std::string key_;
  std::string value_;
  int idx_;

  SearchItem() = default;
  SearchItem(std::string key, std::string value, int idx) : key_(std::move(key)), value_(std::move(value)), idx_(idx) {}
};

bool operator<(const SearchItem &lhs, const SearchItem &rhs);
bool operator>(const SearchItem &lhs, const SearchItem &rhs);
bool operator==(const SearchItem &lhs, const SearchItem &rhs);

class HeapIterator {
  using ValueType = std::pair<std::string, std::string>;

 private:
  std::priority_queue<SearchItem, std::vector<SearchItem>, std::greater<>> heap_;
  std::shared_ptr<ValueType> current_;  // store the current value
 private:
  void UpdateCurrent();

 public:
  HeapIterator() = default;
  explicit HeapIterator(const std::vector<SearchItem> &items);
  ~HeapIterator() = default;

  HeapIterator &operator++();
  HeapIterator operator++(int) = delete;
  bool operator==(const HeapIterator &other) const;
  bool operator!=(const HeapIterator &other) const;
  ValueType *operator->() const;
  ValueType &operator*() const;
  bool IsEnd() const;
};