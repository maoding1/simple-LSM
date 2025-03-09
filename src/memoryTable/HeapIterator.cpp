#include <memoryTable/HeapIterator.h>

bool operator<(const SearchItem &lhs, const SearchItem &rhs) {
  if (lhs.key_ == rhs.key_) {
    return lhs.idx_ < rhs.idx_;
  }
  return lhs.key_ < rhs.key_;
}

bool operator>(const SearchItem &lhs, const SearchItem &rhs) {
  if (lhs.key_ == rhs.key_) {
    return lhs.idx_ > rhs.idx_;
  }
  return lhs.key_ > rhs.key_;
}

bool operator==(const SearchItem &lhs, const SearchItem &rhs) { return lhs.key_ == rhs.key_ && lhs.idx_ == rhs.idx_; }

HeapIterator::HeapIterator(const std::vector<SearchItem> &items) {
  for (const auto &item : items) {
    heap_.push(item);
  }

  while (!heap_.empty() && heap_.top().value_.empty()) {
    auto deleted = heap_.top();
    while (!heap_.empty() && heap_.top().key_ == deleted.key_) {
      heap_.pop();
    }
  }
  UpdateCurrent();
}

void HeapIterator::UpdateCurrent() {
  if (heap_.empty()) {
    current_.reset();
    return;
  }
  current_ = std::make_shared<ValueType>(heap_.top().key_, heap_.top().value_);
}

HeapIterator &HeapIterator::operator++() {
  if (heap_.empty()) {
    return *this;
  }

  auto top = heap_.top();
  heap_.pop();

  // delete all the items with the same key
  while (!heap_.empty() && heap_.top().key_ == top.key_) {
    heap_.pop();
  }

  while (!heap_.empty() && heap_.top().value_.empty()) {
    auto deleted = heap_.top();
    while (!heap_.empty() && heap_.top().key_ == deleted.key_) {
      heap_.pop();
    }
  }

  UpdateCurrent();
  return *this;
}

bool HeapIterator::operator==(const HeapIterator &other) const {
  if (heap_.empty() && other.heap_.empty()) {
    return true;
  }
  if (heap_.empty() || other.heap_.empty()) {
    return false;
  }
  return *current_ == *other.current_;
}

bool HeapIterator::operator!=(const HeapIterator &other) const { return !(*this == other); }

HeapIterator::ValueType *HeapIterator::operator->() const { return current_.get(); }

HeapIterator::ValueType &HeapIterator::operator*() const { return *current_; }

bool HeapIterator::IsEnd() const { return heap_.empty(); }