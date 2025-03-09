#pragma once

#include <limits>
#include <string>

template <typename Key>
class KeyComparator {
 public:
  KeyComparator() = default;
  ~KeyComparator() = default;

  // if lhs < rhs, return -1
  // if lhs > rhs, return 1
  // if lhs == rhs, return 0
  int operator()(const Key &lhs, const Key &rhs) const {
    if (lhs < rhs) {
      return -1;
    }
    if (lhs > rhs) {
      return 1;
    }
    return 0;
  }

  static Key MaxValue() { return std::numeric_limits<Key>::max(); }
};

template <>
class KeyComparator<std::string> {
 public:
  KeyComparator() = default;
  ~KeyComparator() = default;

  int operator()(const std::string &lhs, const std::string &rhs) const {
    if (lhs == MaxValue() || rhs == MaxValue()) {
      if (lhs != MaxValue()) {
        return -1;
      }
      if (rhs != MaxValue()) {
        return 1;
      }
      return 0;
    }
    return lhs.compare(rhs);
  }

  static std::string MaxValue() { return "MaybeMikeMaoHere"; }
};
