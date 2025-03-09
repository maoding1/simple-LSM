#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

/***
reference: https://skyzh.github.io/mini-lsm/week1-03-block.html
block layout :
----------------------------------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section             |              Extra |
----------------------------------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements(2B) |
hash(optional)(4B) |
----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------
|                           Entry #1                            | ... |
-----------------------------------------------------------------------
| key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
-----------------------------------------------------------------------

*/
class BlockIterator;

class Block : public std::enable_shared_from_this<Block> {
  friend class BlockIterator;

 private:
  std::vector<uint8_t> data_;
  std::vector<uint16_t> offsets_;
  size_t capacity_;

  struct Entry {
    std::string key_;
    std::string value_;
  };

 private:
  Entry GetEntryAt(size_t offset) const;
  std::string GetKeyAt(size_t offset) const;
  std::string GetValueAt(size_t offset) const;
  int CompareKeyAt(size_t offset, const std::string &key) const;

 public:
  Block() = default;
  explicit Block(size_t capacity) : capacity_(capacity) {}
  std::vector<uint8_t> Encode();
  static std::shared_ptr<Block> Decode(const std::vector<uint8_t> &encoded, bool with_hashi = false);
  size_t GetOffsetAt(size_t index) const;
  size_t GetCurSize() const { return data_.size() + offsets_.size() * sizeof(uint16_t) + sizeof(uint16_t); }
  bool AddEntry(const std::string &key, const std::string &value);
  std::optional<size_t> FindEntryIdx(const std::string &key) const;
  std::optional<std::string> FindValue(const std::string &key) const;
  bool IsEmpty() const { return offsets_.empty(); }
  std::string GetFirstKey() const;

  // return the first and the last position of the monotony predicate
  std::optional<std::pair<BlockIterator, BlockIterator>> GetMonotonyPredicateIters(
      const std::function<int(const std::string &)> &monotony_predicate);

  BlockIterator begin();  // NOLINT
  BlockIterator end();    // NOLINT
};