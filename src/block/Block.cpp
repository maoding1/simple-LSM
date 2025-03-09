#include <block/Block.h>
#include <block/BlockIterator.h>
#include <cstring>
#include <stdexcept>

std::vector<uint8_t> Block::Encode() {
  size_t total_bytes = data_.size() * sizeof(uint8_t) + offsets_.size() * sizeof(uint16_t) + sizeof(uint16_t);
  std::vector<uint8_t> result(total_bytes, 0);

  memcpy(result.data(), data_.data(), data_.size() * sizeof(uint8_t));

  size_t offset_pos = data_.size() * sizeof(uint8_t);
  memcpy(result.data() + offset_pos, offsets_.data(), offsets_.size() * sizeof(uint16_t));

  size_t extra_pos = offset_pos + offsets_.size() * sizeof(uint16_t);
  uint16_t num_of_elements = offsets_.size();
  memcpy(result.data() + extra_pos, &num_of_elements, sizeof(uint16_t));

  return result;
}

std::shared_ptr<Block> Block::Decode(const std::vector<uint8_t> &encoded, bool with_hash) {
  std::shared_ptr<Block> block = std::make_shared<Block>();

  if (encoded.size() < sizeof(uint16_t)) {
    throw std::runtime_error("Invalid block data, too small");
  }

  size_t num_elements_pos = encoded.size() - sizeof(uint16_t);
  if (with_hash) {
    num_elements_pos -= sizeof(uint32_t);
    auto hash_pos = encoded.size() - sizeof(uint32_t);
    uint32_t hash_val = 0;
    memcpy(&hash_val, encoded.data() + hash_pos, sizeof(uint32_t));
    uint32_t encoded_hash_val =
        std::hash<std::string_view>{}(std::string_view(reinterpret_cast<const char *>(encoded.data()), hash_pos));
    if (hash_val != encoded_hash_val) {
      throw std::runtime_error("Invalid block data, hash mismatch");
    }
  }
  uint16_t num_of_elements = 0;
  memcpy(&num_of_elements, encoded.data() + num_elements_pos, sizeof(uint16_t));

  size_t required_size = sizeof(uint16_t) + num_of_elements * sizeof(uint16_t);
  if (encoded.size() < required_size) {
    throw std::runtime_error("Invalid block data, too small");
  }

  size_t offsets_section_start = num_elements_pos - num_of_elements * sizeof(uint16_t);
  block->offsets_.resize(num_of_elements);
  memcpy(block->offsets_.data(), encoded.data() + offsets_section_start, num_of_elements * sizeof(uint16_t));

  block->data_.resize(offsets_section_start);
  memcpy(block->data_.data(), encoded.data(), offsets_section_start);

  return block;
}

size_t Block::GetOffsetAt(size_t index) const {
  if (index >= offsets_.size()) {
    throw std::runtime_error("Invalid index");
  }
  return offsets_[index];
}

bool Block::AddEntry(const std::string &key, const std::string &value) {
  size_t key_len = key.size();
  size_t value_len = value.size();
  size_t entry_size = key_len + value_len + 2 * sizeof(uint16_t);
  if (GetCurSize() + entry_size > capacity_ && !offsets_.empty()) {
    return false;
  }

  data_.resize(data_.size() + entry_size);
  memcpy(data_.data() + data_.size() - entry_size, &key_len, sizeof(uint16_t));
  memcpy(data_.data() + data_.size() - entry_size + sizeof(uint16_t), key.data(), key_len);
  memcpy(data_.data() + data_.size() - entry_size + sizeof(uint16_t) + key_len, &value_len, sizeof(uint16_t));
  memcpy(data_.data() + data_.size() - entry_size + sizeof(uint16_t) + key_len + sizeof(uint16_t), value.data(),
         value_len);

  offsets_.push_back(data_.size() - entry_size);
  return true;
}

Block::Entry Block::GetEntryAt(size_t offset) const {
  if (offset >= data_.size()) {
    throw std::runtime_error("Invalid offset");
  }

  uint16_t key_len = 0;
  memcpy(&key_len, data_.data() + offset, sizeof(uint16_t));
  std::string key(reinterpret_cast<const char *>(data_.data() + offset + sizeof(uint16_t)), key_len);

  uint16_t value_len = 0;
  memcpy(&value_len, data_.data() + offset + sizeof(uint16_t) + key_len, sizeof(uint16_t));
  std::string value(
      reinterpret_cast<const char *>(data_.data() + offset + sizeof(uint16_t) + key_len + sizeof(uint16_t)), value_len);

  return {key, value};
}

std::string Block::GetKeyAt(size_t offset) const { return GetEntryAt(offset).key_; }

std::string Block::GetValueAt(size_t offset) const { return GetEntryAt(offset).value_; }

int Block::CompareKeyAt(size_t offset, const std::string &key) const { return GetEntryAt(offset).key_.compare(key); }

std::optional<size_t> Block::FindEntryIdx(const std::string &key) const {
  if (offsets_.empty()) {
    return std::nullopt;
  }

  int left = -1;
  int right = offsets_.size();
  while (left + 1 < right) {
    int mid = (left + right) / 2;
    int cmp = CompareKeyAt(GetOffsetAt(mid), key);
    if (cmp == 0) {
      return mid;
    }
    if (cmp < 0) {
      left = mid;
    } else {
      right = mid;
    }
  }
  if (static_cast<size_t>(right) < offsets_.size() && CompareKeyAt(GetOffsetAt(right), key) == 0) {
    return right;
  }
  return std::nullopt;
}

std::optional<std::string> Block::FindValue(const std::string &key) const {
  auto idx = FindEntryIdx(key);
  if (!idx.has_value()) {
    return std::nullopt;
  }
  return GetValueAt(GetOffsetAt(idx.value()));
}

std::string Block::GetFirstKey() const {
  if (offsets_.empty()) {
    return "";
  }
  return GetKeyAt(GetOffsetAt(0));
}


// predicate(key) < 0: find left
// predicate(key) = 0: satisfy
// predicate(key) > 0: find right
std::optional<std::pair<BlockIterator, BlockIterator>> Block::GetMonotonyPredicateIters(
    const std::function<int(const std::string &)> &predicate) {
  if (offsets_.empty()) {
    return std::nullopt;
  }

  int left = -1;
  int right = offsets_.size();
  while (left + 1 != right) {
    int mid = (left + right) / 2;
    int cmp = predicate(GetKeyAt(GetOffsetAt(mid)));
    if (cmp > 0) {
      left = mid;
    } else {
      right = mid;
    }
  }

  int begin = right;
  if (begin == static_cast<int>(offsets_.size()) || predicate(GetKeyAt(GetOffsetAt(begin))) != 0) {
    return std::nullopt;
  }

  left = -1;
  right = offsets_.size();
  while (left + 1 != right) {
    int mid = (left + right) / 2;
    int cmp = predicate(GetKeyAt(GetOffsetAt(mid)));
    if (cmp >= 0) {
      left = mid;
    } else {
      right = mid;
    }
  }

  int end = left + 1;

  return std::make_pair(BlockIterator(shared_from_this(), begin), BlockIterator(shared_from_this(), end));
}

BlockIterator Block::begin() { return BlockIterator(shared_from_this(), 0); }

BlockIterator Block::end() { return BlockIterator(shared_from_this(), offsets_.size()); }