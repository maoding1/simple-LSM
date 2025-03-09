#include <block/BlockIterator.h>
#include <stdexcept>

BlockIterator::BlockIterator(std::shared_ptr<Block> block, const std::string &key)
    : block_(std::move(block)), cached_value_(std::nullopt) {
  auto idx = block_->FindEntryIdx(key);
  if (idx.has_value()) {
    current_idx_ = idx.value();
  } else {
    current_idx_ = block_->offsets_.size();
  }
}

void BlockIterator::UpdateCurrent() const {
  if (current_idx_ < block_->offsets_.size()) {
    auto entry = block_->GetEntryAt(block_->GetOffsetAt(current_idx_));
    cached_value_ = std::make_pair(entry.key_, entry.value_);
  } else {
    cached_value_ = std::nullopt;
  }
}

BlockIterator::value_type *BlockIterator::operator->() const {
  if (!cached_value_.has_value()) {
    UpdateCurrent();
  }
  return &cached_value_.value();
}

BlockIterator &BlockIterator::operator++() {
  if (block_ && current_idx_ < block_->offsets_.size()) {
    ++current_idx_;
  }
  UpdateCurrent();
  return *this;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
  if (block_ != other.block_) {
    return false;
  }
  if (current_idx_ != other.current_idx_) {
    return false;
  }
  return true;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
  return !(*this == other);
}

BlockIterator::value_type &BlockIterator::operator*() const {
  if (!block_ || current_idx_ >= block_->offsets_.size()) {
    throw std::runtime_error("Invalid iterator dereference");
  }
  if (!cached_value_.has_value()) {
    UpdateCurrent();
  }
  return cached_value_.value();
}

bool BlockIterator::IsEnd() {
  return current_idx_ >= block_->offsets_.size();
}