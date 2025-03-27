#include <sst/SST.h>
#include <sst/SSTIterator.h>
#include <block/BlockIterator.h>
#include <utility>

SSTIterator::SSTIterator(std::shared_ptr<SST> sst) : sst_(std::move(sst)), block_idx_(0), block_iter_(nullptr) {
  if (sst_ != nullptr) {
    SeekFirst();
  }
}

SSTIterator::SSTIterator(std::shared_ptr<SST> sst, const std::string &key) : sst_(std::move(sst)), block_idx_(0), block_iter_(nullptr) {
  if (sst_ != nullptr) {
    Seek(key);
  }
}

void SSTIterator::SeekFirst() {
  if (!sst_ || sst_->NumBlocks() == 0) {
    block_iter_ = nullptr;
    return;
  }

  block_idx_ = 0;
  auto block = sst_->ReadBlock(block_idx_);
  block_iter_ = std::make_shared<BlockIterator>(block);
}

void SSTIterator::Seek(const std::string &key) {
  if (!sst_ || sst_->NumBlocks() == 0) {
    block_iter_ = nullptr;
    return;
  }

  block_idx_ = sst_->FindBlockIndex(key);
  auto block = sst_->ReadBlock(block_idx_);

  block_iter_ = std::make_shared<BlockIterator>(block, key);
}

bool SSTIterator::IsEnd() {
  return block_iter_ == nullptr;
}

std::string SSTIterator::GetKey() {
  if (block_iter_ == nullptr) {
    throw std::runtime_error("SSTIterator: Invalid iterator dereference");
  }
  return (**block_iter_).first;
}

std::string SSTIterator::GetValue() {
  if (block_iter_ == nullptr) {
    throw std::runtime_error("SSTIterator: Invalid iterator dereference");
  }
  return (**block_iter_).second;
}

SSTIterator &SSTIterator::operator++() {
  if (block_iter_ == nullptr) {
    return *this;
  }

  ++(*block_iter_);
  if (block_iter_->IsEnd()) {
    block_idx_++;
    if (block_idx_ < sst_->NumBlocks()) {
      auto next_block = sst_->ReadBlock(block_idx_);
      block_iter_ = std::make_shared<BlockIterator>(next_block);
    } else {
      block_iter_ = nullptr;
    }
  }
  return *this;
}

bool SSTIterator::operator==(const SSTIterator &other) const {
  if (block_iter_ == nullptr && other.block_iter_ == nullptr) {
    return true;
  }
  if (block_iter_ == nullptr || other.block_iter_ == nullptr) {
    return false;
  }
  return sst_ == other.sst_ && block_idx_ == other.block_idx_ && *block_iter_ == *other.block_iter_;
}

bool SSTIterator::operator!=(const SSTIterator &other) const {
  return !(*this == other);
}

SSTIterator::value_type SSTIterator::operator*() const {
  if (block_iter_ == nullptr) {
    throw std::runtime_error("SSTIterator: Invalid iterator dereference");
  }
  return **block_iter_;
}
