#include <block/BlockIterator.h>
#include <sst/SSTIterator.h>
#include <utility>

SSTIterator::SSTIterator(std::shared_ptr<SST> sst) : sst_(std::move(sst)), block_idx_(0), block_iter_(nullptr) {
  if (sst_ != nullptr) {
    SeekFirst();
  }
}

SSTIterator::SSTIterator(std::shared_ptr<SST> sst, const std::string &key)
    : sst_(std::move(sst)), block_idx_(0), block_iter_(nullptr) {
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

bool SSTIterator::IsEnd() { return block_iter_ == nullptr; }

bool SSTIterator::IsValid() const {
  return block_iter_ != nullptr && !block_iter_->IsEnd() && block_idx_ < sst_->NumBlocks();
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

void SSTIterator::SetBlockIdx(size_t block_idx) { block_idx_ = block_idx; }

void SSTIterator::SetBlockIter(std::shared_ptr<BlockIterator> block_iter) { block_iter_ = std::move(block_iter); }

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
  if (!this->IsValid() && !other.IsValid()) {
    return true;
  }
  if (block_iter_ == nullptr && other.block_iter_ == nullptr) {
    return true;
  }
  if (block_iter_ == nullptr || other.block_iter_ == nullptr) {
    return false;
  }
  return sst_ == other.sst_ && block_idx_ == other.block_idx_ && *block_iter_ == *other.block_iter_;
}

bool SSTIterator::operator!=(const SSTIterator &other) const { return !(*this == other); }

SSTIterator::value_type SSTIterator::operator*() const {
  if (block_iter_ == nullptr) {
    throw std::runtime_error("SSTIterator: Invalid iterator dereference");
  }
  return **block_iter_;
}

std::optional<std::pair<SSTIterator, SSTIterator>> SSTItersMonotonyPredicate(
    const std::shared_ptr<SST> &sst, const std::function<int(const std::string &)> &predicate) {
  std::optional<SSTIterator> final_begin = std::nullopt;
  std::optional<SSTIterator> final_end = std::nullopt;
  for (size_t block_idx = 0; block_idx < sst->meta_.size(); block_idx++) {
    auto block = sst->ReadBlock(block_idx);
    BlockMeta &meta = sst->meta_[block_idx];
    if (predicate(meta.first_key_) < 0 || predicate(meta.last_key_) > 0) {
      break;
    }

    auto result = block->GetMonotonyPredicateIters(predicate);
    if (result.has_value()) {
      auto [begin, end] = result.value();
      if (!final_begin.has_value()) {
        final_begin = SSTIterator(sst);
        final_begin->SetBlockIdx(block_idx);
        final_begin->SetBlockIter(std::make_shared<BlockIterator>(begin));
      }
      final_end = SSTIterator(sst);
      final_end->SetBlockIdx(block_idx);
      final_end->SetBlockIter(std::make_shared<BlockIterator>(end));
    }
  }
  if (!final_begin.has_value() || !final_end.has_value()) {
    return std::nullopt;
  }
  return std::make_pair(final_begin.value(), final_end.value());
}