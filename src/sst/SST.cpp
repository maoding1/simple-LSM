#include <sst/SST.h>
#include <sst/SSTIterator.h>
#include <utility>

std::shared_ptr<SST> SST::Open(size_t sst_id, FileObj file, std::shared_ptr<BlockCache> block_cache) {
  auto sst = std::make_shared<SST>();
  sst->sst_id_ = sst_id;
  sst->file_ = std::move(file);
  sst->block_cache_ = std::move(block_cache);

  size_t file_size = sst->file_.Size();
  if (file_size < sizeof(size_t)) {
    throw std::runtime_error("Invalid SST file size, too small");
  }

  auto meta_offset_bytes = sst->file_.Read(file_size - sizeof(uint32_t), sizeof(uint32_t));
  sst->meta_offset_ = *reinterpret_cast<uint32_t *>(meta_offset_bytes.data());

  if (sst->meta_offset_ > file_size) {
    throw std::runtime_error("Invalid SST meta offset");
  }

  auto meta_bytes = sst->file_.Read(sst->meta_offset_, file_size - sst->meta_offset_ - sizeof(uint32_t));
  sst->meta_ = BlockMeta::DecodeMeta(meta_bytes);

  if (sst->meta_.empty()) {
    throw std::runtime_error("Invalid SST meta");
  }

  sst->first_key_ = sst->meta_.front().first_key_;
  sst->last_key_ = sst->meta_.back().last_key_;

  return sst;
}

std::shared_ptr<SST> SST::CreateSSTWithMetaOnly(size_t sst_id, size_t file_size, const std::string &first_key,
                                                const std::string &last_key, std::shared_ptr<BlockCache> block_cache) {
  auto sst = std::make_shared<SST>();
  sst->sst_id_ = sst_id;
  sst->file_.SetSize(file_size);
  sst->first_key_ = first_key;
  sst->last_key_ = last_key;
  sst->block_cache_ = std::move(block_cache);

  sst->meta_offset_ = 0;
  return sst;
}

std::shared_ptr<Block> SST::ReadBlock(size_t block_idx) {
  if (block_idx >= meta_.size()) {
    throw std::out_of_range("Invalid block index");
  }

  if (block_cache_ != nullptr) {
    auto block = block_cache_->Get(sst_id_, block_idx);
    if (block != nullptr) {
      return block;
    }
  } else {
    throw std::runtime_error("Block cache not set");
  }

  auto &meta = meta_[block_idx];
  size_t block_size;
  if (block_idx == meta_.size() - 1) {
    block_size = meta_offset_ - meta.offset_;
  } else {
    block_size = meta_[block_idx + 1].offset_ - meta.offset_;
  }

  auto block_data = file_.Read(meta.offset_, block_size);
  auto res = Block::Decode(block_data, true);

  block_cache_->Put(sst_id_, block_idx, res);
  return res;
}

size_t SST::FindBlockIndex(const std::string &key) {
  if (key < first_key_ || key > last_key_) {
    throw std::out_of_range("Key out of range");
  }

  int left = -1;
  int right = meta_.size();
  while (left + 1 != right) {
    int mid = (left + right) / 2;
    if (key > meta_[mid].last_key_) {
      left = mid;
    } else {
      right = mid;
    }
  }
  return right;
}

size_t SST::NumBlocks() const { return meta_.size(); }

std::string SST::GetFirstKey() const { return first_key_; }

std::string SST::GetLastKey() const { return last_key_; }

size_t SST::GetSSTSize() const { return file_.Size(); }

size_t SST::GetSSTId() const { return sst_id_; }

SSTIterator SST::Get(const std::string &key) {
  if (key < first_key_ || key > last_key_) {
    return this->End();
  }

  return SSTIterator(this->shared_from_this(), key);
}

SSTIterator SST::Begin() { return SSTIterator(this->shared_from_this()); }

SSTIterator SST::End() {
  SSTIterator res(this->shared_from_this());
  res.block_idx_ = meta_.size();
  res.block_iter_ = nullptr;
  return res;
}

SSTBuilder::SSTBuilder(size_t block_size) : block_(block_size) {}

void SSTBuilder::Add(const std::string &key, const std::string &value) {
  if (first_key_.empty()) {
    first_key_ = key;
  }

  uint32_t key_hash = std::hash<std::string>{}(key);
  key_hashes_.push_back(key_hash);

  if (block_.AddEntry(key, value)) {
    last_key_ = key;
    return;
  }

  // The block is full, finish current block  start a new block
  FinishBlock();
  block_.AddEntry(key, value);
  first_key_ = key;
  last_key_ = key;
}

size_t SSTBuilder::EstimateSize() const { return data_.size(); }

void SSTBuilder::FinishBlock() {
  auto old_block = std::move(block_);
  auto encoded = old_block.Encode();

  meta_.emplace_back(data_.size(), first_key_, last_key_);

  uint32_t hash =
      std::hash<std::string_view>{}(std::string_view(reinterpret_cast<const char *>(encoded.data()), encoded.size()));
  data_.insert(data_.end(), encoded.begin(), encoded.end());
  data_.insert(data_.end(), reinterpret_cast<uint8_t *>(&hash), reinterpret_cast<uint8_t *>(&hash) + sizeof(uint32_t));
}

std::shared_ptr<SST> SSTBuilder::Build(size_t sst_id, const std::string &path,
                                       std::shared_ptr<BlockCache> block_cache) {
  if (!block_.IsEmpty()) {
    FinishBlock();
  }

  if (meta_.empty()) {
    throw std::runtime_error("No data to build SST");
  }

  // encode meta
  std::vector<uint8_t> meta_data;
  BlockMeta::EncodeMeta(meta_, &meta_data);

  uint32_t meta_offset = data_.size();
  data_.insert(data_.end(), meta_data.begin(), meta_data.end());
  data_.insert(data_.end(), reinterpret_cast<uint8_t *>(&meta_offset),
               reinterpret_cast<uint8_t *>(&meta_offset) + sizeof(uint32_t));

  FileObj file = FileObj::CreateAndWrite(path, data_);
  auto res = SST::CreateSSTWithMetaOnly(sst_id, file.Size(), meta_.front().first_key_, meta_.back().last_key_,
                                        std::move(block_cache));
  res->file_ = std::move(file);
  res->meta_offset_ = meta_offset;
  res->meta_ = std::move(meta_);
  return res;
}