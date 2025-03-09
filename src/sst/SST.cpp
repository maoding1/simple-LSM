#include <sst/SST.h>

std::shared_ptr<SST> SST::Open(size_t sst_id, FileObj file) {
  auto sst = std::make_shared<SST>();
  sst->sst_id_ = sst_id;
  sst->file_ = std::move(file);

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
                                                const std::string &last_key) {
  auto sst = std::make_shared<SST>();
  sst->sst_id_ = sst_id;
  sst->file_.SetSize(file_size);
  sst->first_key_ = first_key;
  sst->last_key_ = last_key;

  sst->meta_offset_ = 0;
  return sst;
}

std::shared_ptr<Block> SST::ReadBlock(size_t block_idx) {
  if (block_idx >= meta_.size()) {
    throw std::out_of_range("Invalid block index");
  }

  auto &meta = meta_[block_idx];
  size_t block_size;
  if (block_idx == meta_.size() - 1) {
    block_size = meta_offset_ - meta.offset_;
  } else {
    block_size = meta_[block_idx + 1].offset_ - meta.offset_;
  }

  auto block_data = file_.Read(meta.offset_, block_size);
  return Block::Decode(block_data, true);
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

SSTBuilder::SSTBuilder(size_t block_size) : block_(block_size) {}

void SSTBuilder::Add(const std::string &key, const std::string &value) {
  if (first_key_.empty()) {
    first_key_ = key;
  }

  uint32_t key_hash = std::hash<std::string>{}(key);
  key_hashes_.push_back(key_hash);

  if (block_.AddEntry(key, value)) {
    last_key_ = key;
    block_size_ = block_.GetCurSize();
    return;
  }

  // The block is full, finish current block  start a new block
  FinishBlock();
  block_.AddEntry(key, value);
  first_key_ = key;
  last_key_ = key;
  block_size_ = block_.GetCurSize();
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

std::shared_ptr<SST> SSTBuilder::Build(size_t sst_id, const std::string &path) {
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
  auto res = SST::CreateSSTWithMetaOnly(sst_id, file.Size(), meta_.front().first_key_, meta_.back().last_key_);
  res->file_ = std::move(file);
  res->meta_offset_ = meta_offset;
  res->meta_ = std::move(meta_);
  return res;
}