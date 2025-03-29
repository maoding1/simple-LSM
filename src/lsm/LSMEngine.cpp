#include <lsm/LSMEngine.h>
#include <utils/Macro.h>
#include <filesystem>

LSMEngine::LSMEngine(std::string data_dir) : data_dir_(std::move(data_dir)) {
  block_cache_ = std::make_shared<BlockCache>(BLOCK_CACHE_CAPACITY, BLOCK_CACHE_K);

  if (!std::filesystem::exists(data_dir_)) {
    std::filesystem::create_directories(data_dir_);
  } else {
    for (const auto &entry : std::filesystem::directory_iterator(data_dir_)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      std::string filename = entry.path().filename().string();

      if (filename.substr(0, 4) != "sst_") {
        continue;
      }

      std::string id_str = filename.substr(4);
      if (id_str.empty()) {
        continue;
      }
      SST_ID sst_id = std::stoull(id_str);

      std::unique_lock<std::shared_mutex> lock(mutex_);
      std::string sst_path = GetSSTPath(sst_id);
      auto sst = SST::Open(sst_id, FileObj::Open(sst_path), block_cache_);
      ssts_[sst_id] = sst;

      l0_sst_ids_.push_back(sst_id);
    }
  }
  l0_sst_ids_.sort();
  l0_sst_ids_.reverse();
}

LSMEngine::~LSMEngine() { FlushAll(); }

void LSMEngine::Put(const std::string &key, const std::string &value) {
  memtable_.Put(key, value);

  if (memtable_.GetCurSize() >= LSM_TOL_MEM_SIZE_LIMIT) {
    Flush();
  }
}

std::optional<std::string> LSMEngine::Get(const std::string &key) {
  // search in memtable
  auto it = memtable_.Get(key);
  if (it.has_value()) {
    if (it.value().empty()) {
      return std::nullopt;
    }
    return it.value();
  }

  // search in L0 SST
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (auto sst_id : l0_sst_ids_) {
    auto sst = ssts_[sst_id];
    auto sst_it = sst->Get(key);
    if (sst_it != sst->End()) {
      if (sst_it.GetValue().empty()) {
        return std::nullopt;
      }
      return sst_it.GetValue();
    }
  }

  return std::nullopt;
}

void LSMEngine::Remove(const std::string &key) { memtable_.Remove(key); }

void LSMEngine::Flush() {
  if (memtable_.GetTotalSize() == 0) {
    return;
  }

  size_t new_sst_id = l0_sst_ids_.empty() ? 0 : l0_sst_ids_.front() + 1;

  std::shared_ptr<SSTBuilder> builder = std::make_shared<SSTBuilder>(LSM_BLOCK_SIZE);

  auto sst_path = GetSSTPath(new_sst_id);
  auto new_sst = memtable_.FlushLast(builder, sst_path, new_sst_id, block_cache_);

  std::unique_lock<std::shared_mutex> lock(mutex_);
  l0_sst_ids_.push_front(new_sst_id);
  ssts_[new_sst_id] = new_sst;
}

void LSMEngine::FlushAll() {
  while (memtable_.GetTotalSize() > 0) {
    Flush();
  }
}

std::string LSMEngine::GetSSTPath(SST_ID sst_id) {
  std::stringstream ss;
  ss << data_dir_ << "/sst_" << std::setfill('0') << std::setw(4) << sst_id;
  return ss.str();
}

MergeIterator LSMEngine::Begin() {
  std::vector<SearchItem> items;
  std::shared_lock<std::shared_mutex> lock(mutex_);
  for (auto sst_id : l0_sst_ids_) {
    auto sst = ssts_[sst_id];
    auto sst_it = sst->Begin();
    while (!sst_it.IsEnd()) {
      items.emplace_back(sst_it.GetKey(), sst_it.GetValue(), sst_id);
      ++sst_it;
    }
  }
  HeapIterator sst_iter(items);
  auto mem_table_iter = memtable_.Begin();
  return MergeIterator(mem_table_iter, sst_iter);
}

MergeIterator LSMEngine::End() { return MergeIterator{}; }

std::optional<std::pair<MergeIterator, MergeIterator>> LSMEngine::LSMItersMonotonyPredicate(
    const std::function<int(const std::string &)> &predicate) {
  auto mem_result = memtable_.ItersMonotonyPredicate(predicate);
  std::vector<SearchItem> items;
  for (auto &[sst_idx, sst] : ssts_) {
    auto result = SSTItersMonotonyPredicate(sst, predicate);
    if (!result.has_value()) {
      continue;
    }
    auto &[begin, end] = result.value();
    while (begin != end) {
      items.emplace_back(begin.GetKey(), begin.GetValue(), -sst_idx);
      ++begin;
    }
  }

  if (!mem_result.has_value() && items.empty()) {
    return std::nullopt;
  }

  HeapIterator l0_iter(items);
  if (mem_result.has_value()) {
    auto [mem_start, mem_end] = mem_result.value();
    auto start = MergeIterator(mem_start, l0_iter);
    auto end = MergeIterator{};
    return std::make_pair(start, end);
  }
  auto start = MergeIterator(HeapIterator{}, l0_iter);
  auto end = MergeIterator{};
  return std::make_pair(start, end);
}

LSM::LSM(std::string data_dir) : engine_(std::move(data_dir)) {}

LSM::~LSM() { engine_.FlushAll(); }

std::optional<std::string> LSM::Get(const std::string &key) { return engine_.Get(key); }

void LSM::Put(const std::string &key, const std::string &value) { engine_.Put(key, value); }

void LSM::Remove(const std::string &key) { engine_.Remove(key); }

void LSM::Flush() { engine_.Flush(); }

void LSM::FlushAll() { engine_.FlushAll(); }

LSM::LSMIterator LSM::Begin() { return engine_.Begin(); }

LSM::LSMIterator LSM::End() { return engine_.End(); }

std::optional<std::pair<MergeIterator, MergeIterator>> LSM::LSMItersMonotonyPredicate(
    const std::function<int(const std::string &)> &predicate) {
  return engine_.LSMItersMonotonyPredicate(predicate);
}