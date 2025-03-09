#include <memoryTable/HeapIterator.h>
#include <memoryTable/MemoryTable.h>
#include <utils/Macro.h>
#include <memory>
#include <mutex>
#include <optional>

MemoryTable::MemoryTable() {
  KeyComparator<std::string> key_comparator;
  current_table_ = std::make_shared<StringSkipList>(key_comparator);
  frozen_bytes_ = 0;
}

void MemoryTable::InternalPut(const std::string &key, const std::string &value) { current_table_->Put(key, value); }

void MemoryTable::Put(const std::string &key, const std::string &value) {
  std::unique_lock<std::shared_mutex> lock(current_table_mutex_);
  InternalPut(key, value);
  if (current_table_->UsedBytes() > LSM_PER_MEM_SIZE_LIMIT) {
    std::unique_lock<std::shared_mutex> lock(frozen_tables_mutex_);
    InternalFrozenCurrentTable();
  }
}

void MemoryTable::PutBatch(const std::vector<std::pair<std::string, std::string> > &batch) {
  std::unique_lock<std::shared_mutex> lock(current_table_mutex_);
  for (const auto &item : batch) {
    InternalPut(item.first, item.second);
  }
  if (current_table_->UsedBytes() > LSM_PER_MEM_SIZE_LIMIT) {
    std::unique_lock<std::shared_mutex> lock(frozen_tables_mutex_);
    InternalFrozenCurrentTable();
  }
}

std::optional<std::string> MemoryTable::CurGet(const std::string &key) {
  auto result = current_table_->Get(key);
  if (result.has_value()) {
    return result.value();
  }
  return std::nullopt;
}

std::optional<std::string> MemoryTable::FrozenGet(const std::string &key) {
  std::shared_lock<std::shared_mutex> lock(frozen_tables_mutex_);
  for (const auto &table : frozen_tables_) {
    auto result = table->Get(key);
    if (result.has_value()) {
      return result.value();
    }
  }
  return std::nullopt;
}

std::optional<std::string> MemoryTable::Get(const std::string &key) {
  std::shared_lock<std::shared_mutex> lock(current_table_mutex_);
  auto result = CurGet(key);
  if (result.has_value()) {
    return result.value();
  }
  lock.unlock();
  std::shared_lock<std::shared_mutex> lock2(frozen_tables_mutex_);
  return FrozenGet(key);
}

void MemoryTable::InternalRemove(const std::string &key) { current_table_->Put(key, ""); }

void MemoryTable::Remove(const std::string &key) {
  std::unique_lock<std::shared_mutex> lock(current_table_mutex_);
  InternalRemove(key);
}

void MemoryTable::RemoveBatch(const std::vector<std::string> &keys) {
  std::unique_lock<std::shared_mutex> lock(current_table_mutex_);
  for (const auto &key : keys) {
    InternalRemove(key);
  }
}

void MemoryTable::Clear() {
  std::unique_lock<std::shared_mutex> lock(current_table_mutex_);
  std::unique_lock<std::shared_mutex> lock2(frozen_tables_mutex_);
  current_table_->Clear();
  frozen_tables_.clear();
}

void MemoryTable::InternalFrozenCurrentTable() {
  frozen_tables_.push_front(current_table_);
  frozen_bytes_ += current_table_->UsedBytes();
  KeyComparator<std::string> key_comparator;
  current_table_ = std::make_shared<StringSkipList>(key_comparator);
}

void MemoryTable::FrozenCurrentTable() {
  std::unique_lock<std::shared_mutex> lock(current_table_mutex_);
  std::unique_lock<std::shared_mutex> lock2(frozen_tables_mutex_);
  InternalFrozenCurrentTable();
}

HeapIterator MemoryTable::Begin() {
  std::shared_lock<std::shared_mutex> lock(current_table_mutex_);
  std::shared_lock<std::shared_mutex> lock2(frozen_tables_mutex_);
  std::vector<SearchItem> items;
  for (auto iter = current_table_->Begin(); iter != current_table_->End(); ++iter) {
    items.emplace_back(iter.GetKey(), iter.GetValue(), 0);
  }

  int table_idx = 1;
  for (const auto &table : frozen_tables_) {
    for (auto iter = table->Begin(); iter != table->End(); ++iter) {
      items.emplace_back(iter.GetKey(), iter.GetValue(), table_idx);
    }
    table_idx++;
  }

  return HeapIterator(items);
}

HeapIterator MemoryTable::End() {
  std::shared_lock<std::shared_mutex> lock(current_table_mutex_);
  std::shared_lock<std::shared_mutex> lock2(frozen_tables_mutex_);
  return HeapIterator();
}

size_t MemoryTable::GetCurSize() {
  std::shared_lock<std::shared_mutex> lock(current_table_mutex_);
  return current_table_->UsedBytes();
}

size_t MemoryTable::GetFrozenSize() {
  std::shared_lock<std::shared_mutex> lock(frozen_tables_mutex_);
  return frozen_bytes_;
}

size_t MemoryTable::GetTotalSize() {
  std::shared_lock<std::shared_mutex> lock(current_table_mutex_);
  std::shared_lock<std::shared_mutex> lock2(frozen_tables_mutex_);
  return current_table_->UsedBytes() + frozen_bytes_;
}

HeapIterator MemoryTable::ItersPreffix(const std::string &preffix) {
  std::shared_lock<std::shared_mutex> lock(current_table_mutex_);
  std::shared_lock<std::shared_mutex> lock2(frozen_tables_mutex_);
  std::vector<SearchItem> items;
  for (auto iter = current_table_->BeginPreffix(preffix); iter != current_table_->EndPreffix(preffix); ++iter) {
    items.emplace_back(iter.GetKey(), iter.GetValue(), 0);
  }

  int table_idx = 1;
  for (const auto &table : frozen_tables_) {
    for (auto iter = table->BeginPreffix(preffix); iter != table->EndPreffix(preffix); ++iter) {
      items.emplace_back(iter.GetKey(), iter.GetValue(), table_idx);
    }
    table_idx++;
  }

  return HeapIterator(items);
}