#pragma once

#include <memoryTable/HeapIterator.h>
#include <skiplist/SkipList.h>
#include <sst/SST.h>
#include <type/KeyComparator.h>
#include <list>

using StringSkipList = SkipList<std::string, std::string, KeyComparator<std::string>>;

class MemoryTable {
 private:
  std::shared_ptr<StringSkipList> current_table_;
  std::list<std::shared_ptr<StringSkipList>> frozen_tables_;
  size_t frozen_bytes_;
  std::shared_mutex frozen_tables_mutex_;
  std::shared_mutex current_table_mutex_;

 private:
  // Internal Version of functions don't need to get lock
  void InternalPut(const std::string &key, const std::string &value);
  std::optional<std::string> CurGet(const std::string &key);
  std::optional<std::string> FrozenGet(const std::string &key);
  void InternalRemove(const std::string &key);
  void InternalFrozenCurrentTable();

 public:
  MemoryTable();
  ~MemoryTable() = default;

  void Put(const std::string &key, const std::string &value);
  void PutBatch(const std::vector<std::pair<std::string, std::string>> &batch);

  std::optional<std::string> Get(const std::string &key);
  void Remove(const std::string &key);
  void RemoveBatch(const std::vector<std::string> &keys);

  void Clear();

  void FrozenCurrentTable();

  HeapIterator Begin();
  HeapIterator End();

  size_t GetCurSize();
  size_t GetFrozenSize();
  size_t GetTotalSize();

  std::shared_ptr<SST> FlushLast(const std::shared_ptr<SSTBuilder> &builder, const std::string &sst_path, size_t sst_id,
                                 std::shared_ptr<BlockCache> block_cache);

  std::optional<std::pair<HeapIterator, HeapIterator>> ItersMonotonyPredicate(
      const std::function<int(const std::string &)> &predicate);
  HeapIterator ItersPreffix(const std::string &preffix);
};