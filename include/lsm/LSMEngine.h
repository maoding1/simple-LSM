#pragma once

#include <lsm/MergeIterator.h>
#include <memoryTable/MemoryTable.h>
#include <sst/SST.h>
#include <sst/SSTIterator.h>
#include <list>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

using SST_ID = size_t;
class LSMEngine {
 private:
  std::string data_dir_;  // directory to store SST files
  MemoryTable memtable_;
  std::list<size_t> l0_sst_ids_;                           // list of SST IDs in L0 level
  std::unordered_map<SST_ID, std::shared_ptr<SST>> ssts_;  // map from SST ID to SST
  std::shared_mutex mutex_;                                // rw-mutex to protect L0_sst_ids_ and ssts_
  std::shared_ptr<BlockCache> block_cache_;

 public:
  explicit LSMEngine(std::string data_dir);
  ~LSMEngine();

  std::optional<std::string> Get(const std::string &key);
  void Put(const std::string &key, const std::string &value);
  // void PutBatch(const std::vector<std::pair<std::string, std::string>> &batch);
  void Remove(const std::string &key);
  // void RemoveBatch(const std::vector<std::string> &keys);
  // void Clear();
  void Flush();
  void FlushAll();

  std::string GetSSTPath(SST_ID sst_id);

  MergeIterator Begin();
  MergeIterator End();
};

class LSM {
 private:
  LSMEngine engine_;

 public:
  explicit LSM(std::string data_dir);
  ~LSM();

  std::optional<std::string> Get(const std::string &key);
  void Put(const std::string &key, const std::string &value);
  // void PutBatch(const std::vector<std::pair<std::string, std::string>> &batch);
  void Remove(const std::string &key);
  // void RemoveBatch(const std::vector<std::string> &keys);

  using LSMIterator = MergeIterator;
  LSMIterator Begin();
  LSMIterator End();
  void Flush();
  void FlushAll();
};