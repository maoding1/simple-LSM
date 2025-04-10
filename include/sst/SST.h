#pragma once
/**
 * SSTable layout:
 * -------------------------------------------------------------------------------------------
 * |         Block Section         |          Meta Section         |          Extra          |
 * -------------------------------------------------------------------------------------------
 * | data block | ... | data block |            metadata           | meta block offset (u32) |
 * -------------------------------------------------------------------------------------------

 * Meta Section layout:
 * --------------------------------------------------------------------------------------------------------------
 * | num_entries (32) | MetaEntry | ... | MetaEntry | Hash (32) |
 * --------------------------------------------------------------------------------------------------------------

 * MetaEntry layout:
 * --------------------------------------------------------------------------------------------------------------
 * | offset (4B) | first_key_len (2B) | first_key (first_key_len) | last_key_len(2B) | last_key (last_key_len) |
 * --------------------------------------------------------------------------------------------------------------
 */

#include <block/Block.h>
#include <block/BlockCache.h>
#include <block/BlockMeta.h>
#include <utils/File.h>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

class SSTIterator;

/** SST Class is a descriptor for SSTable(sorted string table) file, which contains metadata and data blocks
 * the metadata always store in memory
 * but the blocks is loaded into memory only when it was needed*/
class SST : public std::enable_shared_from_this<SST> {
  friend class SSTBuilder;
  friend std::optional<std::pair<SSTIterator, SSTIterator>> SSTItersMonotonyPredicate(
      const std::shared_ptr<SST> &sst, const std::function<int(const std::string &)> &predicate);

 private:
  FileObj file_;
  std::vector<BlockMeta> meta_;
  uint32_t meta_offset_;
  size_t sst_id_;
  std::string first_key_;
  std::string last_key_;
  std::shared_ptr<BlockCache> block_cache_;

 public:
  SST() = default;

  // Open an existing SST file
  static std::shared_ptr<SST> Open(size_t sst_id, FileObj file, std::shared_ptr<BlockCache> block_cache);
  // Create an SST with metadata only
  static std::shared_ptr<SST> CreateSSTWithMetaOnly(size_t sst_id, size_t file_size, const std::string &first_key,
                                                    const std::string &last_key,
                                                    std::shared_ptr<BlockCache> block_cache);
  std::shared_ptr<Block> ReadBlock(size_t block_idx);
  size_t FindBlockIndex(const std::string &key);
  size_t NumBlocks() const;
  std::string GetFirstKey() const;
  std::string GetLastKey() const;
  size_t GetSSTSize() const;
  size_t GetSSTId() const;
  SSTIterator Get(const std::string &key);
  SSTIterator Begin();  // NOLINT
  SSTIterator End();    // NOLINT
};

class SSTBuilder {
 private:
  Block block_;            // current block
  std::string first_key_;  // first key of the current block
  std::string last_key_;   // last key of the current block
  std::vector<BlockMeta> meta_;
  std::vector<uint8_t> data_;  // encoded SST data
  size_t block_size_;
  std::vector<uint32_t> key_hashes_;

 public:
  explicit SSTBuilder(size_t block_size);
  void Add(const std::string &key, const std::string &value);  // add key-value pair
  size_t EstimateSize() const;
  void FinishBlock();
  std::shared_ptr<SST> Build(size_t sst_id, const std::string &path, std::shared_ptr<BlockCache> block_cache);
  size_t GetBlockSize() const { return block_size_; }
};