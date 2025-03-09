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

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
class BlockMeta {
 public:
  size_t offset_;
  std::string first_key_;
  std::string last_key_;

  // Encode the meta entries into a byte array
  static void EncodeMeta(const std::vector<BlockMeta> &meta_entries, std::vector<uint8_t> *meta);

  // Decode the meta byte array into meta entries
  static std::vector<BlockMeta> DecodeMeta(const std::vector<uint8_t> &meta);
  BlockMeta() = default;
  BlockMeta(size_t offset, std::string first_key, std::string last_key);
};
