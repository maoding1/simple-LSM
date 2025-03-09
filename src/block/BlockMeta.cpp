#include <block/BlockMeta.h>
#include <cstring>
#include <stdexcept>

BlockMeta::BlockMeta(size_t offset, std::string first_key, std::string last_key)
    : offset_(offset), first_key_(std::move(first_key)), last_key_(std::move(last_key)) {}

void BlockMeta::EncodeMeta(const std::vector<BlockMeta> &meta_entries, std::vector<uint8_t> *meta) {
  meta->clear();
  uint32_t num_entries = meta_entries.size();
  size_t total_size = 2 * sizeof(uint32_t);  // num_entries and hash
  for (const auto &entry : meta_entries) {
    total_size +=
        sizeof(uint32_t) + sizeof(uint16_t) + entry.first_key_.size() + sizeof(uint16_t) + entry.last_key_.size();
  }
  meta->resize(total_size);
  uint8_t *data = meta->data();

  // Write the number of entries
  memcpy(data, &num_entries, sizeof(uint32_t));
  data += sizeof(uint32_t);

  // Write the meta entries
  for (const auto &entry : meta_entries) {
    memcpy(data, &entry.offset_, sizeof(uint32_t));
    data += sizeof(uint32_t);

    uint16_t first_key_len = entry.first_key_.size();
    memcpy(data, &first_key_len, sizeof(uint16_t));
    data += sizeof(uint16_t);
    memcpy(data, entry.first_key_.data(), first_key_len);  // NOLINT
    data += first_key_len;

    uint16_t last_key_len = entry.last_key_.size();
    memcpy(data, &last_key_len, sizeof(uint16_t));
    data += sizeof(uint16_t);
    memcpy(data, entry.last_key_.data(), last_key_len);  // NOLINT
    data += last_key_len;
  }

  // Write the hash
  const uint8_t *data_start = meta->data() + sizeof(uint32_t);
  const uint8_t *data_end = data;
  uint32_t hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_end - data_start));
  memcpy(data, &hash, sizeof(uint32_t));
}

std::vector<BlockMeta> BlockMeta::DecodeMeta(const std::vector<uint8_t> &meta) {
  if (meta.size() < 2 * sizeof(uint32_t)) {
    throw std::runtime_error("Invalid meta size");
  }

  std::vector<BlockMeta> meta_entries;
  const uint8_t *data = meta.data();
  uint32_t num_entries;
  memcpy(&num_entries, data, sizeof(uint32_t));
  data += sizeof(uint32_t);

  for (uint32_t i = 0; i < num_entries; i++) {
    uint32_t offset;
    memcpy(&offset, data, sizeof(uint32_t));
    data += sizeof(uint32_t);

    uint16_t first_key_len;
    memcpy(&first_key_len, data, sizeof(uint16_t));
    data += sizeof(uint16_t);
    std::string first_key(reinterpret_cast<const char *>(data), first_key_len);
    data += first_key_len;

    uint16_t last_key_len;
    memcpy(&last_key_len, data, sizeof(uint16_t));
    data += sizeof(uint16_t);
    std::string last_key(reinterpret_cast<const char *>(data), last_key_len);
    data += last_key_len;

    meta_entries.emplace_back(offset, first_key, last_key);
  }

  // Verify the hash
  uint32_t stored_hash;
  memcpy(&stored_hash, data, sizeof(uint32_t));

  const uint8_t *data_start = meta.data() + sizeof(uint32_t);
  const uint8_t *data_end = data;
  uint32_t hash = std::hash<std::string_view>{}(
      std::string_view(reinterpret_cast<const char *>(data_start), data_end - data_start));
  if (hash != stored_hash) {
    throw std::runtime_error("Meta Hash mismatch");
  }

  return meta_entries;
}