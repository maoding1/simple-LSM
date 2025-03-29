#pragma once

#include <sst/SST.h>
class SSTIterator {
  friend class SST;

 private:
  std::shared_ptr<SST> sst_;
  size_t block_idx_;
  std::shared_ptr<BlockIterator> block_iter_;

 public:
  using value_type = std::pair<std::string, std::string>;

  explicit SSTIterator(std::shared_ptr<SST> sst);
  SSTIterator(std::shared_ptr<SST> sst, const std::string &key);

  void SeekFirst();
  void Seek(const std::string &key);
  bool IsEnd();
  bool IsValid() const;

  std::string GetKey();
  std::string GetValue();
  void SetBlockIdx(size_t block_idx);
  void SetBlockIter(std::shared_ptr<BlockIterator> block_iter);

  SSTIterator &operator++();
  SSTIterator operator++(int) = delete;
  bool operator==(const SSTIterator &other) const;
  bool operator!=(const SSTIterator &other) const;
  value_type operator*() const;
};