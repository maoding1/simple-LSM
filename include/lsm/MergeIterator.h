#pragma once

#include <memoryTable/HeapIterator.h>

/** use MergeIterator to iterate LSMEngine
    MergeIterator contains 2 HeapIterators
    the mem_table_iter_ is constructed from memtable, which combines <key, value> pairs from Active SkipList and Frozen
   SkipList
    the sst_iter_ is constructed from All the SST we have
    */
class MergeIterator {
  using value_type = std::pair<std::string, std::string>;

 private:
  HeapIterator mem_table_iter_;
  HeapIterator sst_iter_;
  bool choose_mem_table_ = false;
  mutable std::shared_ptr<value_type> current_value_;
  void UpdateCurrent() const;
  // choose which iterator to use
  bool ChooseIter();
  // Skip the key in sst_iter_ which is the same as the key in mem_table_iter_
  void SkipSSTIter();

 public:
  MergeIterator() = default;
  MergeIterator(const HeapIterator &mem_table_iter, const HeapIterator &sst_iter);
  bool IsEnd() const;

  value_type operator*() const;
  MergeIterator &operator++();
  MergeIterator operator++(int) = delete;
  bool operator==(const MergeIterator &other) const;
  bool operator!=(const MergeIterator &other) const;
  value_type *operator->() const;
};
