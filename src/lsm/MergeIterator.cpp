#include <lsm/MergeIterator.h>

MergeIterator::MergeIterator(const HeapIterator &mem_table_iter, const HeapIterator &sst_iter)
    : mem_table_iter_(mem_table_iter), sst_iter_(sst_iter) {
  SkipSSTIter();
  choose_mem_table_ = ChooseIter();
}

bool MergeIterator::ChooseIter() {
  if (mem_table_iter_.IsEnd()) {
    return false;
  }
  if (sst_iter_.IsEnd()) {
    return true;
  }
  return mem_table_iter_->first < sst_iter_->first;
}

void MergeIterator::SkipSSTIter() {
  while (!mem_table_iter_.IsEnd() && !sst_iter_.IsEnd() && sst_iter_->first == mem_table_iter_->first) {
    ++sst_iter_;
  }
}

bool MergeIterator::IsEnd() const { return mem_table_iter_.IsEnd() && sst_iter_.IsEnd(); }

MergeIterator::value_type MergeIterator::operator*() const { return choose_mem_table_ ? *mem_table_iter_ : *sst_iter_; }

MergeIterator &MergeIterator::operator++() {
  if (choose_mem_table_) {
    ++mem_table_iter_;
  } else {
    ++sst_iter_;
  }
  SkipSSTIter();
  choose_mem_table_ = ChooseIter();
  return *this;
}

bool MergeIterator::operator==(const MergeIterator &other) const {
  if (this->IsEnd() && other.IsEnd()) {
    return true;
  }
  return this->mem_table_iter_ == other.mem_table_iter_ && this->sst_iter_ == other.sst_iter_ &&
         this->choose_mem_table_ == other.choose_mem_table_;
}

bool MergeIterator::operator!=(const MergeIterator &other) const { return !(*this == other); }

MergeIterator::value_type *MergeIterator::operator->() const {
  UpdateCurrent();
  return current_value_.get();
}

void MergeIterator::UpdateCurrent() const {
  if (choose_mem_table_) {
    current_value_ = std::make_shared<value_type>(*mem_table_iter_);
  } else {
    current_value_ = std::make_shared<value_type>(*sst_iter_);
  }
}