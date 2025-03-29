#include <skiplist/SkipList.h>
#include <type/KeyComparator.h>
#include <type/Size.h>
// **************** SkipList ****************
SKIPLIST_TEMPLATE_ARGUMENTS
SKIPLIST_TYPE::SkipList(const KeyComparator &comparator, int maxLevel)
    : max_level_(maxLevel), level_(0), used_bytes_(0), comp_(std::move(comparator)) {
  head_ = std::make_shared<SkipListNode<K, V>>(max_level_ + 1);
  tail_ = std::make_shared<SkipListNode<K, V>>(max_level_ + 1);
  tail_key_ = comp_.MaxValue();
  tail_->key_ = tail_key_;
  for (int i = 0; i <= max_level_; i++) {
    head_->SetForward(i, tail_);
    tail_->SetBackward(i, head_);
  }
  last_.resize(max_level_ + 1, head_);

  std::random_device rd;
  gen_ = std::mt19937(rd());
  dist_01_ = std::uniform_int_distribution<int>(0, 1);
  dist_level_ = std::uniform_int_distribution<int>(0, (1 << max_level_) - 1);
};

SKIPLIST_TEMPLATE_ARGUMENTS
std::optional<V> SKIPLIST_TYPE::Get(const K &key) {
  if (comp_(key, tail_key_) >= 0) {
    return std::nullopt;
  }
  auto p = head_;
  for (int i = level_; i >= 0; i--) {
    while (comp_(p->forward_[i]->key_, key) < 0) {
      p = p->forward_[i];
    }
  }
  p = p->forward_[0];
  if (comp_(p->key_, key) == 0) {
    return p->value_;
  }
  return std::nullopt;
}

SKIPLIST_TEMPLATE_ARGUMENTS
std::optional<std::shared_ptr<SkipListNode<K, V>>> SKIPLIST_TYPE::InternalSearch(const K &key) {
  if (comp_(key, tail_key_) >= 0) {
    return std::nullopt;
  }
  auto p = head_;
  for (int i = level_; i >= 0; i--) {
    while (comp_(p->forward_[i]->key_, key) < 0) {
      p = p->forward_[i];
    }
    last_[i] = p;
  }
  return p->forward_[0];
}

SKIPLIST_TEMPLATE_ARGUMENTS
int SKIPLIST_TYPE::RandomLevel() {
  int level = 0;
  while ((dist_01_(gen_) != 0) && level < max_level_) {
    level++;
  }
  return level;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Put(const K &key, const V &value) {
  if (comp_(key, tail_key_) >= 0) {
    return false;
  }
  auto p = InternalSearch(key);
  if (p == std::nullopt) {
    // return nullopt means the key is bigger or equal to tail_key_ which is invalid
    return false;
  }
  if (comp_((*p)->key_, key) == 0) {
    // if exists, update value and return
    (*p)->value_ = value;
    return true;
  }
  // insert new node
  int new_level = RandomLevel();
  if (new_level > level_) {
    new_level = level_ + 1;
    last_[new_level] = head_;
  }
  auto new_node = std::make_shared<SkipListNode<K, V>>(new_level + 1);
  new_node->key_ = key;
  new_node->value_ = value;
  used_bytes_ += GetSize(key) + GetSize(value);

  // randomly update the pointers in each level
  int random_bits = dist_level_(gen_);
  for (int i = 0; i <= new_level; i++) {
    bool need_update = false;
    /** 1. always update the 0 level */
    /** 2. if the new level is bigger than the current level, update all levels */
    /** 3. randomly update the pointers in each level with 50% probability*/
    if (i == 0 || (new_level > level_) || ((random_bits & (1 << i)) != 0)) {
      need_update = true;
    }

    if (need_update) {
      new_node->SetForward(i, last_[i]->forward_[i]);
      new_node->forward_[i]->SetBackward(i, new_node);
      last_[i]->SetForward(i, new_node);
      new_node->SetBackward(i, last_[i]);
    } else {
      // if not to update current level, higher level will not be updated
      break;
    }
  }

  if (new_level > level_) {
    level_ = new_level;
  }
  return true;
}

SKIPLIST_TEMPLATE_ARGUMENTS
bool SKIPLIST_TYPE::Remove(const K &key) {
  if (comp_(key, tail_key_) >= 0) {
    return false;
  }
  auto p = InternalSearch(key);
  if (p == std::nullopt) {
    return false;
  }
  if (comp_((*p)->key_, key) != 0) {
    return false;
  }
  for (int i = 0; i <= level_ && last_[i]->forward_[i] == p; i++) {
    last_[i]->SetForward(i, (*p)->forward_[i]);
    (*p)->forward_[i]->SetBackward(i, last_[i]);
  }
  while (level_ > 0 && head_->forward_[level_] == tail_) {
    level_--;
  }
  used_bytes_ -= GetSize(key) + GetSize((*p)->value_);
  return true;
}

SKIPLIST_TEMPLATE_ARGUMENTS
std::vector<std::pair<K, V>> SKIPLIST_TYPE::Dump() {
  std::vector<std::pair<K, V>> result;
  auto p = head_->forward_[0];
  while (p != tail_) {
    result.emplace_back(p->key_, p->value_);
    p = p->forward_[0];
  }
  return result;
}

SKIPLIST_TEMPLATE_ARGUMENTS
void SKIPLIST_TYPE::Clear() {
  for (int i = 0; i <= level_; i++) {
    head_->SetForward(i, tail_);
    tail_->SetBackward(i, head_);
  }
  level_ = 0;
  used_bytes_ = 0;
}

SKIPLIST_TEMPLATE_ARGUMENTS
std::optional<std::pair<SKIPLIST_ITERATOR_TYPE, SKIPLIST_ITERATOR_TYPE>> SKIPLIST_TYPE::ItersMonotonyPredicate(
    std::function<int(const K &)> predicate) {
  auto current = head_;
  Iterator begin = Iterator(nullptr);
  Iterator end = Iterator(nullptr);

  bool find = false;
  for (int i = level_; i >= 0; i--) {
    while (!find) {
      auto forward = current->forward_[i];
      if (forward == tail_) {
        break;
      }
      auto direction = predicate(forward->key_);
      if (direction == 0) {
        find = true;
        current = forward;
        break;
      }
      if (direction < 0) {
        break;
      }
      current = forward;
    }
  }
  if (!find) {
    return std::nullopt;
  }

  auto tmp = current;
  for (int i = current->backward_.size() - 1; i >= 0; i--) {
    while (true) {
      if (current->backward_[i].lock() == nullptr || current->backward_[i].lock() == head_) {
        break;
      }
      auto direction = predicate(current->backward_[i].lock()->key_);
      if (direction == 0) {
        current = current->backward_[i].lock();
        continue;
      }
      if (direction > 0) {
        break;
      }
      throw std::runtime_error("Monotony predicate: Wrong direction");
    }
  }

  begin = Iterator(current);
  current = tmp;
  for (int i = current->forward_.size() - 1; i >= 0; i--) {
    while (true) {
      if (current->forward_[i] == tail_) {
        break;
      }
      auto direction = predicate(current->forward_[i]->key_);
      if (direction == 0) {
        current = current->forward_[i];
        continue;
      }
      if (direction < 0) {
        break;
      }
      throw std::runtime_error("Monotony predicate: Wrong direction");
    }
  }
  end = Iterator(current);
  ++end;
  return std::make_pair(begin, end);
}

// instantiate all the templates we need
template class SkipList<std::string, std::string, KeyComparator<std::string>>;