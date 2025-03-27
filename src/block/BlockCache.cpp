#include <block/BlockCache.h>

#include <stdexcept>
#include <utility>

std::shared_ptr<Block> BlockCache::Get(int sst_id, int block_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++total_requests_;
  auto key = std::make_pair(sst_id, block_id);
  auto it = cache_map_.find(key);
  if (it == cache_map_.end()) {
    return nullptr;
  }

  ++hit_requests_;
  auto node = it->second;
  auto res = node->GetBlock();
  RecordAccess(node);
  return res;
}

void BlockCache::Put(int sst_id, int block_id, std::shared_ptr<Block> block) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto key = std::make_pair(sst_id, block_id);
  auto it = cache_map_.find(key);
  if (it != cache_map_.end()) {
    return;
  }

  if (cache_map_.size() >= capacity_) {
    Evict();
  }

  auto node = cold_list_.emplace(cold_list_.end(), sst_id, block_id, k_);
  node->SetBlock(std::move(block));
  cache_map_[key] = node;
  RecordAccess(node);
}

double BlockCache::GetHitRate() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_requests_ == 0 ? 0 : static_cast<double>(hit_requests_) / total_requests_;
}

void BlockCache::Evict() {
  if (!cold_list_.empty()) {
    auto node = cold_list_.front();
    cache_map_.erase(std::make_pair(node.GetSSTId(), node.GetBlockId()));
    cold_list_.pop_front();
    return;
  }
  if (hot_list_.empty()) {
    throw std::runtime_error("no cached_block to evict");
  }
  auto node = hot_list_.front();
  cache_map_.erase(std::make_pair(node.GetSSTId(), node.GetBlockId()));
  hot_list_.pop_front();
}

void BlockCache::RecordAccess(std::list<CacheNode>::iterator it) {
  if (it->GetHistory().size() == k_ - 1) {
    it->AddTimeStamp(timestamp_++);
    auto cache_node = *it;
    auto hot_list_it = hot_list_.begin();
    while (hot_list_it != hot_list_.end() && hot_list_it->GetHistory().front() < cache_node.GetHistory().front()) {
      hot_list_it++;
    }
    auto new_it = hot_list_.insert(hot_list_it, cache_node);
    cold_list_.erase(it);
    cache_map_[std::make_pair(cache_node.GetSSTId(), cache_node.GetBlockId())] = new_it;
  } else if (it->GetHistory().size() == k_) {
    it->AddTimeStamp(timestamp_++);
    auto cache_node = *it;
    hot_list_.erase(it);
    auto hot_list_it = hot_list_.begin();
    while (hot_list_it != hot_list_.end() && hot_list_it->GetHistory().front() < cache_node.GetHistory().front()) {
      hot_list_it++;
    }
    auto new_it = hot_list_.insert(hot_list_it, cache_node);
    cache_map_[std::make_pair(cache_node.GetSSTId(), cache_node.GetBlockId())] = new_it;
  } else {
    it->AddTimeStamp(timestamp_++);
    cold_list_.splice(cold_list_.end(), cold_list_, it);
    cache_map_[std::make_pair(it->GetSSTId(), it->GetBlockId())] = it;
  }
}
