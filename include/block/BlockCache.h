#include <block/Block.h>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
class CacheNode {
 private:
  int sst_id_;
  int block_id_;
  std::list<size_t> history_;
  size_t k_;  // k for lru_k algorithm
  std::shared_ptr<Block> block_;

 public:
  CacheNode(int sst_id, int block_id, int k) : sst_id_(sst_id), block_id_(block_id), k_(k) {}
  void AddTimeStamp(size_t time) {
    history_.push_back(time);
    if (history_.size() > k_) {
      history_.pop_front();
    }
  }
  void SetBlock(std::shared_ptr<Block> block) { block_ = std::move(block); }
  std::shared_ptr<Block> GetBlock() { return block_; }
  int GetSSTId() { return sst_id_; }
  int GetBlockId() { return block_id_; }
  std::list<size_t> &GetHistory() { return history_; }
};

struct PairHash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2> &p) const {
    auto hash1 = std::hash<T1>{}(p.first);
    auto hash2 = std::hash<T2>{}(p.second);
    return hash1 ^ (hash2 * 31);
  }
};

struct PairEqual {
  template <class T1, class T2>
  bool operator()(const std::pair<T1, T2> &lhs, const std::pair<T1, T2> &rhs) const {
    return lhs.first == rhs.first && lhs.second == rhs.second;
  }
};

class BlockCache {
 private:
  size_t capacity_;
  size_t k_;
  size_t timestamp_;
  mutable std::mutex mutex_;
  std::unordered_map<std::pair<int, int>, std::list<CacheNode>::iterator, PairHash, PairEqual> cache_map_;
  std::list<CacheNode> cold_list_;  // store CacheNode access less than k times
  std::list<CacheNode> hot_list_;   // store CacheNode access equal to or more than k times
  mutable size_t total_requests_;
  mutable size_t hit_requests_;
  void Evict();
  void RecordAccess(std::list<CacheNode>::iterator it);

 public:
  BlockCache(size_t capacity, size_t k)
      : capacity_(capacity), k_(k), timestamp_(0), total_requests_(0), hit_requests_(0) {}
  ~BlockCache() = default;
  std::shared_ptr<Block> Get(int sst_id, int block_id);
  void Put(int sst_id, int block_id, std::shared_ptr<Block> block);
  double GetHitRate() const;
};