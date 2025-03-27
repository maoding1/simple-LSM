#include <block/Block.h>
#include <block/BlockCache.h>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

class BlockCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 初始化缓存池，容量为3，K值为2
    cache_ = std::make_unique<BlockCache>(3, 2);
  }

  std::unique_ptr<BlockCache> cache_;
};

TEST_F(BlockCacheTest, PutAndGet) {
  auto block1 = std::make_shared<Block>();
  auto block2 = std::make_shared<Block>();
  auto block3 = std::make_shared<Block>();

  cache_->Put(1, 1, block1);
  cache_->Put(1, 2, block2);
  cache_->Put(1, 3, block3);

  EXPECT_EQ(cache_->Get(1, 1), block1);
  EXPECT_EQ(cache_->Get(1, 2), block2);
  EXPECT_EQ(cache_->Get(1, 3), block3);
}

TEST_F(BlockCacheTest, CacheEviction1) {
  auto block1 = std::make_shared<Block>();
  auto block2 = std::make_shared<Block>();
  auto block3 = std::make_shared<Block>();
  auto block4 = std::make_shared<Block>();

  cache_->Put(1, 1, block1);
  cache_->Put(1, 2, block2);
  cache_->Put(1, 3, block3);

  // 访问 block1 和 block2
  cache_->Get(1, 1);
  cache_->Get(1, 2);

  // 插入 block4，应该驱逐 block3
  cache_->Put(1, 4, block4);

  EXPECT_EQ(cache_->Get(1, 1), block1);
  EXPECT_EQ(cache_->Get(1, 2), block2);
  EXPECT_EQ(cache_->Get(1, 3), nullptr);  // block3 被驱逐
  EXPECT_EQ(cache_->Get(1, 4), block4);
}

TEST_F(BlockCacheTest, CacheEviction2) {
  auto block1 = std::make_shared<Block>();
  auto block2 = std::make_shared<Block>();
  auto block3 = std::make_shared<Block>();
  auto block4 = std::make_shared<Block>();

  cache_->Put(1, 1, block1);
  cache_->Put(1, 2, block2);
  cache_->Put(1, 3, block3);

  // 访问 block1 和 block2
  cache_->Get(1, 1);
  cache_->Get(1, 2);
  cache_->Get(1, 3);

  // 插入 block4，应该驱逐 block3
  cache_->Put(1, 4, block4);

  EXPECT_EQ(cache_->Get(1, 1), nullptr);  // block1 被驱逐
  EXPECT_EQ(cache_->Get(1, 2), block2);
  EXPECT_EQ(cache_->Get(1, 3), block3);
  EXPECT_EQ(cache_->Get(1, 4), block4);
}

TEST_F(BlockCacheTest, CacheEviction3) {
  auto cache = std::make_unique<BlockCache>(5, 2);

  auto block1 = std::make_shared<Block>();
  auto block2 = std::make_shared<Block>();
  auto block3 = std::make_shared<Block>();
  auto block4 = std::make_shared<Block>();
  auto block5 = std::make_shared<Block>();
  auto block6 = std::make_shared<Block>();

  cache->Put(1, 1, block1);
  cache->Put(1, 2, block2);
  cache->Put(1, 3, block3);
  cache->Put(1, 4, block4);
  cache->Put(1, 5, block5);

  cache->Get(1, 1);
  cache->Put(1, 6, block6);

  EXPECT_EQ(cache->Get(1, 1), block1);
  EXPECT_EQ(cache->Get(1, 2), nullptr);  // block2 被驱逐
}

TEST_F(BlockCacheTest, HitRate) {
  auto block1 = std::make_shared<Block>();
  auto block2 = std::make_shared<Block>();

  cache_->Put(1, 1, block1);
  cache_->Put(1, 2, block2);

  // 访问 block1 和 block2
  cache_->Get(1, 1);
  cache_->Get(1, 2);

  // 访问不存在的 block3
  cache_->Get(1, 3);

  EXPECT_EQ(cache_->GetHitRate(), 2.0 / 3.0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}