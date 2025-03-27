#include <gtest/gtest.h>
#include <sst/SST.h>
#include <utils/Macro.h>
#include <filesystem>

class SSTTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // 创建测试目录
    if (!std::filesystem::exists("test_data")) {
      std::filesystem::create_directory("test_data");
    }
  }

  void TearDown() override {
    // 清理测试文件
    std::filesystem::remove_all("test_data");
  }

  // 辅助函数：创建一个包含有序数据的SST
  std::shared_ptr<SST> CreateTestSST(size_t block_size, size_t num_entries) {
    SSTBuilder builder(block_size);

    for (size_t i = 0; i < num_entries; i++) {
      std::string key = "key" + std::to_string(i);
      std::string value = "value" + std::to_string(i);
      builder.Add(key, value);
    }

    auto block_cache = std::make_shared<BlockCache>(BLOCK_CACHE_CAPACITY, BLOCK_CACHE_K);

    return builder.Build(1, "test_data/test.sst", block_cache);
  }
};

// 测试基本的写入和读取
TEST_F(SSTTest, BasicWriteAndRead) {
  SSTBuilder builder(1024);  // 1KB block size

  // 添加一些数据
  builder.Add("key1", "value1");
  builder.Add("key2", "value2");
  builder.Add("key3", "value3");

  // 构建SST
  auto block_cache = std::make_shared<BlockCache>(BLOCK_CACHE_CAPACITY, BLOCK_CACHE_K);
  auto sst = builder.Build(1, "test_data/basic.sst", block_cache);

  // 验证基本属性
  EXPECT_EQ(sst->GetFirstKey(), "key1");
  EXPECT_EQ(sst->GetLastKey(), "key3");
  EXPECT_EQ(sst->GetSSTId(), 1);
  EXPECT_GT(sst->GetSSTSize(), 0);

  // 读取并验证数据
  auto block = sst->ReadBlock(0);
  EXPECT_TRUE(block != nullptr);
  auto value = block->FindValue("key2");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "value2");
}

// 测试block分裂
TEST_F(SSTTest, BlockSplitting) {
  // 使用小的block size强制分裂
  SSTBuilder builder(64);  // 很小的block size

  // 添加足够多的数据以触发分裂
  for (int i = 0; i < 10; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = std::string(20, 'v') + std::to_string(i);  // 较大的value
    builder.Add(key, value);
  }

  auto block_cache = std::make_shared<BlockCache>(BLOCK_CACHE_CAPACITY, BLOCK_CACHE_K);
  auto sst = builder.Build(1, "test_data/split.sst", block_cache);

  // 验证有多个block
  EXPECT_GT(sst->NumBlocks(), 1);

  // 验证每个block都可以正确读取
  for (size_t i = 0; i < sst->NumBlocks(); i++) {
    auto block = sst->ReadBlock(i);
    EXPECT_TRUE(block != nullptr);
  }
}

// 测试key查找
TEST_F(SSTTest, KeySearch) {
  auto sst = CreateTestSST(256, 100);  // 创建包含100个entry的SST

  // 测试find_block_idx
  size_t idx = sst->FindBlockIndex("key50");
  auto block = sst->ReadBlock(idx);
  auto value = block->FindValue("key50");
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(*value, "value50");

  // 测试边界情况
  EXPECT_THROW(sst->FindBlockIndex("key999"), std::out_of_range);
}

// 测试元数据
TEST_F(SSTTest, Metadata) {
  auto sst = CreateTestSST(512, 10);

  // 验证block数量
  EXPECT_GT(sst->NumBlocks(), 0);

  // 验证首尾key
  EXPECT_EQ(sst->GetFirstKey(), "key0");
  EXPECT_EQ(sst->GetLastKey(), "key9");
}

// 测试空SST构建
TEST_F(SSTTest, EmptySST) {
  SSTBuilder builder(1024);
  auto block_cache = std::make_shared<BlockCache>(BLOCK_CACHE_CAPACITY, BLOCK_CACHE_K);
  EXPECT_THROW(builder.Build(1, "test_data/empty.sst", block_cache), std::runtime_error);
}

// 测试SST重新打开
TEST_F(SSTTest, ReopenSST) {
  // 首先创建一个SST
  auto sst = CreateTestSST(256, 10);

  // 重新打开SST
  auto block_cache = std::make_shared<BlockCache>(BLOCK_CACHE_CAPACITY, BLOCK_CACHE_K);
  FileObj file = FileObj::Open("test_data/test.sst");
  auto reopened_sst = SST::Open(1, std::move(file), block_cache);

  // 验证数据一致性
  EXPECT_EQ(sst->GetFirstKey(), reopened_sst->GetFirstKey());
  EXPECT_EQ(sst->GetLastKey(), reopened_sst->GetLastKey());
  EXPECT_EQ(sst->NumBlocks(), reopened_sst->NumBlocks());
}

// 测试大文件
TEST_F(SSTTest, LargeSST) {
  SSTBuilder builder(4096);  // 4KB blocks

  // 添加大量数据
  for (int i = 0; i < 1000; i++) {
    // 确保key长度一致：key000, key001, ..., key999
    std::string key = "key" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);

    // 确保value长度一致：添加前导零，然后用'v'填充到固定长度
    std::string value = std::string(100 - 3 - std::to_string(i).length(), 'v') +
                        std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);

    builder.Add(key, value);
  }

  auto block_cache = std::make_shared<BlockCache>(BLOCK_CACHE_CAPACITY, BLOCK_CACHE_K);
  auto sst = builder.Build(1, "test_data/large.sst", block_cache);

  // 验证数据完整性
  EXPECT_GT(sst->NumBlocks(), 1);
  EXPECT_EQ(sst->GetFirstKey(), "key000");
  EXPECT_EQ(sst->GetLastKey(), "key999");

  // 随机访问一些key
  std::vector<int> test_indices = {0, 100, 500, 999};
  for (int i : test_indices) {
    std::string key = "key" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
    size_t idx = sst->FindBlockIndex(key);
    auto block = sst->ReadBlock(idx);
    auto value = block->FindValue(key);
    EXPECT_TRUE(value.has_value());

    // 构造期望的value
    std::string expected_value = std::string(100 - 3 - std::to_string(i).length(), 'v') +
                                 std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
    EXPECT_EQ(*value, expected_value);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}