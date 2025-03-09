#include <block/Block.h>
#include <block/BlockIterator.h>
#include <gtest/gtest.h>
#include <utils/Macro.h>
#include <iomanip>
#include <memory>
#include <vector>

class BlockTest : public ::testing::Test {
 protected:
  // 预定义的编码数据
  std::vector<uint8_t> GetEncodedBlock() {
    /*
    Block layout (3 entries):
    Entry1: key="apple", value="red"
    Entry2: key="banana", value="yellow"
    Entry3: key="orange", value="orange"
    */
    std::vector<uint8_t> encoded = {
        // Data Section
        // Entry 1: "apple" -> "red"
        5, 0,                     // key_len = 5
        'a', 'p', 'p', 'l', 'e',  // key
        3, 0,                     // value_len = 3
        'r', 'e', 'd',            // value

        // Entry 2: "banana" -> "yellow"
        6, 0,                          // key_len = 6
        'b', 'a', 'n', 'a', 'n', 'a',  // key
        6, 0,                          // value_len = 6
        'y', 'e', 'l', 'l', 'o', 'w',  // value

        // Entry 3: "orange" -> "orange"
        6, 0,                          // key_len = 6
        'o', 'r', 'a', 'n', 'g', 'e',  // key
        6, 0,                          // value_len = 6
        'o', 'r', 'a', 'n', 'g', 'e',  // value

        // Offset Section (每个entry的起始位置)
        0, 0,   // offset[0] = 0
        12, 0,  // offset[1] = 12 (第二个entry的起始位置)
        28, 0,  // offset[2] = 24 (第三个entry的起始位置)

        // Num of elements
        3, 0  // num_elements = 3
    };
    return encoded;
  }
};

// 测试解码
TEST_F(BlockTest, DecodeTest) {
  auto encoded = GetEncodedBlock();
  auto block = Block::Decode(encoded);

  // 验证第一个key
  EXPECT_EQ(block->GetFirstKey(), "apple");

  // 验证所有key-value对
  EXPECT_EQ(block->FindValue("apple").value(), "red");
  EXPECT_EQ(block->FindValue("banana").value(), "yellow");
  EXPECT_EQ(block->FindValue("orange").value(), "orange");
}

// 测试编码
TEST_F(BlockTest, EncodeTest) {
  Block block(1024);
  block.AddEntry("apple", "red");
  block.AddEntry("banana", "yellow");
  block.AddEntry("orange", "orange");

  auto encoded = block.Encode();

  // 解码并验证
  auto decoded = Block::Decode(encoded);
  EXPECT_EQ(decoded->FindValue("apple").value(), "red");
  EXPECT_EQ(decoded->FindValue("banana").value(), "yellow");
  EXPECT_EQ(decoded->FindValue("orange").value(), "orange");
}

// 测试二分查找
TEST_F(BlockTest, BinarySearchTest) {
  Block block(1024);
  block.AddEntry("apple", "red");
  block.AddEntry("banana", "yellow");
  block.AddEntry("orange", "orange");

  // 测试存在的key
  EXPECT_EQ(block.FindValue("apple").value(), "red");
  EXPECT_EQ(block.FindValue("banana").value(), "yellow");
  EXPECT_EQ(block.FindValue("orange").value(), "orange");

  // 测试不存在的key
  EXPECT_FALSE(block.FindValue("grape").has_value());
  EXPECT_FALSE(block.FindValue("").has_value());
}

// 测试边界情况
TEST_F(BlockTest, EdgeCasesTest) {
  Block block(1024);

  // 空block
  EXPECT_EQ(block.GetFirstKey(), "");
  EXPECT_FALSE(block.FindValue("any").has_value());

  // 添加空key和value
  block.AddEntry("", "");
  EXPECT_EQ(block.GetFirstKey(), "");
  EXPECT_EQ(block.FindValue("").value(), "");

  // 添加包含特殊字符的key和value
  block.AddEntry(std::string("key\0with\tnull", 12), std::string("value\rwith\nnull", 15));
  std::string special_key("key\0with\tnull", 12);
  std::string special_value("value\rwith\nnull");
  EXPECT_EQ(block.FindValue(special_key).value(), special_value);
}

// 测试大数据量
TEST_F(BlockTest, LargeDataTest) {
  Block block(1024 * 32);
  const int n = 1000;

  // 添加大量数据
  for (int i = 0; i < n; i++) {
    // 使用 std::format 或 sprintf 进行补零
    char key_buf[16];
    snprintf(key_buf, sizeof(key_buf), "key%03d", i);  // 补零到3位
    std::string key = key_buf;

    char value_buf[16];
    snprintf(value_buf, sizeof(value_buf), "value%03d", i);
    std::string value = value_buf;

    block.AddEntry(key, value);
  }

  // 验证所有数据
  for (int i = 0; i < n; i++) {
    char key_buf[16];
    snprintf(key_buf, sizeof(key_buf), "key%03d", i);
    std::string key = key_buf;

    char value_buf[16];
    snprintf(value_buf, sizeof(value_buf), "value%03d", i);
    std::string expected_value = value_buf;

    EXPECT_EQ(block.FindValue(key).value(), expected_value);
  }
}

// 测试错误处理
TEST_F(BlockTest, ErrorHandlingTest) {
  // 测试解码无效数据
  std::vector<uint8_t> invalid_data = {1, 2, 3};  // 太短
  EXPECT_THROW(Block::Decode(invalid_data), std::runtime_error);

  // 测试空vector
  std::vector<uint8_t> empty_data;
  EXPECT_THROW(Block::Decode(empty_data), std::runtime_error);
}

// 测试迭代器
TEST_F(BlockTest, IteratorTest) {
  // 使用 make_shared 创建 Block
  auto block = std::make_shared<Block>(4096);

  // 1. 测试空block的迭代器
  EXPECT_EQ(block->begin(), block->end());

  // 2. 添加有序数据
  const int n = 100;
  std::vector<std::pair<std::string, std::string>> test_data;

  for (int i = 0; i < n; i++) {
    char key_buf[16];
    char value_buf[16];
    snprintf(key_buf, sizeof(key_buf), "key%03d", i);
    snprintf(value_buf, sizeof(value_buf), "value%03d", i);

    block->AddEntry(key_buf, value_buf);
    test_data.emplace_back(key_buf, value_buf);
  }

  // 3. 测试正向遍历和数据正确性
  size_t count = 0;
  for (const auto &[key, value] : *block) {  // 注意这里使用 *block
    EXPECT_EQ(key, test_data[count].first);
    EXPECT_EQ(value, test_data[count].second);
    count++;
  }
  EXPECT_EQ(count, test_data.size());

  // 4. 测试迭代器的比较和移动
  auto it = block->begin();
  EXPECT_EQ(it->first, "key000");
  ++it;
  EXPECT_EQ(it->first, "key001");
  ++it;
  EXPECT_EQ(it->first, "key002");

  // 5. 测试编码后的迭代
  auto encoded = block->Encode();
  auto decoded_block = Block::Decode(encoded);
  count = 0;
  for (auto it = decoded_block->begin(); it != decoded_block->end(); ++it) {
    EXPECT_EQ(it->first, test_data[count].first);
    EXPECT_EQ(it->second, test_data[count].second);
    count++;
  }
}

TEST_F(BlockTest, PredicateTest) {
  std::vector<uint8_t> encoded_p;
  {
    std::shared_ptr<Block> block1 = std::make_shared<Block>(LSM_BLOCK_SIZE);
    int num = 50;

    for (int i = 0; i < num; ++i) {
      std::ostringstream oss_key;
      std::ostringstream oss_value;

      // 设置数字为4位长度，不足的部分用前导零填充
      oss_key << "key" << std::setw(4) << std::setfill('0') << i;
      oss_value << "value" << std::setw(4) << std::setfill('0') << i;

      std::string key = oss_key.str();
      std::string value = oss_value.str();

      block1->AddEntry(key, value);
    }

    auto result = block1->GetMonotonyPredicateIters([](const std::string &key) {
      if (key < "key0020") {
        return 1;
      }
      if (key >= "key0030") {
        return -1;
      }
      return 0;
    });
    EXPECT_TRUE(result.has_value());
    auto [it_begin, it_end] = result.value();
    EXPECT_EQ((*it_begin).first, "key0020");
    EXPECT_EQ((*it_end).first, "key0030");
    for (int i = 0; i < 5; i++) {
      ++it_begin;
    }
    EXPECT_EQ((*it_begin).first, "key0025");

    encoded_p = block1->Encode();
  }
  std::shared_ptr<Block> block2 = Block::Decode(encoded_p);

  auto result = block2->GetMonotonyPredicateIters([](const std::string &key) {
    if (key < "key0020") {
      return 1;
    }
    if (key >= "key0030") {
      return -1;
    }
    return 0;
  });
  EXPECT_TRUE(result.has_value());
  auto [it_begin, it_end] = result.value();
  EXPECT_EQ((*it_begin).first, "key0020");
  EXPECT_EQ((*it_end).first, "key0030");
  for (int i = 0; i < 5; i++) {
    ++it_begin;
  }
  EXPECT_EQ((*it_begin).first, "key0025");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}