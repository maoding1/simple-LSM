#include <gtest/gtest.h>
#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>
#include "skiplist/SkipList.h"
#include "type/KeyComparator.h"

// 测试基本插入、查找和删除
TEST(SkipListTest, BasicOperations) {
  KeyComparator<std::string> key_comparator;
  SkipList<std::string, std::string, KeyComparator<std::string>> skip_list(key_comparator);

  // 测试插入和查找
  skip_list.Put("key1", "value1");
  EXPECT_EQ(skip_list.Get("key1").value(), "value1");

  // 测试更新
  skip_list.Put("key1", "new_value");
  EXPECT_EQ(skip_list.Get("key1").value(), "new_value");

  // 测试删除
  skip_list.Remove("key1");
  EXPECT_FALSE(skip_list.Get("key1").has_value());
}

// 测试迭代器
TEST(SkipListTest, Iterator) {
  KeyComparator<std::string> key_comparator;
  SkipList<std::string, std::string, KeyComparator<std::string>> skip_list(key_comparator);
  skip_list.Put("key1", "value1");
  skip_list.Put("key2", "value2");
  skip_list.Put("key3", "value3");

  // 测试迭代器
  std::vector<std::pair<std::string, std::string>> result;
  for (auto it = skip_list.Begin(); it != skip_list.End(); ++it) {
    result.push_back(*it);
  }

  EXPECT_EQ(result.size(), 3);
  EXPECT_EQ(result[0].first, "key1");
  EXPECT_EQ(result[1].first, "key2");
  EXPECT_EQ(result[2].first, "key3");
}

// 测试大量数据插入和查找
TEST(SkipListTest, LargeScaleInsertAndGet) {
  KeyComparator<std::string> key_comparator;
  SkipList<std::string, std::string, KeyComparator<std::string>> skip_list(key_comparator);
  const int num_elements = 10000;

  // 插入大量数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    skip_list.Put(key, value);
  }

  // 验证插入的数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string expected_value = "value" + std::to_string(i);
    EXPECT_EQ(skip_list.Get(key).value(), expected_value);
  }
}

// 测试大量数据删除
TEST(SkipListTest, LargeScaleRemove) {
  KeyComparator<std::string> key_comparator;
  SkipList<std::string, std::string, KeyComparator<std::string>> skip_list(key_comparator);
  const int num_elements = 10000;

  // 插入大量数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    skip_list.Put(key, value);
  }

  // 删除所有数据
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    skip_list.Remove(key);
  }

  // 验证所有数据已被删除
  for (int i = 0; i < num_elements; ++i) {
    std::string key = "key" + std::to_string(i);
    EXPECT_FALSE(skip_list.Get(key).has_value());
  }
}

// 测试重复插入
TEST(SkipListTest, DuplicateInsert) {
  KeyComparator<std::string> key_comparator;
  SkipList<std::string, std::string, KeyComparator<std::string>> skip_list(key_comparator);

  // 重复插入相同的key
  skip_list.Put("key1", "value1");
  skip_list.Put("key1", "value2");
  skip_list.Put("key1", "value3");

  // 验证最后一次插入的值
  EXPECT_EQ(skip_list.Get("key1").value(), "value3");
}

// 测试空跳表
TEST(SkipListTest, EmptySkipList) {
  KeyComparator<std::string> key_comparator;
  SkipList<std::string, std::string, KeyComparator<std::string>> skip_list(key_comparator);

  // 验证空跳表的查找和删除
  EXPECT_FALSE(skip_list.Get("nonexistent_key").has_value());
  EXPECT_FALSE(skip_list.Remove("nonexistent_key"));  // 删除不存在的key
}

// 测试随机插入和删除
TEST(SkipListTest, RandomInsertAndRemove) {
  KeyComparator<std::string> key_comparator;
  SkipList<std::string, std::string, KeyComparator<std::string>> skip_list(key_comparator);
  std::unordered_set<std::string> keys;
  const int num_operations = 10000;

  for (int i = 0; i < num_operations; ++i) {
    std::string key = "key" + std::to_string(rand() % 1000);
    std::string value = "value" + std::to_string(rand() % 1000);

    if (keys.find(key) == keys.end()) {
      // 插入新key
      skip_list.Put(key, value);
      keys.insert(key);
    } else {
      // 删除已存在的key
      skip_list.Remove(key);
      keys.erase(key);
    }

    // 验证当前状态
    if (keys.find(key) != keys.end()) {
      EXPECT_EQ(skip_list.Get(key).value(), value);
    } else {
      EXPECT_FALSE(skip_list.Get(key).has_value());
    }
  }
}

// 测试内存大小跟踪
TEST(SkipListTest, MemorySizeTracking) {
  KeyComparator<std::string> key_comparator;
  SkipList<std::string, std::string, KeyComparator<std::string>> skip_list(key_comparator);

  // 插入数据
  skip_list.Put("key1", "value1");
  skip_list.Put("key2", "value2");

  // 验证内存大小
  size_t expected_size = sizeof("key1") - 1 + sizeof("value1") - 1 + sizeof("key2") - 1 + sizeof("value2") - 1;
  EXPECT_EQ(skip_list.UsedBytes(), expected_size);

  // 删除数据
  skip_list.Remove("key1");
  expected_size -= sizeof("key1") - 1 + sizeof("value1") - 1;
  EXPECT_EQ(skip_list.UsedBytes(), expected_size);

  skip_list.Clear();
  EXPECT_EQ(skip_list.UsedBytes(), 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}