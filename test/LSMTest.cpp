#include <gtest/gtest.h>
#include <lsm/LSMEngine.h>
#include <utils/Macro.h>
#include <filesystem>
#include <random>
#include <string>
#include <unordered_map>

class LSMTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a temporary test directory
    test_dir_ = "test_lsm_data";
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
    std::filesystem::create_directory(test_dir_);
  }

  void TearDown() override {
    // Clean up test directory
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::string test_dir_;
};

// Test basic operations: Put, Get, Remove
TEST_F(LSMTest, BasicOperations) {
  LSM lsm(test_dir_);

  // Test Put and Get
  lsm.Put("key1", "value1");
  EXPECT_EQ(lsm.Get("key1").value(), "value1");

  // Test update
  lsm.Put("key1", "new_value");
  EXPECT_EQ(lsm.Get("key1").value(), "new_value");

  // Test Remove
  lsm.Remove("key1");
  EXPECT_FALSE(lsm.Get("key1").has_value());

  // Test non-existent key
  EXPECT_FALSE(lsm.Get("nonexistent").has_value());
}

// Test persistence across restarts
TEST_F(LSMTest, Persistence) {
  std::unordered_map<std::string, std::string> kvs;
  int num = 100000;
  {
    LSM lsm(test_dir_);
    for (int i = 0; i < num; ++i) {
      std::string key = "key" + std::to_string(i);
      std::string value = "value" + std::to_string(i);
      lsm.Put(key, value);
      kvs[key] = value;

      // 删除之前被10整除的key
      if (i % 10 == 0 && i != 0) {
        std::string del_key = "key" + std::to_string(i - 10);
        lsm.Remove(del_key);
        kvs.erase(del_key);
      }
    }
  }  // LSM destructor called here

  // Create new LSM instance
  LSM lsm(test_dir_);
  for (int i = 0; i < num; ++i) {
    std::string key = "key" + std::to_string(i);
    if (kvs.find(key) != kvs.end()) {
      EXPECT_EQ(lsm.Get(key).value(), kvs[key]);
    } else {
      EXPECT_FALSE(lsm.Get(key).has_value());
    }
  }
}

// Test large scale operations
TEST_F(LSMTest, LargeScaleOperations) {
  LSM lsm(test_dir_);
  std::vector<std::pair<std::string, std::string>> data;

  // Insert enough data to trigger multiple flushes
  for (int i = 0; i < 1000; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    lsm.Put(key, value);
    data.emplace_back(key, value);
  }

  // Verify all data
  for (const auto &[key, value] : data) {
    EXPECT_EQ(lsm.Get(key).value(), value);
  }
}

// Test iterator functionality
TEST_F(LSMTest, IteratorOperations) {
  LSM lsm(test_dir_);
  std::map<std::string, std::string> reference;

  // Insert data
  for (int i = 0; i < 100; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    lsm.Put(key, value);
    reference[key] = value;
  }

  // Test iterator
  auto it = lsm.Begin();
  auto ref_it = reference.begin();

  while (it != lsm.End() && ref_it != reference.end()) {
    EXPECT_EQ(it->first, ref_it->first);
    EXPECT_EQ(it->second, ref_it->second);
    ++it;
    ++ref_it;
  }

  EXPECT_EQ(it == lsm.End(), ref_it == reference.end());
}

// Test mixed operations
TEST_F(LSMTest, MixedOperations) {
  LSM lsm(test_dir_);
  std::map<std::string, std::string> reference;

  // Perform mixed operations
  lsm.Put("key1", "value1");
  reference["key1"] = "value1";

  lsm.Put("key2", "value2");
  reference["key2"] = "value2";

  lsm.Remove("key1");
  reference.erase("key1");

  lsm.Put("key3", "value3");
  reference["key3"] = "value3";

  // Verify final state
  for (const auto &[key, value] : reference) {
    EXPECT_EQ(lsm.Get(key).value(), value);
  }
  EXPECT_FALSE(lsm.Get("key1").has_value());
}

TEST_F(LSMTest, MonotonyPredicate) {
  LSM lsm(test_dir_);

  // Insert data
  for (int i = 0; i < 100; i++) {
    std::ostringstream oss_key;
    std::ostringstream oss_value;
    oss_key << "key" << std::setw(2) << std::setfill('0') << i;
    oss_value << "value" << std::setw(2) << std::setfill('0') << i;
    std::string key = oss_key.str();
    std::string value = oss_value.str();
    lsm.Put(key, value);
    if (i == 50) {
      // 主动刷一次盘
      lsm.Flush();
    }
  }

  // Define a predicate function
  auto predicate = [](const std::string &key) -> int {
    int key_num = std::stoi(key.substr(3));  // Extract the number from the key
    if (key_num < 20) {
      return 1;
    }
    if (key_num > 60) {
      return -1;
    }
    return 0;
  };

  // Call the method under test
  auto result = lsm.LSMItersMonotonyPredicate(predicate);

  // Check if the result is not empty
  ASSERT_TRUE(result.has_value());

  // Extract the iterators from the result
  auto [start, end] = result.value();

  // Verify the range of keys returned by the iterators
  std::set<std::string> expected_keys;
  for (int i = 20; i <= 60; i++) {
    std::ostringstream oss;
    oss << "key" << std::setw(2) << std::setfill('0') << i;
    expected_keys.insert(oss.str());
  }

  std::set<std::string> actual_keys;
  for (auto it = start; it != end; ++it) {
    actual_keys.insert(it->first);
  }

  EXPECT_EQ(actual_keys, expected_keys);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}