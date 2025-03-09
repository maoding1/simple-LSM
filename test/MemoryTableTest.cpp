#include <gtest/gtest.h>
#include <memoryTable/MemoryTable.h>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// 测试基本的插入和查询操作
TEST(MemTableTest, BasicOperations) {
  MemoryTable memtable;

  // 测试插入和查找
  memtable.Put("key1", "value1");
  EXPECT_EQ(memtable.Get("key1").value(), "value1");

  // 测试更新
  memtable.Put("key1", "new_value");
  EXPECT_EQ(memtable.Get("key1").value(), "new_value");

  // 测试不存在的key
  EXPECT_FALSE(memtable.Get("nonexistent").has_value());
}

// 测试删除操作
TEST(MemTableTest, RemoveOperations) {
  MemoryTable memtable;

  // 插入并删除
  memtable.Put("key1", "value1");
  memtable.Remove("key1");
  EXPECT_TRUE(memtable.Get("key1").value().empty());

  // 删除不存在的key
  memtable.Remove("nonexistent");
  EXPECT_TRUE(memtable.Get("nonexistent").value().empty());
}

// 测试冻结表操作
TEST(MemTableTest, FrozenTableOperations) {
  MemoryTable memtable;

  // 在当前表中插入数据
  memtable.Put("key1", "value1");
  memtable.Put("key2", "value2");

  // 冻结当前表
  memtable.FrozenCurrentTable();

  // 在新的当前表中插入数据
  memtable.Put("key3", "value3");

  // 验证所有数据都能被访问到
  EXPECT_EQ(memtable.Get("key1").value(), "value1");
  EXPECT_EQ(memtable.Get("key2").value(), "value2");
  EXPECT_EQ(memtable.Get("key3").value(), "value3");
}

// 测试大量数据操作
TEST(MemTableTest, LargeScaleOperations) {
  MemoryTable memtable;
  const int num_entries = 1000;

  // 插入大量数据
  for (int i = 0; i < num_entries; i++) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    memtable.Put(key, value);
  }

  // 验证数据
  for (int i = 0; i < num_entries; i++) {
    std::string key = "key" + std::to_string(i);
    std::string expected = "value" + std::to_string(i);
    EXPECT_EQ(memtable.Get(key).value(), expected);
  }
}

// 测试内存大小跟踪
TEST(MemTableTest, MemorySizeTracking) {
  MemoryTable memtable;

  // 初始大小应该为0
  EXPECT_EQ(memtable.GetTotalSize(), 0);

  // 添加数据后大小应该增加
  memtable.Put("key1", "value1");
  EXPECT_GT(memtable.GetCurSize(), 0);

  // 冻结表后，frozen_size应该增加
  size_t size_before_freeze = memtable.GetTotalSize();
  memtable.FrozenCurrentTable();
  EXPECT_EQ(memtable.GetFrozenSize(), size_before_freeze);
}

// 测试多次冻结表操作
TEST(MemTableTest, MultipleFrozenTables) {
  MemoryTable memtable;

  // 第一次冻结
  memtable.Put("key1", "value1");
  memtable.FrozenCurrentTable();

  // 第二次冻结
  memtable.Put("key2", "value2");
  memtable.FrozenCurrentTable();

  // 在当前表中添加数据
  memtable.Put("key3", "value3");

  // 验证所有数据都能访问
  EXPECT_EQ(memtable.Get("key1").value(), "value1");
  EXPECT_EQ(memtable.Get("key2").value(), "value2");
  EXPECT_EQ(memtable.Get("key3").value(), "value3");
}

// 测试迭代器在复杂操作序列下的行为
TEST(MemTableTest, IteratorComplexOperations) {
  MemoryTable memtable;

  // 第一批操作：基本插入
  memtable.Put("key1", "value1");
  memtable.Put("key2", "value2");
  memtable.Put("key3", "value3");

  // 验证第一批操作
  std::vector<std::pair<std::string, std::string>> result1;
  for (auto it = memtable.Begin(); it != memtable.End(); ++it) {
    result1.push_back(*it);
  }
  ASSERT_EQ(result1.size(), 3);
  EXPECT_EQ(result1[0].first, "key1");
  EXPECT_EQ(result1[0].second, "value1");
  EXPECT_EQ(result1[2].second, "value3");

  // 冻结当前表
  memtable.FrozenCurrentTable();

  // 第二批操作：更新和删除
  memtable.Put("key2", "value2_updated");  // 更新已存在的key
  memtable.Remove("key1");                 // 删除一个key
  memtable.Put("key4", "value4");          // 插入新key

  // 验证第二批操作
  std::vector<std::pair<std::string, std::string>> result2;
  for (auto it = memtable.Begin(); it != memtable.End(); ++it) {
    result2.push_back(*it);
  }
  ASSERT_EQ(result2.size(), 3);  // key1被删除，key4被添加
  EXPECT_EQ(result2[0].first, "key2");
  EXPECT_EQ(result2[0].second, "value2_updated");
  EXPECT_EQ(result2[2].first, "key4");

  // 再次冻结当前表
  memtable.FrozenCurrentTable();

  // 第三批操作：混合操作
  memtable.Put("key1", "value1_new");    // 重新插入被删除的key
  memtable.Remove("key3");               // 删除一个在第一个frozen table中的key
  memtable.Put("key2", "value2_final");  // 再次更新key2
  memtable.Put("key5", "value5");        // 插入新key

  // 验证最终结果
  std::vector<std::pair<std::string, std::string>> final_result;
  for (auto it = memtable.Begin(); it != memtable.End(); ++it) {
    final_result.push_back(*it);
  }

  // 验证最终状态
  ASSERT_EQ(final_result.size(), 4);  // key1, key2, key4, key5

  // 验证具体内容
  EXPECT_EQ(final_result[0].first, "key1");
  EXPECT_EQ(final_result[0].second, "value1_new");

  EXPECT_EQ(final_result[1].first, "key2");
  EXPECT_EQ(final_result[1].second, "value2_final");

  EXPECT_EQ(final_result[2].first, "key4");
  EXPECT_EQ(final_result[2].second, "value4");

  EXPECT_EQ(final_result[3].first, "key5");
  EXPECT_EQ(final_result[3].second, "value5");

  // 验证被删除的key确实不存在
  auto res = memtable.Get("key3");
  EXPECT_TRUE(res.value().empty());
}

TEST(MemTableTest, ConcurrentOperations) {
  MemoryTable memtable;
  const int num_readers = 4;        // 读线程数
  const int num_writers = 2;        // 写线程数
  const int num_operations = 1000;  // 每个线程的操作数

  // 用于同步所有线程的开始
  std::atomic<bool> start{false};
  // 用于等待所有线程完成
  std::atomic<int> completion_counter{num_readers + num_writers + 1};  // +1 for freeze thread

  // 记录写入的键，用于验证
  std::vector<std::string> inserted_keys;
  std::mutex keys_mutex;

  // 写线程函数
  auto writer_func = [&](int thread_id) {
    while (!start) {
      std::this_thread::yield();
    }

    for (int i = 0; i < num_operations; ++i) {
      std::string key = "key_" + std::to_string(thread_id) + "_" + std::to_string(i);
      std::string value = "value_" + std::to_string(thread_id) + "_" + std::to_string(i);

      if (i % 3 == 0) {
        // 插入操作
        memtable.Put(key, value);
        {
          std::lock_guard<std::mutex> lock(keys_mutex);
          inserted_keys.push_back(key);
        }
      } else if (i % 3 == 1) {
        // 删除操作
        memtable.Remove(key);
      } else {
        // 更新操作
        memtable.Put(key, value + "_updated");
      }

      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100));
    }

    completion_counter--;
  };

  // 读线程函数
  auto reader_func = [&](int thread_id) {
    while (!start) {
      std::this_thread::yield();
    }

    // int found_count = 0;
    for (int i = 0; i < num_operations; ++i) {
      // 随机选择一个已插入的key进行查询
      std::string key_to_find;
      {
        std::lock_guard<std::mutex> lock(keys_mutex);
        if (!inserted_keys.empty()) {
          key_to_find = inserted_keys[rand() % inserted_keys.size()];
        }
      }

      if (!key_to_find.empty()) {
        auto result = memtable.Get(key_to_find);
        if (result.has_value()) {
          // found_count++;
        }
      }

      // 每隔一段时间进行一次遍历操作
      if (i % 100 == 0) {
        std::vector<std::pair<std::string, std::string>> items;
        for (auto it = memtable.Begin(); it != memtable.End(); ++it) {
          items.push_back(*it);
        }
      }

      std::this_thread::sleep_for(std::chrono::microseconds(rand() % 50));
    }

    completion_counter--;
  };

  // 冻结线程函数
  auto freeze_func = [&]() {
    while (!start) {
      std::this_thread::yield();
    }

    // 定期执行冻结操作
    for (int i = 0; i < 5; ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      memtable.FrozenCurrentTable();

      // 验证冻结后的表
      size_t frozen_size = memtable.GetFrozenSize();
      EXPECT_GE(frozen_size, 0);

      // 验证总大小
      size_t total_size = memtable.GetTotalSize();
      EXPECT_GE(total_size, frozen_size);
    }

    completion_counter--;
  };

  // 创建并启动写线程
  std::vector<std::thread> writers;
  writers.reserve(num_writers);
  for (int i = 0; i < num_writers; ++i) {
    writers.emplace_back(writer_func, i);
  }

  // 创建并启动读线程
  std::vector<std::thread> readers;
  readers.reserve(num_readers);
  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back(reader_func, i);
  }

  // 创建并启动冻结线程
  std::thread freeze_thread(freeze_func);

  // 给线程一点时间进入等待状态
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // 记录开始时间
  auto start_time = std::chrono::high_resolution_clock::now();

  // 发送开始信号
  start = true;

  // 等待所有线程完成
  while (completion_counter > 0) {
    std::this_thread::yield();
  }

  // 记录结束时间
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  // 等待所有线程结束
  for (auto &w : writers) {
    w.join();
  }
  for (auto &r : readers) {
    r.join();
  }
  freeze_thread.join();

  // 验证最终状态
  size_t final_size = 0;
  for (auto it = memtable.Begin(); it != memtable.End(); ++it) {
    final_size++;
  }

  // 输出统计信息
  std::cout << "Concurrent test completed in " << duration.count() << "ms\nFinal memtable size: " << final_size
            << "\nTotal size: " << memtable.GetTotalSize() << "\nFrozen size: " << memtable.GetFrozenSize()
            << std::endl;

  // 基本正确性检查
  EXPECT_GT(memtable.GetTotalSize(), 0);                // 总大小应该大于0
  EXPECT_LE(final_size, num_writers * num_operations);  // 大小不应超过最大可能值
}

TEST(MemTableTest, PreffixIter) {
  MemoryTable memtable;

  // 在当前表中插入数据
  memtable.Put("abc", "3");
  memtable.Put("abcde", "5");
  memtable.Put("abcd", "4");
  memtable.Put("xxx", "-1");
  memtable.Put("abcdef", "6");
  memtable.Put("yyyy", "-1");

  // 冻结当前表
  memtable.FrozenCurrentTable();

  // 在新的当前表中插入数据
  memtable.Put("zz", "-1");
  memtable.Put("abcdefg", "7");
  memtable.Remove("abcd");
  memtable.Put("abcdefgh", "8");
  memtable.Put("ab", "2");
  memtable.Put("wwwwww", "-1");

  // 冻结当前表
  memtable.FrozenCurrentTable();

  // 在新的当前表中插入数据
  memtable.Put("mmmmm", "-1");
  memtable.Remove("ab");
  memtable.Put("abc", "33");

  int id = 0;
  std::vector<std::pair<std::string, std::string>> answer{
      {"abc", "33"}, {"abcde", "5"}, {"abcdef", "6"}, {"abcdefg", "7"}, {"abcdefgh", "8"}};

  for (auto it = memtable.ItersPreffix("ab"); !it.IsEnd(); ++it) {
    EXPECT_EQ(it->first, answer[id].first);
    EXPECT_EQ(it->second, answer[id].second);
    id++;
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}