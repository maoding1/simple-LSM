cmake_minimum_required(VERSION 3.16.0)
project(SIMPLE-LSM VERSION 0.1.0 LANGUAGES C CXX)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(COMMON_FLAGS "-Wall -Wextra -Werror -Wno-unused-parameter -Wno-attributes")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${COMMON_FLAGS} -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${COMMON_FLAGS} -fno-limit-debug-info -O0")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-unused-parameter -Wno-attributes -fno-limit-debug-info")

include(FetchContent)
FetchContent_Declare(
  googletest
  # Specify the commit you depend on and update it regularly.
  URL https://github.com/google/googletest/archive/24a9e940d481f992ba852599c78bb2217362847b.zip
)
FetchContent_MakeAvailable(googletest)

# Output directory.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

include_directories(include)

file (GLOB_RECURSE SKIPLIST_SRC src/skiplist/*.cpp)
add_library(skiplist_lib STATIC ${SKIPLIST_SRC})

file (GLOB_RECURSE MEMORYTABLE_SRC src/memoryTable/*.cpp ${SKIPLIST_SRC})
add_library(memorytable_lib STATIC ${MEMORYTABLE_SRC})

file (GLOB_RECURSE BLOCK_SRC src/block/*.cpp ${MEMORYTABLE_SRC})
add_library(block_lib STATIC ${BLOCK_SRC})

file (GLOB_RECURSE SST_SRC src/sst/*.cpp ${BLOCK_SRC})
add_library(sst_lib STATIC ${SST_SRC})

# tests
add_executable(test_skipList test/SkipListTest.cpp)
target_link_libraries(test_skipList skiplist_lib GTest::gtest_main)

add_executable(test_MemoryTable test/MemoryTableTest.cpp)
target_link_libraries(test_MemoryTable memorytable_lib GTest::gtest_main)

add_executable(test_Block test/BlockTest.cpp)
target_link_libraries(test_Block block_lib GTest::gtest_main)

add_executable(test_BlockMeta test/BlockMetaTest.cpp)
target_link_libraries(test_BlockMeta block_lib GTest::gtest_main)

add_executable(test_Utils test/UtilsTest.cpp)
target_link_libraries(test_Utils GTest::gtest_main)

add_executable(test_SST test/SSTTest.cpp)
target_link_libraries(test_SST sst_lib GTest::gtest_main)


enable_testing()
add_test(NAME skiplist_test COMMAND test_skipList)
add_test(NAME memorytable_test COMMAND test_MemoryTable)
add_test(NAME block_test COMMAND test_Block)
add_test(NAME blockmeta_test COMMAND test_BlockMeta)
add_test(NAME utils_test COMMAND test_Utils)
add_test(NAME sst_test COMMAND test_SST)