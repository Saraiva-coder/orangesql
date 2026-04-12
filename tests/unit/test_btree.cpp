// tests/unit/test_btree.cpp
#include <gtest/gtest.h>
#include "../../index/btree.h"
#include "../../include/logger.h"
#include <random>
#include <chrono>

using namespace orangesql;

class BTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        Logger::getInstance().setLevel(LogLevel::ERROR);
    }
    
    void TearDown() override {
    }
};

TEST_F(BTreeTest, InsertAndSearchInt) {
    BTree<int64_t> btree;
    
    btree.insert(100, 1);
    btree.insert(200, 2);
    btree.insert(50, 3);
    btree.insert(150, 4);
    btree.insert(75, 5);
    
    std::vector<uint64_t> results;
    btree.search(100, results);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], 1);
    
    results.clear();
    btree.search(50, results);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], 3);
    
    results.clear();
    btree.search(300, results);
    ASSERT_EQ(results.size(), 0);
}

TEST_F(BTreeTest, InsertAndSearchString) {
    BTree<std::string> btree;
    
    btree.insert("apple", 1);
    btree.insert("banana", 2);
    btree.insert("cherry", 3);
    btree.insert("date", 4);
    
    std::vector<uint64_t> results;
    btree.search("banana", results);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], 2);
    
    results.clear();
    btree.search("grape", results);
    ASSERT_EQ(results.size(), 0);
}

TEST_F(BTreeTest, RangeSearchInt) {
    BTree<int64_t> btree;
    
    for (int i = 0; i < 1000; i++) {
        btree.insert(i, i);
    }
    
    std::vector<uint64_t> results;
    btree.searchRange(100, 200, results);
    
    ASSERT_EQ(results.size(), 101);
    for (size_t i = 0; i < results.size(); i++) {
        ASSERT_EQ(results[i], 100 + i);
    }
}

TEST_F(BTreeTest, RangeSearchString) {
    BTree<std::string> btree;
    
    std::vector<std::string> words = {"apple", "apricot", "banana", "blueberry", 
                                       "cherry", "cranberry", "date", "fig", "grape"};
    for (size_t i = 0; i < words.size(); i++) {
        btree.insert(words[i], i);
    }
    
    std::vector<uint64_t> results;
    btree.searchRange("banana", "cherry", results);
    
    ASSERT_EQ(results.size(), 3);
}

TEST_F(BTreeTest, BulkLoad) {
    BTree<int64_t> btree;
    std::vector<std::pair<int64_t, uint64_t>> data;
    
    for (int i = 0; i < 10000; i++) {
        data.push_back({i, i});
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    btree.bulkLoad(data);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Bulk load 10000 items: " << duration.count() << "ms" << std::endl;
    
    std::vector<uint64_t> results;
    btree.search(5000, results);
    ASSERT_EQ(results.size(), 1);
    ASSERT_EQ(results[0], 5000);
    
    ASSERT_EQ(btree.size(), 10000);
}

TEST_F(BTreeTest, DuplicateKeys) {
    BTree<int64_t> btree;
    
    btree.insert(100, 1);
    btree.insert(100, 2);
    btree.insert(100, 3);
    
    std::vector<uint64_t> results;
    btree.search(100, results);
    
    ASSERT_EQ(results.size(), 3);
    ASSERT_EQ(results[0], 1);
    ASSERT_EQ(results[1], 2);
    ASSERT_EQ(results[2], 3);
}

TEST_F(BTreeTest, LargeDataSet) {
    BTree<int64_t> btree;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(1, 1000000);
    
    std::vector<int64_t> keys;
    for (int i = 0; i < 50000; i++) {
        int64_t key = dist(gen);
        keys.push_back(key);
        btree.insert(key, i);
    }
    
    for (size_t i = 0; i < keys.size(); i += 100) {
        std::vector<uint64_t> results;
        btree.search(keys[i], results);
        ASSERT_GT(results.size(), 0);
    }
    
    std::cout << "Tree size: " << btree.size() << std::endl;
    std::cout << "Tree height: " << btree.height() << std::endl;
}

TEST_F(BTreeTest, RemoveKeys) {
    BTree<int64_t> btree;
    
    for (int i = 0; i < 100; i++) {
        btree.insert(i, i);
    }
    
    btree.remove(50);
    
    std::vector<uint64_t> results;
    btree.search(50, results);
    ASSERT_EQ(results.size(), 0);
    
    for (int i = 0; i < 100; i++) {
        if (i != 50) {
            results.clear();
            btree.search(i, results);
            ASSERT_EQ(results.size(), 1);
        }
    }
}