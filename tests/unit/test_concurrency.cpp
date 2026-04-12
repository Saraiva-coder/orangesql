// tests/unit/test_concurrency.cpp
#include <gtest/gtest.h>
#include "../../index/btree_concurrent.h"
#include "../../transaction/lock_manager.h"
#include "../../include/logger.h"
#include <thread>
#include <atomic>
#include <vector>

using namespace orangesql;

class ConcurrencyTest : public ::testing::Test {
protected:
    void SetUp() override {
        Logger::getInstance().setLevel(LogLevel::ERROR);
    }
};

TEST_F(ConcurrencyTest, ConcurrentBTreeInsert) {
    ConcurrentBTree<int64_t> btree;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int t = 0; t < 10; t++) {
        threads.emplace_back([&btree, &success_count, t]() {
            for (int i = 0; i < 1000; i++) {
                int64_t key = t * 1000 + i;
                if (btree.insert(key, key).ok()) {
                    success_count++;
                }
            }
        });
    }
    
    for (auto& th : threads) {
        th.join();
    }
    
    ASSERT_EQ(success_count, 10000);
}

TEST_F(ConcurrencyTest, ConcurrentBTreeSearch) {
    ConcurrentBTree<int64_t> btree;
    
    for (int i = 0; i < 5000; i++) {
        btree.insert(i, i);
    }
    
    std::vector<std::thread> threads;
    std::atomic<int> found_count{0};
    
    for (int t = 0; t < 20; t++) {
        threads.emplace_back([&btree, &found_count]() {
            for (int i = 0; i < 5000; i++) {
                std::vector<uint64_t> results;
                if (btree.search(i, results).ok() && !results.empty()) {
                    found_count++;
                }
            }
        });
    }
    
    for (auto& th : threads) {
        th.join();
    }
    
    ASSERT_EQ(found_count, 20 * 5000);
}

TEST_F(ConcurrencyTest, LockManagerDeadlockDetection) {
    LockManager& lm = LockManager::getInstance();
    
    std::atomic<bool> deadlock_detected{false};
    
    auto txn1 = std::thread([&lm, &deadlock_detected]() {
        Status s1 = lm.acquireLock(1, 100, LockType::EXCLUSIVE);
        if (!s1.ok()) return;
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        Status s2 = lm.acquireLock(1, 200, LockType::EXCLUSIVE);
        if (!s2.ok()) deadlock_detected = true;
        
        lm.releaseAllLocks(1);
    });
    
    auto txn2 = std::thread([&lm, &deadlock_detected]() {
        Status s1 = lm.acquireLock(2, 200, LockType::EXCLUSIVE);
        if (!s1.ok()) return;
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        Status s2 = lm.acquireLock(2, 100, LockType::EXCLUSIVE);
        if (!s2.ok()) deadlock_detected = true;
        
        lm.releaseAllLocks(2);
    });
    
    txn1.join();
    txn2.join();
}

TEST_F(ConcurrencyTest, MVCCConcurrentReadWrite) {
    MVCCManager mvcc;
    std::vector<std::thread> threads;
    std::atomic<int> commits{0};
    
    auto writer = [&mvcc, &commits](int id) {
        uint64_t txn_id;
        mvcc.beginTransaction(txn_id);
        
        Row row;
        row.row_id = id;
        row.values.push_back(Value(static_cast<int64_t>(id)));
        
        mvcc.insertRecord(txn_id, "test", row);
        mvcc.commitTransaction(txn_id);
        commits++;
    };
    
    auto reader = [&mvcc](int id) {
        uint64_t txn_id;
        mvcc.beginTransaction(txn_id);
        
        Row row;
        if (mvcc.selectRecord(txn_id, "test", id, row).ok()) {
            mvcc.commitTransaction(txn_id);
        } else {
            mvcc.abortTransaction(txn_id);
        }
    };
    
    for (int i = 0; i < 50; i++) {
        threads.emplace_back(writer, i);
    }
    
    for (int i = 0; i < 50; i++) {
        threads.emplace_back(reader, i);
    }
    
    for (auto& th : threads) {
        th.join();
    }
    
    ASSERT_EQ(commits, 50);
}