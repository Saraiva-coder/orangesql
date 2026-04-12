// index/btree_concurrent.h
#ifndef BTREE_CONCURRENT_H
#define BTREE_CONCURRENT_H

#include "btree.h"
#include <shared_mutex>
#include <atomic>

namespace orangesql {

template<typename KeyType>
class ConcurrentBTree {
public:
    explicit ConcurrentBTree(int order = BTREE_ORDER);
    ~ConcurrentBTree();
    
    Status insert(const KeyType& key, uint64_t record_id);
    Status search(const KeyType& key, std::vector<uint64_t>& record_ids);
    Status searchRange(const KeyType& start, const KeyType& end, std::vector<uint64_t>& record_ids);
    Status remove(const KeyType& key);
    
    size_t size() const { return btree_.size(); }
    
private:
    BTree<KeyType> btree_;
    mutable std::shared_mutex mutex_;
    std::atomic<bool> is_latched_{false};
    
    void latch();
    void unlatch();
    bool tryLatch();
};

}

#endif