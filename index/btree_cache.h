// index/btree_cache.h
#ifndef BTREE_CACHE_H
#define BTREE_CACHE_H

#include "btree_node.h"
#include "../../cache/lru_cache.h"
#include <unordered_map>
#include <mutex>

namespace orangesql {

template<typename KeyType>
class BTreeCache {
public:
    explicit BTreeCache(size_t max_size = 10000);
    ~BTreeCache();
    
    BTreeNode<KeyType>* get(uint64_t node_id);
    void put(uint64_t node_id, BTreeNode<KeyType>* node);
    void remove(uint64_t node_id);
    void clear();
    size_t size() const;
    
private:
    struct CachedNode {
        std::unique_ptr<BTreeNode<KeyType>> node;
        uint64_t last_access;
    };
    
    size_t max_size_;
    std::unordered_map<uint64_t, CachedNode> cache_;
    std::mutex mutex_;
    std::atomic<uint64_t> access_counter_{0};
    
    void evictOldest();
};

}

#endif