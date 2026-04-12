// index/btree_cache.cpp
#include "btree_cache.h"
#include <algorithm>

namespace orangesql {

template<typename KeyType>
BTreeCache<KeyType>::BTreeCache(size_t max_size) : max_size_(max_size) {}

template<typename KeyType>
BTreeCache<KeyType>::~BTreeCache() {
    clear();
}

template<typename KeyType>
BTreeNode<KeyType>* BTreeCache<KeyType>::get(uint64_t node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = cache_.find(node_id);
    if (it != cache_.end()) {
        it->second.last_access = access_counter_++;
        return it->second.node.get();
    }
    
    return nullptr;
}

template<typename KeyType>
void BTreeCache<KeyType>::put(uint64_t node_id, BTreeNode<KeyType>* node) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (cache_.size() >= max_size_) {
        evictOldest();
    }
    
    CachedNode cached;
    cached.node = std::unique_ptr<BTreeNode<KeyType>>(new BTreeNode<KeyType>(*node));
    cached.last_access = access_counter_++;
    
    cache_[node_id] = std::move(cached);
}

template<typename KeyType>
void BTreeCache<KeyType>::remove(uint64_t node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.erase(node_id);
}

template<typename KeyType>
void BTreeCache<KeyType>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
}

template<typename KeyType>
size_t BTreeCache<KeyType>::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.size();
}

template<typename KeyType>
void BTreeCache<KeyType>::evictOldest() {
    if (cache_.empty()) return;
    
    auto oldest = std::min_element(cache_.begin(), cache_.end(),
        [](const auto& a, const auto& b) {
            return a.second.last_access < b.second.last_access;
        });
    
    if (oldest != cache_.end()) {
        cache_.erase(oldest);
    }
}

template class BTreeCache<int64_t>;
template class BTreeCache<std::string>;

}