// index/btree_cache.h
#ifndef BTREE_CACHE_H
#define BTREE_CACHE_H

#include <unordered_map>
#include <list>
#include <mutex>
#include <optional>
#include <cstdint>

namespace orangesql {

template<typename KeyType>
class BTreeCache {
public:
    explicit BTreeCache(size_t max_size = 10000) : max_size_(max_size) {}
    
    ~BTreeCache() {
        clear();
    }
    
    void put(uint64_t node_id, void* node) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = cache_map_.find(node_id);
        if (it != cache_map_.end()) {
            lru_list_.erase(it->second);
        }
        
        lru_list_.push_front(node_id);
        cache_map_[node_id] = lru_list_.begin();
        node_map_[node_id] = node;
        
        if (cache_map_.size() > max_size_) {
            uint64_t oldest = lru_list_.back();
            lru_list_.pop_back();
            cache_map_.erase(oldest);
            node_map_.erase(oldest);
        }
    }
    
    void* get(uint64_t node_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = cache_map_.find(node_id);
        if (it == cache_map_.end()) {
            return nullptr;
        }
        
        lru_list_.erase(it->second);
        lru_list_.push_front(node_id);
        it->second = lru_list_.begin();
        
        auto node_it = node_map_.find(node_id);
        if (node_it != node_map_.end()) {
            return node_it->second;
        }
        
        return nullptr;
    }
    
    bool contains(uint64_t node_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return cache_map_.find(node_id) != cache_map_.end();
    }
    
    void remove(uint64_t node_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = cache_map_.find(node_id);
        if (it != cache_map_.end()) {
            lru_list_.erase(it->second);
            cache_map_.erase(it);
            node_map_.erase(node_id);
        }
    }
    
    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        lru_list_.clear();
        cache_map_.clear();
        node_map_.clear();
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return cache_map_.size();
    }
    
    void resize(size_t new_max_size) {
        std::lock_guard<std::mutex> lock(mutex_);
        max_size_ = new_max_size;
        
        while (cache_map_.size() > max_size_) {
            uint64_t oldest = lru_list_.back();
            lru_list_.pop_back();
            cache_map_.erase(oldest);
            node_map_.erase(oldest);
        }
    }
    
private:
    size_t max_size_;
    std::list<uint64_t> lru_list_;
    std::unordered_map<uint64_t, typename std::list<uint64_t>::iterator> cache_map_;
    std::unordered_map<uint64_t, void*> node_map_;
    mutable std::mutex mutex_;
};

}

#endif