// cache/lru_cache.h
#ifndef LRU_CACHE_H
#define LRU_CACHE_H

#include <unordered_map>
#include <list>
#include <mutex>
#include <optional>
#include <atomic>
#include <chrono>

namespace orangesql {

template<typename Key, typename Value>
class LRUCache {
public:
    explicit LRUCache(size_t capacity);
    ~LRUCache();
    
    void put(const Key& key, const Value& value);
    std::optional<Value> get(const Key& key);
    bool contains(const Key& key) const;
    void remove(const Key& key);
    void clear();
    
    size_t size() const;
    size_t capacity() const { return capacity_; }
    void resize(size_t new_capacity);
    
    double getHitRate() const;
    void resetStats();
    
private:
    struct CacheEntry {
        Value value;
        std::chrono::steady_clock::time_point last_access;
        
        CacheEntry(const Value& v) : value(v), last_access(std::chrono::steady_clock::now()) {}
    };
    
    size_t capacity_;
    std::list<Key> lru_list_;
    std::unordered_map<Key, typename std::list<Key>::iterator> map_iterator_;
    std::unordered_map<Key, CacheEntry> cache_map_;
    mutable std::mutex mutex_;
    
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    
    void touch(const Key& key);
    void evict();
};

}

#endif