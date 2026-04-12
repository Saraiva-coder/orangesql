// cache/lru_cache.cpp
#include "lru_cache.h"

namespace orangesql {

template<typename Key, typename Value>
LRUCache<Key, Value>::LRUCache(size_t capacity) : capacity_(capacity) {}

template<typename Key, typename Value>
LRUCache<Key, Value>::~LRUCache() {
    clear();
}

template<typename Key, typename Value>
void LRUCache<Key, Value>::put(const Key& key, const Value& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = cache_map_.find(key);
    if (it != cache_map_.end()) {
        it->second = CacheEntry(value);
        touch(key);
        return;
    }
    
    if (cache_map_.size() >= capacity_) {
        evict();
    }
    
    cache_map_.emplace(key, CacheEntry(value));
    lru_list_.push_front(key);
    map_iterator_[key] = lru_list_.begin();
}

template<typename Key, typename Value>
std::optional<Value> LRUCache<Key, Value>::get(const Key& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) {
        misses_++;
        return std::nullopt;
    }
    
    hits_++;
    touch(key);
    return it->second.value;
}

template<typename Key, typename Value>
bool LRUCache<Key, Value>::contains(const Key& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_map_.find(key) != cache_map_.end();
}

template<typename Key, typename Value>
void LRUCache<Key, Value>::remove(const Key& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = cache_map_.find(key);
    if (it != cache_map_.end()) {
        auto list_it = map_iterator_[key];
        lru_list_.erase(list_it);
        map_iterator_.erase(key);
        cache_map_.erase(it);
    }
}

template<typename Key, typename Value>
void LRUCache<Key, Value>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    lru_list_.clear();
    map_iterator_.clear();
    cache_map_.clear();
    hits_ = 0;
    misses_ = 0;
}

template<typename Key, typename Value>
size_t LRUCache<Key, Value>::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_map_.size();
}

template<typename Key, typename Value>
void LRUCache<Key, Value>::resize(size_t new_capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    capacity_ = new_capacity;
    while (cache_map_.size() > capacity_) {
        evict();
    }
}

template<typename Key, typename Value>
double LRUCache<Key, Value>::getHitRate() const {
    uint64_t total = hits_ + misses_;
    if (total == 0) return 0.0;
    return static_cast<double>(hits_) / static_cast<double>(total);
}

template<typename Key, typename Value>
void LRUCache<Key, Value>::resetStats() {
    hits_ = 0;
    misses_ = 0;
}

template<typename Key, typename Value>
void LRUCache<Key, Value>::touch(const Key& key) {
    auto it = map_iterator_[key];
    lru_list_.splice(lru_list_.begin(), lru_list_, it);
}

template<typename Key, typename Value>
void LRUCache<Key, Value>::evict() {
    if (lru_list_.empty()) return;
    
    Key key_to_evict = lru_list_.back();
    lru_list_.pop_back();
    map_iterator_.erase(key_to_evict);
    cache_map_.erase(key_to_evict);
}

template class LRUCache<int, void*>;
template class LRUCache<std::string, std::string>;
template class LRUCache<uint64_t, std::vector<char>>;

}