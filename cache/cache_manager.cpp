// cache/cache_manager.cpp
#include "cache_manager.h"
#include <zlib.h>
#include <chrono>

namespace orangesql {

CacheManager& CacheManager::getInstance() {
    static CacheManager instance;
    return instance;
}

CacheManager::CacheManager() {}

CacheManager::~CacheManager() {
    clearAll();
}

void CacheManager::registerCache(CacheType type, const CacheConfig& config) {
    std::unique_lock<std::shared_mutex> lock(global_mutex_);
    
    auto wrapper = std::make_unique<CacheWrapper>();
    wrapper->cache = std::make_unique<LRUCache<std::string, std::string>>(config.max_size);
    wrapper->config = config;
    
    caches_[type] = std::move(wrapper);
}

void CacheManager::unregisterCache(CacheType type) {
    std::unique_lock<std::shared_mutex> lock(global_mutex_);
    caches_.erase(type);
}

template<typename Key, typename Value>
bool CacheManager::put(CacheType type, const Key& key, const Value& value) {
    std::shared_lock<std::shared_mutex> global_lock(global_mutex_);
    
    auto it = caches_.find(type);
    if (it == caches_.end()) {
        return false;
    }
    
    auto& wrapper = it->second;
    std::unique_lock<std::shared_mutex> cache_lock(wrapper->mutex);
    
    std::string key_str = serializeKey(std::to_string(reinterpret_cast<uint64_t>(&key)));
    std::string value_str = serializeValue(std::to_string(reinterpret_cast<uint64_t>(&value)));
    
    size_t memory_usage = estimateMemoryUsage(key_str, value_str);
    
    if (current_memory_ + memory_usage > max_memory_) {
        return false;
    }
    
    wrapper->cache->put(key_str, value_str);
    current_memory_ += memory_usage;
    
    return true;
}

template<typename Key, typename Value>
std::optional<Value> CacheManager::get(CacheType type, const Key& key) {
    std::shared_lock<std::shared_mutex> global_lock(global_mutex_);
    
    auto it = caches_.find(type);
    if (it == caches_.end()) {
        return std::nullopt;
    }
    
    auto& wrapper = it->second;
    std::unique_lock<std::shared_mutex> cache_lock(wrapper->mutex);
    
    std::string key_str = serializeKey(std::to_string(reinterpret_cast<uint64_t>(&key)));
    auto result = wrapper->cache->get(key_str);
    
    if (result.has_value()) {
        wrapper->hits++;
        Value val;
        return val;
    } else {
        wrapper->misses++;
        return std::nullopt;
    }
}

template<typename Key>
void CacheManager::invalidate(CacheType type, const Key& key) {
    std::shared_lock<std::shared_mutex> global_lock(global_mutex_);
    
    auto it = caches_.find(type);
    if (it == caches_.end()) {
        return;
    }
    
    auto& wrapper = it->second;
    std::unique_lock<std::shared_mutex> cache_lock(wrapper->mutex);
    
    std::string key_str = serializeKey(std::to_string(reinterpret_cast<uint64_t>(&key)));
    wrapper->cache->remove(key_str);
}

void CacheManager::invalidateAll(CacheType type) {
    std::shared_lock<std::shared_mutex> global_lock(global_mutex_);
    
    auto it = caches_.find(type);
    if (it == caches_.end()) {
        return;
    }
    
    auto& wrapper = it->second;
    std::unique_lock<std::shared_mutex> cache_lock(wrapper->mutex);
    wrapper->cache->clear();
}

void CacheManager::clearAll() {
    std::unique_lock<std::shared_mutex> global_lock(global_mutex_);
    
    for (auto& pair : caches_) {
        std::unique_lock<std::shared_mutex> cache_lock(pair.second->mutex);
        pair.second->cache->clear();
    }
    
    current_memory_ = 0;
}

void CacheManager::setGlobalTTL(size_t seconds) {
    global_ttl_ = seconds;
}

void CacheManager::setMaxMemory(size_t bytes) {
    max_memory_ = bytes;
}

size_t CacheManager::getTotalSize() const {
    size_t total = 0;
    std::shared_lock<std::shared_mutex> global_lock(global_mutex_);
    
    for (const auto& pair : caches_) {
        std::shared_lock<std::shared_mutex> cache_lock(pair.second->mutex);
        total += pair.second->cache->size();
    }
    
    return total;
}

double CacheManager::getGlobalHitRate() const {
    uint64_t total_hits = 0;
    uint64_t total_misses = 0;
    
    std::shared_lock<std::shared_mutex> global_lock(global_mutex_);
    
    for (const auto& pair : caches_) {
        total_hits += pair.second->hits;
        total_misses += pair.second->misses;
    }
    
    uint64_t total = total_hits + total_misses;
    if (total == 0) return 0.0;
    return static_cast<double>(total_hits) / static_cast<double>(total);
}

std::string CacheManager::serializeKey(const std::string& key) {
    return key;
}

std::string CacheManager::serializeValue(const std::string& value) {
    return value;
}

std::string CacheManager::deserializeValue(const std::string& data) {
    return data;
}

size_t CacheManager::estimateMemoryUsage(const std::string& key, const std::string& value) {
    return key.size() + value.size() + sizeof(void*) * 4;
}

template bool CacheManager::put<int, std::string>(CacheType, const int&, const std::string&);
template std::optional<std::string> CacheManager::get<int, std::string>(CacheType, const int&);
template void CacheManager::invalidate<int>(CacheType, const int&);

}