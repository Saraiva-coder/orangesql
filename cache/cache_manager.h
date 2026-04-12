// cache/cache_manager.h
#ifndef CACHE_MANAGER_H
#define CACHE_MANAGER_H

#include "lru_cache.h"
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <atomic>

namespace orangesql {

enum class CacheType {
    PAGE_CACHE,
    INDEX_CACHE,
    QUERY_CACHE,
    METADATA_CACHE
};

struct CacheConfig {
    size_t max_size;
    size_t ttl_seconds;
    bool enable_compression;
    bool enable_prefetch;
    
    CacheConfig() : max_size(10000), ttl_seconds(3600), 
                    enable_compression(false), enable_prefetch(true) {}
};

class CacheManager {
public:
    static CacheManager& getInstance();
    
    void registerCache(CacheType type, const CacheConfig& config);
    void unregisterCache(CacheType type);
    
    template<typename Key, typename Value>
    bool put(CacheType type, const Key& key, const Value& value);
    
    template<typename Key, typename Value>
    std::optional<Value> get(CacheType type, const Key& key);
    
    template<typename Key>
    void invalidate(CacheType type, const Key& key);
    
    void invalidateAll(CacheType type);
    void clearAll();
    
    void setGlobalTTL(size_t seconds);
    void setMaxMemory(size_t bytes);
    
    size_t getTotalSize() const;
    double getGlobalHitRate() const;
    
private:
    CacheManager();
    ~CacheManager();
    
    struct CacheWrapper {
        std::unique_ptr<LRUCache<std::string, std::string>> cache;
        CacheConfig config;
        std::shared_mutex mutex;
        std::atomic<uint64_t> hits{0};
        std::atomic<uint64_t> misses{0};
        
        CacheWrapper() : cache(nullptr) {}
    };
    
    std::unordered_map<CacheType, std::unique_ptr<CacheWrapper>> caches_;
    std::shared_mutex global_mutex_;
    std::atomic<size_t> max_memory_{512 * 1024 * 1024};
    std::atomic<size_t> current_memory_{0};
    std::atomic<size_t> global_ttl_{3600};
    
    std::string serializeKey(const std::string& key);
    std::string serializeValue(const std::string& value);
    std::string deserializeValue(const std::string& data);
    size_t estimateMemoryUsage(const std::string& key, const std::string& value);
};

}

#endif