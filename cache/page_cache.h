// cache/page_cache.h
#ifndef PAGE_CACHE_H
#define PAGE_CACHE_H

#include "../storage/page.h"
#include <vector>
#include <unordered_map>
#include <list>
#include <shared_mutex>
#include <atomic>
#include <thread>

namespace orangesql {

struct CachedPage {
    uint32_t page_id;
    std::vector<char> data;
    uint32_t file_id;
    bool is_dirty;
    uint64_t last_access;
    uint32_t ref_count;
    
    CachedPage() : page_id(0), file_id(0), is_dirty(false), last_access(0), ref_count(0) {}
    CachedPage(uint32_t pid, uint32_t fid) : page_id(pid), file_id(fid), 
                is_dirty(false), last_access(0), ref_count(0) {}
};

class PageCache {
public:
    static PageCache& getInstance();
    
    bool init(size_t max_pages, size_t prefetch_threshold);
    void shutdown();
    
    bool getPage(uint32_t file_id, uint32_t page_id, std::vector<char>& data);
    void putPage(uint32_t file_id, uint32_t page_id, const std::vector<char>& data, bool dirty);
    void markDirty(uint32_t file_id, uint32_t page_id);
    void flushPage(uint32_t file_id, uint32_t page_id);
    void flushAll();
    
    void prefetchPages(uint32_t file_id, uint32_t start_page, uint32_t count);
    void invalidateFile(uint32_t file_id);
    
    size_t getHitCount() const { return hit_count_; }
    size_t getMissCount() const { return miss_count_; }
    double getHitRate() const;
    size_t getDirtyCount() const;
    size_t getSize() const { return cache_.size(); }
    
private:
    PageCache();
    ~PageCache();
    
    struct PageKey {
        uint32_t file_id;
        uint32_t page_id;
        
        bool operator==(const PageKey& other) const {
            return file_id == other.file_id && page_id == other.page_id;
        }
    };
    
    struct PageKeyHash {
        size_t operator()(const PageKey& key) const {
            return (static_cast<size_t>(key.file_id) << 32) | key.page_id;
        }
    };
    
    std::unordered_map<PageKey, CachedPage, PageKeyHash> cache_;
    std::list<PageKey> lru_list_;
    mutable std::shared_mutex mutex_;
    
    std::atomic<size_t> max_pages_{10000};
    std::atomic<size_t> prefetch_threshold_{100};
    std::atomic<size_t> hit_count_{0};
    std::atomic<size_t> miss_count_{0};
    std::atomic<bool> running_{true};
    
    std::thread flush_thread_;
    
    void evictLRU();
    void updateLRU(const PageKey& key);
    void backgroundFlusher();
};

}

#endif