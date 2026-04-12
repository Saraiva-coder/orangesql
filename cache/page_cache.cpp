// cache/page_cache.cpp
#include "page_cache.h"
#include "../storage/file_manager.h"
#include "../include/logger.h"
#include "../include/constants.h"
#include <thread>
#include <chrono>
#include <cstring>

namespace orangesql {

PageCache& PageCache::getInstance() {
    static PageCache instance;
    return instance;
}

PageCache::PageCache() {
    running_ = true;
    flush_thread_ = std::thread(&PageCache::backgroundFlusher, this);
}

PageCache::~PageCache() {
    shutdown();
}

bool PageCache::init(size_t max_pages, size_t prefetch_threshold) {
    max_pages_ = max_pages;
    prefetch_threshold_ = prefetch_threshold;
    LOG_INFO("PageCache initialized with max_pages=" + std::to_string(max_pages) + 
             ", prefetch_threshold=" + std::to_string(prefetch_threshold));
    return true;
}

void PageCache::shutdown() {
    running_ = false;
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    flushAll();
    
    std::unique_lock<std::shared_mutex> lock(mutex_);
    cache_.clear();
    lru_list_.clear();
    
    LOG_INFO("PageCache shutdown complete. Final stats - Hits: " + std::to_string(hit_count_) + 
             ", Misses: " + std::to_string(miss_count_) + 
             ", Hit Rate: " + std::to_string(getHitRate() * 100) + "%");
}

bool PageCache::getPage(uint32_t file_id, uint32_t page_id, std::vector<char>& data) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    PageKey key{file_id, page_id};
    auto it = cache_.find(key);
    
    if (it != cache_.end()) {
        hit_count_++;
        it->second.last_access = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        it->second.ref_count++;
        data = it->second.data;
        updateLRU(key);
        return true;
    }
    
    miss_count_++;
    
    FileManager fm;
    char buffer[PAGE_SIZE];
    Status status = fm.readPage(static_cast<int>(file_id), static_cast<int>(page_id), buffer);
    
    if (!status.ok()) {
        LOG_ERROR("Failed to read page " + std::to_string(page_id) + 
                  " from file " + std::to_string(file_id));
        return false;
    }
    
    data.assign(buffer, buffer + PAGE_SIZE);
    
    if (cache_.size() >= max_pages_) {
        evictLRU();
    }
    
    CachedPage cached(page_id, file_id);
    cached.data.assign(buffer, buffer + PAGE_SIZE);
    cached.last_access = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    cached.ref_count = 1;
    
    cache_[key] = cached;
    lru_list_.push_front(key);
    
    return true;
}

void PageCache::putPage(uint32_t file_id, uint32_t page_id, const std::vector<char>& data, bool dirty) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    PageKey key{file_id, page_id};
    auto it = cache_.find(key);
    
    if (it != cache_.end()) {
        it->second.data = data;
        it->second.is_dirty = dirty;
        it->second.last_access = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        updateLRU(key);
    } else {
        if (cache_.size() >= max_pages_) {
            evictLRU();
        }
        
        CachedPage cached(page_id, file_id);
        cached.data = data;
        cached.is_dirty = dirty;
        cached.last_access = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        cached.ref_count = 1;
        
        cache_[key] = cached;
        lru_list_.push_front(key);
    }
}

void PageCache::markDirty(uint32_t file_id, uint32_t page_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    PageKey key{file_id, page_id};
    auto it = cache_.find(key);
    if (it != cache_.end()) {
        it->second.is_dirty = true;
    }
}

void PageCache::flushPage(uint32_t file_id, uint32_t page_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    PageKey key{file_id, page_id};
    auto it = cache_.find(key);
    
    if (it != cache_.end() && it->second.is_dirty) {
        FileManager fm;
        Status status = fm.writePage(static_cast<int>(file_id), static_cast<int>(page_id), 
                                      it->second.data.data());
        if (status.ok()) {
            it->second.is_dirty = false;
            LOG_DEBUG("Flushed page " + std::to_string(page_id) + " from file " + std::to_string(file_id));
        } else {
            LOG_ERROR("Failed to flush page " + std::to_string(page_id));
        }
    }
}

void PageCache::flushAll() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    FileManager fm;
    size_t flushed = 0;
    
    for (auto& pair : cache_) {
        if (pair.second.is_dirty) {
            Status status = fm.writePage(static_cast<int>(pair.first.file_id), 
                                         static_cast<int>(pair.first.page_id), 
                                         pair.second.data.data());
            if (status.ok()) {
                pair.second.is_dirty = false;
                flushed++;
            }
        }
    }
    
    if (flushed > 0) {
        LOG_INFO("Flushed " + std::to_string(flushed) + " dirty pages");
    }
}

void PageCache::prefetchPages(uint32_t file_id, uint32_t start_page, uint32_t count) {
    if (count > prefetch_threshold_) {
        LOG_DEBUG("Prefetch skipped for " + std::to_string(count) + " pages (threshold: " + 
                  std::to_string(prefetch_threshold_) + ")");
        return;
    }
    
    std::thread([this, file_id, start_page, count]() {
        for (uint32_t i = 0; i < count; i++) {
            uint32_t page_id = start_page + i;
            std::vector<char> data;
            if (getPage(file_id, page_id, data)) {
                LOG_DEBUG("Prefetched page " + std::to_string(page_id));
            }
        }
    }).detach();
}

void PageCache::invalidateFile(uint32_t file_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    size_t invalidated = 0;
    auto it = cache_.begin();
    
    while (it != cache_.end()) {
        if (it->first.file_id == file_id) {
            if (it->second.is_dirty && it->second.ref_count == 0) {
                FileManager fm;
                fm.writePage(static_cast<int>(it->first.file_id), 
                            static_cast<int>(it->first.page_id), 
                            it->second.data.data());
            }
            it = cache_.erase(it);
            invalidated++;
        } else {
            ++it;
        }
    }
    
    lru_list_.remove_if([file_id](const PageKey& key) {
        return key.file_id == file_id;
    });
    
    if (invalidated > 0) {
        LOG_DEBUG("Invalidated " + std::to_string(invalidated) + " pages from file " + 
                  std::to_string(file_id));
    }
}

double PageCache::getHitRate() const {
    uint64_t total = hit_count_ + miss_count_;
    if (total == 0) return 0.0;
    return static_cast<double>(hit_count_) / static_cast<double>(total);
}

size_t PageCache::getDirtyCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    size_t count = 0;
    for (const auto& pair : cache_) {
        if (pair.second.is_dirty) count++;
    }
    return count;
}

void PageCache::evictLRU() {
    if (lru_list_.empty()) return;
    
    PageKey key = lru_list_.back();
    auto it = cache_.find(key);
    
    if (it != cache_.end()) {
        if (it->second.is_dirty && it->second.ref_count == 0) {
            FileManager fm;
            fm.writePage(static_cast<int>(key.file_id), static_cast<int>(key.page_id), 
                        it->second.data.data());
        }
        
        if (it->second.ref_count == 0) {
            cache_.erase(it);
            lru_list_.pop_back();
        }
    }
}

void PageCache::updateLRU(const PageKey& key) {
    lru_list_.remove(key);
    lru_list_.push_front(key);
}

void PageCache::backgroundFlusher() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        
        if (!running_) break;
        
        std::unique_lock<std::shared_mutex> lock(mutex_);
        
        size_t flushed = 0;
        for (auto& pair : cache_) {
            if (pair.second.is_dirty && pair.second.ref_count == 0) {
                FileManager fm;
                Status status = fm.writePage(static_cast<int>(pair.first.file_id), 
                                            static_cast<int>(pair.first.page_id), 
                                            pair.second.data.data());
                if (status.ok()) {
                    pair.second.is_dirty = false;
                    flushed++;
                }
            }
        }
        
        if (flushed > 0) {
            LOG_DEBUG("Background flusher wrote " + std::to_string(flushed) + " pages");
        }
    }
}

}