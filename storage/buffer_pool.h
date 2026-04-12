// storage/buffer_pool.h
#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H

#include "page.h"
#include "file_manager.h"
#include "../include/constants.h"
#include "../include/status.h"
#include <unordered_map>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <atomic>

namespace orangesql {

class BufferPool {
public:
    BufferPool();
    explicit BufferPool(size_t size);
    ~BufferPool();
    
    Page* getPage(int file_id, uint32_t page_id);
    Status createPage(int file_id, uint32_t& page_id, Page*& page);
    Status flushPage(int file_id, uint32_t page_id);
    Status flushAll();
    void unpinPage(Page* page);
    void markDirty(Page* page);
    
    size_t getSize() const { return pool_size_; }
    size_t getUsed() const { return page_table_.size(); }
    double getHitRate() const;
    
private:
    struct Frame {
        Page* page;
        int file_id;
        uint32_t page_id;
        bool pinned;
        uint32_t pin_count;
        bool dirty;
        
        Frame() : page(nullptr), file_id(-1), page_id(0), pinned(false), pin_count(0), dirty(false) {}
    };
    
    size_t pool_size_;
    std::vector<Frame> frames_;
    std::unordered_map<uint64_t, size_t> page_table_;
    std::list<size_t> lru_list_;
    std::shared_mutex mutex_;
    FileManager file_manager_;
    
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    
    size_t evictFrame();
    void updateLRU(size_t frame_id);
    uint64_t makeKey(int file_id, uint32_t page_id) const;
};

}

#endif