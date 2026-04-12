// storage/buffer_pool_manager.h
#ifndef BUFFER_POOL_MANAGER_H
#define BUFFER_POOL_MANAGER_H

#include "buffer_pool.h"
#include <vector>
#include <memory>
#include <atomic>

namespace orangesql {

class BufferPoolManager {
public:
    static BufferPoolManager& getInstance();
    
    BufferPool* getPool(int pool_id);
    Page* getPage(int file_id, uint32_t page_id, int preferred_pool = -1);
    Status createPage(int file_id, uint32_t& page_id, Page*& page, int preferred_pool = -1);
    void unpinPage(Page* page);
    void markDirty(Page* page);
    Status flushAll();
    
    void addPool(std::unique_ptr<BufferPool> pool);
    size_t getPoolCount() const { return pools_.size(); }
    
private:
    BufferPoolManager() = default;
    ~BufferPoolManager() = default;
    
    std::vector<std::unique_ptr<BufferPool>> pools_;
    std::atomic<size_t> next_pool_{0};
    
    size_t selectPool(int preferred_pool);
};

}

#endif