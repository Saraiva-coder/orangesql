// storage/buffer_pool_manager.cpp
#include "buffer_pool_manager.h"

namespace orangesql {

BufferPoolManager& BufferPoolManager::getInstance() {
    static BufferPoolManager instance;
    return instance;
}

BufferPool* BufferPoolManager::getPool(int pool_id) {
    if (pool_id >= 0 && static_cast<size_t>(pool_id) < pools_.size()) {
        return pools_[pool_id].get();
    }
    return nullptr;
}

Page* BufferPoolManager::getPage(int file_id, uint32_t page_id, int preferred_pool) {
    size_t pool_idx = selectPool(preferred_pool);
    if (pool_idx >= pools_.size()) {
        return nullptr;
    }
    
    return pools_[pool_idx]->getPage(file_id, page_id);
}

Status BufferPoolManager::createPage(int file_id, uint32_t& page_id, Page*& page, int preferred_pool) {
    size_t pool_idx = selectPool(preferred_pool);
    if (pool_idx >= pools_.size()) {
        return Status::Error("No buffer pool available");
    }
    
    return pools_[pool_idx]->createPage(file_id, page_id, page);
}

void BufferPoolManager::unpinPage(Page* page) {
    for (auto& pool : pools_) {
        pool->unpinPage(page);
    }
}

void BufferPoolManager::markDirty(Page* page) {
    for (auto& pool : pools_) {
        pool->markDirty(page);
    }
}

Status BufferPoolManager::flushAll() {
    for (auto& pool : pools_) {
        Status status = pool->flushAll();
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

void BufferPoolManager::addPool(std::unique_ptr<BufferPool> pool) {
    pools_.push_back(std::move(pool));
}

size_t BufferPoolManager::selectPool(int preferred_pool) {
    if (preferred_pool >= 0 && static_cast<size_t>(preferred_pool) < pools_.size()) {
        return preferred_pool;
    }
    
    size_t idx = next_pool_.fetch_add(1);
    return idx % pools_.size();
}

}