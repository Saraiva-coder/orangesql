// storage/buffer_pool.cpp
#include "buffer_pool.h"
#include <algorithm>

namespace orangesql {

BufferPool::BufferPool() : pool_size_(BUFFER_POOL_SIZE) {
    frames_.resize(pool_size_);
}

BufferPool::BufferPool(size_t size) : pool_size_(size) {
    frames_.resize(pool_size_);
}

BufferPool::~BufferPool() {
    flushAll();
    for (auto& frame : frames_) {
        delete frame.page;
    }
}

Page* BufferPool::getPage(int file_id, uint32_t page_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    uint64_t key = makeKey(file_id, page_id);
    auto it = page_table_.find(key);
    
    if (it != page_table_.end()) {
        hits_++;
        size_t frame_id = it->second;
        frames_[frame_id].pinned = true;
        frames_[frame_id].pin_count++;
        updateLRU(frame_id);
        return frames_[frame_id].page;
    }
    
    misses_++;
    
    size_t frame_id = evictFrame();
    if (frame_id >= pool_size_) {
        return nullptr;
    }
    
    Frame& frame = frames_[frame_id];
    
    if (frame.page && frame.dirty) {
        flushPage(frame.file_id, frame.page_id);
    }
    
    delete frame.page;
    
    frame.page = new Page(page_id);
    frame.file_id = file_id;
    frame.page_id = page_id;
    frame.pinned = true;
    frame.pin_count = 1;
    frame.dirty = false;
    
    char buffer[PAGE_SIZE];
    Status status = file_manager_.readPage(file_id, page_id, buffer);
    if (status.ok()) {
        memcpy(frame.page->getData(), buffer, PAGE_SIZE);
    }
    
    page_table_[key] = frame_id;
    updateLRU(frame_id);
    
    return frame.page;
}

Status BufferPool::createPage(int file_id, uint32_t& page_id, Page*& page) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    Status status = file_manager_.allocatePage(file_id, reinterpret_cast<int&>(page_id));
    if (!status.ok()) {
        return status;
    }
    
    uint64_t key = makeKey(file_id, page_id);
    auto it = page_table_.find(key);
    
    if (it != page_table_.end()) {
        page = frames_[it->second].page;
        frames_[it->second].pinned = true;
        frames_[it->second].pin_count++;
        return Status::OK();
    }
    
    size_t frame_id = evictFrame();
    if (frame_id >= pool_size_) {
        return Status::Error("No free frames");
    }
    
    Frame& frame = frames_[frame_id];
    
    if (frame.page && frame.dirty) {
        flushPage(frame.file_id, frame.page_id);
    }
    
    delete frame.page;
    
    frame.page = new Page(page_id);
    frame.file_id = file_id;
    frame.page_id = page_id;
    frame.pinned = true;
    frame.pin_count = 1;
    frame.dirty = true;
    
    memset(frame.page->getData(), 0, PAGE_SIZE);
    
    page_table_[key] = frame_id;
    updateLRU(frame_id);
    
    page = frame.page;
    
    return Status::OK();
}

size_t BufferPool::evictFrame() {
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        size_t frame_id = *it;
        if (!frames_[frame_id].pinned && frames_[frame_id].pin_count == 0) {
            lru_list_.remove(frame_id);
            lru_list_.push_front(frame_id);
            return frame_id;
        }
    }
    
    for (size_t i = 0; i < pool_size_; i++) {
        if (!frames_[i].pinned && frames_[i].pin_count == 0) {
            lru_list_.remove(i);
            lru_list_.push_front(i);
            return i;
        }
    }
    
    return pool_size_;
}

void BufferPool::updateLRU(size_t frame_id) {
    lru_list_.remove(frame_id);
    lru_list_.push_front(frame_id);
}

void BufferPool::unpinPage(Page* page) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    for (size_t i = 0; i < pool_size_; i++) {
        if (frames_[i].page == page) {
            if (frames_[i].pin_count > 0) {
                frames_[i].pin_count--;
                if (frames_[i].pin_count == 0) {
                    frames_[i].pinned = false;
                }
            }
            break;
        }
    }
}

void BufferPool::markDirty(Page* page) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    for (size_t i = 0; i < pool_size_; i++) {
        if (frames_[i].page == page) {
            frames_[i].dirty = true;
            break;
        }
    }
}

Status BufferPool::flushPage(int file_id, uint32_t page_id) {
    uint64_t key = makeKey(file_id, page_id);
    auto it = page_table_.find(key);
    
    if (it != page_table_.end()) {
        size_t frame_id = it->second;
        Frame& frame = frames_[frame_id];
        
        if (frame.dirty && frame.page) {
            Status status = file_manager_.writePage(file_id, page_id, 
                                                     reinterpret_cast<const char*>(frame.page->getData()));
            if (status.ok()) {
                frame.dirty = false;
                frame.page->clearDirty();
            }
            return status;
        }
    }
    
    return Status::OK();
}

Status BufferPool::flushAll() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    for (auto& pair : page_table_) {
        size_t frame_id = pair.second;
        Frame& frame = frames_[frame_id];
        
        if (frame.dirty && frame.page) {
            Status status = file_manager_.writePage(frame.file_id, frame.page_id,
                                                     reinterpret_cast<const char*>(frame.page->getData()));
            if (!status.ok()) {
                return status;
            }
            frame.dirty = false;
            frame.page->clearDirty();
        }
    }
    
    return Status::OK();
}

double BufferPool::getHitRate() const {
    uint64_t total = hits_ + misses_;
    if (total == 0) return 0.0;
    return static_cast<double>(hits_) / static_cast<double>(total);
}

uint64_t BufferPool::makeKey(int file_id, uint32_t page_id) const {
    return (static_cast<uint64_t>(file_id) << 32) | page_id;
}

}