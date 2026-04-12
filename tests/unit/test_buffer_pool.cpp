// tests/unit/test_buffer_pool.cpp
#include <gtest/gtest.h>
#include "../../storage/buffer_pool.h"
#include "../../storage/file_manager.h"
#include "../../include/logger.h"
#include <thread>
#include <vector>

using namespace orangesql;

class BufferPoolTest : public ::testing::Test {
protected:
    void SetUp() override {
        Logger::getInstance().setLevel(LogLevel::ERROR);
        buffer_pool_ = new BufferPool(10);
    }
    
    void TearDown() override {
        delete buffer_pool_;
    }
    
    BufferPool* buffer_pool_;
};

TEST_F(BufferPoolTest, GetPage) {
    Page* page = buffer_pool_->getPage(1, 0);
    ASSERT_NE(page, nullptr);
    ASSERT_EQ(page->getPageId(), 0);
    
    buffer_pool_->unpinPage(page);
}

TEST_F(BufferPoolTest, CreatePage) {
    uint32_t page_id;
    Page* page;
    
    Status status = buffer_pool_->createPage(1, page_id, page);
    ASSERT_TRUE(status.ok());
    ASSERT_NE(page, nullptr);
    ASSERT_EQ(page_id, 0);
    
    buffer_pool_->unpinPage(page);
}

TEST_F(BufferPoolTest, MultiplePages) {
    std::vector<Page*> pages;
    
    for (int i = 0; i < 15; i++) {
        uint32_t page_id;
        Page* page;
        Status status = buffer_pool_->createPage(1, page_id, page);
        ASSERT_TRUE(status.ok());
        pages.push_back(page);
    }
    
    for (auto* page : pages) {
        buffer_pool_->unpinPage(page);
    }
}

TEST_F(BufferPoolTest, PagePinning) {
    Page* page = buffer_pool_->getPage(1, 0);
    ASSERT_NE(page, nullptr);
    
    buffer_pool_->unpinPage(page);
    
    Page* same_page = buffer_pool_->getPage(1, 0);
    ASSERT_EQ(same_page, page);
    
    buffer_pool_->unpinPage(same_page);
}

TEST_F(BufferPoolTest, DirtyPages) {
    uint32_t page_id;
    Page* page;
    buffer_pool_->createPage(1, page_id, page);
    
    buffer_pool_->markDirty(page);
    buffer_pool_->unpinPage(page);
    
    Status status = buffer_pool_->flushPage(1, page_id);
    ASSERT_TRUE(status.ok());
}

TEST_F(BufferPoolTest, ConcurrentAccess) {
    std::vector<std::thread> threads;
    
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([this, i]() {
            for (int j = 0; j < 100; j++) {
                Page* page = buffer_pool_->getPage(1, j % 10);
                if (page) {
                    buffer_pool_->unpinPage(page);
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
}

TEST_F(BufferPoolTest, HitRate) {
    for (int i = 0; i < 100; i++) {
        Page* page = buffer_pool_->getPage(1, i % 20);
        if (page) {
            buffer_pool_->unpinPage(page);
        }
    }
    
    double hit_rate = buffer_pool_->getHitRate();
    std::cout << "Hit rate: " << hit_rate << std::endl;
    ASSERT_GE(hit_rate, 0.0);
    ASSERT_LE(hit_rate, 1.0);
}