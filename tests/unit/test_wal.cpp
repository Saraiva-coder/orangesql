// tests/unit/test_wal.cpp
#include <gtest/gtest.h>
#include "../../transaction/wal.h"
#include "../../include/logger.h"
#include <filesystem>
#include <fstream>

using namespace orangesql;

class WALTest : public ::testing::Test {
protected:
    void SetUp() override {
        Logger::getInstance().setLevel(LogLevel::ERROR);
        
        if (std::filesystem::exists("test_wal")) {
            std::filesystem::remove_all("test_wal");
        }
        std::filesystem::create_directories("test_wal");
        
        WALManager::getInstance().init("test_wal");
    }
    
    void TearDown() override {
        std::filesystem::remove_all("test_wal");
    }
};

TEST_F(WALTest, AppendRecord) {
    WALRecord record;
    record.type = WALRecordType::INSERT;
    record.transaction_id = 1;
    record.table_name = "test_table";
    record.old_data = {'a', 'b', 'c'};
    record.new_data = {'d', 'e', 'f'};
    
    Status status = WALManager::getInstance().appendRecord(record);
    ASSERT_TRUE(status.ok());
    ASSERT_GT(WALManager::getInstance().getLastLSN(), 0);
}

TEST_F(WALTest, MultipleRecords) {
    for (int i = 0; i < 100; i++) {
        WALRecord record;
        record.type = WALRecordType::INSERT;
        record.transaction_id = i;
        record.table_name = "test_table_" + std::to_string(i);
        
        Status status = WALManager::getInstance().appendRecord(record);
        ASSERT_TRUE(status.ok());
    }
    
    ASSERT_EQ(WALManager::getInstance().getLastLSN(), 100);
}

TEST_F(WALTest, Flush) {
    for (int i = 0; i < 10; i++) {
        WALRecord record;
        record.type = WALRecordType::INSERT;
        record.transaction_id = i;
        
        WALManager::getInstance().appendRecord(record);
    }
    
    Status status = WALManager::getInstance().flush();
    ASSERT_TRUE(status.ok());
}

TEST_F(WALTest, Recovery) {
    std::vector<WALRecord> recovered;
    
    for (int i = 0; i < 50; i++) {
        WALRecord record;
        record.type = WALRecordType::INSERT;
        record.transaction_id = i;
        record.table_name = "table_" + std::to_string(i);
        
        WALManager::getInstance().appendRecord(record);
    }
    
    WALManager::getInstance().flush();
    
    auto replay_func = [&recovered](const WALRecord& record) -> Status {
        recovered.push_back(record);
        return Status::OK();
    };
    
    Status status = WALManager::getInstance().recover(replay_func);
    ASSERT_TRUE(status.ok());
    ASSERT_GE(recovered.size(), 50);
}