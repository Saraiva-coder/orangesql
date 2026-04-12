// tests/integration/test_recovery.cpp
#include <gtest/gtest.h>
#include "../../transaction/recovery.h"
#include "../../transaction/wal.h"
#include "../../transaction/checkpoint.h"
#include "../../include/logger.h"
#include <filesystem>

using namespace orangesql;

class RecoveryIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        Logger::getInstance().setLevel(LogLevel::ERROR);
        
        if (std::filesystem::exists("test_recovery")) {
            std::filesystem::remove_all("test_recovery");
        }
        std::filesystem::create_directories("test_recovery");
        
        WALManager::getInstance().init("test_recovery");
        CheckpointManager::getInstance().init(1000);
    }
    
    void TearDown() override {
        CheckpointManager::getInstance().shutdown();
        std::filesystem::remove_all("test_recovery");
    }
};

TEST_F(RecoveryIntegrationTest, BasicRecovery) {
    std::vector<WALRecord> recovered_records;
    
    for (int i = 0; i < 100; i++) {
        WALRecord record;
        record.type = WALRecordType::INSERT;
        record.transaction_id = i;
        record.table_name = "test";
        
        WALManager::getInstance().appendRecord(record);
        
        if (i % 10 == 0) {
            WALRecord commit;
            commit.type = WALRecordType::COMMIT;
            commit.transaction_id = i;
            WALManager::getInstance().appendRecord(commit);
        }
    }
    
    WALManager::getInstance().flush();
    CheckpointManager::getInstance().createCheckpoint();
    
    auto replay_func = [&recovered_records](const WALRecord& record) -> Status {
        recovered_records.push_back(record);
        return Status::OK();
    };
    
    Status status = RecoveryManager::getInstance().recover(replay_func);
    ASSERT_TRUE(status.ok());
}

TEST_F(RecoveryIntegrationTest, CrashRecovery) {
    for (int i = 0; i < 50; i++) {
        WALRecord record;
        record.type = WALRecordType::BEGIN;
        record.transaction_id = i;
        WALManager::getInstance().appendRecord(record);
        
        record.type = WALRecordType::INSERT;
        WALManager::getInstance().appendRecord(record);
        
        if (i % 2 == 0) {
            record.type = WALRecordType::COMMIT;
            WALManager::getInstance().appendRecord(record);
        }
    }
    
    WALManager::getInstance().flush();
    
    std::vector<WALRecord> recovered;
    auto replay_func = [&recovered](const WALRecord& record) -> Status {
        recovered.push_back(record);
        return Status::OK();
    };
    
    RecoveryManager::getInstance().recover(replay_func);
    
    int committed = 0;
    for (const auto& rec : recovered) {
        if (rec.type == WALRecordType::COMMIT) {
            committed++;
        }
    }
    
    ASSERT_EQ(committed, 25);
}