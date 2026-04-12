// transaction/recovery.cpp
#include "recovery.h"
#include "checkpoint.h"
#include "../include/logger.h"

namespace orangesql {

RecoveryManager& RecoveryManager::getInstance() {
    static RecoveryManager instance;
    return instance;
}

Status RecoveryManager::recover(std::function<Status(const WALRecord&)> replay_func) {
    LOG_INFO("Starting recovery process");
    
    CheckpointManager::getInstance().recoverFromCheckpoint();
    
    std::unordered_map<uint64_t, TransactionStatus> txn_status;
    std::vector<WALRecord> records_to_replay;
    
    auto replay_callback = [&](const WALRecord& record) -> Status {
        if (record.type == WALRecordType::BEGIN) {
            TransactionStatus status;
            status.id = record.transaction_id;
            status.committed = false;
            status.prepared = false;
            status.last_lsn = record.lsn;
            txn_status[record.transaction_id] = status;
        } else if (record.type == WALRecordType::COMMIT) {
            auto it = txn_status.find(record.transaction_id);
            if (it != txn_status.end()) {
                it->second.committed = true;
            }
        } else if (record.type == WALRecordType::PREPARE) {
            auto it = txn_status.find(record.transaction_id);
            if (it != txn_status.end()) {
                it->second.prepared = true;
            }
        } else if (record.type == WALRecordType::INSERT ||
                   record.type == WALRecordType::UPDATE ||
                   record.type == WALRecordType::DELETE) {
            records_to_replay.push_back(record);
        }
        
        return Status::OK();
    };
    
    Status status = WALManager::getInstance().recover(replay_callback);
    if (!status.ok()) {
        LOG_ERROR("Recovery failed: " + status.message());
        return status;
    }
    
    uint64_t recovered = 0;
    uint64_t rolled_back = 0;
    
    for (const auto& record : records_to_replay) {
        auto it = txn_status.find(record.transaction_id);
        if (it != txn_status.end() && it->second.committed) {
            status = replay_func(record);
            if (status.ok()) {
                recovered++;
            }
        }
    }
    
    for (const auto& pair : txn_status) {
        if (!pair.second.committed) {
            rolled_back++;
            LOG_INFO("Rolled back transaction " + std::to_string(pair.first));
        }
    }
    
    LOG_INFO("Recovery complete: " + std::to_string(recovered) + 
             " transactions recovered, " + std::to_string(rolled_back) + " rolled back");
    
    return Status::OK();
}

Status RecoveryManager::analyze() {
    LOG_INFO("Analyzing WAL for recovery");
    return Status::OK();
}

Status RecoveryManager::getRecoveryStats(uint64_t& recovered_txns, uint64_t& rolled_back_txns) {
    recovered_txns = 0;
    rolled_back_txns = 0;
    return Status::OK();
}

}