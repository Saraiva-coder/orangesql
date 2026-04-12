// transaction/checkpoint.cpp
#include "checkpoint.h"
#include "wal.h"
#include "transaction_manager.h"
#include "../include/logger.h"
#include <fstream>
#include <nlohmann/json.hpp>

namespace orangesql {

CheckpointManager& CheckpointManager::getInstance() {
    static CheckpointManager instance;
    return instance;
}

CheckpointManager::CheckpointManager() : running_(false), interval_ms_(1000) {}

CheckpointManager::~CheckpointManager() {
    shutdown();
}

Status CheckpointManager::init(int interval_ms) {
    interval_ms_ = interval_ms;
    running_ = true;
    checkpoint_thread_ = std::thread(&CheckpointManager::checkpointLoop, this);
    return Status::OK();
}

Status CheckpointManager::shutdown() {
    running_ = false;
    if (checkpoint_thread_.joinable()) {
        checkpoint_thread_.join();
    }
    return Status::OK();
}

void CheckpointManager::checkpointLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
        if (running_) {
            createCheckpoint();
        }
    }
}

Status CheckpointManager::createCheckpoint() {
    WALManager::getInstance().flush();
    
    uint64_t last_lsn = WALManager::getInstance().getLastLSN();
    auto active_txns = TransactionManager::getInstance().getActiveTransactions();
    
    Status status = writeCheckpointInfo();
    if (!status.ok()) {
        LOG_ERROR("Failed to write checkpoint");
        return status;
    }
    
    WALRecord record;
    record.type = WALRecordType::CHECKPOINT;
    record.lsn = last_lsn;
    WALManager::getInstance().appendRecord(record);
    
    LOG_INFO("Checkpoint created at LSN " + std::to_string(last_lsn));
    
    return Status::OK();
}

Status CheckpointManager::writeCheckpointInfo() {
    nlohmann::json json;
    json["last_lsn"] = WALManager::getInstance().getLastLSN();
    
    auto active_txns = TransactionManager::getInstance().getActiveTransactions();
    for (auto txn : active_txns) {
        json["active_transactions"].push_back(txn);
    }
    
    std::ofstream file("data/system/checkpoint.json");
    if (!file.is_open()) {
        return Status::Error("Cannot open checkpoint file");
    }
    
    file << json.dump(4);
    return Status::OK();
}

Status CheckpointManager::readCheckpointInfo(uint64_t& last_lsn, std::vector<uint64_t>& active_txns) {
    std::ifstream file("data/system/checkpoint.json");
    if (!file.is_open()) {
        return Status::NotFound("Checkpoint not found");
    }
    
    nlohmann::json json;
    file >> json;
    
    last_lsn = json.value("last_lsn", 0);
    
    if (json.contains("active_transactions")) {
        for (const auto& txn : json["active_transactions"]) {
            active_txns.push_back(txn);
        }
    }
    
    return Status::OK();
}

Status CheckpointManager::recoverFromCheckpoint() {
    uint64_t last_lsn;
    std::vector<uint64_t> active_txns;
    
    Status status = readCheckpointInfo(last_lsn, active_txns);
    if (!status.ok()) {
        return Status::OK();
    }
    
    for (uint64_t txn : active_txns) {
        TransactionManager::getInstance().abortTransaction(txn);
    }
    
    LOG_INFO("Recovered from checkpoint, aborted " + std::to_string(active_txns.size()) + " transactions");
    
    return Status::OK();
}

void CheckpointManager::setInterval(int interval_ms) {
    interval_ms_ = interval_ms;
}

}