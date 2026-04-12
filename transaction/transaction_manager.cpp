// transaction/transaction_manager.cpp
#include "transaction_manager.h"
#include "wal.h"
#include "../include/logger.h"
#include <algorithm>

namespace orangesql {

TransactionManager& TransactionManager::getInstance() {
    static TransactionManager instance;
    return instance;
}

TransactionManager::TransactionManager() 
    : next_transaction_id_(1), default_isolation_level_(IsolationLevel::REPEATABLE_READ) {}

TransactionManager::~TransactionManager() {}

uint64_t TransactionManager::beginTransaction(IsolationLevel level) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    uint64_t tid = next_transaction_id_++;
    
    Transaction tx;
    tx.id = tid;
    tx.state = TransactionState::ACTIVE;
    tx.isolation_level = level;
    tx.start_time = std::chrono::system_clock::now();
    
    transactions_[tid] = tx;
    
    logTransactionEvent(tid, "BEGIN");
    LOG_INFO("Transaction " + std::to_string(tid) + " started");
    
    return tid;
}

Status TransactionManager::commitTransaction(uint64_t transaction_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = transactions_.find(transaction_id);
    if (it == transactions_.end()) {
        return Status::NotFound("Transaction not found");
    }
    
    if (it->second.state != TransactionState::ACTIVE &&
        it->second.state != TransactionState::PREPARED) {
        return Status::Error("Transaction cannot be committed");
    }
    
    it->second.state = TransactionState::COMMITTED;
    it->second.end_time = std::chrono::system_clock::now();
    
    logTransactionEvent(transaction_id, "COMMIT");
    LOG_INFO("Transaction " + std::to_string(transaction_id) + " committed");
    
    cleanupCommittedTransactions();
    
    return Status::OK();
}

Status TransactionManager::abortTransaction(uint64_t transaction_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = transactions_.find(transaction_id);
    if (it == transactions_.end()) {
        return Status::NotFound("Transaction not found");
    }
    
    it->second.state = TransactionState::ABORTED;
    it->second.end_time = std::chrono::system_clock::now();
    
    logTransactionEvent(transaction_id, "ABORT");
    LOG_INFO("Transaction " + std::to_string(transaction_id) + " aborted");
    
    cleanupCommittedTransactions();
    
    return Status::OK();
}

Status TransactionManager::prepareTransaction(uint64_t transaction_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = transactions_.find(transaction_id);
    if (it == transactions_.end()) {
        return Status::NotFound("Transaction not found");
    }
    
    if (it->second.state != TransactionState::ACTIVE) {
        return Status::Error("Transaction cannot be prepared");
    }
    
    it->second.state = TransactionState::PREPARED;
    logTransactionEvent(transaction_id, "PREPARE");
    
    return Status::OK();
}

Transaction* TransactionManager::getTransaction(uint64_t transaction_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = transactions_.find(transaction_id);
    if (it != transactions_.end()) {
        return &it->second;
    }
    return nullptr;
}

bool TransactionManager::isActive(uint64_t transaction_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = transactions_.find(transaction_id);
    if (it != transactions_.end()) {
        return it->second.state == TransactionState::ACTIVE;
    }
    return false;
}

std::vector<uint64_t> TransactionManager::getActiveTransactions() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<uint64_t> active;
    for (const auto& pair : transactions_) {
        if (pair.second.state == TransactionState::ACTIVE) {
            active.push_back(pair.first);
        }
    }
    return active;
}

void TransactionManager::setDefaultIsolationLevel(IsolationLevel level) {
    default_isolation_level_ = level;
}

size_t TransactionManager::getActiveCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    size_t count = 0;
    for (const auto& pair : transactions_) {
        if (pair.second.state == TransactionState::ACTIVE) {
            count++;
        }
    }
    return count;
}

void TransactionManager::cleanupCommittedTransactions() {
    auto now = std::chrono::system_clock::now();
    auto it = transactions_.begin();
    
    while (it != transactions_.end()) {
        if (it->second.state != TransactionState::ACTIVE) {
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                now - it->second.end_time);
            if (duration.count() > 60) {
                it = transactions_.erase(it);
                continue;
            }
        }
        ++it;
    }
}

void TransactionManager::logTransactionEvent(uint64_t transaction_id, const std::string& event) {
    WALRecord record;
    record.type = WALRecordType::BEGIN;
    record.transaction_id = transaction_id;
    record.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    WALManager::getInstance().appendRecord(record);
}

}