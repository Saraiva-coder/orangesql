// transaction/mvcc.cpp
#include "mvcc.h"
#include "lock_manager.h"
#include "../include/logger.h"
#include <algorithm>

namespace orangesql {

MVCCManager::MVCCManager() : next_transaction_id_(1), global_timestamp_(1) {}

MVCCManager::~MVCCManager() {
    shutdown();
}

Status MVCCManager::beginTransaction(uint64_t& transaction_id) {
    transaction_id = TransactionManager::getInstance().beginTransaction();
    return Status::OK();
}

Status MVCCManager::commitTransaction(uint64_t transaction_id) {
    LockManager::getInstance().releaseAllLocks(transaction_id);
    
    std::unique_lock<std::shared_mutex> lock(global_mutex_);
    
    for (auto& table_pair : tables_) {
        std::unique_lock<std::shared_mutex> table_lock(table_pair.second.mutex);
        
        for (auto& version_pair : table_pair.second.versions) {
            Version* version = version_pair.second.get();
            while (version) {
                if (version->transaction_id == transaction_id) {
                    version->end_ts = getCurrentTimestamp();
                    break;
                }
                version = version->next.get();
            }
        }
    }
    
    TransactionManager::getInstance().commitTransaction(transaction_id);
    garbageCollect();
    
    return Status::OK();
}

Status MVCCManager::abortTransaction(uint64_t transaction_id) {
    LockManager::getInstance().releaseAllLocks(transaction_id);
    
    std::unique_lock<std::shared_mutex> lock(global_mutex_);
    
    for (auto& table_pair : tables_) {
        std::unique_lock<std::shared_mutex> table_lock(table_pair.second.mutex);
        
        auto it = table_pair.second.versions.begin();
        while (it != table_pair.second.versions.end()) {
            Version* version = it->second.get();
            if (version->transaction_id == transaction_id) {
                if (version->next) {
                    it->second = std::move(version->next);
                } else {
                    it = table_pair.second.versions.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }
    
    TransactionManager::getInstance().abortTransaction(transaction_id);
    
    return Status::OK();
}

Status MVCCManager::insertRecord(uint64_t transaction_id, const std::string& table, const Row& row) {
    LockManager::getInstance().acquireLock(transaction_id, 
        std::hash<std::string>{}(table + std::to_string(row.row_id)), 
        LockType::EXCLUSIVE);
    
    auto& table_versions = tables_[table];
    std::unique_lock<std::shared_mutex> lock(table_versions.mutex);
    
    auto version = std::make_unique<Version>();
    version->transaction_id = transaction_id;
    version->begin_ts = getCurrentTimestamp();
    version->end_ts = UINT64_MAX;
    version->data = row;
    version->is_deleted = false;
    
    table_versions.versions[row.row_id] = std::move(version);
    
    return Status::OK();
}

Status MVCCManager::selectRecord(uint64_t transaction_id, const std::string& table, 
                                  uint64_t row_id, Row& row) {
    LockManager::getInstance().acquireLock(transaction_id,
        std::hash<std::string>{}(table + std::to_string(row_id)),
        LockType::SHARED);
    
    auto it = tables_.find(table);
    if (it == tables_.end()) {
        return Status::NotFound("Table not found");
    }
    
    auto& table_versions = it->second;
    std::shared_lock<std::shared_mutex> lock(table_versions.mutex);
    
    auto version_it = table_versions.versions.find(row_id);
    if (version_it == table_versions.versions.end()) {
        return Status::NotFound("Row not found");
    }
    
    Version* version = version_it->second.get();
    while (version) {
        if (isVisible(transaction_id, version) && !version->is_deleted) {
            row = version->data;
            row.row_id = row_id;
            row.transaction_id = version->transaction_id;
            return Status::OK();
        }
        version = version->next.get();
    }
    
    return Status::NotFound("No visible version found");
}

Status MVCCManager::updateRecord(uint64_t transaction_id, const std::string& table,
                                  uint64_t row_id, const Row& new_row) {
    LockManager::getInstance().acquireLock(transaction_id,
        std::hash<std::string>{}(table + std::to_string(row_id)),
        LockType::EXCLUSIVE);
    
    auto& table_versions = tables_[table];
    std::unique_lock<std::shared_mutex> lock(table_versions.mutex);
    
    auto version_it = table_versions.versions.find(row_id);
    if (version_it == table_versions.versions.end()) {
        return Status::NotFound("Row not found");
    }
    
    Version* current = version_it->second.get();
    Version* latest = nullptr;
    
    while (current) {
        if (isVisible(transaction_id, current)) {
            latest = current;
        }
        current = current->next.get();
    }
    
    if (!latest) {
        return Status::NotFound("No visible version found");
    }
    
    auto new_version = std::make_unique<Version>();
    new_version->transaction_id = transaction_id;
    new_version->begin_ts = getCurrentTimestamp();
    new_version->end_ts = UINT64_MAX;
    new_version->data = new_row;
    new_version->is_deleted = false;
    new_version->next = std::move(table_versions.versions[row_id]);
    
    table_versions.versions[row_id] = std::move(new_version);
    
    return Status::OK();
}

Status MVCCManager::deleteRecord(uint64_t transaction_id, const std::string& table,
                                  uint64_t row_id) {
    LockManager::getInstance().acquireLock(transaction_id,
        std::hash<std::string>{}(table + std::to_string(row_id)),
        LockType::EXCLUSIVE);
    
    auto& table_versions = tables_[table];
    std::unique_lock<std::shared_mutex> lock(table_versions.mutex);
    
    auto version_it = table_versions.versions.find(row_id);
    if (version_it == table_versions.versions.end()) {
        return Status::NotFound("Row not found");
    }
    
    auto new_version = std::make_unique<Version>();
    new_version->transaction_id = transaction_id;
    new_version->begin_ts = getCurrentTimestamp();
    new_version->end_ts = UINT64_MAX;
    new_version->is_deleted = true;
    new_version->next = std::move(version_it->second);
    
    table_versions.versions[row_id] = std::move(new_version);
    
    return Status::OK();
}

bool MVCCManager::isVisible(uint64_t transaction_id, const Version* version) {
    uint64_t begin_ts = version->begin_ts;
    uint64_t end_ts = version->end_ts;
    
    if (version->transaction_id == transaction_id) {
        return true;
    }
    
    Transaction* txn = TransactionManager::getInstance().getTransaction(transaction_id);
    if (!txn) return false;
    
    if (txn->isolation_level == IsolationLevel::READ_UNCOMMITTED) {
        return !version->is_deleted;
    }
    
    if (txn->isolation_level == IsolationLevel::READ_COMMITTED) {
        return end_ts > getCurrentTimestamp() && !version->is_deleted;
    }
    
    if (txn->isolation_level == IsolationLevel::REPEATABLE_READ ||
        txn->isolation_level == IsolationLevel::SERIALIZABLE) {
        return begin_ts <= txn->start_time.time_since_epoch().count() && 
               end_ts > txn->start_time.time_since_epoch().count() &&
               !version->is_deleted;
    }
    
    return false;
}

void MVCCManager::garbageCollect() {
    uint64_t oldest_active = UINT64_MAX;
    auto active_txns = TransactionManager::getInstance().getActiveTransactions();
    
    for (uint64_t txn : active_txns) {
        Transaction* t = TransactionManager::getInstance().getTransaction(txn);
        if (t) {
            uint64_t ts = t->start_time.time_since_epoch().count();
            oldest_active = std::min(oldest_active, ts);
        }
    }
    
    if (oldest_active == UINT64_MAX) return;
    
    for (auto& table_pair : tables_) {
        std::unique_lock<std::shared_mutex> lock(table_pair.second.mutex);
        
        for (auto& version_pair : table_pair.second.versions) {
            Version* version = version_pair.second.get();
            Version* prev = nullptr;
            
            while (version && version->end_ts < oldest_active) {
                if (prev) {
                    prev->next = std::move(version->next);
                    version = prev->next.get();
                } else {
                    version_pair.second = std::move(version->next);
                    version = version_pair.second.get();
                    prev = nullptr;
                }
            }
            
            while (version) {
                prev = version;
                version = version->next.get();
            }
        }
    }
}

uint64_t MVCCManager::getCurrentTimestamp() {
    return ++global_timestamp_;
}

void MVCCManager::shutdown() {
    garbageCollect();
}

}