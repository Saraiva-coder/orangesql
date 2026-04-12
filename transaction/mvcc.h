// transaction/mvcc.h
#ifndef MVCC_H
#define MVCC_H

#include "../include/types.h"
#include "../include/status.h"
#include "transaction_manager.h"
#include <unordered_map>
#include <shared_mutex>
#include <memory>

namespace orangesql {

struct Version {
    uint64_t transaction_id;
    uint64_t begin_ts;
    uint64_t end_ts;
    Row data;
    std::unique_ptr<Version> next;
    bool is_deleted;
    
    Version() : transaction_id(0), begin_ts(0), end_ts(UINT64_MAX), is_deleted(false) {}
};

class MVCCManager {
public:
    MVCCManager();
    ~MVCCManager();
    
    Status beginTransaction(uint64_t& transaction_id);
    Status commitTransaction(uint64_t transaction_id);
    Status abortTransaction(uint64_t transaction_id);
    
    Status insertRecord(uint64_t transaction_id, const std::string& table, const Row& row);
    Status selectRecord(uint64_t transaction_id, const std::string& table, uint64_t row_id, Row& row);
    Status updateRecord(uint64_t transaction_id, const std::string& table, uint64_t row_id, const Row& new_row);
    Status deleteRecord(uint64_t transaction_id, const std::string& table, uint64_t row_id);
    
    uint64_t getCurrentTimestamp();
    void shutdown();
    
private:
    struct TableVersions {
        std::unordered_map<uint64_t, std::unique_ptr<Version>> versions;
        std::shared_mutex mutex;
    };
    
    std::unordered_map<std::string, TableVersions> tables_;
    std::shared_mutex global_mutex_;
    std::atomic<uint64_t> next_transaction_id_;
    std::atomic<uint64_t> global_timestamp_;
    
    bool isVisible(uint64_t transaction_id, const Version* version);
    void garbageCollect();
    Version* getLatestVersion(const std::string& table, uint64_t row_id);
};

}

#endif