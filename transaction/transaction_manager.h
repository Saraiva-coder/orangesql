// transaction/transaction_manager.h
#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include "../include/types.h"
#include "../include/status.h"
#include <unordered_map>
#include <atomic>
#include <shared_mutex>
#include <chrono>

namespace orangesql {

enum class TransactionState {
    ACTIVE,
    COMMITTED,
    ABORTED,
    PREPARED
};

enum class IsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
};

struct Transaction {
    uint64_t id;
    TransactionState state;
    IsolationLevel isolation_level;
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
    std::vector<uint64_t> write_set;
    std::vector<uint64_t> read_set;
    
    Transaction() : id(0), state(TransactionState::ACTIVE), 
                    isolation_level(IsolationLevel::REPEATABLE_READ) {}
};

class TransactionManager {
public:
    static TransactionManager& getInstance();
    
    uint64_t beginTransaction(IsolationLevel level = IsolationLevel::REPEATABLE_READ);
    Status commitTransaction(uint64_t transaction_id);
    Status abortTransaction(uint64_t transaction_id);
    Status prepareTransaction(uint64_t transaction_id);
    
    Transaction* getTransaction(uint64_t transaction_id);
    bool isActive(uint64_t transaction_id);
    std::vector<uint64_t> getActiveTransactions();
    
    void setDefaultIsolationLevel(IsolationLevel level);
    IsolationLevel getDefaultIsolationLevel() const { return default_isolation_level_; }
    
    size_t getActiveCount() const;
    
private:
    TransactionManager();
    ~TransactionManager();
    
    std::unordered_map<uint64_t, Transaction> transactions_;
    std::shared_mutex mutex_;
    std::atomic<uint64_t> next_transaction_id_;
    IsolationLevel default_isolation_level_;
    
    void cleanupCommittedTransactions();
    void logTransactionEvent(uint64_t transaction_id, const std::string& event);
};

}

#endif