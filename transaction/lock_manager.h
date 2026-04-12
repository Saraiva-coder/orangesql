// transaction/lock_manager.h
#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include "../include/types.h"
#include "../include/status.h"
#include <unordered_map>
#include <queue>
#include <shared_mutex>
#include <condition_variable>

namespace orangesql {

enum class LockType {
    SHARED,
    EXCLUSIVE,
    UPDATE
};

struct LockRequest {
    uint64_t transaction_id;
    LockType type;
    bool granted;
    std::chrono::steady_clock::time_point request_time;
    
    LockRequest() : transaction_id(0), type(LockType::SHARED), granted(false) {}
    LockRequest(uint64_t tid, LockType t) : transaction_id(tid), type(t), granted(false) {}
};

struct Lock {
    uint64_t resource_id;
    LockType granted_type;
    std::vector<LockRequest> queue;
    std::unordered_map<uint64_t, LockType> holders;
    
    Lock() : resource_id(0), granted_type(LockType::SHARED) {}
    explicit Lock(uint64_t rid) : resource_id(rid), granted_type(LockType::SHARED) {}
};

class LockManager {
public:
    static LockManager& getInstance();
    
    Status acquireLock(uint64_t transaction_id, uint64_t resource_id, LockType type);
    Status releaseLock(uint64_t transaction_id, uint64_t resource_id);
    Status releaseAllLocks(uint64_t transaction_id);
    
    bool isLocked(uint64_t resource_id);
    LockType getLockType(uint64_t transaction_id, uint64_t resource_id);
    
    void setDeadlockTimeout(int milliseconds);
    void setLockTimeout(int milliseconds);
    
private:
    LockManager();
    ~LockManager();
    
    std::unordered_map<uint64_t, Lock> locks_;
    std::unordered_map<uint64_t, std::vector<uint64_t>> transaction_locks_;
    std::shared_mutex mutex_;
    
    int deadlock_timeout_ms_;
    int lock_timeout_ms_;
    
    bool detectDeadlock(uint64_t transaction_id, uint64_t resource_id);
    bool isCompatible(LockType held, LockType requested);
    void grantLock(Lock& lock, LockRequest& request);
    void upgradeLock(Lock& lock, uint64_t transaction_id);
    bool waitForLock(Lock& lock, uint64_t transaction_id);
};

}

#endif