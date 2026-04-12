// transaction/lock_manager.cpp
#include "lock_manager.h"
#include "../include/logger.h"
#include <chrono>
#include <thread>

namespace orangesql {

LockManager& LockManager::getInstance() {
    static LockManager instance;
    return instance;
}

LockManager::LockManager() : deadlock_timeout_ms_(5000), lock_timeout_ms_(10000) {}

LockManager::~LockManager() {}

Status LockManager::acquireLock(uint64_t transaction_id, uint64_t resource_id, LockType type) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = locks_.find(resource_id);
    if (it == locks_.end()) {
        Lock new_lock(resource_id);
        LockRequest request(transaction_id, type);
        request.granted = true;
        new_lock.queue.push_back(request);
        new_lock.holders[transaction_id] = type;
        locks_[resource_id] = new_lock;
        transaction_locks_[transaction_id].push_back(resource_id);
        return Status::OK();
    }
    
    Lock& lock_obj = it->second;
    
    auto holder_it = lock_obj.holders.find(transaction_id);
    if (holder_it != lock_obj.holders.end()) {
        if (holder_it->second == type) {
            return Status::OK();
        }
        
        if (holder_it->second == LockType::SHARED && type == LockType::EXCLUSIVE) {
            upgradeLock(lock_obj, transaction_id);
            return Status::OK();
        }
    }
    
    if (isCompatible(lock_obj.granted_type, type) && lock_obj.queue.empty()) {
        LockRequest request(transaction_id, type);
        request.granted = true;
        lock_obj.queue.push_back(request);
        lock_obj.holders[transaction_id] = type;
        transaction_locks_[transaction_id].push_back(resource_id);
        return Status::OK();
    }
    
    if (detectDeadlock(transaction_id, resource_id)) {
        return Status::Error("Deadlock detected");
    }
    
    LockRequest request(transaction_id, type);
    request.request_time = std::chrono::steady_clock::now();
    lock_obj.queue.push_back(request);
    
    lock.unlock();
    
    auto start = std::chrono::steady_clock::now();
    bool granted = false;
    
    while (!granted) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        lock.lock();
        
        auto& current_lock = locks_[resource_id];
        for (auto& req : current_lock.queue) {
            if (req.transaction_id == transaction_id && req.granted) {
                granted = true;
                break;
            }
        }
        
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
        
        if (elapsed > lock_timeout_ms_) {
            lock.unlock();
            return Status::Error("Lock timeout");
        }
        
        lock.unlock();
    }
    
    return Status::OK();
}

Status LockManager::releaseLock(uint64_t transaction_id, uint64_t resource_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = locks_.find(resource_id);
    if (it == locks_.end()) {
        return Status::NotFound("Lock not found");
    }
    
    Lock& lock_obj = it->second;
    lock_obj.holders.erase(transaction_id);
    
    auto& txn_locks = transaction_locks_[transaction_id];
    txn_locks.erase(std::remove(txn_locks.begin(), txn_locks.end(), resource_id), txn_locks.end());
    
    auto queue_it = lock_obj.queue.begin();
    while (queue_it != lock_obj.queue.end()) {
        if (queue_it->transaction_id == transaction_id) {
            queue_it = lock_obj.queue.erase(queue_it);
        } else {
            ++queue_it;
        }
    }
    
    if (lock_obj.holders.empty()) {
        locks_.erase(it);
    } else {
        lock_obj.granted_type = LockType::SHARED;
        for (const auto& holder : lock_obj.holders) {
            if (holder.second == LockType::EXCLUSIVE) {
                lock_obj.granted_type = LockType::EXCLUSIVE;
                break;
            }
        }
        
        for (auto& request : lock_obj.queue) {
            if (isCompatible(lock_obj.granted_type, request.type) && !request.granted) {
                request.granted = true;
                lock_obj.holders[request.transaction_id] = request.type;
                
                if (request.type == LockType::EXCLUSIVE) {
                    lock_obj.granted_type = LockType::EXCLUSIVE;
                    break;
                }
            }
        }
    }
    
    return Status::OK();
}

Status LockManager::releaseAllLocks(uint64_t transaction_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = transaction_locks_.find(transaction_id);
    if (it != transaction_locks_.end()) {
        std::vector<uint64_t> resources = it->second;
        for (uint64_t resource : resources) {
            auto lock_it = locks_.find(resource);
            if (lock_it != locks_.end()) {
                lock_it->second.holders.erase(transaction_id);
                
                if (lock_it->second.holders.empty()) {
                    locks_.erase(lock_it);
                }
            }
        }
        transaction_locks_.erase(it);
    }
    
    return Status::OK();
}

bool LockManager::isLocked(uint64_t resource_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return locks_.find(resource_id) != locks_.end();
}

LockType LockManager::getLockType(uint64_t transaction_id, uint64_t resource_id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = locks_.find(resource_id);
    if (it != locks_.end()) {
        auto holder_it = it->second.holders.find(transaction_id);
        if (holder_it != it->second.holders.end()) {
            return holder_it->second;
        }
    }
    return LockType::SHARED;
}

void LockManager::setDeadlockTimeout(int milliseconds) {
    deadlock_timeout_ms_ = milliseconds;
}

void LockManager::setLockTimeout(int milliseconds) {
    lock_timeout_ms_ = milliseconds;
}

bool LockManager::detectDeadlock(uint64_t transaction_id, uint64_t resource_id) {
    auto it = locks_.find(resource_id);
    if (it == locks_.end()) return false;
    
    std::vector<uint64_t> waiting_for;
    
    for (const auto& holder : it->second.holders) {
        if (holder.first != transaction_id) {
            waiting_for.push_back(holder.first);
        }
    }
    
    for (uint64_t waiter : waiting_for) {
        auto waiter_it = transaction_locks_.find(waiter);
        if (waiter_it != transaction_locks_.end()) {
            for (uint64_t res : waiter_it->second) {
                auto res_it = locks_.find(res);
                if (res_it != locks_.end()) {
                    if (res_it->second.holders.find(transaction_id) != res_it->second.holders.end()) {
                        return true;
                    }
                }
            }
        }
    }
    
    return false;
}

bool LockManager::isCompatible(LockType held, LockType requested) {
    if (held == LockType::EXCLUSIVE) return false;
    if (requested == LockType::EXCLUSIVE) return false;
    if (held == LockType::UPDATE && requested == LockType::EXCLUSIVE) return false;
    return true;
}

void LockManager::grantLock(Lock& lock, LockRequest& request) {
    request.granted = true;
    lock.holders[request.transaction_id] = request.type;
    
    if (request.type == LockType::EXCLUSIVE) {
        lock.granted_type = LockType::EXCLUSIVE;
    }
}

void LockManager::upgradeLock(Lock& lock, uint64_t transaction_id) {
    lock.holders[transaction_id] = LockType::EXCLUSIVE;
    lock.granted_type = LockType::EXCLUSIVE;
}

}