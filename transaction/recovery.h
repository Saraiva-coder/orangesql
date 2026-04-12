// transaction/recovery.h
#ifndef RECOVERY_H
#define RECOVERY_H

#include "../include/status.h"
#include "wal.h"
#include <vector>
#include <functional>

namespace orangesql {

class RecoveryManager {
public:
    static RecoveryManager& getInstance();
    
    Status recover(std::function<Status(const WALRecord&)> replay_func);
    Status analyze();
    Status getRecoveryStats(uint64_t& recovered_txns, uint64_t& rolled_back_txns);
    
private:
    RecoveryManager() = default;
    ~RecoveryManager() = default;
    
    struct TransactionStatus {
        uint64_t id;
        bool committed;
        bool prepared;
        uint64_t last_lsn;
    };
};

}

#endif