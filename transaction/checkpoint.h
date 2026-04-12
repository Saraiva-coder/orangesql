// transaction/checkpoint.h
#ifndef CHECKPOINT_H
#define CHECKPOINT_H

#include "../include/status.h"
#include <atomic>
#include <thread>

namespace orangesql {

class CheckpointManager {
public:
    static CheckpointManager& getInstance();
    
    Status init(int interval_ms);
    Status shutdown();
    Status createCheckpoint();
    Status recoverFromCheckpoint();
    
    void setInterval(int interval_ms);
    bool isRunning() const { return running_; }
    
private:
    CheckpointManager();
    ~CheckpointManager();
    
    std::thread checkpoint_thread_;
    std::atomic<bool> running_;
    int interval_ms_;
    
    void checkpointLoop();
    Status writeCheckpointInfo();
    Status readCheckpointInfo(uint64_t& last_lsn, std::vector<uint64_t>& active_txns);
};

}

#endif