// transaction/wal.h
#ifndef WAL_H
#define WAL_H

#include "../include/types.h"
#include "../include/status.h"
#include <vector>
#include <string>
#include <mutex>
#include <fstream>
#include <atomic>

namespace orangesql {

enum class WALRecordType {
    BEGIN,
    COMMIT,
    ABORT,
    INSERT,
    UPDATE,
    DELETE,
    CHECKPOINT,
    PREPARE
};

struct WALRecord {
    WALRecordType type;
    uint64_t lsn;
    uint64_t transaction_id;
    uint64_t timestamp;
    uint32_t page_id;
    uint32_t offset;
    std::vector<char> old_data;
    std::vector<char> new_data;
    std::string table_name;
    uint64_t prev_lsn;
    
    WALRecord() : lsn(0), transaction_id(0), timestamp(0), page_id(0), offset(0), prev_lsn(0) {}
    
    std::vector<char> serialize() const;
    static WALRecord deserialize(const std::vector<char>& data);
    size_t getSize() const;
};

class WALManager {
public:
    static WALManager& getInstance();
    
    Status init(const std::string& wal_dir);
    Status appendRecord(const WALRecord& record);
    Status flush();
    Status truncate(uint64_t lsn);
    
    uint64_t getLastLSN() const { return last_lsn_; }
    uint64_t getFlushedLSN() const { return flushed_lsn_; }
    
    Status recover(std::function<Status(const WALRecord&)> replay_func);
    
private:
    WALManager();
    ~WALManager();
    
    std::string wal_dir_;
    std::ofstream wal_file_;
    std::atomic<uint64_t> last_lsn_;
    std::atomic<uint64_t> flushed_lsn_;
    uint64_t current_segment_;
    std::mutex mutex_;
    
    Status openSegment();
    Status rotateSegment();
    std::string getSegmentPath(uint64_t segment_id);
    Status readSegment(uint64_t segment_id, std::vector<WALRecord>& records);
};

}

#endif