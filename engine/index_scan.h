// engine/index_scan.h
#ifndef INDEX_SCAN_H
#define INDEX_SCAN_H

#include "../index/index_manager.h"
#include "../include/types.h"
#include "../include/status.h"
#include <vector>
#include <memory>

namespace orangesql {

class IndexScan {
public:
    IndexScan(Index* index, const Value& start_key, const Value& end_key);
    ~IndexScan();
    
    Status init();
    bool next(uint64_t& row_id);
    void reset();
    size_t getCount() const { return row_ids_.size(); }
    bool isInitialized() const { return initialized_; }
    
private:
    Index* index_;
    Value start_key_;
    Value end_key_;
    std::vector<uint64_t> row_ids_;
    size_t current_pos_;
    bool initialized_;
    bool range_scan_;
};

}

#endif