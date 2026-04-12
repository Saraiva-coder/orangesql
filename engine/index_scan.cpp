// engine/index_scan.cpp
#include "index_scan.h"

namespace orangesql {

IndexScan::IndexScan(Index* index, const Value& start_key, const Value& end_key)
    : index_(index), start_key_(start_key), end_key_(end_key), 
      current_pos_(0), initialized_(false), range_scan_(false) {
    
    if (start_key_.type != DataType::TEXT || end_key_.type != DataType::TEXT) {
        range_scan_ = (start_key_.toString() != end_key_.toString());
    } else {
        range_scan_ = (start_key_.str_val != end_key_.str_val);
    }
}

IndexScan::~IndexScan() {}

Status IndexScan::init() {
    if (!index_) {
        return Status::Error("Index is null");
    }
    
    if (range_scan_) {
        Status status = index_->searchRange(start_key_, end_key_, row_ids_);
        if (!status.ok()) {
            return status;
        }
    } else {
        Status status = index_->search(start_key_, row_ids_);
        if (!status.ok()) {
            return status;
        }
    }
    
    initialized_ = true;
    current_pos_ = 0;
    
    return Status::OK();
}

bool IndexScan::next(uint64_t& row_id) {
    if (!initialized_) {
        return false;
    }
    
    if (current_pos_ >= row_ids_.size()) {
        return false;
    }
    
    row_id = row_ids_[current_pos_];
    current_pos_++;
    
    return true;
}

void IndexScan::reset() {
    current_pos_ = 0;
}

}