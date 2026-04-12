// storage/record.cpp
#include "record.h"

namespace orangesql {

Record::Record() : page_id_(0), offset_(0) {}

Record::Record(uint32_t page_id, uint32_t offset) : page_id_(page_id), offset_(offset) {}

Record::Record(uint64_t row_id) {
    page_id_ = static_cast<uint32_t>(row_id >> 32);
    offset_ = static_cast<uint32_t>(row_id & 0xFFFFFFFF);
}

uint64_t Record::getRowId() const {
    return (static_cast<uint64_t>(page_id_) << 32) | offset_;
}

bool Record::isValid() const {
    return page_id_ != 0 || offset_ != 0;
}

void Record::reset() {
    page_id_ = 0;
    offset_ = 0;
}

bool Record::operator==(const Record& other) const {
    return page_id_ == other.page_id_ && offset_ == other.offset_;
}

bool Record::operator!=(const Record& other) const {
    return !(*this == other);
}

}