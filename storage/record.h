// storage/record.h
#ifndef RECORD_H
#define RECORD_H

#include <cstdint>
#include <vector>

namespace orangesql {

class Record {
public:
    Record();
    Record(uint32_t page_id, uint32_t offset);
    explicit Record(uint64_t row_id);
    
    uint32_t getPageId() const { return page_id_; }
    uint32_t getOffset() const { return offset_; }
    uint64_t getRowId() const;
    bool isValid() const;
    void reset();
    
    bool operator==(const Record& other) const;
    bool operator!=(const Record& other) const;
    
private:
    uint32_t page_id_;
    uint32_t offset_;
};

}

#endif