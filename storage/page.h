// storage/page.h
#ifndef PAGE_H
#define PAGE_H

#include "../include/types.h"
#include "../include/constants.h"
#include "../include/status.h"
#include <vector>
#include <cstring>

namespace orangesql {

struct PageHeader {
    uint32_t page_id;
    uint32_t next_page_id;
    uint32_t prev_page_id;
    uint32_t free_space_offset;
    uint32_t record_count;
    uint32_t transaction_id;
    uint32_t checksum;
    bool dirty;
    bool pinned;
    
    PageHeader() : page_id(0), next_page_id(0), prev_page_id(0), 
                   free_space_offset(sizeof(PageHeader)), record_count(0),
                   transaction_id(0), checksum(0), dirty(false), pinned(false) {}
};

class Page {
public:
    Page(uint32_t page_id);
    ~Page();
    
    uint32_t getPageId() const { return header_.page_id; }
    uint32_t getFreeSpace() const { return PAGE_SIZE - header_.free_space_offset; }
    bool hasSpace(uint32_t size) const { return getFreeSpace() >= size + sizeof(uint32_t); }
    
    Status insertRecord(const std::vector<char>& data, uint32_t& offset);
    Status getRecord(uint32_t offset, std::vector<char>& data);
    Status updateRecord(uint32_t offset, const std::vector<char>& data);
    Status deleteRecord(uint32_t offset);
    void* getData() { return data_; }
    const void* getData() const { return data_; }
    
    void markDirty() { header_.dirty = true; }
    bool isDirty() const { return header_.dirty; }
    void clearDirty() { header_.dirty = false; }
    
    void pin() { header_.pinned = true; }
    void unpin() { header_.pinned = false; }
    bool isPinned() const { return header_.pinned; }
    
    uint32_t getRecordCount() const { return header_.record_count; }
    void setTransactionId(uint32_t tid) { header_.transaction_id = tid; }
    uint32_t getTransactionId() const { return header_.transaction_id; }
    
    uint32_t computeChecksum();
    bool verifyChecksum();
    
private:
    PageHeader header_;
    char data_[PAGE_SIZE];
    
    void updateChecksum();
};

}

#endif