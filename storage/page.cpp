// storage/page.cpp
#include "page.h"
#include <algorithm>
#include <crc32.h>

namespace orangesql {

Page::Page(uint32_t page_id) {
    memset(this, 0, sizeof(Page));
    header_.page_id = page_id;
    header_.free_space_offset = sizeof(PageHeader);
    header_.record_count = 0;
    header_.dirty = false;
    header_.pinned = false;
    updateChecksum();
}

Page::~Page() {}

Status Page::insertRecord(const std::vector<char>& data, uint32_t& offset) {
    uint32_t record_size = static_cast<uint32_t>(data.size());
    
    if (!hasSpace(record_size)) {
        return Status::Error("No space on page");
    }
    
    offset = header_.free_space_offset;
    
    memcpy(data_ + offset, data.data(), record_size);
    
    memcpy(data_ + offset + record_size, &record_size, sizeof(uint32_t));
    
    header_.free_space_offset += record_size + sizeof(uint32_t);
    header_.record_count++;
    header_.dirty = true;
    
    updateChecksum();
    
    return Status::OK();
}

Status Page::getRecord(uint32_t offset, std::vector<char>& data) {
    if (offset >= PAGE_SIZE) {
        return Status::Error("Invalid offset");
    }
    
    uint32_t record_size;
    memcpy(&record_size, data_ + offset, sizeof(uint32_t));
    
    if (record_size == 0 || record_size > PAGE_SIZE) {
        return Status::Error("Corrupted record");
    }
    
    data.resize(record_size);
    memcpy(data.data(), data_ + offset + sizeof(uint32_t), record_size);
    
    return Status::OK();
}

Status Page::updateRecord(uint32_t offset, const std::vector<char>& data) {
    if (offset >= PAGE_SIZE) {
        return Status::Error("Invalid offset");
    }
    
    uint32_t old_size;
    memcpy(&old_size, data_ + offset, sizeof(uint32_t));
    
    uint32_t new_size = static_cast<uint32_t>(data.size());
    
    if (new_size > old_size) {
        int32_t diff = new_size - old_size;
        if (static_cast<uint32_t>(header_.free_space_offset + diff) > PAGE_SIZE) {
            return Status::Error("Not enough space for update");
        }
        
        memmove(data_ + offset + sizeof(uint32_t) + new_size,
                data_ + offset + sizeof(uint32_t) + old_size,
                header_.free_space_offset - (offset + sizeof(uint32_t) + old_size));
        
        header_.free_space_offset += diff;
    } else if (new_size < old_size) {
        int32_t diff = old_size - new_size;
        
        memmove(data_ + offset + sizeof(uint32_t) + new_size,
                data_ + offset + sizeof(uint32_t) + old_size,
                header_.free_space_offset - (offset + sizeof(uint32_t) + old_size));
        
        header_.free_space_offset -= diff;
    }
    
    memcpy(data_ + offset + sizeof(uint32_t), data.data(), new_size);
    memcpy(data_ + offset, &new_size, sizeof(uint32_t));
    
    header_.dirty = true;
    updateChecksum();
    
    return Status::OK();
}

Status Page::deleteRecord(uint32_t offset) {
    if (offset >= PAGE_SIZE) {
        return Status::Error("Invalid offset");
    }
    
    uint32_t record_size;
    memcpy(&record_size, data_ + offset, sizeof(uint32_t));
    
    uint32_t record_end = offset + sizeof(uint32_t) + record_size;
    uint32_t move_size = header_.free_space_offset - record_end;
    
    if (move_size > 0) {
        memmove(data_ + offset, data_ + record_end, move_size);
    }
    
    header_.free_space_offset -= (sizeof(uint32_t) + record_size);
    header_.record_count--;
    header_.dirty = true;
    
    updateChecksum();
    
    return Status::OK();
}

uint32_t Page::computeChecksum() {
    uint32_t crc = 0;
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(&header_), offsetof(PageHeader, checksum));
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(&header_.checksum) + sizeof(uint32_t), 
                sizeof(PageHeader) - offsetof(PageHeader, checksum) - sizeof(uint32_t));
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(data_), PAGE_SIZE);
    return crc;
}

bool Page::verifyChecksum() {
    uint32_t stored = header_.checksum;
    uint32_t computed = computeChecksum();
    return stored == computed;
}

void Page::updateChecksum() {
    header_.checksum = computeChecksum();
}

}