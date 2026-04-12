// transaction/wal.cpp
#include "wal.h"
#include "../include/logger.h"
#include <filesystem>
#include <chrono>

namespace fs = std::filesystem;

namespace orangesql {

WALManager& WALManager::getInstance() {
    static WALManager instance;
    return instance;
}

WALManager::WALManager() : last_lsn_(0), flushed_lsn_(0), current_segment_(1) {}

WALManager::~WALManager() {
    if (wal_file_.is_open()) {
        flush();
        wal_file_.close();
    }
}

Status WALManager::init(const std::string& wal_dir) {
    wal_dir_ = wal_dir;
    fs::create_directories(wal_dir);
    return openSegment();
}

Status WALManager::appendRecord(const WALRecord& record) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WALRecord rec = record;
    rec.lsn = ++last_lsn_;
    rec.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    auto data = rec.serialize();
    wal_file_.write(data.data(), data.size());
    wal_file_.flush();
    
    flushed_lsn_ = rec.lsn;
    
    if (wal_file_.tellp() > WAL_SEGMENT_SIZE) {
        rotateSegment();
    }
    
    return Status::OK();
}

Status WALManager::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (wal_file_.is_open()) {
        wal_file_.flush();
    }
    return Status::OK();
}

Status WALManager::truncate(uint64_t lsn) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    wal_file_.close();
    
    uint64_t segment_to_keep = lsn / WAL_SEGMENT_SIZE + 1;
    
    for (uint64_t seg = 1; seg < segment_to_keep; seg++) {
        std::string path = getSegmentPath(seg);
        if (fs::exists(path)) {
            fs::remove(path);
        }
    }
    
    current_segment_ = segment_to_keep;
    return openSegment();
}

Status WALManager::openSegment() {
    if (wal_file_.is_open()) {
        wal_file_.close();
    }
    
    std::string path = getSegmentPath(current_segment_);
    wal_file_.open(path, std::ios::binary | std::ios::app);
    
    if (!wal_file_.is_open()) {
        return Status::Error("Failed to open WAL segment");
    }
    
    return Status::OK();
}

Status WALManager::rotateSegment() {
    flush();
    current_segment_++;
    return openSegment();
}

std::string WALManager::getSegmentPath(uint64_t segment_id) {
    char buffer[256];
    snprintf(buffer, sizeof(buffer), "wal_%05lu.log", segment_id);
    return (fs::path(wal_dir_) / buffer).string();
}

Status WALManager::readSegment(uint64_t segment_id, std::vector<WALRecord>& records) {
    std::string path = getSegmentPath(segment_id);
    std::ifstream file(path, std::ios::binary);
    
    if (!file.is_open()) {
        return Status::OK();
    }
    
    while (file.peek() != EOF) {
        uint32_t record_size;
        file.read(reinterpret_cast<char*>(&record_size), sizeof(uint32_t));
        
        if (file.gcount() != sizeof(uint32_t)) break;
        
        std::vector<char> data(record_size);
        file.read(data.data(), record_size);
        
        if (file.gcount() != record_size) break;
        
        records.push_back(WALRecord::deserialize(data));
    }
    
    return Status::OK();
}

Status WALManager::recover(std::function<Status(const WALRecord&)> replay_func) {
    uint64_t segment_id = 1;
    std::vector<WALRecord> records;
    
    while (true) {
        std::vector<WALRecord> seg_records;
        Status status = readSegment(segment_id, seg_records);
        if (!status.ok() || seg_records.empty()) break;
        
        records.insert(records.end(), seg_records.begin(), seg_records.end());
        segment_id++;
    }
    
    std::unordered_map<uint64_t, std::vector<WALRecord>> txn_records;
    
    for (const auto& record : records) {
        if (record.type == WALRecordType::BEGIN ||
            record.type == WALRecordType::INSERT ||
            record.type == WALRecordType::UPDATE ||
            record.type == WALRecordType::DELETE) {
            txn_records[record.transaction_id].push_back(record);
        } else if (record.type == WALRecordType::COMMIT) {
            for (const auto& rec : txn_records[record.transaction_id]) {
                Status s = replay_func(rec);
                if (!s.ok()) return s;
            }
            txn_records.erase(record.transaction_id);
        }
    }
    
    return Status::OK();
}

std::vector<char> WALRecord::serialize() const {
    std::vector<char> data;
    
    uint32_t type_val = static_cast<uint32_t>(type);
    data.insert(data.end(), reinterpret_cast<const char*>(&type_val), 
                reinterpret_cast<const char*>(&type_val) + sizeof(uint32_t));
    
    data.insert(data.end(), reinterpret_cast<const char*>(&lsn), 
                reinterpret_cast<const char*>(&lsn) + sizeof(uint64_t));
    data.insert(data.end(), reinterpret_cast<const char*>(&transaction_id), 
                reinterpret_cast<const char*>(&transaction_id) + sizeof(uint64_t));
    data.insert(data.end(), reinterpret_cast<const char*>(&timestamp), 
                reinterpret_cast<const char*>(&timestamp) + sizeof(uint64_t));
    data.insert(data.end(), reinterpret_cast<const char*>(&page_id), 
                reinterpret_cast<const char*>(&page_id) + sizeof(uint32_t));
    data.insert(data.end(), reinterpret_cast<const char*>(&offset), 
                reinterpret_cast<const char*>(&offset) + sizeof(uint32_t));
    
    uint32_t old_size = old_data.size();
    data.insert(data.end(), reinterpret_cast<const char*>(&old_size), 
                reinterpret_cast<const char*>(&old_size) + sizeof(uint32_t));
    data.insert(data.end(), old_data.begin(), old_data.end());
    
    uint32_t new_size = new_data.size();
    data.insert(data.end(), reinterpret_cast<const char*>(&new_size), 
                reinterpret_cast<const char*>(&new_size) + sizeof(uint32_t));
    data.insert(data.end(), new_data.begin(), new_data.end());
    
    uint32_t table_len = table_name.size();
    data.insert(data.end(), reinterpret_cast<const char*>(&table_len), 
                reinterpret_cast<const char*>(&table_len) + sizeof(uint32_t));
    data.insert(data.end(), table_name.begin(), table_name.end());
    
    data.insert(data.end(), reinterpret_cast<const char*>(&prev_lsn), 
                reinterpret_cast<const char*>(&prev_lsn) + sizeof(uint64_t));
    
    uint32_t total_size = data.size();
    data.insert(data.begin(), reinterpret_cast<const char*>(&total_size), 
                reinterpret_cast<const char*>(&total_size) + sizeof(uint32_t));
    
    return data;
}

WALRecord WALRecord::deserialize(const std::vector<char>& data) {
    WALRecord record;
    size_t pos = 0;
    
    uint32_t type_val;
    memcpy(&type_val, data.data() + pos, sizeof(uint32_t));
    record.type = static_cast<WALRecordType>(type_val);
    pos += sizeof(uint32_t);
    
    memcpy(&record.lsn, data.data() + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&record.transaction_id, data.data() + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&record.timestamp, data.data() + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&record.page_id, data.data() + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    memcpy(&record.offset, data.data() + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    
    uint32_t old_size;
    memcpy(&old_size, data.data() + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    record.old_data.resize(old_size);
    memcpy(record.old_data.data(), data.data() + pos, old_size);
    pos += old_size;
    
    uint32_t new_size;
    memcpy(&new_size, data.data() + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    record.new_data.resize(new_size);
    memcpy(record.new_data.data(), data.data() + pos, new_size);
    pos += new_size;
    
    uint32_t table_len;
    memcpy(&table_len, data.data() + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    record.table_name.assign(data.data() + pos, table_len);
    pos += table_len;
    
    memcpy(&record.prev_lsn, data.data() + pos, sizeof(uint64_t));
    
    return record;
}

size_t WALRecord::getSize() const {
    return sizeof(uint32_t) * 5 + sizeof(uint64_t) * 5 + 
           old_data.size() + new_data.size() + table_name.size();
}

}