// storage/table.cpp
#include "table.h"
#include "../include/logger.h"
#include "../include/config.h"
#include <cstring>
#include <algorithm>
#include <fstream>
#include <nlohmann/json.hpp>

namespace orangesql {

Table::Table(const std::string& name, const std::vector<Column>& schema)
    : name_(name), schema_(schema), next_page_id_(1), row_count_(0) {
    
    for (size_t i = 0; i < schema_.size(); i++) {
        column_index_[schema_[i].name] = static_cast<int>(i);
    }
    
    buffer_pool_ = new BufferPool(Config::getInstance().getBufferPoolSize());
    
    std::string filename = name + ".data";
    Status status = file_manager_.openFile(filename, file_id_);
    if (!status.ok()) {
        LOG_ERROR("Failed to open table file: " + filename);
    }
    
    loadMetadata();
}

Table::~Table() {
    saveMetadata();
    file_manager_.closeFile(file_id_);
    delete buffer_pool_;
}

Status Table::insertRow(Row& row) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<char> data;
    Status status = serializeRow(row, data);
    if (!status.ok()) return status;
    
    uint32_t page_id = next_page_id_;
    Page* page = buffer_pool_->getPage(file_id_, page_id);
    
    if (!page) {
        page_id = ++next_page_id_;
        page = buffer_pool_->getPage(file_id_, page_id);
        if (!page) {
            return Status::Error("Failed to allocate page");
        }
    }
    
    uint32_t offset;
    status = page->insertRecord(data, offset);
    
    if (!status.ok()) {
        page_id = ++next_page_id_;
        page = buffer_pool_->getPage(file_id_, page_id);
        if (!page) {
            return Status::Error("Failed to allocate new page");
        }
        status = page->insertRecord(data, offset);
        if (!status.ok()) {
            buffer_pool_->unpinPage(page);
            return status;
        }
    }
    
    row.row_id = generateRowId(page_id, offset);
    row_count_++;
    
    buffer_pool_->markDirty(page);
    buffer_pool_->unpinPage(page);
    
    return Status::OK();
}

Status Table::getRow(uint64_t row_id, Row& row) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    uint32_t page_id, offset;
    parseRowId(row_id, page_id, offset);
    
    Page* page = buffer_pool_->getPage(file_id_, page_id);
    if (!page) {
        return Status::NotFound("Page not found");
    }
    
    std::vector<char> data;
    Status status = page->getRecord(offset, data);
    
    if (status.ok()) {
        status = deserializeRow(data, row);
        row.row_id = row_id;
    }
    
    buffer_pool_->unpinPage(page);
    return status;
}

Status Table::updateRow(uint64_t row_id, const Row& row) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    uint32_t page_id, offset;
    parseRowId(row_id, page_id, offset);
    
    Page* page = buffer_pool_->getPage(file_id_, page_id);
    if (!page) {
        return Status::NotFound("Page not found");
    }
    
    std::vector<char> data;
    Status status = serializeRow(row, data);
    
    if (status.ok()) {
        status = page->updateRecord(offset, data);
        if (status.ok()) {
            buffer_pool_->markDirty(page);
        }
    }
    
    buffer_pool_->unpinPage(page);
    return status;
}

Status Table::deleteRow(uint64_t row_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    uint32_t page_id, offset;
    parseRowId(row_id, page_id, offset);
    
    Page* page = buffer_pool_->getPage(file_id_, page_id);
    if (!page) {
        return Status::NotFound("Page not found");
    }
    
    Status status = page->deleteRecord(offset);
    if (status.ok()) {
        buffer_pool_->markDirty(page);
        row_count_--;
    }
    
    buffer_pool_->unpinPage(page);
    return status;
}

Status Table::getAllRowIds(std::vector<uint64_t>& row_ids) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    row_ids.clear();
    row_ids.reserve(row_count_);
    
    for (uint32_t page_id = 1; page_id <= next_page_id_; page_id++) {
        Page* page = buffer_pool_->getPage(file_id_, page_id);
        if (!page) continue;
        
        uint32_t offset = sizeof(PageHeader);
        uint32_t end = page->getFreeSpace();
        
        while (offset < end) {
            uint32_t record_size;
            memcpy(&record_size, reinterpret_cast<const char*>(page->getData()) + offset, sizeof(uint32_t));
            
            if (record_size > 0 && record_size < PAGE_SIZE) {
                row_ids.push_back(generateRowId(page_id, offset));
            }
            
            offset += sizeof(uint32_t) + record_size;
        }
        
        buffer_pool_->unpinPage(page);
    }
    
    return Status::OK();
}

Status Table::serializeRow(const Row& row, std::vector<char>& data) {
    data.clear();
    
    for (size_t i = 0; i < schema_.size() && i < row.values.size(); i++) {
        const Value& value = row.values[i];
        
        switch (schema_[i].type) {
            case DataType::INTEGER:
            case DataType::BIGINT: {
                int64_t val = value.int_val;
                data.insert(data.end(), reinterpret_cast<char*>(&val), 
                           reinterpret_cast<char*>(&val) + sizeof(int64_t));
                break;
            }
            case DataType::FLOAT:
            case DataType::DOUBLE: {
                double val = value.double_val;
                data.insert(data.end(), reinterpret_cast<char*>(&val),
                           reinterpret_cast<char*>(&val) + sizeof(double));
                break;
            }
            case DataType::BOOLEAN: {
                char val = value.bool_val ? 1 : 0;
                data.push_back(val);
                break;
            }
            case DataType::TEXT:
            case DataType::VARCHAR: {
                uint32_t len = static_cast<uint32_t>(value.str_val.length());
                data.insert(data.end(), reinterpret_cast<char*>(&len),
                           reinterpret_cast<char*>(&len) + sizeof(uint32_t));
                data.insert(data.end(), value.str_val.begin(), value.str_val.end());
                break;
            }
            default:
                break;
        }
    }
    
    return Status::OK();
}

Status Table::deserializeRow(const std::vector<char>& data, Row& row) {
    size_t pos = 0;
    row.values.clear();
    
    for (const auto& column : schema_) {
        Value value;
        value.type = column.type;
        
        switch (column.type) {
            case DataType::INTEGER:
            case DataType::BIGINT: {
                if (pos + sizeof(int64_t) > data.size()) {
                    return Status::Error("Corrupt data");
                }
                memcpy(&value.int_val, data.data() + pos, sizeof(int64_t));
                pos += sizeof(int64_t);
                break;
            }
            case DataType::FLOAT:
            case DataType::DOUBLE: {
                if (pos + sizeof(double) > data.size()) {
                    return Status::Error("Corrupt data");
                }
                memcpy(&value.double_val, data.data() + pos, sizeof(double));
                pos += sizeof(double);
                break;
            }
            case DataType::BOOLEAN: {
                if (pos >= data.size()) {
                    return Status::Error("Corrupt data");
                }
                value.bool_val = (data[pos] != 0);
                pos++;
                break;
            }
            case DataType::TEXT:
            case DataType::VARCHAR: {
                if (pos + sizeof(uint32_t) > data.size()) {
                    return Status::Error("Corrupt data");
                }
                uint32_t len;
                memcpy(&len, data.data() + pos, sizeof(uint32_t));
                pos += sizeof(uint32_t);
                
                if (pos + len > data.size()) {
                    return Status::Error("Corrupt data");
                }
                value.str_val.assign(data.data() + pos, len);
                pos += len;
                break;
            }
            default:
                break;
        }
        
        row.values.push_back(value);
    }
    
    return Status::OK();
}

int Table::getColumnIndex(const std::string& name) const {
    auto it = column_index_.find(name);
    if (it != column_index_.end()) {
        return it->second;
    }
    return -1;
}

bool Table::hasIndexOnColumn(const std::string& column) const {
    return indexed_columns_.find(column) != indexed_columns_.end();
}

void Table::addIndex(const std::string& column) {
    indexed_columns_.insert(column);
}

uint64_t Table::generateRowId(uint32_t page_id, uint32_t offset) {
    return (static_cast<uint64_t>(page_id) << 32) | offset;
}

void Table::parseRowId(uint64_t row_id, uint32_t& page_id, uint32_t& offset) {
    page_id = static_cast<uint32_t>(row_id >> 32);
    offset = static_cast<uint32_t>(row_id & 0xFFFFFFFF);
}

Status Table::loadMetadata() {
    std::string filename = name_ + ".schema";
    if (!file_manager_.fileExists(filename)) {
        return Status::OK();
    }
    
    std::ifstream file("data/tables/" + filename);
    if (!file.is_open()) {
        return Status::OK();
    }
    
    nlohmann::json json;
    file >> json;
    
    if (json.contains("next_page_id")) {
        next_page_id_ = json["next_page_id"];
    }
    if (json.contains("row_count")) {
        row_count_ = json["row_count"];
    }
    if (json.contains("indexed_columns")) {
        for (const auto& col : json["indexed_columns"]) {
            indexed_columns_.insert(col);
        }
    }
    
    return Status::OK();
}

Status Table::saveMetadata() {
    nlohmann::json json;
    json["next_page_id"] = next_page_id_;
    json["row_count"] = row_count_;
    json["indexed_columns"] = indexed_columns_;
    
    std::string filename = "data/tables/" + name_ + ".schema";
    std::ofstream file(filename);
    if (!file.is_open()) {
        return Status::Error("Cannot save metadata");
    }
    
    file << json.dump(4);
    return Status::OK();
}

}