// storage/table.h
#ifndef TABLE_H
#define TABLE_H

#include "../include/types.h"
#include "../include/status.h"
#include "page.h"
#include "buffer_pool.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <shared_mutex>

namespace orangesql {

class Table {
public:
    Table(const std::string& name, const std::vector<Column>& schema);
    ~Table();
    
    Status insertRow(Row& row);
    Status getRow(uint64_t row_id, Row& row);
    Status updateRow(uint64_t row_id, const Row& row);
    Status deleteRow(uint64_t row_id);
    Status getAllRowIds(std::vector<uint64_t>& row_ids);
    
    const std::string& getName() const { return name_; }
    const std::vector<Column>& getSchema() const { return schema_; }
    int getColumnIndex(const std::string& name) const;
    const Column& getColumn(int index) const { return schema_[index]; }
    bool hasIndexOnColumn(const std::string& column) const;
    void addIndex(const std::string& column);
    size_t getRowCount() const { return row_count_; }
    
private:
    std::string name_;
    std::vector<Column> schema_;
    std::unordered_map<std::string, int> column_index_;
    std::unordered_set<std::string> indexed_columns_;
    BufferPool* buffer_pool_;
    FileManager file_manager_;
    int file_id_;
    uint32_t next_page_id_;
    size_t row_count_;
    mutable std::shared_mutex mutex_;
    
    Status serializeRow(const Row& row, std::vector<char>& data);
    Status deserializeRow(const std::vector<char>& data, Row& row);
    uint64_t generateRowId(uint32_t page_id, uint32_t offset);
    void parseRowId(uint64_t row_id, uint32_t& page_id, uint32_t& offset);
    Status loadMetadata();
    Status saveMetadata();
};

}

#endif