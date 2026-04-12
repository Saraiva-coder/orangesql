// metadata/table_metadata.h
#ifndef TABLE_METADATA_H
#define TABLE_METADATA_H

#include "../include/types.h"
#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>

namespace orangesql {

struct TableMetadata {
    std::string table_name;
    std::vector<Column> schema;
    std::vector<std::string> primary_keys;
    std::vector<std::string> foreign_keys;
    std::unordered_map<std::string, std::string> foreign_key_refs;
    
    size_t estimated_row_count;
    size_t page_count;
    size_t avg_row_size;
    size_t total_size;
    
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_modified;
    std::chrono::system_clock::time_point last_analyzed;
    
    std::string engine;
    std::string charset;
    std::string collation;
    std::string comment;
    
    TableMetadata() : estimated_row_count(0), page_count(0), avg_row_size(0),
                      total_size(0), engine("BTREE"), charset("UTF-8"),
                      collation("UTF-8_GENERAL_CI") {}
    
    std::string serialize() const;
    static TableMetadata deserialize(const std::string& data);
    
    bool hasColumn(const std::string& column_name) const;
    int getColumnIndex(const std::string& column_name) const;
};

class TableMetadataManager {
public:
    static TableMetadataManager& getInstance();
    
    Status addTable(const TableMetadata& metadata);
    Status removeTable(const std::string& table_name);
    Status updateTable(const std::string& table_name, const TableMetadata& metadata);
    TableMetadata* getTable(const std::string& table_name);
    std::vector<TableMetadata> getAllTables();
    
    Status updateStats(const std::string& table_name, size_t row_count,
                       size_t page_count, size_t total_size);
    Status persist();
    Status load();
    
private:
    TableMetadataManager() = default;
    
    std::unordered_map<std::string, TableMetadata> tables_;
    std::shared_mutex mutex_;
};

}

#endif