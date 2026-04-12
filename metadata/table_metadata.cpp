// metadata/table_metadata.cpp
#include "table_metadata.h"
#include <fstream>
#include <nlohmann/json.hpp>

namespace orangesql {

std::string TableMetadata::serialize() const {
    nlohmann::json json;
    json["table_name"] = table_name;
    
    nlohmann::json schema_json;
    for (const auto& col : schema) {
        nlohmann::json col_json;
        col_json["name"] = col.name;
        col_json["type"] = static_cast<int>(col.type);
        col_json["nullable"] = col.nullable;
        col_json["primary_key"] = col.primary_key;
        col_json["length"] = col.length;
        schema_json.push_back(col_json);
    }
    json["schema"] = schema_json;
    
    json["primary_keys"] = primary_keys;
    json["foreign_keys"] = foreign_keys;
    json["foreign_key_refs"] = foreign_key_refs;
    
    json["estimated_row_count"] = estimated_row_count;
    json["page_count"] = page_count;
    json["avg_row_size"] = avg_row_size;
    json["total_size"] = total_size;
    
    json["created_at"] = std::chrono::duration_cast<std::chrono::seconds>(
        created_at.time_since_epoch()).count();
    json["last_modified"] = std::chrono::duration_cast<std::chrono::seconds>(
        last_modified.time_since_epoch()).count();
    json["last_analyzed"] = std::chrono::duration_cast<std::chrono::seconds>(
        last_analyzed.time_since_epoch()).count();
    
    json["engine"] = engine;
    json["charset"] = charset;
    json["collation"] = collation;
    json["comment"] = comment;
    
    return json.dump();
}

TableMetadata TableMetadata::deserialize(const std::string& data) {
    nlohmann::json json = nlohmann::json::parse(data);
    
    TableMetadata metadata;
    metadata.table_name = json["table_name"];
    
    if (json.contains("schema")) {
        for (const auto& col_json : json["schema"]) {
            Column col;
            col.name = col_json["name"];
            col.type = static_cast<DataType>(col_json["type"].get<int>());
            col.nullable = col_json.value("nullable", true);
            col.primary_key = col_json.value("primary_key", false);
            col.length = col_json.value("length", 0);
            metadata.schema.push_back(col);
        }
    }
    
    if (json.contains("primary_keys")) {
        for (const auto& pk : json["primary_keys"]) {
            metadata.primary_keys.push_back(pk);
        }
    }
    
    if (json.contains("foreign_keys")) {
        for (const auto& fk : json["foreign_keys"]) {
            metadata.foreign_keys.push_back(fk);
        }
    }
    
    if (json.contains("foreign_key_refs")) {
        for (const auto& [fk, ref] : json["foreign_key_refs"].items()) {
            metadata.foreign_key_refs[fk] = ref;
        }
    }
    
    metadata.estimated_row_count = json.value("estimated_row_count", 0);
    metadata.page_count = json.value("page_count", 0);
    metadata.avg_row_size = json.value("avg_row_size", 0);
    metadata.total_size = json.value("total_size", 0);
    
    if (json.contains("created_at")) {
        metadata.created_at = std::chrono::system_clock::time_point(
            std::chrono::seconds(json["created_at"].get<int64_t>()));
    }
    
    if (json.contains("last_modified")) {
        metadata.last_modified = std::chrono::system_clock::time_point(
            std::chrono::seconds(json["last_modified"].get<int64_t>()));
    }
    
    if (json.contains("last_analyzed")) {
        metadata.last_analyzed = std::chrono::system_clock::time_point(
            std::chrono::seconds(json["last_analyzed"].get<int64_t>()));
    }
    
    metadata.engine = json.value("engine", "BTREE");
    metadata.charset = json.value("charset", "UTF-8");
    metadata.collation = json.value("collation", "UTF-8_GENERAL_CI");
    metadata.comment = json.value("comment", "");
    
    return metadata;
}

bool TableMetadata::hasColumn(const std::string& column_name) const {
    for (const auto& col : schema) {
        if (col.name == column_name) return true;
    }
    return false;
}

int TableMetadata::getColumnIndex(const std::string& column_name) const {
    for (size_t i = 0; i < schema.size(); i++) {
        if (schema[i].name == column_name) return static_cast<int>(i);
    }
    return -1;
}

TableMetadataManager& TableMetadataManager::getInstance() {
    static TableMetadataManager instance;
    return instance;
}

Status TableMetadataManager::addTable(const TableMetadata& metadata) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    tables_[metadata.table_name] = metadata;
    return persist();
}

Status TableMetadataManager::removeTable(const std::string& table_name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    tables_.erase(table_name);
    return persist();
}

Status TableMetadataManager::updateTable(const std::string& table_name, const TableMetadata& metadata) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        it->second = metadata;
        return persist();
    }
    
    return Status::NotFound("Table not found: " + table_name);
}

TableMetadata* TableMetadataManager::getTable(const std::string& table_name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return &it->second;
    }
    return nullptr;
}

std::vector<TableMetadata> TableMetadataManager::getAllTables() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<TableMetadata> result;
    for (const auto& pair : tables_) {
        result.push_back(pair.second);
    }
    return result;
}

Status TableMetadataManager::updateStats(const std::string& table_name, size_t row_count,
                                          size_t page_count, size_t total_size) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        it->second.estimated_row_count = row_count;
        it->second.page_count = page_count;
        it->second.total_size = total_size;
        if (row_count > 0) {
            it->second.avg_row_size = total_size / row_count;
        }
        it->second.last_analyzed = std::chrono::system_clock::now();
        return persist();
    }
    
    return Status::NotFound("Table not found: " + table_name);
}

Status TableMetadataManager::persist() {
    nlohmann::json json;
    
    for (const auto& pair : tables_) {
        json[pair.first] = nlohmann::json::parse(pair.second.serialize());
    }
    
    std::ofstream file("data/system/table_metadata.json");
    if (!file.is_open()) {
        return Status::Error("Cannot save table metadata");
    }
    
    file << json.dump(4);
    return Status::OK();
}

Status TableMetadataManager::load() {
    std::ifstream file("data/system/table_metadata.json");
    if (!file.is_open()) {
        return Status::OK();
    }
    
    nlohmann::json json;
    file >> json;
    
    for (const auto& pair : json.items()) {
        tables_[pair.key()] = TableMetadata::deserialize(pair.value().dump());
    }
    
    return Status::OK();
}

}