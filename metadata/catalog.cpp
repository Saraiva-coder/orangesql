// metadata/catalog.cpp
#include "catalog.h"
#include "../include/logger.h"
#include "../include/config.h"
#include <fstream>
#include <filesystem>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;

namespace orangesql {

Catalog* Catalog::getInstance() {
    static Catalog instance;
    return &instance;
}

Catalog::Catalog() {
    fs::create_directories("data/system");
    load();
}

Catalog::~Catalog() {
    persist();
}

Status Catalog::createTable(const std::string& name, const std::vector<Column>& schema) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    if (tables_.find(name) != tables_.end()) {
        return Status::Error("Table already exists: " + name);
    }
    
    auto table = std::make_unique<Table>(name, schema);
    tables_[name] = std::move(table);
    
    Status status = persist();
    if (status.ok()) {
        LOG_INFO("Table created: " + name);
    }
    
    return status;
}

Status Catalog::dropTable(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return Status::NotFound("Table not found: " + name);
    }
    
    auto index_it = table_indexes_.find(name);
    if (index_it != table_indexes_.end()) {
        for (const auto& idx_name : index_it->second) {
            indexes_.erase(idx_name);
        }
        table_indexes_.erase(index_it);
    }
    
    tables_.erase(it);
    
    std::string data_path = "data/tables/" + name + ".data";
    std::string schema_path = "data/tables/" + name + ".schema";
    
    if (fs::exists(data_path)) fs::remove(data_path);
    if (fs::exists(schema_path)) fs::remove(schema_path);
    
    Status status = persist();
    if (status.ok()) {
        LOG_INFO("Table dropped: " + name);
    }
    
    return status;
}

Table* Catalog::getTable(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        return it->second.get();
    }
    return nullptr;
}

std::vector<Table*> Catalog::getAllTables() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<Table*> result;
    for (const auto& pair : tables_) {
        result.push_back(pair.second.get());
    }
    return result;
}

bool Catalog::tableExists(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return tables_.find(name) != tables_.end();
}

Status Catalog::createIndex(const std::string& name, const std::string& table,
                            const std::string& column, bool unique) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    if (indexes_.find(name) != indexes_.end()) {
        return Status::Error("Index already exists: " + name);
    }
    
    auto table_it = tables_.find(table);
    if (table_it == tables_.end()) {
        return Status::NotFound("Table not found: " + table);
    }
    
    auto index = std::make_unique<Index>(name, table, column, unique);
    indexes_[name] = std::move(index);
    table_indexes_[table].push_back(name);
    
    table_it->second->addIndex(column);
    
    Status status = persist();
    if (status.ok()) {
        LOG_INFO("Index created: " + name + " ON " + table + "(" + column + ")");
    }
    
    return status;
}

Status Catalog::dropIndex(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = indexes_.find(name);
    if (it == indexes_.end()) {
        return Status::NotFound("Index not found: " + name);
    }
    
    std::string table = it->second->getTable();
    auto table_it = tables_.find(table);
    if (table_it != tables_.end()) {
        table_it->second->addIndex(it->second->getColumn());
    }
    
    auto table_idx_it = table_indexes_.find(table);
    if (table_idx_it != table_indexes_.end()) {
        auto& vec = table_idx_it->second;
        vec.erase(std::remove(vec.begin(), vec.end(), name), vec.end());
    }
    
    indexes_.erase(it);
    
    Status status = persist();
    if (status.ok()) {
        LOG_INFO("Index dropped: " + name);
    }
    
    return status;
}

Index* Catalog::getIndex(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = indexes_.find(name);
    if (it != indexes_.end()) {
        return it->second.get();
    }
    return nullptr;
}

Index* Catalog::getIndexOnColumn(const std::string& table, const std::string& column) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = table_indexes_.find(table);
    if (it != table_indexes_.end()) {
        for (const auto& idx_name : it->second) {
            auto idx_it = indexes_.find(idx_name);
            if (idx_it != indexes_.end() && idx_it->second->getColumn() == column) {
                return idx_it->second.get();
            }
        }
    }
    return nullptr;
}

std::vector<Index*> Catalog::getIndexesForTable(const std::string& table) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<Index*> result;
    auto it = table_indexes_.find(table);
    if (it != table_indexes_.end()) {
        for (const auto& idx_name : it->second) {
            auto idx_it = indexes_.find(idx_name);
            if (idx_it != indexes_.end()) {
                result.push_back(idx_it->second.get());
            }
        }
    }
    return result;
}

Status Catalog::load() {
    std::string path = getCatalogPath();
    if (!fs::exists(path)) {
        return Status::OK();
    }
    
    std::ifstream file(path);
    if (!file.is_open()) {
        return Status::Error("Cannot open catalog file");
    }
    
    nlohmann::json json;
    file >> json;
    
    if (json.contains("tables")) {
        for (const auto& item : json["tables"]) {
            std::string name = item["name"];
            std::vector<Column> schema;
            
            for (const auto& col : item["schema"]) {
                Column column;
                column.name = col["name"];
                column.type = static_cast<DataType>(col["type"].get<int>());
                column.nullable = col["nullable"];
                column.primary_key = col.value("primary_key", false);
                column.length = col.value("length", 0);
                column.position = col.value("position", 0);
                schema.push_back(column);
            }
            
            auto table = std::make_unique<Table>(name, schema);
            tables_[name] = std::move(table);
        }
    }
    
    if (json.contains("indexes")) {
        for (const auto& item : json["indexes"]) {
            std::string name = item["name"];
            std::string table = item["table"];
            std::string column = item["column"];
            bool unique = item["unique"];
            
            auto index = std::make_unique<Index>(name, table, column, unique);
            indexes_[name] = std::move(index);
            table_indexes_[table].push_back(name);
        }
    }
    
    return Status::OK();
}

Status Catalog::persist() {
    nlohmann::json json;
    nlohmann::json tables_json = nlohmann::json::array();
    nlohmann::json indexes_json = nlohmann::json::array();
    
    for (const auto& [name, table] : tables_) {
        nlohmann::json table_json;
        table_json["name"] = name;
        
        nlohmann::json schema_json = nlohmann::json::array();
        int pos = 0;
        for (const auto& col : table->getSchema()) {
            nlohmann::json col_json;
            col_json["name"] = col.name;
            col_json["type"] = static_cast<int>(col.type);
            col_json["nullable"] = col.nullable;
            col_json["primary_key"] = col.primary_key;
            col_json["length"] = col.length;
            col_json["position"] = pos++;
            schema_json.push_back(col_json);
        }
        
        table_json["schema"] = schema_json;
        tables_json.push_back(table_json);
    }
    
    for (const auto& [name, index] : indexes_) {
        nlohmann::json index_json;
        index_json["name"] = name;
        index_json["table"] = index->getTable();
        index_json["column"] = index->getColumn();
        index_json["unique"] = index->isUnique();
        indexes_json.push_back(index_json);
    }
    
    json["tables"] = tables_json;
    json["indexes"] = indexes_json;
    json["version"] = 1;
    json["timestamp"] = std::chrono::system_clock::now().time_since_epoch().count();
    
    std::ofstream file(getCatalogPath());
    if (!file.is_open()) {
        return Status::Error("Cannot save catalog");
    }
    
    file << json.dump(4);
    return Status::OK();
}

void Catalog::refresh() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    tables_.clear();
    indexes_.clear();
    table_indexes_.clear();
    load();
}

std::string Catalog::getCatalogPath() {
    return "data/system/catalog.json";
}

}