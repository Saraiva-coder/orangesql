// index/index_manager.cpp
#include "index_manager.h"
#include "../include/logger.h"
#include <fstream>
#include <nlohmann/json.hpp>

namespace orangesql {

Index::Index(const std::string& name, const std::string& table,
             const std::string& column, bool unique)
    : name_(name), table_(table), column_(column), unique_(unique), enabled_(true) {
    btree_ = std::make_unique<ConcurrentBTree<std::string>>(BTREE_ORDER);
}

Index::~Index() {}

Status Index::insert(const Value& key, uint64_t row_id) {
    if (!enabled_) return Status::OK();
    return btree_->insert(keyToString(key), row_id);
}

Status Index::search(const Value& key, std::vector<uint64_t>& row_ids) {
    if (!enabled_) return Status::Error("Index disabled");
    return btree_->search(keyToString(key), row_ids);
}

Status Index::searchRange(const Value& start, const Value& end, std::vector<uint64_t>& row_ids) {
    if (!enabled_) return Status::Error("Index disabled");
    return btree_->searchRange(keyToString(start), keyToString(end), row_ids);
}

Status Index::remove(const Value& key) {
    if (!enabled_) return Status::OK();
    return btree_->remove(keyToString(key));
}

Status Index::bulkLoad(const std::vector<Row>& rows, const std::vector<uint64_t>& row_ids) {
    if (rows.size() != row_ids.size()) {
        return Status::Error("Size mismatch");
    }
    
    std::vector<std::pair<std::string, uint64_t>> data;
    data.reserve(rows.size());
    
    for (size_t i = 0; i < rows.size(); i++) {
        data.push_back({keyToString(rows[i].values[0]), row_ids[i]});
    }
    
    return btree_->bulkLoad(data);
}

size_t Index::size() const {
    return btree_->size();
}

std::string Index::keyToString(const Value& key) {
    switch (key.type) {
        case DataType::INTEGER:
            return std::to_string(key.int_val);
        case DataType::BIGINT:
            return std::to_string(key.int_val);
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return std::to_string(key.double_val);
        case DataType::TEXT:
        case DataType::VARCHAR:
            return key.str_val;
        case DataType::BOOLEAN:
            return key.bool_val ? "true" : "false";
        default:
            return "";
    }
}

IndexManager& IndexManager::getInstance() {
    static IndexManager instance;
    return instance;
}

Status IndexManager::createIndex(const std::string& name, const std::string& table,
                                 const std::string& column, bool unique) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    if (indexes_.find(name) != indexes_.end()) {
        return Status::Error("Index already exists: " + name);
    }
    
    auto index = std::make_unique<Index>(name, table, column, unique);
    indexes_[name] = std::move(index);
    table_indexes_[table].push_back(name);
    
    persist();
    LOG_INFO("Index created: " + name);
    
    return Status::OK();
}

Status IndexManager::dropIndex(const std::string& name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = indexes_.find(name);
    if (it == indexes_.end()) {
        return Status::NotFound("Index not found: " + name);
    }
    
    const std::string& table = it->second->getTable();
    auto& table_indices = table_indexes_[table];
    table_indices.erase(std::remove(table_indices.begin(), table_indices.end(), name), table_indices.end());
    
    indexes_.erase(it);
    persist();
    LOG_INFO("Index dropped: " + name);
    
    return Status::OK();
}

Index* IndexManager::getIndex(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = indexes_.find(name);
    if (it != indexes_.end()) {
        return it->second.get();
    }
    return nullptr;
}

Index* IndexManager::getIndexOnColumn(const std::string& table, const std::string& column) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = table_indexes_.find(table);
    if (it != table_indexes_.end()) {
        for (const auto& index_name : it->second) {
            auto index_it = indexes_.find(index_name);
            if (index_it != indexes_.end() && index_it->second->getColumn() == column) {
                return index_it->second.get();
            }
        }
    }
    return nullptr;
}

std::vector<Index*> IndexManager::getIndexesForTable(const std::string& table) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<Index*> result;
    auto it = table_indexes_.find(table);
    if (it != table_indexes_.end()) {
        for (const auto& index_name : it->second) {
            auto index_it = indexes_.find(index_name);
            if (index_it != indexes_.end()) {
                result.push_back(index_it->second.get());
            }
        }
    }
    return result;
}

Status IndexManager::load() {
    std::ifstream file("data/system/indexes.json");
    if (!file.is_open()) {
        return Status::OK();
    }
    
    nlohmann::json json;
    file >> json;
    
    for (const auto& item : json["indexes"]) {
        std::string name = item["name"];
        std::string table = item["table"];
        std::string column = item["column"];
        bool unique = item["unique"];
        
        auto index = std::make_unique<Index>(name, table, column, unique);
        indexes_[name] = std::move(index);
        table_indexes_[table].push_back(name);
    }
    
    return Status::OK();
}

Status IndexManager::persist() {
    nlohmann::json json;
    nlohmann::json indexes_json = nlohmann::json::array();
    
    for (const auto& [name, index] : indexes_) {
        nlohmann::json item;
        item["name"] = name;
        item["table"] = index->getTable();
        item["column"] = index->getColumn();
        item["unique"] = index->isUnique();
        indexes_json.push_back(item);
    }
    
    json["indexes"] = indexes_json;
    
    std::ofstream file("data/system/indexes.json");
    if (!file.is_open()) {
        return Status::Error("Cannot save indexes metadata");
    }
    
    file << json.dump(4);
    return Status::OK();
}

}