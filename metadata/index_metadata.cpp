// metadata/index_metadata.cpp
#include "index_metadata.h"
#include <fstream>
#include <nlohmann/json.hpp>

namespace orangesql {

std::string IndexMetadata::serialize() const {
    nlohmann::json json;
    json["index_name"] = index_name;
    json["table_name"] = table_name;
    json["column_name"] = column_name;
    json["is_unique"] = is_unique;
    json["is_primary"] = is_primary;
    json["index_type"] = index_type;
    json["btree_order"] = btree_order;
    json["total_size"] = total_size;
    json["node_count"] = node_count;
    json["height"] = height;
    json["fill_factor"] = fill_factor;
    json["created_at"] = std::chrono::duration_cast<std::chrono::seconds>(
        created_at.time_since_epoch()).count();
    json["last_optimized"] = std::chrono::duration_cast<std::chrono::seconds>(
        last_optimized.time_since_epoch()).count();
    
    return json.dump();
}

IndexMetadata IndexMetadata::deserialize(const std::string& data) {
    nlohmann::json json = nlohmann::json::parse(data);
    
    IndexMetadata metadata;
    metadata.index_name = json["index_name"];
    metadata.table_name = json["table_name"];
    metadata.column_name = json["column_name"];
    metadata.is_unique = json["is_unique"];
    metadata.is_primary = json.value("is_primary", false);
    metadata.index_type = json.value("index_type", "BTREE");
    metadata.btree_order = json.value("btree_order", 128);
    metadata.total_size = json.value("total_size", 0);
    metadata.node_count = json.value("node_count", 0);
    metadata.height = json.value("height", 0);
    metadata.fill_factor = json.value("fill_factor", 0.75);
    
    if (json.contains("created_at")) {
        metadata.created_at = std::chrono::system_clock::time_point(
            std::chrono::seconds(json["created_at"].get<int64_t>()));
    }
    
    if (json.contains("last_optimized")) {
        metadata.last_optimized = std::chrono::system_clock::time_point(
            std::chrono::seconds(json["last_optimized"].get<int64_t>()));
    }
    
    return metadata;
}

IndexMetadataManager& IndexMetadataManager::getInstance() {
    static IndexMetadataManager instance;
    return instance;
}

Status IndexMetadataManager::addIndex(const IndexMetadata& metadata) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    indexes_[metadata.index_name] = metadata;
    return persist();
}

Status IndexMetadataManager::removeIndex(const std::string& index_name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    indexes_.erase(index_name);
    return persist();
}

IndexMetadata* IndexMetadataManager::getIndex(const std::string& index_name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = indexes_.find(index_name);
    if (it != indexes_.end()) {
        return &it->second;
    }
    return nullptr;
}

std::vector<IndexMetadata> IndexMetadataManager::getIndexesForTable(const std::string& table_name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<IndexMetadata> result;
    for (const auto& pair : indexes_) {
        if (pair.second.table_name == table_name) {
            result.push_back(pair.second);
        }
    }
    return result;
}

Status IndexMetadataManager::updateIndexStats(const std::string& index_name, size_t total_size,
                                               size_t node_count, int height) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = indexes_.find(index_name);
    if (it != indexes_.end()) {
        it->second.total_size = total_size;
        it->second.node_count = node_count;
        it->second.height = height;
        it->second.last_optimized = std::chrono::system_clock::now();
        return persist();
    }
    
    return Status::NotFound("Index not found: " + index_name);
}

Status IndexMetadataManager::persist() {
    nlohmann::json json;
    
    for (const auto& pair : indexes_) {
        json[pair.first] = nlohmann::json::parse(pair.second.serialize());
    }
    
    std::ofstream file("data/system/index_metadata.json");
    if (!file.is_open()) {
        return Status::Error("Cannot save index metadata");
    }
    
    file << json.dump(4);
    return Status::OK();
}

Status IndexMetadataManager::load() {
    std::ifstream file("data/system/index_metadata.json");
    if (!file.is_open()) {
        return Status::OK();
    }
    
    nlohmann::json json;
    file >> json;
    
    for (const auto& pair : json.items()) {
        indexes_[pair.key()] = IndexMetadata::deserialize(pair.value().dump());
    }
    
    return Status::OK();
}

}