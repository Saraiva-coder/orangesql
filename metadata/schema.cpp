// metadata/schema.cpp
#include "schema.h"
#include <nlohmann/json.hpp>

namespace orangesql {

Schema::Schema() {}

Schema::Schema(const std::vector<Column>& columns) : columns_(columns) {
    rebuildIndex();
}

Schema::~Schema() {}

Status Schema::addColumn(const Column& column) {
    if (column_index_.find(column.name) != column_index_.end()) {
        return Status::Error("Column already exists: " + column.name);
    }
    
    columns_.push_back(column);
    rebuildIndex();
    return Status::OK();
}

Status Schema::removeColumn(const std::string& name) {
    auto it = column_index_.find(name);
    if (it == column_index_.end()) {
        return Status::NotFound("Column not found: " + name);
    }
    
    columns_.erase(columns_.begin() + it->second);
    rebuildIndex();
    return Status::OK();
}

Status Schema::renameColumn(const std::string& old_name, const std::string& new_name) {
    auto it = column_index_.find(old_name);
    if (it == column_index_.end()) {
        return Status::NotFound("Column not found: " + old_name);
    }
    
    if (column_index_.find(new_name) != column_index_.end()) {
        return Status::Error("Column already exists: " + new_name);
    }
    
    columns_[it->second].name = new_name;
    rebuildIndex();
    return Status::OK();
}

Status Schema::modifyColumnType(const std::string& name, DataType new_type) {
    auto it = column_index_.find(name);
    if (it == column_index_.end()) {
        return Status::NotFound("Column not found: " + name);
    }
    
    columns_[it->second].type = new_type;
    return Status::OK();
}

const Column* Schema::getColumn(const std::string& name) const {
    auto it = column_index_.find(name);
    if (it != column_index_.end()) {
        return &columns_[it->second];
    }
    return nullptr;
}

const Column* Schema::getColumn(int index) const {
    if (index >= 0 && static_cast<size_t>(index) < columns_.size()) {
        return &columns_[index];
    }
    return nullptr;
}

int Schema::getColumnIndex(const std::string& name) const {
    auto it = column_index_.find(name);
    if (it != column_index_.end()) {
        return it->second;
    }
    return -1;
}

std::vector<Column> Schema::getColumns() const {
    return columns_;
}

size_t Schema::getColumnCount() const {
    return columns_.size();
}

std::vector<std::string> Schema::getPrimaryKeys() const {
    return primary_keys_;
}

bool Schema::hasPrimaryKey() const {
    return !primary_keys_.empty();
}

bool Schema::isPrimaryKey(const std::string& column) const {
    for (const auto& pk : primary_keys_) {
        if (pk == column) return true;
    }
    return false;
}

std::string Schema::serialize() const {
    nlohmann::json json;
    
    for (const auto& col : columns_) {
        nlohmann::json col_json;
        col_json["name"] = col.name;
        col_json["type"] = static_cast<int>(col.type);
        col_json["nullable"] = col.nullable;
        col_json["primary_key"] = col.primary_key;
        col_json["length"] = col.length;
        json["columns"].push_back(col_json);
    }
    
    json["primary_keys"] = primary_keys_;
    
    return json.dump(4);
}

Status Schema::deserialize(const std::string& data) {
    nlohmann::json json = nlohmann::json::parse(data);
    
    columns_.clear();
    primary_keys_.clear();
    
    if (json.contains("columns")) {
        for (const auto& col_json : json["columns"]) {
            Column col;
            col.name = col_json["name"];
            col.type = static_cast<DataType>(col_json["type"].get<int>());
            col.nullable = col_json.value("nullable", true);
            col.primary_key = col_json.value("primary_key", false);
            col.length = col_json.value("length", 0);
            columns_.push_back(col);
            
            if (col.primary_key) {
                primary_keys_.push_back(col.name);
            }
        }
    }
    
    if (json.contains("primary_keys")) {
        for (const auto& pk : json["primary_keys"]) {
            primary_keys_.push_back(pk);
        }
    }
    
    rebuildIndex();
    return Status::OK();
}

bool Schema::validateRow(const Row& row) const {
    if (row.values.size() != columns_.size()) {
        return false;
    }
    
    for (size_t i = 0; i < columns_.size(); i++) {
        const auto& col = columns_[i];
        const auto& val = row.values[i];
        
        if (col.primary_key && (val.type == DataType::TEXT && val.str_val.empty())) {
            return false;
        }
        
        if (!col.nullable && (val.type == DataType::TEXT && val.str_val.empty())) {
            return false;
        }
        
        if (val.type != col.type) {
            if (col.type == DataType::TEXT && val.type == DataType::VARCHAR) {
                continue;
            }
            if (col.type == DataType::INTEGER && val.type == DataType::BIGINT) {
                continue;
            }
            return false;
        }
    }
    
    return true;
}

void Schema::rebuildIndex() {
    column_index_.clear();
    for (size_t i = 0; i < columns_.size(); i++) {
        column_index_[columns_[i].name] = static_cast<int>(i);
    }
}

}