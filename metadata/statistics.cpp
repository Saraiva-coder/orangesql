// metadata/statistics.cpp
#include "statistics.h"
#include "catalog.h"
#include "../include/logger.h"
#include <fstream>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cmath>

namespace orangesql {

StatisticsManager& StatisticsManager::getInstance() {
    static StatisticsManager instance;
    return instance;
}

StatisticsManager::StatisticsManager() {
    load();
}

StatisticsManager::~StatisticsManager() {
    persist();
}

Status StatisticsManager::updateTableStats(const std::string& table_name) {
    Table* table = Catalog::getInstance()->getTable(table_name);
    if (!table) {
        return Status::NotFound("Table not found: " + table_name);
    }
    
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    TableStats& stats = stats_[table_name];
    stats.table_name = table_name;
    stats.row_count = table->getRowCount();
    stats.last_updated = std::chrono::system_clock::now();
    
    std::vector<uint64_t> row_ids;
    table->getAllRowIds(row_ids);
    
    size_t sample_size = static_cast<size_t>(row_ids.size() * sample_rate_);
    if (sample_size < 100) sample_size = std::min(row_ids.size(), size_t(100));
    
    std::vector<Row> sample_rows;
    for (size_t i = 0; i < sample_size && i < row_ids.size(); i++) {
        Row row;
        if (table->getRow(row_ids[i], row).ok()) {
            sample_rows.push_back(row);
        }
    }
    
    stats.total_size = 0;
    for (const auto& row : sample_rows) {
        for (const auto& val : row.values) {
            stats.total_size += val.toString().size();
        }
    }
    
    if (!sample_rows.empty()) {
        stats.avg_row_size = stats.total_size / sample_rows.size();
    }
    
    for (size_t col_idx = 0; col_idx < table->getSchema().size(); col_idx++) {
        const auto& column = table->getColumn(col_idx);
        ColumnStats col_stats;
        col_stats.column_name = column.name;
        col_stats.total_count = sample_rows.size();
        
        std::vector<Value> values;
        std::unordered_map<std::string, size_t> value_counts;
        
        for (const auto& row : sample_rows) {
            if (col_idx < row.values.size()) {
                const auto& val = row.values[col_idx];
                std::string str_val = val.toString();
                
                if (str_val.empty()) {
                    col_stats.null_count++;
                } else {
                    values.push_back(val);
                    value_counts[str_val]++;
                }
            }
        }
        
        col_stats.distinct_count = value_counts.size();
        
        if (!values.empty()) {
            if (column.type == DataType::INTEGER || column.type == DataType::BIGINT ||
                column.type == DataType::FLOAT || column.type == DataType::DOUBLE) {
                
                double min = std::numeric_limits<double>::max();
                double max = std::numeric_limits<double>::lowest();
                double sum = 0;
                
                for (const auto& val : values) {
                    double num = val.toInt();
                    min = std::min(min, num);
                    max = std::max(max, num);
                    sum += num;
                }
                
                col_stats.min_value = min;
                col_stats.max_value = max;
                col_stats.avg_value = sum / values.size();
            }
            
            computeHistogram(col_stats, values);
        }
        
        stats.column_stats[column.name] = col_stats;
    }
    
    return Status::OK();
}

Status StatisticsManager::updateColumnStats(const std::string& table_name, const std::string& column_name) {
    Table* table = Catalog::getInstance()->getTable(table_name);
    if (!table) {
        return Status::NotFound("Table not found: " + table_name);
    }
    
    int col_idx = table->getColumnIndex(column_name);
    if (col_idx < 0) {
        return Status::NotFound("Column not found: " + column_name);
    }
    
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    TableStats& stats = stats_[table_name];
    ColumnStats& col_stats = stats.column_stats[column_name];
    col_stats.column_name = column_name;
    
    std::vector<uint64_t> row_ids;
    table->getAllRowIds(row_ids);
    
    size_t sample_size = static_cast<size_t>(row_ids.size() * sample_rate_);
    if (sample_size < 100) sample_size = std::min(row_ids.size(), size_t(100));
    
    std::vector<Value> values;
    std::unordered_map<std::string, size_t> value_counts;
    col_stats.null_count = 0;
    
    for (size_t i = 0; i < sample_size && i < row_ids.size(); i++) {
        Row row;
        if (table->getRow(row_ids[i], row).ok() && col_idx < (int)row.values.size()) {
            const auto& val = row.values[col_idx];
            std::string str_val = val.toString();
            
            if (str_val.empty()) {
                col_stats.null_count++;
            } else {
                values.push_back(val);
                value_counts[str_val]++;
            }
        }
    }
    
    col_stats.total_count = sample_size;
    col_stats.distinct_count = value_counts.size();
    
    if (!values.empty()) {
        const auto& column = table->getColumn(col_idx);
        if (column.type == DataType::INTEGER || column.type == DataType::BIGINT ||
            column.type == DataType::FLOAT || column.type == DataType::DOUBLE) {
            
            double min = std::numeric_limits<double>::max();
            double max = std::numeric_limits<double>::lowest();
            double sum = 0;
            
            for (const auto& val : values) {
                double num = val.toInt();
                min = std::min(min, num);
                max = std::max(max, num);
                sum += num;
            }
            
            col_stats.min_value = min;
            col_stats.max_value = max;
            col_stats.avg_value = sum / values.size();
        }
        
        computeHistogram(col_stats, values);
    }
    
    stats.last_updated = std::chrono::system_clock::now();
    
    return Status::OK();
}

void StatisticsManager::computeHistogram(ColumnStats& stats, const std::vector<Value>& values) {
    const int BUCKET_COUNT = 10;
    stats.histogram.clear();
    
    if (values.empty()) return;
    
    std::vector<double> numeric_values;
    for (const auto& val : values) {
        numeric_values.push_back(val.toInt());
    }
    
    std::sort(numeric_values.begin(), numeric_values.end());
    
    size_t bucket_size = numeric_values.size() / BUCKET_COUNT;
    if (bucket_size == 0) bucket_size = 1;
    
    for (int i = 0; i < BUCKET_COUNT && i * bucket_size < numeric_values.size(); i++) {
        size_t start = i * bucket_size;
        size_t end = std::min(start + bucket_size, numeric_values.size());
        
        if (start < end) {
            std::string bucket_label = std::to_string(numeric_values[start]) + 
                                       " - " + std::to_string(numeric_values[end - 1]);
            stats.histogram.push_back({bucket_label, end - start});
        }
    }
}

Status StatisticsManager::updateAllStats() {
    auto tables = Catalog::getInstance()->getAllTables();
    
    for (auto* table : tables) {
        updateTableStats(table->getName());
    }
    
    persist();
    return Status::OK();
}

TableStats* StatisticsManager::getTableStats(const std::string& table_name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = stats_.find(table_name);
    if (it != stats_.end()) {
        return &it->second;
    }
    return nullptr;
}

ColumnStats* StatisticsManager::getColumnStats(const std::string& table_name, const std::string& column_name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = stats_.find(table_name);
    if (it != stats_.end()) {
        auto col_it = it->second.column_stats.find(column_name);
        if (col_it != it->second.column_stats.end()) {
            return &col_it->second;
        }
    }
    return nullptr;
}

double StatisticsManager::estimateCardinality(const std::string& table_name, 
                                               const std::string& column_name,
                                               const std::string& value) {
    ColumnStats* stats = getColumnStats(table_name, column_name);
    if (!stats) return 1.0;
    
    if (stats->distinct_count == 0) return 0;
    
    return static_cast<double>(stats->total_count) / static_cast<double>(stats->distinct_count);
}

double StatisticsManager::estimateSelectivity(const std::string& table_name,
                                               const std::string& column_name,
                                               const std::string& op,
                                               const std::string& value) {
    ColumnStats* stats = getColumnStats(table_name, column_name);
    if (!stats) return 0.1;
    
    if (op == "=") {
        return 1.0 / stats->distinct_count;
    } else if (op == "<" || op == ">") {
        return 0.33;
    } else if (op == "<=" || op == ">=") {
        return 0.4;
    } else if (op == "LIKE") {
        if (value.front() == '%' && value.back() == '%') return 0.5;
        if (value.back() == '%') return 0.2;
        if (value.front() == '%') return 0.3;
        return 0.1;
    }
    
    return 0.1;
}

void StatisticsManager::enableAutoUpdate(bool enable) {
    auto_update_ = enable;
}

void StatisticsManager::setSampleRate(double rate) {
    sample_rate_ = std::max(0.01, std::min(1.0, rate));
}

Status StatisticsManager::persist() {
    nlohmann::json json;
    
    for (const auto& [name, stats] : stats_) {
        nlohmann::json table_json;
        table_json["table_name"] = stats.table_name;
        table_json["row_count"] = stats.row_count;
        table_json["page_count"] = stats.page_count;
        table_json["avg_row_size"] = stats.avg_row_size;
        table_json["total_size"] = stats.total_size;
        
        nlohmann::json columns_json;
        for (const auto& [col_name, col_stats] : stats.column_stats) {
            nlohmann::json col_json;
            col_json["column_name"] = col_stats.column_name;
            col_json["null_count"] = col_stats.null_count;
            col_json["distinct_count"] = col_stats.distinct_count;
            col_json["total_count"] = col_stats.total_count;
            col_json["min_value"] = col_stats.min_value;
            col_json["max_value"] = col_stats.max_value;
            col_json["avg_value"] = col_stats.avg_value;
            
            nlohmann::json histogram_json;
            for (const auto& [bucket, count] : col_stats.histogram) {
                nlohmann::json bucket_json;
                bucket_json["bucket"] = bucket;
                bucket_json["count"] = count;
                histogram_json.push_back(bucket_json);
            }
            col_json["histogram"] = histogram_json;
            columns_json[col_name] = col_json;
        }
        
        table_json["columns"] = columns_json;
        json["tables"][name] = table_json;
    }
    
    std::ofstream file("data/system/statistics.json");
    if (!file.is_open()) {
        return Status::Error("Cannot save statistics");
    }
    
    file << json.dump(4);
    return Status::OK();
}

Status StatisticsManager::load() {
    std::ifstream file("data/system/statistics.json");
    if (!file.is_open()) {
        return Status::OK();
    }
    
    nlohmann::json json;
    file >> json;
    
    if (json.contains("tables")) {
        for (const auto& [name, table_json] : json["tables"].items()) {
            TableStats stats;
            stats.table_name = table_json.value("table_name", name);
            stats.row_count = table_json.value("row_count", 0);
            stats.page_count = table_json.value("page_count", 0);
            stats.avg_row_size = table_json.value("avg_row_size", 0);
            stats.total_size = table_json.value("total_size", 0);
            
            if (table_json.contains("columns")) {
                for (const auto& [col_name, col_json] : table_json["columns"].items()) {
                    ColumnStats col_stats;
                    col_stats.column_name = col_json.value("column_name", col_name);
                    col_stats.null_count = col_json.value("null_count", 0);
                    col_stats.distinct_count = col_json.value("distinct_count", 0);
                    col_stats.total_count = col_json.value("total_count", 0);
                    col_stats.min_value = col_json.value("min_value", 0);
                    col_stats.max_value = col_json.value("max_value", 0);
                    col_stats.avg_value = col_json.value("avg_value", 0);
                    
                    if (col_json.contains("histogram")) {
                        for (const auto& bucket_json : col_json["histogram"]) {
                            std::string bucket = bucket_json["bucket"];
                            size_t count = bucket_json["count"];
                            col_stats.histogram.push_back({bucket, count});
                        }
                    }
                    
                    stats.column_stats[col_name] = col_stats;
                }
            }
            
            stats_[name] = stats;
        }
    }
    
    return Status::OK();
}

}