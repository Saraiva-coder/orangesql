// index/index_stats.cpp
#include "index_stats.h"
#include <fstream>
#include <nlohmann/json.hpp>

namespace orangesql {

IndexStatistics& IndexStatistics::getInstance() {
    static IndexStatistics instance;
    return instance;
}

void IndexStatistics::recordHit(const std::string& index_name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    stats_[index_name].cache_hits++;
}

void IndexStatistics::recordMiss(const std::string& index_name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    stats_[index_name].cache_misses++;
}

void IndexStatistics::recordInsert(const std::string& index_name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    stats_[index_name].total_inserts++;
    stats_[index_name].total_keys++;
}

void IndexStatistics::recordSearch(const std::string& index_name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    stats_[index_name].total_searches++;
}

void IndexStatistics::recordDelete(const std::string& index_name) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    stats_[index_name].total_deletes++;
    stats_[index_name].total_keys--;
}

IndexStats IndexStatistics::getStats(const std::string& index_name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = stats_.find(index_name);
    if (it != stats_.end()) {
        return it->second;
    }
    return IndexStats();
}

void IndexStatistics::updateStats(const std::string& index_name, size_t total_keys,
                                   size_t node_count, int height) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto& stats = stats_[index_name];
    stats.total_keys = total_keys;
    stats.node_count = node_count;
    stats.height = height;
    
    if (node_count > 0) {
        stats.avg_fanout = static_cast<double>(total_keys) / static_cast<double>(node_count);
    }
    
    if (total_keys > 0) {
        stats.fill_factor = static_cast<double>(total_keys) / 
                           (static_cast<double>(node_count) * BTREE_ORDER);
    }
    
    stats.search_cost_estimate = height * std::log2(total_keys);
}

void IndexStatistics::persist() {
    nlohmann::json json;
    nlohmann::json stats_json = nlohmann::json::array();
    
    for (const auto& [name, stats] : stats_) {
        nlohmann::json item;
        item["index_name"] = stats.index_name;
        item["total_keys"] = stats.total_keys;
        item["node_count"] = stats.node_count;
        item["height"] = stats.height;
        item["cache_hits"] = stats.cache_hits;
        item["cache_misses"] = stats.cache_misses;
        stats_json.push_back(item);
    }
    
    json["stats"] = stats_json;
    
    std::ofstream file("data/system/index_stats.json");
    if (file.is_open()) {
        file << json.dump(4);
    }
}

void IndexStatistics::load() {
    std::ifstream file("data/system/index_stats.json");
    if (!file.is_open()) return;
    
    nlohmann::json json;
    file >> json;
    
    for (const auto& item : json["stats"]) {
        IndexStats stats;
        stats.index_name = item["index_name"];
        stats.total_keys = item["total_keys"];
        stats.node_count = item["node_count"];
        stats.height = item["height"];
        stats.cache_hits = item.value("cache_hits", 0);
        stats.cache_misses = item.value("cache_misses", 0);
        stats_[stats.index_name] = stats;
    }
}

}