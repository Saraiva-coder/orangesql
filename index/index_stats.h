// index/index_stats.h
#ifndef INDEX_STATS_H
#define INDEX_STATS_H

#include <cstdint>
#include <string>
#include <chrono>

namespace orangesql {

struct IndexStats {
    std::string index_name;
    std::string table_name;
    std::string column_name;
    
    size_t total_keys;
    size_t unique_keys;
    size_t node_count;
    int height;
    size_t cache_hits;
    size_t cache_misses;
    size_t total_inserts;
    size_t total_searches;
    size_t total_deletes;
    
    double avg_fanout;
    double fill_factor;
    double search_cost_estimate;
    
    std::chrono::microseconds last_optimization_time;
    
    IndexStats() : total_keys(0), unique_keys(0), node_count(0), height(0),
                   cache_hits(0), cache_misses(0), total_inserts(0),
                   total_searches(0), total_deletes(0), avg_fanout(0),
                   fill_factor(0), search_cost_estimate(0) {}
    
    double getHitRate() const {
        uint64_t total = cache_hits + cache_misses;
        if (total == 0) return 0.0;
        return static_cast<double>(cache_hits) / static_cast<double>(total);
    }
    
    void reset() {
        cache_hits = 0;
        cache_misses = 0;
        total_inserts = 0;
        total_searches = 0;
        total_deletes = 0;
    }
};

class IndexStatistics {
public:
    static IndexStatistics& getInstance();
    
    void recordHit(const std::string& index_name);
    void recordMiss(const std::string& index_name);
    void recordInsert(const std::string& index_name);
    void recordSearch(const std::string& index_name);
    void recordDelete(const std::string& index_name);
    
    IndexStats getStats(const std::string& index_name);
    void updateStats(const std::string& index_name, size_t total_keys, 
                     size_t node_count, int height);
    
    void persist();
    void load();
    
private:
    IndexStatistics() = default;
    std::unordered_map<std::string, IndexStats> stats_;
    std::shared_mutex mutex_;
};

}

#endif