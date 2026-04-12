// metadata/statistics.h
#ifndef STATISTICS_H
#define STATISTICS_H

#include "../include/types.h"
#include "../include/status.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <atomic>

namespace orangesql {

struct ColumnStats {
    std::string column_name;
    size_t null_count;
    size_t distinct_count;
    size_t total_count;
    double min_value;
    double max_value;
    double avg_value;
    std::vector<std::pair<std::string, size_t>> histogram;
    
    ColumnStats() : null_count(0), distinct_count(0), total_count(0),
                    min_value(0), max_value(0), avg_value(0) {}
    
    double getSelectivity() const {
        if (total_count == 0) return 1.0;
        return static_cast<double>(distinct_count) / static_cast<double>(total_count);
    }
};

struct TableStats {
    std::string table_name;
    size_t row_count;
    size_t page_count;
    size_t avg_row_size;
    size_t total_size;
    std::unordered_map<std::string, ColumnStats> column_stats;
    std::chrono::system_clock::time_point last_updated;
    
    TableStats() : row_count(0), page_count(0), avg_row_size(0), total_size(0) {}
};

class StatisticsManager {
public:
    static StatisticsManager& getInstance();
    
    Status updateTableStats(const std::string& table_name);
    Status updateColumnStats(const std::string& table_name, const std::string& column_name);
    Status updateAllStats();
    
    TableStats* getTableStats(const std::string& table_name);
    ColumnStats* getColumnStats(const std::string& table_name, const std::string& column_name);
    
    double estimateCardinality(const std::string& table_name, const std::string& column_name,
                               const std::string& value);
    double estimateSelectivity(const std::string& table_name, const std::string& column_name,
                               const std::string& op, const std::string& value);
    
    void enableAutoUpdate(bool enable);
    void setSampleRate(double rate);
    
    Status persist();
    Status load();
    
private:
    StatisticsManager();
    ~StatisticsManager();
    
    std::unordered_map<std::string, TableStats> stats_;
    std::shared_mutex mutex_;
    std::atomic<bool> auto_update_{true};
    std::atomic<double> sample_rate_{0.1};
    
    void computeHistogram(ColumnStats& stats, const std::vector<Value>& values);
    void mergeStats(TableStats& target, const TableStats& source);
};

}

#endif