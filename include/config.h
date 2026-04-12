// include/config.h
#ifndef CONFIG_H
#define CONFIG_H

#include "constants.h"
#include <string>
#include <unordered_map>
#include <fstream>
#include <nlohmann/json.hpp>

namespace orangesql {

struct DatabaseConfig {
    std::string data_dir = "./data";
    std::string wal_dir = "./data/wal";
    int page_size = PAGE_SIZE;
    int buffer_pool_size = BUFFER_POOL_SIZE;
    int max_connections = MAX_CONNECTION_POOL;
    int max_transactions = MAX_TRANSACTIONS;
    
    struct PerformanceConfig {
        int cache_size_mb = 256;
        int parallel_workers = 4;
        int bulk_insert_threshold = BULK_INSERT_THRESHOLD;
        bool use_direct_io = false;
        int prefetch_pages = 8;
        double sample_rate = DEFAULT_SAMPLE_RATE;
    } performance;
    
    struct IndexConfig {
        int btree_order = BTREE_ORDER;
        bool enable_bloom_filter = true;
        int max_cache_size = CACHE_SIZE;
        bool enable_adaptive_merge = true;
    } index;
    
    struct TransactionConfig {
        bool wal_enabled = true;
        int checkpoint_interval_ms = CHECKPOINT_INTERVAL_MS;
        int lock_timeout_ms = MAX_LOCK_TIMEOUT_MS;
        int deadlock_timeout_ms = DEADLOCK_TIMEOUT_MS;
        std::string isolation_level = "REPEATABLE_READ";
    } transaction;
    
    struct LoggingConfig {
        std::string level = "INFO";
        std::string file = "orangesql.log";
        int max_size_mb = 100;
        bool rotate_daily = true;
        bool console_output = true;
    } logging;
};

class Config {
public:
    static Config& getInstance() {
        static Config instance;
        return instance;
    }
    
    void load(const std::string& path = "config.json") {
        std::ifstream file(path);
        if (file.is_open()) {
            nlohmann::json json;
            file >> json;
            
            if (json.contains("database")) {
                auto& db = json["database"];
                config_.data_dir = db.value("data_dir", config_.data_dir);
                config_.wal_dir = db.value("wal_dir", config_.wal_dir);
                config_.page_size = db.value("page_size", config_.page_size);
                config_.buffer_pool_size = db.value("buffer_pool_size", config_.buffer_pool_size);
                config_.max_connections = db.value("max_connections", config_.max_connections);
            }
            
            if (json.contains("performance")) {
                auto& perf = json["performance"];
                config_.performance.cache_size_mb = perf.value("cache_size_mb", config_.performance.cache_size_mb);
                config_.performance.parallel_workers = perf.value("parallel_workers", config_.performance.parallel_workers);
                config_.performance.bulk_insert_threshold = perf.value("bulk_insert_threshold", config_.performance.bulk_insert_threshold);
                config_.performance.prefetch_pages = perf.value("prefetch_pages", config_.performance.prefetch_pages);
                config_.performance.sample_rate = perf.value("sample_rate", config_.performance.sample_rate);
            }
            
            if (json.contains("index")) {
                auto& idx = json["index"];
                config_.index.btree_order = idx.value("btree_order", config_.index.btree_order);
                config_.index.enable_bloom_filter = idx.value("enable_bloom_filter", config_.index.enable_bloom_filter);
                config_.index.max_cache_size = idx.value("max_cache_size", config_.index.max_cache_size);
            }
            
            if (json.contains("transaction")) {
                auto& txn = json["transaction"];
                config_.transaction.wal_enabled = txn.value("wal_enabled", config_.transaction.wal_enabled);
                config_.transaction.checkpoint_interval_ms = txn.value("checkpoint_interval_ms", config_.transaction.checkpoint_interval_ms);
                config_.transaction.lock_timeout_ms = txn.value("lock_timeout_ms", config_.transaction.lock_timeout_ms);
                config_.transaction.deadlock_timeout_ms = txn.value("deadlock_timeout_ms", config_.transaction.deadlock_timeout_ms);
                config_.transaction.isolation_level = txn.value("isolation_level", config_.transaction.isolation_level);
            }
            
            if (json.contains("logging")) {
                auto& log = json["logging"];
                config_.logging.level = log.value("level", config_.logging.level);
                config_.logging.file = log.value("file", config_.logging.file);
                config_.logging.max_size_mb = log.value("max_size_mb", config_.logging.max_size_mb);
                config_.logging.rotate_daily = log.value("rotate_daily", config_.logging.rotate_daily);
                config_.logging.console_output = log.value("console_output", config_.logging.console_output);
            }
        }
    }
    
    void save(const std::string& path = "config.json") {
        nlohmann::json json;
        
        json["database"]["data_dir"] = config_.data_dir;
        json["database"]["wal_dir"] = config_.wal_dir;
        json["database"]["page_size"] = config_.page_size;
        json["database"]["buffer_pool_size"] = config_.buffer_pool_size;
        json["database"]["max_connections"] = config_.max_connections;
        
        json["performance"]["cache_size_mb"] = config_.performance.cache_size_mb;
        json["performance"]["parallel_workers"] = config_.performance.parallel_workers;
        json["performance"]["bulk_insert_threshold"] = config_.performance.bulk_insert_threshold;
        json["performance"]["prefetch_pages"] = config_.performance.prefetch_pages;
        json["performance"]["sample_rate"] = config_.performance.sample_rate;
        
        json["index"]["btree_order"] = config_.index.btree_order;
        json["index"]["enable_bloom_filter"] = config_.index.enable_bloom_filter;
        json["index"]["max_cache_size"] = config_.index.max_cache_size;
        
        json["transaction"]["wal_enabled"] = config_.transaction.wal_enabled;
        json["transaction"]["checkpoint_interval_ms"] = config_.transaction.checkpoint_interval_ms;
        json["transaction"]["lock_timeout_ms"] = config_.transaction.lock_timeout_ms;
        json["transaction"]["deadlock_timeout_ms"] = config_.transaction.deadlock_timeout_ms;
        json["transaction"]["isolation_level"] = config_.transaction.isolation_level;
        
        json["logging"]["level"] = config_.logging.level;
        json["logging"]["file"] = config_.logging.file;
        json["logging"]["max_size_mb"] = config_.logging.max_size_mb;
        json["logging"]["rotate_daily"] = config_.logging.rotate_daily;
        json["logging"]["console_output"] = config_.logging.console_output;
        
        std::ofstream file(path);
        if (file.is_open()) {
            file << json.dump(4);
        }
    }
    
    const DatabaseConfig& get() const { return config_; }
    DatabaseConfig& get() { return config_; }
    
private:
    Config() = default;
    DatabaseConfig config_;
};

}

#endif