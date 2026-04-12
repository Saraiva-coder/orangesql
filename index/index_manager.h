// index/index_manager.h
#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

#include "btree.h"
#include "btree_concurrent.h"
#include "../include/types.h"
#include "../include/status.h"
#include <unordered_map>
#include <string>
#include <memory>
#include <shared_mutex>

namespace orangesql {

class Index {
public:
    Index(const std::string& name, const std::string& table, 
          const std::string& column, bool unique);
    ~Index();
    
    Status insert(const Value& key, uint64_t row_id);
    Status search(const Value& key, std::vector<uint64_t>& row_ids);
    Status searchRange(const Value& start, const Value& end, std::vector<uint64_t>& row_ids);
    Status remove(const Value& key);
    Status bulkLoad(const std::vector<Row>& rows, const std::vector<uint64_t>& row_ids);
    
    void disable() { enabled_ = false; }
    void enable() { enabled_ = true; }
    bool isEnabled() const { return enabled_; }
    
    const std::string& getName() const { return name_; }
    const std::string& getTable() const { return table_; }
    const std::string& getColumn() const { return column_; }
    bool isUnique() const { return unique_; }
    
    size_t size() const;
    
private:
    std::string name_;
    std::string table_;
    std::string column_;
    bool unique_;
    bool enabled_;
    std::unique_ptr<ConcurrentBTree<std::string>> btree_;
    
    std::string keyToString(const Value& key);
    Value stringToKey(const std::string& str, DataType type);
};

class IndexManager {
public:
    static IndexManager& getInstance();
    
    Status createIndex(const std::string& name, const std::string& table,
                       const std::string& column, bool unique);
    Status dropIndex(const std::string& name);
    Index* getIndex(const std::string& name);
    Index* getIndexOnColumn(const std::string& table, const std::string& column);
    std::vector<Index*> getIndexesForTable(const std::string& table);
    
    Status load();
    Status persist();
    
private:
    IndexManager() = default;
    
    std::unordered_map<std::string, std::unique_ptr<Index>> indexes_;
    std::unordered_map<std::string, std::vector<std::string>> table_indexes_;
    std::shared_mutex mutex_;
};

}

#endif