// metadata/catalog.h
#ifndef CATALOG_H
#define CATALOG_H

#include "../include/types.h"
#include "../include/status.h"
#include "../storage/table.h"
#include "../index/index_manager.h"
#include <unordered_map>
#include <shared_mutex>
#include <string>
#include <vector>
#include <memory>

namespace orangesql {

class Catalog {
public:
    static Catalog* getInstance();
    
    Status createTable(const std::string& name, const std::vector<Column>& schema);
    Status dropTable(const std::string& name);
    Table* getTable(const std::string& name);
    std::vector<Table*> getAllTables();
    bool tableExists(const std::string& name);
    
    Status createIndex(const std::string& name, const std::string& table, 
                       const std::string& column, bool unique);
    Status dropIndex(const std::string& name);
    Index* getIndex(const std::string& name);
    Index* getIndexOnColumn(const std::string& table, const std::string& column);
    std::vector<Index*> getIndexesForTable(const std::string& table);
    
    Status load();
    Status persist();
    void refresh();
    
private:
    Catalog();
    ~Catalog();
    
    std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
    std::unordered_map<std::string, std::unique_ptr<Index>> indexes_;
    std::unordered_map<std::string, std::vector<std::string>> table_indexes_;
    mutable std::shared_mutex mutex_;
    
    Status loadSystemTables();
    Status saveSystemTables();
    std::string getCatalogPath();
};

}

#endif