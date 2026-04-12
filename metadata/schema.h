// metadata/schema.h
#ifndef SCHEMA_H
#define SCHEMA_H

#include "../include/types.h"
#include "../include/status.h"
#include <string>
#include <vector>
#include <unordered_map>

namespace orangesql {

class Schema {
public:
    Schema();
    explicit Schema(const std::vector<Column>& columns);
    ~Schema();
    
    Status addColumn(const Column& column);
    Status removeColumn(const std::string& name);
    Status renameColumn(const std::string& old_name, const std::string& new_name);
    Status modifyColumnType(const std::string& name, DataType new_type);
    
    const Column* getColumn(const std::string& name) const;
    const Column* getColumn(int index) const;
    int getColumnIndex(const std::string& name) const;
    std::vector<Column> getColumns() const;
    size_t getColumnCount() const;
    
    std::vector<std::string> getPrimaryKeys() const;
    bool hasPrimaryKey() const;
    bool isPrimaryKey(const std::string& column) const;
    
    std::string serialize() const;
    Status deserialize(const std::string& data);
    
    bool validateRow(const Row& row) const;
    
private:
    std::vector<Column> columns_;
    std::unordered_map<std::string, int> column_index_;
    std::vector<std::string> primary_keys_;
    
    void rebuildIndex();
};

}

#endif