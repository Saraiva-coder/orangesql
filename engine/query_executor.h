// engine/query_executor.h
#ifndef QUERY_EXECUTOR_H
#define QUERY_EXECUTOR_H

#include "../parser/ast.h"
#include "../storage/table.h"
#include "../storage/buffer_pool.h"
#include "../index/index_manager.h"
#include "../transaction/mvcc.h"
#include "../include/status.h"
#include "../include/types.h"
#include "optimizer.h"
#include <vector>
#include <memory>
#include <unordered_map>

namespace orangesql {

class QueryExecutor {
public:
    QueryExecutor(MVCCManager* mvcc, BufferPool* buffer_pool);
    ~QueryExecutor();
    
    Status execute(const std::string& sql, std::vector<Row>& results);
    Status execute(const ASTNode& ast, std::vector<Row>& results);
    void shutdown();
    
private:
    MVCCManager* mvcc_;
    BufferPool* buffer_pool_;
    Catalog* catalog_;
    IndexManager* index_manager_;
    uint64_t current_transaction_;
    bool in_transaction_;
    
    Status executeSelect(const SelectStatement& stmt, std::vector<Row>& results);
    Status executeInsert(const InsertStatement& stmt);
    Status executeUpdate(const UpdateStatement& stmt);
    Status executeDelete(const DeleteStatement& stmt);
    Status executeCreateTable(const CreateTableStatement& stmt);
    Status executeCreateIndex(const CreateIndexStatement& stmt);
    Status executeDropTable(const DropTableStatement& stmt);
    Status executeDropIndex(const DropIndexStatement& stmt);
    
    bool evaluateCondition(const Row& row, const Condition& cond, const Table* table);
    Value convertValue(const std::string& str, DataType type);
    std::string valueToString(const Value& value);
    
    void beginTransaction();
    void commitTransaction();
    void abortTransaction();
};

}

#endif