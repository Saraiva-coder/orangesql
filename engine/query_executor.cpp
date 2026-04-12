// engine/query_executor.cpp
#include "query_executor.h"
#include "../parser/sql_parser.h"
#include "../include/logger.h"
#include "../include/config.h"
#include <algorithm>
#include <chrono>
#include <cctype>

namespace orangesql {

QueryExecutor::QueryExecutor(MVCCManager* mvcc, BufferPool* buffer_pool)
    : mvcc_(mvcc), buffer_pool_(buffer_pool), current_transaction_(0), in_transaction_(false) {
    catalog_ = Catalog::getInstance();
    index_manager_ = &IndexManager::getInstance();
}

QueryExecutor::~QueryExecutor() {
    shutdown();
}

void QueryExecutor::shutdown() {
    if (in_transaction_) {
        abortTransaction();
    }
}

Status QueryExecutor::execute(const std::string& sql, std::vector<Row>& results) {
    SQLParser parser(sql);
    ASTNode ast;
    
    Status status = parser.parse(ast);
    if (!status.ok()) {
        LOG_ERROR("Parse error: " + status.message());
        return status;
    }
    
    return execute(ast, results);
}

Status QueryExecutor::execute(const ASTNode& ast, std::vector<Row>& results) {
    beginTransaction();
    
    Status status;
    
    try {
        switch (ast.type) {
            case NodeType::SELECT:
                status = executeSelect(ast.select_stmt, results);
                break;
            case NodeType::INSERT:
                status = executeInsert(ast.insert_stmt);
                break;
            case NodeType::UPDATE:
                status = executeUpdate(ast.update_stmt);
                break;
            case NodeType::DELETE:
                status = executeDelete(ast.delete_stmt);
                break;
            case NodeType::CREATE_TABLE:
                status = executeCreateTable(ast.create_table_stmt);
                break;
            case NodeType::CREATE_INDEX:
                status = executeCreateIndex(ast.create_index_stmt);
                break;
            case NodeType::DROP_TABLE:
                status = executeDropTable(ast.drop_table_stmt);
                break;
            case NodeType::DROP_INDEX:
                status = executeDropIndex(ast.drop_index_stmt);
                break;
            default:
                status = Status::Error("Unknown statement type");
        }
    } catch (const std::exception& e) {
        status = Status::Error(e.what());
    }
    
    if (status.ok()) {
        commitTransaction();
    } else {
        abortTransaction();
        LOG_ERROR("Query failed: " + status.message());
    }
    
    return status;
}

void QueryExecutor::beginTransaction() {
    if (!in_transaction_) {
        mvcc_->beginTransaction(current_transaction_);
        in_transaction_ = true;
    }
}

void QueryExecutor::commitTransaction() {
    if (in_transaction_) {
        mvcc_->commitTransaction(current_transaction_);
        in_transaction_ = false;
    }
}

void QueryExecutor::abortTransaction() {
    if (in_transaction_) {
        mvcc_->abortTransaction(current_transaction_);
        in_transaction_ = false;
    }
}

Status QueryExecutor::executeSelect(const SelectStatement& stmt, std::vector<Row>& results) {
    Table* table = catalog_->getTable(stmt.table_name);
    if (!table) {
        return Status::NotFound("Table not found: " + stmt.table_name);
    }
    
    std::vector<uint64_t> row_ids;
    Status status = table->getAllRowIds(row_ids);
    if (!status.ok()) {
        return status;
    }
    
    for (uint64_t row_id : row_ids) {
        Row row;
        status = mvcc_->selectRecord(current_transaction_, stmt.table_name, row_id, row);
        if (!status.ok()) {
            continue;
        }
        
        bool matches = true;
        for (const auto& cond : stmt.where_clause.conditions) {
            if (!evaluateCondition(row, cond, table)) {
                matches = false;
                break;
            }
        }
        
        if (matches) {
            results.push_back(row);
        }
    }
    
    if (!stmt.columns.empty() && stmt.columns[0] != "*") {
        std::vector<Row> projected;
        for (auto& row : results) {
            Row new_row;
            for (const auto& col_name : stmt.columns) {
                int idx = table->getColumnIndex(col_name);
                if (idx >= 0 && idx < (int)row.values.size()) {
                    new_row.values.push_back(row.values[idx]);
                }
            }
            new_row.row_id = row.row_id;
            projected.push_back(new_row);
        }
        results = projected;
    }
    
    if (!stmt.order_by.empty()) {
        std::sort(results.begin(), results.end(), [&](const Row& a, const Row& b) {
            for (const auto& order : stmt.order_by) {
                int idx = table->getColumnIndex(order.first);
                if (idx >= 0) {
                    std::string val_a = a.values[idx].toString();
                    std::string val_b = b.values[idx].toString();
                    if (val_a != val_b) {
                        return order.second == "ASC" ? val_a < val_b : val_a > val_b;
                    }
                }
            }
            return false;
        });
    }
    
    if (stmt.limit >= 0) {
        int start = stmt.offset;
        int end = std::min(start + stmt.limit, (int)results.size());
        if (start < (int)results.size()) {
            results = std::vector<Row>(results.begin() + start, results.begin() + end);
        } else {
            results.clear();
        }
    }
    
    return Status::OK();
}

Status QueryExecutor::executeInsert(const InsertStatement& stmt) {
    Table* table = catalog_->getTable(stmt.table_name);
    if (!table) {
        return Status::NotFound("Table not found: " + stmt.table_name);
    }
    
    const auto& schema = table->getSchema();
    
    for (const auto& row_values : stmt.values) {
        Row row;
        row.transaction_id = current_transaction_;
        
        if (stmt.columns.empty()) {
            if (row_values.size() != schema.size()) {
                return Status::Error("Column count mismatch");
            }
            
            for (size_t i = 0; i < schema.size(); i++) {
                DataType col_type = schema[i].type;
                Value val = convertValue(row_values[i], col_type);
                row.values.push_back(val);
            }
        } else {
            row.values.resize(schema.size());
            for (size_t i = 0; i < schema.size(); i++) {
                row.values[i] = Value();
                row.values[i].type = schema[i].type;
            }
            
            for (size_t i = 0; i < stmt.columns.size(); i++) {
                int col_idx = table->getColumnIndex(stmt.columns[i]);
                if (col_idx < 0) {
                    return Status::Error("Column not found: " + stmt.columns[i]);
                }
                DataType col_type = schema[col_idx].type;
                row.values[col_idx] = convertValue(row_values[i], col_type);
            }
        }
        
        Status status = mvcc_->insertRecord(current_transaction_, stmt.table_name, row);
        if (!status.ok()) {
            return status;
        }
        
        auto indexes = index_manager_->getIndexesForTable(stmt.table_name);
        for (auto* index : indexes) {
            int col_idx = table->getColumnIndex(index->getColumnName());
            if (col_idx >= 0 && col_idx < (int)row.values.size()) {
                index->insert(row.values[col_idx], row.row_id);
            }
        }
    }
    
    LOG_INFO("Inserted " + std::to_string(stmt.values.size()) + " rows into " + stmt.table_name);
    return Status::OK();
}

Status QueryExecutor::executeUpdate(const UpdateStatement& stmt) {
    Table* table = catalog_->getTable(stmt.table_name);
    if (!table) {
        return Status::NotFound("Table not found: " + stmt.table_name);
    }
    
    std::vector<uint64_t> row_ids;
    Status status = table->getAllRowIds(row_ids);
    if (!status.ok()) {
        return status;
    }
    
    int updated = 0;
    
    for (uint64_t row_id : row_ids) {
        Row old_row;
        status = mvcc_->selectRecord(current_transaction_, stmt.table_name, row_id, old_row);
        if (!status.ok()) {
            continue;
        }
        
        bool matches = true;
        for (const auto& cond : stmt.where_clause.conditions) {
            if (!evaluateCondition(old_row, cond, table)) {
                matches = false;
                break;
            }
        }
        
        if (matches) {
            Row new_row = old_row;
            for (const auto& set_pair : stmt.set_clause) {
                int col_idx = table->getColumnIndex(set_pair.first);
                if (col_idx >= 0) {
                    DataType col_type = table->getColumn(col_idx).type;
                    new_row.values[col_idx] = convertValue(set_pair.second, col_type);
                }
            }
            
            status = mvcc_->updateRecord(current_transaction_, stmt.table_name, row_id, new_row);
            if (status.ok()) {
                updated++;
            }
        }
    }
    
    LOG_INFO("Updated " + std::to_string(updated) + " rows in " + stmt.table_name);
    return Status::OK();
}

Status QueryExecutor::executeDelete(const DeleteStatement& stmt) {
    Table* table = catalog_->getTable(stmt.table_name);
    if (!table) {
        return Status::NotFound("Table not found: " + stmt.table_name);
    }
    
    std::vector<uint64_t> row_ids;
    Status status = table->getAllRowIds(row_ids);
    if (!status.ok()) {
        return status;
    }
    
    int deleted = 0;
    
    for (uint64_t row_id : row_ids) {
        Row row;
        status = mvcc_->selectRecord(current_transaction_, stmt.table_name, row_id, row);
        if (!status.ok()) {
            continue;
        }
        
        bool matches = true;
        for (const auto& cond : stmt.where_clause.conditions) {
            if (!evaluateCondition(row, cond, table)) {
                matches = false;
                break;
            }
        }
        
        if (matches) {
            status = mvcc_->deleteRecord(current_transaction_, stmt.table_name, row_id);
            if (status.ok()) {
                deleted++;
            }
        }
    }
    
    LOG_INFO("Deleted " + std::to_string(deleted) + " rows from " + stmt.table_name);
    return Status::OK();
}

Status QueryExecutor::executeCreateTable(const CreateTableStatement& stmt) {
    std::vector<Column> columns;
    
    for (const auto& col_tuple : stmt.columns) {
        Column col;
        col.name = std::get<0>(col_tuple);
        std::string type_str = std::get<1>(col_tuple);
        col.nullable = std::get<2>(col_tuple);
        col.primary_key = false;
        
        std::transform(type_str.begin(), type_str.end(), type_str.begin(), ::toupper);
        
        if (type_str == "INT" || type_str == "INTEGER") {
            col.type = DataType::INTEGER;
        } else if (type_str == "BIGINT") {
            col.type = DataType::BIGINT;
        } else if (type_str == "FLOAT") {
            col.type = DataType::FLOAT;
        } else if (type_str == "DOUBLE") {
            col.type = DataType::DOUBLE;
        } else if (type_str == "TEXT" || type_str == "STRING") {
            col.type = DataType::TEXT;
        } else if (type_str == "VARCHAR") {
            col.type = DataType::VARCHAR;
            col.length = 255;
        } else if (type_str == "BOOLEAN") {
            col.type = DataType::BOOLEAN;
        } else {
            return Status::Error("Unknown data type: " + type_str);
        }
        
        columns.push_back(col);
    }
    
    for (const auto& pk : stmt.primary_keys) {
        for (auto& col : columns) {
            if (col.name == pk) {
                col.primary_key = true;
                col.nullable = false;
                break;
            }
        }
    }
    
    Status status = catalog_->createTable(stmt.table_name, columns);
    if (status.ok()) {
        LOG_INFO("Table created: " + stmt.table_name);
    }
    
    return status;
}

Status QueryExecutor::executeCreateIndex(const CreateIndexStatement& stmt) {
    Table* table = catalog_->getTable(stmt.table_name);
    if (!table) {
        return Status::NotFound("Table not found: " + stmt.table_name);
    }
    
    int col_idx = table->getColumnIndex(stmt.column);
    if (col_idx < 0) {
        return Status::Error("Column not found: " + stmt.column);
    }
    
    Status status = index_manager_->createIndex(stmt.index_name, stmt.table_name, stmt.column, stmt.unique);
    if (status.ok()) {
        LOG_INFO("Index created: " + stmt.index_name + " ON " + stmt.table_name + "(" + stmt.column + ")");
    }
    
    return status;
}

Status QueryExecutor::executeDropTable(const DropTableStatement& stmt) {
    Status status = catalog_->dropTable(stmt.table_name);
    if (status.ok()) {
        LOG_INFO("Table dropped: " + stmt.table_name);
    }
    return status;
}

Status QueryExecutor::executeDropIndex(const DropIndexStatement& stmt) {
    Status status = index_manager_->dropIndex(stmt.index_name);
    if (status.ok()) {
        LOG_INFO("Index dropped: " + stmt.index_name);
    }
    return status;
}

bool QueryExecutor::evaluateCondition(const Row& row, const Condition& cond, const Table* table) {
    int col_idx = table->getColumnIndex(cond.column);
    if (col_idx < 0 || col_idx >= (int)row.values.size()) {
        return false;
    }
    
    const Value& col_value = row.values[col_idx];
    std::string col_str = col_value.toString();
    
    switch (cond.op) {
        case Operator::EQ:
            return col_str == cond.value;
        case Operator::NE:
            return col_str != cond.value;
        case Operator::GT:
            return col_str > cond.value;
        case Operator::LT:
            return col_str < cond.value;
        case Operator::GE:
            return col_str >= cond.value;
        case Operator::LE:
            return col_str <= cond.value;
        case Operator::LIKE: {
            std::string pattern = cond.value;
            size_t pos = 0;
            for (char c : pattern) {
                if (c == '%') {
                    return true;
                }
            }
            return col_str.find(pattern) != std::string::npos;
        }
        case Operator::IN:
            for (const auto& val : cond.in_values) {
                if (col_str == val) return true;
            }
            return false;
        case Operator::BETWEEN:
            return col_str >= cond.between_start && col_str <= cond.between_end;
        case Operator::IS_NULL:
            return col_str.empty() || col_str == "NULL";
        case Operator::IS_NOT_NULL:
            return !col_str.empty() && col_str != "NULL";
        default:
            return false;
    }
}

Value QueryExecutor::convertValue(const std::string& str, DataType type) {
    Value val;
    val.type = type;
    
    std::string upper = str;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    
    if (upper == "NULL") {
        return val;
    }
    
    switch (type) {
        case DataType::INTEGER:
        case DataType::BIGINT:
            val.int_val = std::stoll(str);
            break;
        case DataType::FLOAT:
        case DataType::DOUBLE:
            val.double_val = std::stod(str);
            break;
        case DataType::BOOLEAN:
            val.bool_val = (upper == "TRUE" || upper == "1" || upper == "YES");
            break;
        case DataType::TEXT:
        case DataType::VARCHAR:
            val.str_val = str;
            break;
        default:
            val.str_val = str;
    }
    
    return val;
}

}