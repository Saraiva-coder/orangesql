// parser/ast.h
#ifndef AST_H
#define AST_H

#include <string>
#include <vector>
#include <memory>
#include <variant>

namespace orangesql {

enum class NodeType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    CREATE_TABLE,
    CREATE_INDEX,
    DROP_TABLE,
    DROP_INDEX,
    UNKNOWN
};

enum class Operator {
    EQ,
    NE,
    GT,
    LT,
    GE,
    LE,
    LIKE,
    AND,
    OR,
    IN,
    BETWEEN,
    IS_NULL,
    IS_NOT_NULL
};

struct Condition {
    std::string column;
    Operator op;
    std::string value;
    std::string column2;
    std::vector<std::string> in_values;
    std::string between_start;
    std::string between_end;
    
    Condition() : op(Operator::EQ) {}
};

struct WhereClause {
    std::vector<Condition> conditions;
    bool has_range = false;
    std::string start_value;
    std::string end_value;
    std::string value;
};

struct JoinClause {
    std::string type;
    std::string table;
    std::string left_column;
    std::string right_column;
};

struct SelectStatement {
    std::vector<std::string> columns;
    std::string table_name;
    WhereClause where_clause;
    std::vector<JoinClause> joins;
    std::vector<std::string> group_by;
    std::vector<std::pair<std::string, std::string>> order_by;
    int limit;
    int offset;
    bool distinct;
    
    SelectStatement() : limit(-1), offset(0), distinct(false) {}
};

struct InsertStatement {
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> values;
};

struct UpdateStatement {
    std::string table_name;
    std::vector<std::pair<std::string, std::string>> set_clause;
    WhereClause where_clause;
};

struct DeleteStatement {
    std::string table_name;
    WhereClause where_clause;
};

struct CreateTableStatement {
    std::string table_name;
    std::vector<std::tuple<std::string, std::string, bool>> columns;
    std::vector<std::string> primary_keys;
};

struct CreateIndexStatement {
    std::string index_name;
    std::string table_name;
    std::string column;
    bool unique;
    
    CreateIndexStatement() : unique(false) {}
};

struct DropTableStatement {
    std::string table_name;
};

struct DropIndexStatement {
    std::string index_name;
};

struct ASTNode {
    NodeType type;
    SelectStatement select_stmt;
    InsertStatement insert_stmt;
    UpdateStatement update_stmt;
    DeleteStatement delete_stmt;
    CreateTableStatement create_table_stmt;
    CreateIndexStatement create_index_stmt;
    DropTableStatement drop_table_stmt;
    DropIndexStatement drop_index_stmt;
    
    ASTNode() : type(NodeType::UNKNOWN) {}
};

}

#endif