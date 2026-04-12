// parser/sql_parser.cpp
#include "sql_parser.h"
#include <stdexcept>
#include <algorithm>

namespace orangesql {

SQLParser::SQLParser(const std::string& sql) : pos_(0) {
    Lexer lexer(sql);
    tokens_ = lexer.tokenize();
}

SQLParser::~SQLParser() {}

Status SQLParser::parse(ASTNode& ast) {
    if (tokens_.empty()) {
        return Status::Error("Empty SQL statement");
    }
    
    try {
        return parseStatement(ast);
    } catch (const std::exception& e) {
        return Status::Error(std::string("Parse error: ") + e.what());
    }
}

Status SQLParser::parseStatement(ASTNode& ast) {
    if (pos_ >= tokens_.size()) {
        return Status::Error("Unexpected end of input");
    }
    
    Token token = current();
    
    switch (token.type) {
        case TokenType::TOKEN_SELECT:
            return parseSelect(ast);
        case TokenType::TOKEN_INSERT:
            return parseInsert(ast);
        case TokenType::TOKEN_UPDATE:
            return parseUpdate(ast);
        case TokenType::TOKEN_DELETE:
            return parseDelete(ast);
        case TokenType::TOKEN_CREATE:
            if (peek().type == TokenType::TOKEN_TABLE) {
                return parseCreateTable(ast);
            } else if (peek().type == TokenType::TOKEN_INDEX) {
                return parseCreateIndex(ast);
            }
            return Status::Error("Expected TABLE or INDEX after CREATE");
        case TokenType::TOKEN_DROP:
            if (peek().type == TokenType::TOKEN_TABLE) {
                return parseDropTable(ast);
            } else if (peek().type == TokenType::TOKEN_INDEX) {
                return parseDropIndex(ast);
            }
            return Status::Error("Expected TABLE or INDEX after DROP");
        default:
            return Status::Error("Unknown statement type: " + token.value);
    }
}

Status SQLParser::parseSelect(ASTNode& ast) {
    ast.type = NodeType::SELECT;
    SelectStatement& stmt = ast.select_stmt;
    
    consume();
    
    if (match(TokenType::TOKEN_DISTINCT)) {
        stmt.distinct = true;
    }
    
    if (match(TokenType::TOKEN_STAR)) {
        stmt.columns.push_back("*");
    } else {
        stmt.columns = parseColumnList();
    }
    
    expect(TokenType::TOKEN_FROM);
    stmt.table_name = consume().value;
    
    stmt.joins = parseJoins();
    
    if (match(TokenType::TOKEN_WHERE)) {
        stmt.where_clause = parseWhereClause();
    }
    
    if (match(TokenType::TOKEN_GROUP)) {
        expect(TokenType::TOKEN_BY);
        stmt.group_by = parseGroupBy();
        
        if (match(TokenType::TOKEN_HAVING)) {
            stmt.where_clause = parseWhereClause();
        }
    }
    
    if (match(TokenType::TOKEN_ORDER)) {
        expect(TokenType::TOKEN_BY);
        stmt.order_by = parseOrderBy();
    }
    
    if (match(TokenType::TOKEN_LIMIT)) {
        stmt.limit = std::stoi(consume().value);
    }
    
    if (match(TokenType::TOKEN_OFFSET)) {
        stmt.offset = std::stoi(consume().value);
    }
    
    expect(TokenType::TOKEN_SEMICOLON);
    return Status::OK();
}

Status SQLParser::parseInsert(ASTNode& ast) {
    ast.type = NodeType::INSERT;
    InsertStatement& stmt = ast.insert_stmt;
    
    consume();
    expect(TokenType::TOKEN_INTO);
    stmt.table_name = consume().value;
    
    if (match(TokenType::TOKEN_LPAREN)) {
        stmt.columns = parseColumnList();
        expect(TokenType::TOKEN_RPAREN);
    }
    
    expect(TokenType::TOKEN_VALUES);
    
    do {
        expect(TokenType::TOKEN_LPAREN);
        stmt.values.push_back(parseValueList());
        expect(TokenType::TOKEN_RPAREN);
    } while (match(TokenType::TOKEN_COMMA));
    
    expect(TokenType::TOKEN_SEMICOLON);
    return Status::OK();
}

Status SQLParser::parseUpdate(ASTNode& ast) {
    ast.type = NodeType::UPDATE;
    UpdateStatement& stmt = ast.update_stmt;
    
    consume();
    stmt.table_name = consume().value;
    expect(TokenType::TOKEN_SET);
    
    do {
        std::string column = consume().value;
        expect(TokenType::TOKEN_EQ);
        std::string value = consume().value;
        stmt.set_clause.push_back({column, value});
    } while (match(TokenType::TOKEN_COMMA));
    
    if (match(TokenType::TOKEN_WHERE)) {
        stmt.where_clause = parseWhereClause();
    }
    
    expect(TokenType::TOKEN_SEMICOLON);
    return Status::OK();
}

Status SQLParser::parseDelete(ASTNode& ast) {
    ast.type = NodeType::DELETE;
    DeleteStatement& stmt = ast.delete_stmt;
    
    consume();
    expect(TokenType::TOKEN_FROM);
    stmt.table_name = consume().value;
    
    if (match(TokenType::TOKEN_WHERE)) {
        stmt.where_clause = parseWhereClause();
    }
    
    expect(TokenType::TOKEN_SEMICOLON);
    return Status::OK();
}

Status SQLParser::parseCreateTable(ASTNode& ast) {
    ast.type = NodeType::CREATE_TABLE;
    CreateTableStatement& stmt = ast.create_table_stmt;
    
    consume();
    consume();
    stmt.table_name = consume().value;
    expect(TokenType::TOKEN_LPAREN);
    
    do {
        std::string column_name = consume().value;
        std::string data_type = consume().value;
        bool nullable = true;
        
        std::transform(data_type.begin(), data_type.end(), data_type.begin(), ::toupper);
        
        if (match(TokenType::TOKEN_NOT)) {
            expect(TokenType::TOKEN_NULL);
            nullable = false;
        }
        
        stmt.columns.push_back({column_name, data_type, nullable});
        
        if (match(TokenType::TOKEN_PRIMARY)) {
            expect(TokenType::TOKEN_KEY);
            stmt.primary_keys.push_back(column_name);
        }
        
        if (match(TokenType::TOKEN_UNIQUE)) {
            stmt.primary_keys.push_back(column_name);
        }
        
    } while (match(TokenType::TOKEN_COMMA));
    
    expect(TokenType::TOKEN_RPAREN);
    expect(TokenType::TOKEN_SEMICOLON);
    return Status::OK();
}

Status SQLParser::parseCreateIndex(ASTNode& ast) {
    ast.type = NodeType::CREATE_INDEX;
    CreateIndexStatement& stmt = ast.create_index_stmt;
    
    consume();
    consume();
    
    if (match(TokenType::TOKEN_UNIQUE)) {
        stmt.unique = true;
    }
    
    stmt.index_name = consume().value;
    expect(TokenType::TOKEN_ON);
    stmt.table_name = consume().value;
    expect(TokenType::TOKEN_LPAREN);
    stmt.column = consume().value;
    expect(TokenType::TOKEN_RPAREN);
    expect(TokenType::TOKEN_SEMICOLON);
    return Status::OK();
}

Status SQLParser::parseDropTable(ASTNode& ast) {
    ast.type = NodeType::DROP_TABLE;
    DropTableStatement& stmt = ast.drop_table_stmt;
    
    consume();
    consume();
    stmt.table_name = consume().value;
    expect(TokenType::TOKEN_SEMICOLON);
    return Status::OK();
}

Status SQLParser::parseDropIndex(ASTNode& ast) {
    ast.type = NodeType::DROP_INDEX;
    DropIndexStatement& stmt = ast.drop_index_stmt;
    
    consume();
    consume();
    stmt.index_name = consume().value;
    expect(TokenType::TOKEN_SEMICOLON);
    return Status::OK();
}

std::vector<std::string> SQLParser::parseColumnList() {
    std::vector<std::string> columns;
    
    do {
        columns.push_back(consume().value);
    } while (match(TokenType::TOKEN_COMMA));
    
    return columns;
}

WhereClause SQLParser::parseWhereClause() {
    WhereClause where;
    where.conditions.push_back(parseCondition());
    
    while (match(TokenType::TOKEN_AND) || match(TokenType::TOKEN_OR)) {
        where.conditions.push_back(parseCondition());
    }
    
    return where;
}

Condition SQLParser::parseCondition() {
    Condition cond;
    
    cond.column = consume().value;
    Token op = consume();
    cond.op = toOperator(op.type);
    
    if (match(TokenType::TOKEN_LPAREN)) {
        cond.in_values = parseValueList();
        expect(TokenType::TOKEN_RPAREN);
    } else if (match(TokenType::TOKEN_BETWEEN)) {
        cond.between_start = consume().value;
        expect(TokenType::TOKEN_AND);
        cond.between_end = consume().value;
        cond.op = Operator::BETWEEN;
    } else if (match(TokenType::TOKEN_IS)) {
        if (match(TokenType::TOKEN_NULL)) {
            cond.op = Operator::IS_NULL;
        } else if (match(TokenType::TOKEN_NOT)) {
            expect(TokenType::TOKEN_NULL);
            cond.op = Operator::IS_NOT_NULL;
        }
    } else {
        cond.value = consume().value;
    }
    
    return cond;
}

std::vector<std::string> SQLParser::parseValueList() {
    std::vector<std::string> values;
    
    do {
        values.push_back(consume().value);
    } while (match(TokenType::TOKEN_COMMA));
    
    return values;
}

std::vector<std::pair<std::string, std::string>> SQLParser::parseOrderBy() {
    std::vector<std::pair<std::string, std::string>> order_by;
    
    do {
        std::string column = consume().value;
        std::string direction = "ASC";
        
        if (match(TokenType::TOKEN_ASC)) {
            direction = "ASC";
        } else if (match(TokenType::TOKEN_DESC)) {
            direction = "DESC";
        }
        
        order_by.push_back({column, direction});
    } while (match(TokenType::TOKEN_COMMA));
    
    return order_by;
}

std::vector<std::string> SQLParser::parseGroupBy() {
    std::vector<std::string> group_by;
    
    do {
        group_by.push_back(consume().value);
    } while (match(TokenType::TOKEN_COMMA));
    
    return group_by;
}

std::vector<JoinClause> SQLParser::parseJoins() {
    std::vector<JoinClause> joins;
    
    while (matchAny({TokenType::TOKEN_INNER, TokenType::TOKEN_LEFT, 
                     TokenType::TOKEN_RIGHT, TokenType::TOKEN_JOIN})) {
        JoinClause join;
        
        if (current().type == TokenType::TOKEN_JOIN) {
            join.type = "INNER";
        } else {
            join.type = consume().value;
            if (join.type == "LEFT" || join.type == "RIGHT") {
                if (match(TokenType::TOKEN_OUTER)) {
                    join.type += " OUTER";
                }
            }
            expect(TokenType::TOKEN_JOIN);
        }
        
        join.table = consume().value;
        expect(TokenType::TOKEN_ON);
        join.left_column = consume().value;
        expect(TokenType::TOKEN_EQ);
        join.right_column = consume().value;
        
        joins.push_back(join);
    }
    
    return joins;
}

Token SQLParser::current() {
    if (pos_ < tokens_.size()) {
        return tokens_[pos_];
    }
    return Token(TokenType::TOKEN_EOF, "");
}

Token SQLParser::peek(int offset) {
    if (pos_ + offset - 1 < tokens_.size()) {
        return tokens_[pos_ + offset - 1];
    }
    return Token(TokenType::TOKEN_EOF, "");
}

Token SQLParser::consume() {
    if (pos_ < tokens_.size()) {
        return tokens_[pos_++];
    }
    throw std::runtime_error("Unexpected end of input");
}

bool SQLParser::match(TokenType type) {
    if (pos_ < tokens_.size() && current().type == type) {
        pos_++;
        return true;
    }
    return false;
}

bool SQLParser::matchAny(const std::vector<TokenType>& types) {
    for (auto type : types) {
        if (match(type)) {
            return true;
        }
    }
    return false;
}

void SQLParser::expect(TokenType type) {
    if (!match(type)) {
        std::string expected = toString(type);
        std::string found = current().value;
        throw std::runtime_error("Expected " + expected + ", got " + found);
    }
}

void SQLParser::expectAny(const std::vector<TokenType>& types) {
    for (auto type : types) {
        if (match(type)) {
            return;
        }
    }
    throw std::runtime_error("Expected one of the tokens");
}

std::string SQLParser::toString(TokenType type) {
    switch (type) {
        case TokenType::TOKEN_SELECT: return "SELECT";
        case TokenType::TOKEN_FROM: return "FROM";
        case TokenType::TOKEN_WHERE: return "WHERE";
        case TokenType::TOKEN_SEMICOLON: return ";";
        case TokenType::TOKEN_IDENTIFIER: return "identifier";
        default: return "token";
    }
}

Operator SQLParser::toOperator(TokenType type) {
    switch (type) {
        case TokenType::TOKEN_EQ: return Operator::EQ;
        case TokenType::TOKEN_NE: return Operator::NE;
        case TokenType::TOKEN_GT: return Operator::GT;
        case TokenType::TOKEN_LT: return Operator::LT;
        case TokenType::TOKEN_GE: return Operator::GE;
        case TokenType::TOKEN_LE: return Operator::LE;
        case TokenType::TOKEN_LIKE: return Operator::LIKE;
        case TokenType::TOKEN_IN: return Operator::IN;
        case TokenType::TOKEN_BETWEEN: return Operator::BETWEEN;
        default: return Operator::EQ;
    }
}

void SQLParser::synchronize() {
    while (pos_ < tokens_.size()) {
        TokenType type = current().type;
        if (type == TokenType::TOKEN_SEMICOLON) {
            pos_++;
            return;
        }
        pos_++;
    }
}

}