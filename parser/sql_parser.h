// parser/sql_parser.h
#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include "ast.h"
#include "lexer.h"
#include "../include/status.h"
#include <vector>
#include <memory>

namespace orangesql {

class SQLParser {
public:
    explicit SQLParser(const std::string& sql);
    ~SQLParser();
    
    Status parse(ASTNode& ast);
    
private:
    std::vector<Token> tokens_;
    size_t pos_;
    
    Token current();
    Token peek(int offset = 1);
    Token consume();
    bool match(TokenType type);
    bool matchAny(const std::vector<TokenType>& types);
    void expect(TokenType type);
    void expectAny(const std::vector<TokenType>& types);
    
    Status parseStatement(ASTNode& ast);
    Status parseSelect(ASTNode& ast);
    Status parseInsert(ASTNode& ast);
    Status parseUpdate(ASTNode& ast);
    Status parseDelete(ASTNode& ast);
    Status parseCreateTable(ASTNode& ast);
    Status parseCreateIndex(ASTNode& ast);
    Status parseDropTable(ASTNode& ast);
    Status parseDropIndex(ASTNode& ast);
    
    std::vector<std::string> parseColumnList();
    WhereClause parseWhereClause();
    Condition parseCondition();
    std::vector<std::pair<std::string, std::string>> parseOrderBy();
    std::vector<std::string> parseGroupBy();
    std::vector<JoinClause> parseJoins();
    std::vector<std::string> parseValueList();
    
    std::string toString(TokenType type);
    Operator toOperator(TokenType type);
    
    void synchronize();
};

}

#endif