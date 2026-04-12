// parser/lexer.h
#ifndef LEXER_H
#define LEXER_H

#include <string>
#include <vector>
#include <unordered_map>

namespace orangesql {

enum class TokenType {
    TOKEN_EOF,
    TOKEN_UNKNOWN,
    
    TOKEN_SELECT,
    TOKEN_INSERT,
    TOKEN_UPDATE,
    TOKEN_DELETE,
    TOKEN_CREATE,
    TOKEN_DROP,
    TOKEN_TABLE,
    TOKEN_INDEX,
    TOKEN_FROM,
    TOKEN_WHERE,
    TOKEN_SET,
    TOKEN_VALUES,
    TOKEN_INTO,
    TOKEN_AND,
    TOKEN_OR,
    TOKEN_NOT,
    TOKEN_LIKE,
    TOKEN_IN,
    TOKEN_BETWEEN,
    TOKEN_IS,
    TOKEN_NULL,
    TOKEN_ORDER,
    TOKEN_BY,
    TOKEN_GROUP,
    TOKEN_HAVING,
    TOKEN_LIMIT,
    TOKEN_OFFSET,
    TOKEN_JOIN,
    TOKEN_INNER,
    TOKEN_LEFT,
    TOKEN_RIGHT,
    TOKEN_OUTER,
    TOKEN_ON,
    TOKEN_AS,
    TOKEN_DISTINCT,
    TOKEN_PRIMARY,
    TOKEN_KEY,
    TOKEN_UNIQUE,
    TOKEN_ASC,
    TOKEN_DESC,
    
    TOKEN_EQ,
    TOKEN_NE,
    TOKEN_GT,
    TOKEN_LT,
    TOKEN_GE,
    TOKEN_LE,
    
    TOKEN_SEMICOLON,
    TOKEN_COMMA,
    TOKEN_LPAREN,
    TOKEN_RPAREN,
    TOKEN_STAR,
    TOKEN_DOT,
    
    TOKEN_IDENTIFIER,
    TOKEN_STRING,
    TOKEN_INTEGER,
    TOKEN_FLOAT
};

struct Token {
    TokenType type;
    std::string value;
    int line;
    int column;
    
    Token() : type(TokenType::TOKEN_UNKNOWN), line(0), column(0) {}
    Token(TokenType t, const std::string& v, int l = 0, int c = 0) 
        : type(t), value(v), line(l), column(c) {}
};

class Lexer {
public:
    explicit Lexer(const std::string& source);
    
    std::vector<Token> tokenize();
    Token nextToken();
    void reset();
    
private:
    std::string source_;
    size_t pos_;
    int line_;
    int column_;
    
    char peek();
    char peek(int offset);
    char advance();
    void skipWhitespace();
    void skipComment();
    
    Token readIdentifier();
    Token readNumber();
    Token readString();
    Token readOperator();
    
    bool isAlpha(char c);
    bool isDigit(char c);
    bool isAlphaNumeric(char c);
    
    std::string toString(TokenType type);
    TokenType keywordToType(const std::string& keyword);
    
    static std::unordered_map<std::string, TokenType> keywords_;
};

}

#endif