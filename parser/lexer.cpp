// parser/lexer.cpp
#include "lexer.h"
#include <cctype>
#include <stdexcept>

namespace orangesql {

std::unordered_map<std::string, TokenType> Lexer::keywords_ = {
    {"SELECT", TokenType::TOKEN_SELECT},
    {"INSERT", TokenType::TOKEN_INSERT},
    {"UPDATE", TokenType::TOKEN_UPDATE},
    {"DELETE", TokenType::TOKEN_DELETE},
    {"CREATE", TokenType::TOKEN_CREATE},
    {"DROP", TokenType::TOKEN_DROP},
    {"TABLE", TokenType::TOKEN_TABLE},
    {"INDEX", TokenType::TOKEN_INDEX},
    {"FROM", TokenType::TOKEN_FROM},
    {"WHERE", TokenType::TOKEN_WHERE},
    {"SET", TokenType::TOKEN_SET},
    {"VALUES", TokenType::TOKEN_VALUES},
    {"INTO", TokenType::TOKEN_INTO},
    {"AND", TokenType::TOKEN_AND},
    {"OR", TokenType::TOKEN_OR},
    {"NOT", TokenType::TOKEN_NOT},
    {"LIKE", TokenType::TOKEN_LIKE},
    {"IN", TokenType::TOKEN_IN},
    {"BETWEEN", TokenType::TOKEN_BETWEEN},
    {"IS", TokenType::TOKEN_IS},
    {"NULL", TokenType::TOKEN_NULL},
    {"ORDER", TokenType::TOKEN_ORDER},
    {"BY", TokenType::TOKEN_BY},
    {"GROUP", TokenType::TOKEN_GROUP},
    {"HAVING", TokenType::TOKEN_HAVING},
    {"LIMIT", TokenType::TOKEN_LIMIT},
    {"OFFSET", TokenType::TOKEN_OFFSET},
    {"JOIN", TokenType::TOKEN_JOIN},
    {"INNER", TokenType::TOKEN_INNER},
    {"LEFT", TokenType::TOKEN_LEFT},
    {"RIGHT", TokenType::TOKEN_RIGHT},
    {"OUTER", TokenType::TOKEN_OUTER},
    {"ON", TokenType::TOKEN_ON},
    {"AS", TokenType::TOKEN_AS},
    {"DISTINCT", TokenType::TOKEN_DISTINCT},
    {"PRIMARY", TokenType::TOKEN_PRIMARY},
    {"KEY", TokenType::TOKEN_KEY},
    {"UNIQUE", TokenType::TOKEN_UNIQUE},
    {"ASC", TokenType::TOKEN_ASC},
    {"DESC", TokenType::TOKEN_DESC}
};

Lexer::Lexer(const std::string& source) 
    : source_(source), pos_(0), line_(1), column_(1) {}

std::vector<Token> Lexer::tokenize() {
    std::vector<Token> tokens;
    Token token;
    
    while ((token = nextToken()).type != TokenType::TOKEN_EOF) {
        tokens.push_back(token);
    }
    
    return tokens;
}

Token Lexer::nextToken() {
    skipWhitespace();
    skipComment();
    
    if (pos_ >= source_.length()) {
        return Token(TokenType::TOKEN_EOF, "", line_, column_);
    }
    
    char c = source_[pos_];
    
    if (isAlpha(c)) {
        return readIdentifier();
    }
    
    if (isDigit(c) || (c == '-' && pos_ + 1 < source_.length() && isDigit(source_[pos_ + 1]))) {
        return readNumber();
    }
    
    if (c == '\'' || c == '"') {
        return readString();
    }
    
    return readOperator();
}

void Lexer::reset() {
    pos_ = 0;
    line_ = 1;
    column_ = 1;
}

char Lexer::peek() {
    if (pos_ >= source_.length()) {
        return '\0';
    }
    return source_[pos_];
}

char Lexer::peek(int offset) {
    if (pos_ + offset >= source_.length()) {
        return '\0';
    }
    return source_[pos_ + offset];
}

char Lexer::advance() {
    if (pos_ >= source_.length()) {
        return '\0';
    }
    
    char c = source_[pos_];
    pos_++;
    column_++;
    
    if (c == '\n') {
        line_++;
        column_ = 1;
    }
    
    return c;
}

void Lexer::skipWhitespace() {
    while (pos_ < source_.length()) {
        char c = peek();
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            advance();
        } else {
            break;
        }
    }
}

void Lexer::skipComment() {
    if (peek() == '-' && peek(1) == '-') {
        while (pos_ < source_.length() && peek() != '\n') {
            advance();
        }
        skipWhitespace();
    }
    
    if (peek() == '/' && peek(1) == '*') {
        advance();
        advance();
        while (pos_ < source_.length() && !(peek() == '*' && peek(1) == '/')) {
            advance();
        }
        if (peek() == '*') {
            advance();
            if (peek() == '/') {
                advance();
            }
        }
        skipWhitespace();
    }
}

Token Lexer::readIdentifier() {
    size_t start = pos_;
    int start_line = line_;
    int start_col = column_;
    
    while (pos_ < source_.length() && isAlphaNumeric(peek())) {
        advance();
    }
    
    std::string value = source_.substr(start, pos_ - start);
    std::string upper_value = value;
    for (char& c : upper_value) {
        c = std::toupper(c);
    }
    
    auto it = keywords_.find(upper_value);
    if (it != keywords_.end()) {
        return Token(it->second, value, start_line, start_col);
    }
    
    return Token(TokenType::TOKEN_IDENTIFIER, value, start_line, start_col);
}

Token Lexer::readNumber() {
    size_t start = pos_;
    int start_line = line_;
    int start_col = column_;
    bool has_dot = false;
    
    while (pos_ < source_.length()) {
        char c = peek();
        if (isDigit(c)) {
            advance();
        } else if (c == '.' && !has_dot) {
            has_dot = true;
            advance();
        } else {
            break;
        }
    }
    
    std::string value = source_.substr(start, pos_ - start);
    
    if (has_dot) {
        return Token(TokenType::TOKEN_FLOAT, value, start_line, start_col);
    } else {
        return Token(TokenType::TOKEN_INTEGER, value, start_line, start_col);
    }
}

Token Lexer::readString() {
    char quote = peek();
    advance();
    
    size_t start = pos_;
    int start_line = line_;
    int start_col = column_;
    
    while (pos_ < source_.length()) {
        char c = peek();
        if (c == quote) {
            advance();
            break;
        }
        
        if (c == '\\' && pos_ + 1 < source_.length()) {
            advance();
            if (peek() != '\0') {
                advance();
            }
        } else {
            advance();
        }
    }
    
    std::string value = source_.substr(start, pos_ - start - 1);
    return Token(TokenType::TOKEN_STRING, value, start_line, start_col);
}

Token Lexer::readOperator() {
    char c = advance();
    int start_line = line_;
    int start_col = column_ - 1;
    
    switch (c) {
        case '=':
            if (peek() == '=') {
                advance();
                return Token(TokenType::TOKEN_EQ, "==", start_line, start_col);
            }
            return Token(TokenType::TOKEN_EQ, "=", start_line, start_col);
            
        case '!':
            if (peek() == '=') {
                advance();
                return Token(TokenType::TOKEN_NE, "!=", start_line, start_col);
            }
            return Token(TokenType::TOKEN_NOT, "!", start_line, start_col);
            
        case '>':
            if (peek() == '=') {
                advance();
                return Token(TokenType::TOKEN_GE, ">=", start_line, start_col);
            }
            return Token(TokenType::TOKEN_GT, ">", start_line, start_col);
            
        case '<':
            if (peek() == '=') {
                advance();
                return Token(TokenType::TOKEN_LE, "<=", start_line, start_col);
            }
            return Token(TokenType::TOKEN_LT, "<", start_line, start_col);
            
        case ';':
            return Token(TokenType::TOKEN_SEMICOLON, ";", start_line, start_col);
            
        case ',':
            return Token(TokenType::TOKEN_COMMA, ",", start_line, start_col);
            
        case '(':
            return Token(TokenType::TOKEN_LPAREN, "(", start_line, start_col);
            
        case ')':
            return Token(TokenType::TOKEN_RPAREN, ")", start_line, start_col);
            
        case '*':
            return Token(TokenType::TOKEN_STAR, "*", start_line, start_col);
            
        case '.':
            return Token(TokenType::TOKEN_DOT, ".", start_line, start_col);
            
        default:
            return Token(TokenType::TOKEN_UNKNOWN, std::string(1, c), start_line, start_col);
    }
}

bool Lexer::isAlpha(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

bool Lexer::isDigit(char c) {
    return c >= '0' && c <= '9';
}

bool Lexer::isAlphaNumeric(char c) {
    return isAlpha(c) || isDigit(c);
}

}