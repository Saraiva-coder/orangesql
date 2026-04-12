// include/types.h
#ifndef TYPES_H
#define TYPES_H

#include <cstdint>
#include <string>
#include <vector>
#include <chrono>

namespace orangesql {

using int8 = int8_t;
using int16 = int16_t;
using int32 = int32_t;
using int64 = int64_t;
using uint8 = uint8_t;
using uint16 = uint16_t;
using uint32 = uint32_t;
using uint64 = uint64_t;

enum class DataType : uint8 {
    UNKNOWN = 0,
    INTEGER = 1,
    BIGINT = 2,
    SMALLINT = 3,
    TINYINT = 4,
    FLOAT = 5,
    DOUBLE = 6,
    DECIMAL = 7,
    TEXT = 8,
    VARCHAR = 9,
    CHAR = 10,
    DATE = 11,
    TIMESTAMP = 12,
    TIME = 13,
    BOOLEAN = 14,
    BLOB = 15,
    JSON = 16
};

struct Value {
    DataType type;
    union {
        int64 int_val;
        double double_val;
        bool bool_val;
    };
    std::string str_val;
    std::vector<char> blob_val;
    
    Value() : type(DataType::UNKNOWN), int_val(0) {}
    explicit Value(int64 v) : type(DataType::INTEGER), int_val(v) {}
    explicit Value(int32 v) : type(DataType::INTEGER), int_val(v) {}
    explicit Value(double v) : type(DataType::DOUBLE), double_val(v) {}
    explicit Value(float v) : type(DataType::FLOAT), double_val(v) {}
    explicit Value(bool v) : type(DataType::BOOLEAN), bool_val(v) {}
    explicit Value(const std::string& v) : type(DataType::TEXT), str_val(v) {}
    explicit Value(const char* v) : type(DataType::TEXT), str_val(v) {}
    explicit Value(const std::vector<char>& v) : type(DataType::BLOB), blob_val(v) {}
    
    std::string toString() const;
    int64 toInt() const;
    double toDouble() const;
    bool toBool() const;
    
    bool isNull() const { return type == DataType::UNKNOWN; }
    void setNull() { type = DataType::UNKNOWN; }
    
    bool operator==(const Value& other) const;
    bool operator!=(const Value& other) const;
    bool operator<(const Value& other) const;
    bool operator>(const Value& other) const;
};

struct Column {
    std::string name;
    DataType type;
    bool nullable;
    bool primary_key;
    bool unique;
    int32 length;
    int32 precision;
    int32 scale;
    int32 position;
    std::string default_value;
    std::string comment;
    
    Column() : type(DataType::UNKNOWN), nullable(true), primary_key(false),
               unique(false), length(0), precision(0), scale(0), position(0) {}
};

struct Row {
    std::vector<Value> values;
    uint64 row_id;
    uint64 transaction_id;
    uint64 begin_ts;
    uint64 end_ts;
    bool is_deleted;
    
    Row() : row_id(0), transaction_id(0), begin_ts(0), end_ts(UINT64_MAX), is_deleted(false) {}
};

}

#endif