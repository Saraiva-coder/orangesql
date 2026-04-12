#ifndef TYPES_H
#define TYPES_H

#include <cstdint>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>

namespace orangesql {

using uint64 = uint64_t;
using int64 = int64_t;
using uint32 = uint32_t;
using int32 = int32_t;

enum class DataType {
    UNKNOWN = 0,
    INTEGER = 1,
    BIGINT = 2,
    SMALLINT = 3,
    TINYINT = 4,
    TEXT = 5,
    VARCHAR = 6,
    BOOLEAN = 7,
    DOUBLE = 8,
    FLOAT = 9
};

struct Value {
    DataType type;
    int64 int_val;
    double double_val;
    bool bool_val;
    std::string str_val;
    std::vector<char> blob_val;
    
    Value() : type(DataType::UNKNOWN), int_val(0), double_val(0.0), bool_val(false) {}
    explicit Value(int64 v) : type(DataType::INTEGER), int_val(v), double_val(0.0), bool_val(false) {}
    explicit Value(double v) : type(DataType::DOUBLE), int_val(0), double_val(v), bool_val(false) {}
    explicit Value(bool v) : type(DataType::BOOLEAN), int_val(0), double_val(0.0), bool_val(v) {}
    explicit Value(const std::string& v) : type(DataType::TEXT), int_val(0), double_val(0.0), bool_val(false), str_val(v) {}
    
    std::string toString() const {
        switch(type) {
            case DataType::INTEGER:
            case DataType::BIGINT:
                return std::to_string(int_val);
            case DataType::DOUBLE:
                return std::to_string(double_val);
            case DataType::BOOLEAN:
                return bool_val ? "true" : "false";
            case DataType::TEXT:
            case DataType::VARCHAR:
                return str_val;
            default:
                return "";
        }
    }
    
    int64 toInt() const {
        switch(type) {
            case DataType::INTEGER:
            case DataType::BIGINT:
                return int_val;
            case DataType::DOUBLE:
                return static_cast<int64>(double_val);
            case DataType::BOOLEAN:
                return bool_val ? 1 : 0;
            case DataType::TEXT:
                return std::stoll(str_val);
            default:
                return 0;
        }
    }
    
    double toDouble() const {
        switch(type) {
            case DataType::DOUBLE:
                return double_val;
            case DataType::INTEGER:
            case DataType::BIGINT:
                return static_cast<double>(int_val);
            case DataType::BOOLEAN:
                return bool_val ? 1.0 : 0.0;
            case DataType::TEXT:
                return std::stod(str_val);
            default:
                return 0.0;
        }
    }
    
    bool toBool() const {
        switch(type) {
            case DataType::BOOLEAN:
                return bool_val;
            case DataType::INTEGER:
                return int_val != 0;
            case DataType::TEXT:
                return str_val == "true" || str_val == "1";
            default:
                return false;
        }
    }
};

struct Row {
    std::vector<Value> values;
    uint64 row_id;
    uint64 transaction_id;
    
    Row() : row_id(0), transaction_id(0) {}
};

}

#endif