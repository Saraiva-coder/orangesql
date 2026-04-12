#include "types.h"
#include <sstream>
#include <iomanip>

namespace orangesql {

std::string Value::toString() const {
    switch (type) {
        case DataType::INTEGER:
        case DataType::BIGINT:
        case DataType::SMALLINT:
        case DataType::TINYINT:
            return std::to_string(int_val);
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::DECIMAL: {
            std::stringstream ss;
            ss << std::fixed << std::setprecision(6) << double_val;
            return ss.str();
        }
        case DataType::TEXT:
        case DataType::VARCHAR:
        case DataType::CHAR:
            return str_val;
        case DataType::BOOLEAN:
            return bool_val ? "true" : "false";
        case DataType::DATE:
        case DataType::TIMESTAMP:
        case DataType::TIME:
            return str_val;
        case DataType::BLOB:
            return "[BLOB size=" + std::to_string(blob_val.size()) + "]";
        case DataType::JSON:
            return str_val;
        default:
            return "NULL";
    }
}

int64 Value::toInt() const {
    switch (type) {
        case DataType::INTEGER:
        case DataType::BIGINT:
        case DataType::SMALLINT:
        case DataType::TINYINT:
            return int_val;
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::DECIMAL:
            return static_cast<int64>(double_val);
        case DataType::TEXT:
        case DataType::VARCHAR:
        case DataType::CHAR:
            return std::stoll(str_val);
        case DataType::BOOLEAN:
            return bool_val ? 1 : 0;
        default:
            return 0;
    }
}

double Value::toDouble() const {
    switch (type) {
        case DataType::INTEGER:
        case DataType::BIGINT:
        case DataType::SMALLINT:
        case DataType::TINYINT:
            return static_cast<double>(int_val);
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::DECIMAL:
            return double_val;
        case DataType::TEXT:
        case DataType::VARCHAR:
        case DataType::CHAR:
            return std::stod(str_val);
        case DataType::BOOLEAN:
            return bool_val ? 1.0 : 0.0;
        default:
            return 0.0;
    }
}

bool Value::toBool() const {
    switch (type) {
        case DataType::BOOLEAN:
            return bool_val;
        case DataType::INTEGER:
        case DataType::BIGINT:
            return int_val != 0;
        case DataType::TEXT:
        case DataType::VARCHAR:
            return str_val == "true" || str_val == "1" || str_val == "yes";
        default:
            return false;
    }
}

bool Value::operator==(const Value& other) const {
    if (type != other.type) return false;
    switch (type) {
        case DataType::INTEGER:
        case DataType::BIGINT:
            return int_val == other.int_val;
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return double_val == other.double_val;
        case DataType::TEXT:
        case DataType::VARCHAR:
            return str_val == other.str_val;
        case DataType::BOOLEAN:
            return bool_val == other.bool_val;
        default:
            return false;
    }
}

bool Value::operator!=(const Value& other) const {
    return !(*this == other);
}

bool Value::operator<(const Value& other) const {
    if (type != other.type) return false;
    switch (type) {
        case DataType::INTEGER:
        case DataType::BIGINT:
            return int_val < other.int_val;
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return double_val < other.double_val;
        case DataType::TEXT:
        case DataType::VARCHAR:
            return str_val < other.str_val;
        default:
            return false;
    }
}

bool Value::operator>(const Value& other) const {
    return other < *this;
}

}