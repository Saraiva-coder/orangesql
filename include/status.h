// include/status.h
#ifndef STATUS_H
#define STATUS_H

#include <string>

namespace orangesql {

enum class StatusCode : uint8 {
    SUCCESS = 0,
    ERROR = 1,
    NOT_FOUND = 2,
    DUPLICATE = 3,
    OUT_OF_MEMORY = 4,
    IO_ERROR = 5,
    CORRUPTION = 6,
    TRANSACTION_ABORT = 7,
    DEADLOCK = 8,
    LOCK_TIMEOUT = 9,
    SCHEMA_ERROR = 10,
    CONSTRAINT_VIOLATION = 11,
    PERMISSION_DENIED = 12,
    NOT_IMPLEMENTED = 13,
    BUSY = 14,
    SHUTDOWN = 15,
    TIMEOUT = 16
};

class Status {
public:
    Status() : code_(StatusCode::SUCCESS) {}
    explicit Status(StatusCode code) : code_(code) {}
    Status(StatusCode code, const std::string& msg) : code_(code), message_(msg) {}
    
    bool ok() const { return code_ == StatusCode::SUCCESS; }
    bool isError() const { return code_ != StatusCode::SUCCESS; }
    StatusCode code() const { return code_; }
    std::string message() const { return message_; }
    
    static Status OK() { return Status(); }
    static Status Error(const std::string& msg) { return Status(StatusCode::ERROR, msg); }
    static Status NotFound(const std::string& msg) { return Status(StatusCode::NOT_FOUND, msg); }
    static Status Duplicate(const std::string& msg) { return Status(StatusCode::DUPLICATE, msg); }
    static Status OutOfMemory(const std::string& msg) { return Status(StatusCode::OUT_OF_MEMORY, msg); }
    static Status IOError(const std::string& msg) { return Status(StatusCode::IO_ERROR, msg); }
    static Status Corruption(const std::string& msg) { return Status(StatusCode::CORRUPTION, msg); }
    static Status TransactionAbort(const std::string& msg) { return Status(StatusCode::TRANSACTION_ABORT, msg); }
    static Status Deadlock(const std::string& msg) { return Status(StatusCode::DEADLOCK, msg); }
    static Status LockTimeout(const std::string& msg) { return Status(StatusCode::LOCK_TIMEOUT, msg); }
    static Status SchemaError(const std::string& msg) { return Status(StatusCode::SCHEMA_ERROR, msg); }
    static Status ConstraintViolation(const std::string& msg) { return Status(StatusCode::CONSTRAINT_VIOLATION, msg); }
    static Status NotImplemented(const std::string& msg) { return Status(StatusCode::NOT_IMPLEMENTED, msg); }
    static Status Timeout(const std::string& msg) { return Status(StatusCode::TIMEOUT, msg); }
    
private:
    StatusCode code_;
    std::string message_;
};

}

#endif