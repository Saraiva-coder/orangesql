#ifndef STATUS_H
#define STATUS_H

#include <string>

namespace orangesql {

class Status {
public:
    Status() : code_(0) {}
    explicit Status(int code) : code_(code) {}
    Status(int code, const std::string& msg) : code_(code), message_(msg) {}
    
    bool ok() const { return code_ == 0; }
    bool isError() const { return code_ != 0; }
    int code() const { return code_; }
    std::string message() const { return message_; }
    
    static Status OK() { return Status(); }
    static Status Error(const std::string& msg) { return Status(1, msg); }
    static Status NotFound(const std::string& msg) { return Status(2, msg); }
    static Status Duplicate(const std::string& msg) { return Status(3, msg); }
    
private:
    int code_;
    std::string message_;
};

}

#endif