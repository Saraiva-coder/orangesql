// include/errors.h
#ifndef ERRORS_H
#define ERRORS_H

#include <exception>
#include <string>
#include <cstring>

namespace orangesql {

class DatabaseException : public std::exception {
public:
    explicit DatabaseException(const std::string& msg) : message_(msg) {}
    explicit DatabaseException(const char* msg) : message_(msg) {}
    
    const char* what() const noexcept override {
        return message_.c_str();
    }
    
private:
    std::string message_;
};

class TransactionAbortException : public DatabaseException {
public:
    explicit TransactionAbortException(const std::string& msg) : DatabaseException(msg) {}
    explicit TransactionAbortException(const char* msg) : DatabaseException(msg) {}
};

class DeadlockException : public DatabaseException {
public:
    explicit DeadlockException(const std::string& msg) : DatabaseException(msg) {}
    explicit DeadlockException(const char* msg) : DatabaseException(msg) {}
};

class ConstraintViolationException : public DatabaseException {
public:
    explicit ConstraintViolationException(const std::string& msg) : DatabaseException(msg) {}
    explicit ConstraintViolationException(const char* msg) : DatabaseException(msg) {}
};

class LockTimeoutException : public DatabaseException {
public:
    explicit LockTimeoutException(const std::string& msg) : DatabaseException(msg) {}
    explicit LockTimeoutException(const char* msg) : DatabaseException(msg) {}
};

class IOException : public DatabaseException {
public:
    explicit IOException(const std::string& msg) : DatabaseException(msg) {}
    explicit IOException(const char* msg) : DatabaseException(msg) {}
};

class CorruptionException : public DatabaseException {
public:
    explicit CorruptionException(const std::string& msg) : DatabaseException(msg) {}
    explicit CorruptionException(const char* msg) : DatabaseException(msg) {}
};

class OutOfMemoryException : public DatabaseException {
public:
    explicit OutOfMemoryException(const std::string& msg) : DatabaseException(msg) {}
    explicit OutOfMemoryException(const char* msg) : DatabaseException(msg) {}
};

class NotImplementedException : public DatabaseException {
public:
    explicit NotImplementedException(const std::string& msg) : DatabaseException(msg) {}
    explicit NotImplementedException(const char* msg) : DatabaseException(msg) {}
};

}

#endif