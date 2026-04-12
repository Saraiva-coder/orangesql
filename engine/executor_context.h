// engine/executor_context.h
#ifndef EXECUTOR_CONTEXT_H
#define EXECUTOR_CONTEXT_H

#include "../include/types.h"
#include "../transaction/mvcc.h"
#include <unordered_map>
#include <string>
#include <memory>

namespace orangesql {

class ExecutorContext {
public:
    ExecutorContext(MVCCManager* mvcc, uint64_t transaction_id);
    ~ExecutorContext();
    
    MVCCManager* getMVCC() const { return mvcc_; }
    uint64_t getTransactionId() const { return transaction_id_; }
    
    void setVariable(const std::string& name, const Value& value);
    Value getVariable(const std::string& name);
    bool hasVariable(const std::string& name) const;
    void clearVariables();
    
    void setParameter(int index, const Value& value);
    Value getParameter(int index) const;
    
    void reset();
    
private:
    MVCCManager* mvcc_;
    uint64_t transaction_id_;
    std::unordered_map<std::string, Value> variables_;
    std::unordered_map<int, Value> parameters_;
};

}

#endif