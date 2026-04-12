// engine/executor_context.cpp
#include "executor_context.h"

namespace orangesql {

ExecutorContext::ExecutorContext(MVCCManager* mvcc, uint64_t transaction_id)
    : mvcc_(mvcc), transaction_id_(transaction_id) {}

ExecutorContext::~ExecutorContext() {
    clearVariables();
}

void ExecutorContext::setVariable(const std::string& name, const Value& value) {
    variables_[name] = value;
}

Value ExecutorContext::getVariable(const std::string& name) {
    auto it = variables_.find(name);
    if (it != variables_.end()) {
        return it->second;
    }
    return Value();
}

bool ExecutorContext::hasVariable(const std::string& name) const {
    return variables_.find(name) != variables_.end();
}

void ExecutorContext::clearVariables() {
    variables_.clear();
}

void ExecutorContext::setParameter(int index, const Value& value) {
    parameters_[index] = value;
}

Value ExecutorContext::getParameter(int index) const {
    auto it = parameters_.find(index);
    if (it != parameters_.end()) {
        return it->second;
    }
    return Value();
}

void ExecutorContext::reset() {
    variables_.clear();
    parameters_.clear();
}

}