// engine/optimizer.h
#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "../parser/ast.h"
#include "../metadata/catalog.h"
#include <memory>
#include <vector>
#include <string>

namespace orangesql {

struct QueryPlan {
    bool use_index;
    std::string index_name;
    double estimated_cost;
    std::vector<std::string> join_order;
    std::string access_method;
    bool use_parallel;
    int estimated_rows;
    
    QueryPlan() : use_index(false), estimated_cost(0), use_parallel(false), estimated_rows(0) {}
};

class Optimizer {
public:
    explicit Optimizer(Catalog* catalog);
    ~Optimizer();
    
    std::unique_ptr<QueryPlan> optimize(const ASTNode& ast);
    
private:
    Catalog* catalog_;
    
    double estimateSelectivity(const std::string& table, const Condition& condition);
    double estimateIndexCost(const std::string& table, const std::string& column);
    void optimizeJoinOrder(const SelectStatement& stmt, QueryPlan* plan);
    bool shouldUseParallel(const std::string& table, int estimated_rows);
    bool hasIndexOnColumn(const std::string& table, const std::string& column);
};

}

#endif