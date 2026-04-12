// engine/optimizer.cpp
#include "optimizer.h"
#include "../include/constants.h"
#include <cmath>
#include <algorithm>

namespace orangesql {

Optimizer::Optimizer(Catalog* catalog) : catalog_(catalog) {}

Optimizer::~Optimizer() {}

std::unique_ptr<QueryPlan> Optimizer::optimize(const ASTNode& ast) {
    auto plan = std::make_unique<QueryPlan>();
    
    if (ast.type != NodeType::SELECT) {
        return plan;
    }
    
    const auto& stmt = ast.select_stmt;
    Table* table = catalog_->getTable(stmt.table_name);
    
    if (!table) {
        return plan;
    }
    
    double table_size = static_cast<double>(table->getRowCount());
    plan->estimated_rows = static_cast<int>(table_size);
    plan->estimated_cost = table_size;
    plan->access_method = "SEQUENTIAL_SCAN";
    
    for (const auto& condition : stmt.where_clause.conditions) {
        double selectivity = estimateSelectivity(stmt.table_name, condition);
        double condition_cost = table_size * selectivity;
        
        if (condition_cost < plan->estimated_cost) {
            plan->estimated_cost = condition_cost;
            plan->estimated_rows = static_cast<int>(table_size * selectivity);
        }
        
        if (selectivity < 0.15 && hasIndexOnColumn(stmt.table_name, condition.column)) {
            plan->use_index = true;
            plan->index_name = condition.column;
            plan->access_method = "INDEX_SCAN";
            plan->estimated_cost = std::log2(table_size) * selectivity;
            break;
        }
    }
    
    if (!stmt.joins.empty()) {
        optimizeJoinOrder(stmt, plan.get());
    }
    
    plan->use_parallel = shouldUseParallel(stmt.table_name, plan->estimated_rows);
    
    return plan;
}

double Optimizer::estimateSelectivity(const std::string& table, const Condition& condition) {
    Table* t = catalog_->getTable(table);
    if (!t) {
        return 0.5;
    }
    
    switch (condition.op) {
        case Operator::EQ:
            return 0.01;
        case Operator::NE:
            return 0.9;
        case Operator::GT:
        case Operator::LT:
            return 0.33;
        case Operator::GE:
        case Operator::LE:
            return 0.4;
        case Operator::LIKE:
            if (condition.value.front() == '%' && condition.value.back() == '%') {
                return 0.5;
            } else if (condition.value.back() == '%') {
                return 0.2;
            } else if (condition.value.front() == '%') {
                return 0.3;
            }
            return 0.1;
        case Operator::IN:
            return 0.05 * condition.in_values.size();
        case Operator::BETWEEN:
            return 0.2;
        case Operator::IS_NULL:
            return 0.05;
        default:
            return 0.5;
    }
}

double Optimizer::estimateIndexCost(const std::string& table, const std::string& column) {
    Table* t = catalog_->getTable(table);
    if (!t) {
        return 1000.0;
    }
    
    double row_count = static_cast<double>(t->getRowCount());
    return std::log2(row_count) * 2.0;
}

void Optimizer::optimizeJoinOrder(const SelectStatement& stmt, QueryPlan* plan) {
    std::vector<std::string> tables;
    tables.push_back(stmt.table_name);
    
    for (const auto& join : stmt.joins) {
        tables.push_back(join.table);
    }
    
    std::vector<std::string> optimized_order;
    std::vector<bool> used(tables.size(), false);
    
    for (size_t i = 0; i < tables.size(); i++) {
        size_t best_idx = 0;
        double best_cost = 1e18;
        
        for (size_t j = 0; j < tables.size(); j++) {
            if (!used[j]) {
                Table* t = catalog_->getTable(tables[j]);
                double cost = t ? static_cast<double>(t->getRowCount()) : 1000.0;
                if (cost < best_cost) {
                    best_cost = cost;
                    best_idx = j;
                }
            }
        }
        
        optimized_order.push_back(tables[best_idx]);
        used[best_idx] = true;
    }
    
    plan->join_order = optimized_order;
    plan->estimated_cost *= static_cast<double>(tables.size());
}

bool Optimizer::shouldUseParallel(const std::string& table, int estimated_rows) {
    return estimated_rows > PARALLEL_SCAN_THRESHOLD;
}

bool Optimizer::hasIndexOnColumn(const std::string& table, const std::string& column) {
    Table* t = catalog_->getTable(table);
    if (!t) {
        return false;
    }
    return t->hasIndexOnColumn(column);
}

}