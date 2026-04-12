// cli/main.cpp
#include "console.h"
#include "../include/logger.h"
#include "../include/config.h"
#include "../engine/query_executor.h"
#include "../storage/buffer_pool.h"
#include "../transaction/mvcc.h"
#include "../metadata/catalog.h"
#include <iostream>
#include <memory>
#include <csignal>
#include <cstdlib>

using namespace orangesql;

static std::unique_ptr<Console> g_console;
static std::unique_ptr<QueryExecutor> g_executor;
static std::unique_ptr<BufferPool> g_buffer_pool;
static std::unique_ptr<MVCCManager> g_mvcc;

void signalHandler(int signal) {
    std::cout << "\n\nShutting down OrangeSQL..." << std::endl;
    
    if (g_console) {
        g_console->shutdown();
    }
    
    if (g_executor) {
        g_executor->shutdown();
    }
    
    if (g_buffer_pool) {
        g_buffer_pool->flushAll();
    }
    
    if (g_mvcc) {
        g_mvcc->shutdown();
    }
    
    Catalog::getInstance()->persist();
    
    std::exit(0);
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    std::cout << "OrangeSQL Database System v1.0" << std::endl;
    std::cout << "High-Performance ACID Compliant Database" << std::endl;
    std::cout << "=============================================" << std::endl;
    
    try {
        Config::getInstance().load("config.json");
        Logger::getInstance().setLevel(LogLevel::INFO);
        Logger::getInstance().setFile("orangesql.log");
        
        LOG_INFO("OrangeSQL starting up");
        
        g_buffer_pool = std::make_unique<BufferPool>();
        g_mvcc = std::make_unique<MVCCManager>();
        g_executor = std::make_unique<QueryExecutor>(g_mvcc.get(), g_buffer_pool.get());
        g_console = std::make_unique<Console>(g_executor.get());
        
        Catalog::getInstance()->load();
        
        LOG_INFO("OrangeSQL ready");
        
        g_console->run();
        
    } catch (const std::exception& e) {
        LOG_ERROR(std::string("Fatal error: ") + e.what());
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}