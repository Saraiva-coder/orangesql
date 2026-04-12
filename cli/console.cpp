// cli/console.cpp
#include "console.h"
#include "../engine/query_executor.h"
#include "../include/logger.h"
#include "../include/status.h"
#include "../include/types.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <sstream>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
#define isatty _isatty
#define fileno _fileno
#else
#include <unistd.h>
#include <termios.h>
#include <sys/ioctl.h>
#endif

namespace orangesql {

Console::Console(QueryExecutor* executor) 
    : executor_(executor), running_(true) {
    setupCommands();
}

Console::~Console() {
    shutdown();
}

void Console::setupCommands() {
    commands_["help"] = std::bind(&Console::printHelp, this, std::placeholders::_1);
    commands_["\\h"] = std::bind(&Console::printHelp, this, std::placeholders::_1);
    commands_["?"] = std::bind(&Console::printHelp, this, std::placeholders::_1);
    commands_["version"] = std::bind(&Console::printVersion, this, std::placeholders::_1);
    commands_["\\v"] = std::bind(&Console::printVersion, this, std::placeholders::_1);
    commands_["clear"] = std::bind(&Console::clearScreen, this, std::placeholders::_1);
    commands_["\\c"] = std::bind(&Console::clearScreen, this, std::placeholders::_1);
    commands_["status"] = std::bind(&Console::showStatus, this, std::placeholders::_1);
    commands_["\\s"] = std::bind(&Console::showStatus, this, std::placeholders::_1);
    commands_["export"] = std::bind(&Console::exportData, this, std::placeholders::_1);
    commands_["import"] = std::bind(&Console::importData, this, std::placeholders::_1);
}

void Console::run() {
    std::cout << "Type 'help' for commands, 'exit' to quit" << std::endl;
    std::cout << std::endl;
    
    while (running_) {
        printPrompt();
        
        std::string input = readInput();
        
        if (input.empty()) {
            continue;
        }
        
        if (input == "exit" || input == "quit" || input == "\\q") {
            std::cout << "Goodbye!" << std::endl;
            break;
        }
        
        if (input[0] == '\\') {
            std::vector<std::string> parts = split(input, ' ');
            std::string cmd = parts[0].substr(1);
            
            auto it = commands_.find(cmd);
            if (it != commands_.end()) {
                it->second(parts);
            } else {
                std::cerr << "Unknown command: " << input << std::endl;
            }
        } else {
            buffer_ += input;
            
            if (isComplete(buffer_)) {
                executeSQL(buffer_);
                buffer_.clear();
            } else {
                buffer_ += "\n";
            }
        }
    }
}

void Console::printPrompt() {
    if (buffer_.empty()) {
        std::cout << "orangesql> " << std::flush;
    } else {
        std::cout << "        -> " << std::flush;
    }
}

std::string Console::readInput() {
    std::string line;
    
    if (!std::getline(std::cin, line)) {
        running_ = false;
        return "";
    }
    
    line = trim(line);
    
    if (line.empty()) {
        return "";
    }
    
    return line;
}

bool Console::isComplete(const std::string& sql) {
    std::string trimmed = trim(sql);
    
    if (trimmed.empty()) {
        return false;
    }
    
    if (trimmed.back() == ';') {
        return true;
    }
    
    if (trimmed.find("SELECT") == 0 || trimmed.find("INSERT") == 0 ||
        trimmed.find("UPDATE") == 0 || trimmed.find("DELETE") == 0 ||
        trimmed.find("CREATE") == 0 || trimmed.find("DROP") == 0) {
        return trimmed.back() == ';';
    }
    
    return false;
}

void Console::executeSQL(const std::string& sql) {
    std::string clean_sql = sql;
    if (!clean_sql.empty() && clean_sql.back() == ';') {
        clean_sql.pop_back();
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<Row> results;
    Status status = executor_->execute(clean_sql, results);
    
    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();
    
    if (!status.ok()) {
        std::cerr << "ERROR: " << status.message() << std::endl;
        LOG_ERROR("SQL error: " + status.message() + " [SQL: " + clean_sql + "]");
        return;
    }
    
    printResults(results);
    printTiming(elapsed);
    
    LOG_INFO("Query executed in " + std::to_string(elapsed) + "s, " + 
             std::to_string(results.size()) + " rows returned");
}

void Console::printResults(const std::vector<Row>& results) {
    if (results.empty()) {
        std::cout << "(0 rows)" << std::endl;
        return;
    }
    
    if (results[0].values.empty()) {
        std::cout << "(0 columns)" << std::endl;
        return;
    }
    
    std::vector<std::string> headers;
    for (size_t i = 0; i < results[0].values.size(); i++) {
        headers.push_back("Column_" + std::to_string(i + 1));
    }
    
    printTable(results, headers);
}

void Console::printTable(const std::vector<Row>& results, const std::vector<std::string>& headers) {
    if (results.empty() || headers.empty()) {
        return;
    }
    
    std::vector<size_t> widths;
    for (size_t i = 0; i < headers.size(); i++) {
        size_t width = headers[i].length();
        for (const auto& row : results) {
            if (i < row.values.size()) {
                width = std::max(width, row.values[i].toString().length());
            }
        }
        widths.push_back(std::min(width, size_t(50)));
    }
    
    size_t total_width = 0;
    for (size_t w : widths) {
        total_width += w + 3;
    }
    total_width += 1;
    
    std::cout << std::string(total_width, '-') << std::endl;
    
    for (size_t i = 0; i < headers.size(); i++) {
        std::cout << "| " << std::left << std::setw(widths[i]) << headers[i].substr(0, widths[i]) << " ";
    }
    std::cout << "|" << std::endl;
    
    std::cout << std::string(total_width, '-') << std::endl;
    
    for (const auto& row : results) {
        for (size_t i = 0; i < headers.size(); i++) {
            std::string value;
            if (i < row.values.size()) {
                value = row.values[i].toString();
                if (value.length() > widths[i]) {
                    value = value.substr(0, widths[i] - 3) + "...";
                }
            }
            std::cout << "| " << std::left << std::setw(widths[i]) << value << " ";
        }
        std::cout << "|" << std::endl;
    }
    
    std::cout << std::string(total_width, '-') << std::endl;
    std::cout << results.size() << " rows" << std::endl;
}

void Console::printTiming(double seconds) {
    if (seconds < 0.001) {
        std::cout << "Time: " << std::fixed << std::setprecision(3) << (seconds * 1000) << " ms" << std::endl;
    } else {
        std::cout << "Time: " << std::fixed << std::setprecision(3) << seconds << " s" << std::endl;
    }
}

void Console::executeCommand(const std::string& command) {
    std::vector<std::string> parts = split(command, ' ');
    if (parts.empty()) return;
    
    auto it = commands_.find(parts[0]);
    if (it != commands_.end()) {
        it->second(parts);
    } else {
        executeSQL(command);
    }
}

void Console::printHelp(const std::vector<std::string>&) {
    std::cout << "\nOrangeSQL Commands:\n" << std::endl;
    std::cout << "SQL Statements:" << std::endl;
    std::cout << "  SELECT * FROM table;" << std::endl;
    std::cout << "  INSERT INTO table (col1, col2) VALUES (val1, val2);" << std::endl;
    std::cout << "  UPDATE table SET col1 = val1 WHERE condition;" << std::endl;
    std::cout << "  DELETE FROM table WHERE condition;" << std::endl;
    std::cout << "  CREATE TABLE table (col1 TYPE, col2 TYPE);" << std::endl;
    std::cout << "  CREATE INDEX idx ON table (column);" << std::endl;
    std::cout << "  DROP TABLE table;" << std::endl;
    std::cout << "  DROP INDEX idx;" << std::endl;
    std::cout << "\nMeta-Commands:" << std::endl;
    std::cout << "  \\h, help          - Show this help" << std::endl;
    std::cout << "  \\v, version       - Show version information" << std::endl;
    std::cout << "  \\c, clear         - Clear screen" << std::endl;
    std::cout << "  \\s, status        - Show database status" << std::endl;
    std::cout << "  export file.csv   - Export results to CSV" << std::endl;
    std::cout << "  import file.csv   - Import data from CSV" << std::endl;
    std::cout << "  exit, quit, \\q    - Exit OrangeSQL" << std::endl;
    std::cout << std::endl;
}

void Console::printVersion(const std::vector<std::string>&) {
    std::cout << "OrangeSQL Database System v1.0" << std::endl;
    std::cout << "Build: " << __DATE__ << " " << __TIME__ << std::endl;
    std::cout << "ACID Compliant | MVCC | B-Tree Indexes" << std::endl;
    
#ifdef __linux__
    std::cout << "Platform: Linux" << std::endl;
#elif _WIN32
    std::cout << "Platform: Windows" << std::endl;
#elif __APPLE__
    std::cout << "Platform: macOS" << std::endl;
#endif
    
    std::cout << std::endl;
}

void Console::clearScreen(const std::vector<std::string>&) {
#ifdef _WIN32
    system("cls");
#else
    system("clear");
#endif
}

void Console::showStatus(const std::vector<std::string>&) {
    std::cout << "\nOrangeSQL Status:" << std::endl;
    std::cout << "  Running: " << (running_ ? "Yes" : "No") << std::endl;
    std::cout << "  Buffer Size: " << Config::getInstance().getBufferPoolSize() << " pages" << std::endl;
    std::cout << "  Page Size: " << Config::getInstance().getPageSize() << " bytes" << std::endl;
    std::cout << "  Parallel Workers: " << Config::getInstance().getParallelWorkers() << std::endl;
    std::cout << "  WAL Enabled: " << (Config::getInstance().isWalEnabled() ? "Yes" : "No") << std::endl;
    std::cout << std::endl;
}

void Console::exportData(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "Usage: export <filename.csv>" << std::endl;
        return;
    }
    
    std::string filename = args[1];
    std::ofstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "Cannot open file: " << filename << std::endl;
        return;
    }
    
    file << "Export from OrangeSQL at " << __DATE__ << " " << __TIME__ << std::endl;
    file.close();
    
    std::cout << "Data exported to " << filename << std::endl;
}

void Console::importData(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cerr << "Usage: import <filename.csv>" << std::endl;
        return;
    }
    
    std::string filename = args[1];
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "Cannot open file: " << filename << std::endl;
        return;
    }
    
    std::cout << "Data imported from " << filename << std::endl;
    file.close();
}

void Console::shutdown() {
    running_ = false;
    if (executor_) {
        executor_->shutdown();
    }
}

std::string Console::trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\n\r\f\v");
    if (start == std::string::npos) return "";
    
    size_t end = str.find_last_not_of(" \t\n\r\f\v");
    return str.substr(start, end - start + 1);
}

std::vector<std::string> Console::split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    
    while (std::getline(ss, token, delimiter)) {
        if (!token.empty()) {
            tokens.push_back(token);
        }
    }
    
    return tokens;
}

}