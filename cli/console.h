// cli/console.h
#ifndef CONSOLE_H
#define CONSOLE_H

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <unordered_map>

namespace orangesql {

class QueryExecutor;
class Row;

class Console {
public:
    explicit Console(QueryExecutor* executor);
    ~Console();
    
    void run();
    void shutdown();
    
private:
    QueryExecutor* executor_;
    bool running_;
    std::string buffer_;
    std::unordered_map<std::string, std::function<void(const std::vector<std::string>&)>> commands_;
    
    void printPrompt();
    std::string readInput();
    void executeCommand(const std::string& command);
    void executeSQL(const std::string& sql);
    void printHelp(const std::vector<std::string>& args);
    void printVersion(const std::vector<std::string>& args);
    void clearScreen(const std::vector<std::string>& args);
    void showStatus(const std::vector<std::string>& args);
    void exportData(const std::vector<std::string>& args);
    void importData(const std::vector<std::string>& args);
    void printResults(const std::vector<Row>& results);
    void printTable(const std::vector<Row>& results, const std::vector<std::string>& headers);
    void printTiming(double seconds);
    void setupCommands();
    bool isComplete(const std::string& sql);
    std::string trim(const std::string& str);
    std::vector<std::string> split(const std::string& str, char delimiter);
};

}

#endif