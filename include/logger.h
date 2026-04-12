// include/logger.h
#ifndef LOGGER_H
#define LOGGER_H

#include "config.h"
#include <string>
#include <fstream>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <ctime>

namespace orangesql {

enum class LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARNING = 2,
    ERROR = 3,
    FATAL = 4
};

class Logger {
public:
    static Logger& getInstance() {
        static Logger instance;
        return instance;
    }
    
    void init(const std::string& level = "INFO", const std::string& file = "orangesql.log") {
        setLevel(level);
        setFile(file);
    }
    
    void setLevel(LogLevel level) { level_ = level; }
    void setLevel(const std::string& level) {
        if (level == "DEBUG") level_ = LogLevel::DEBUG;
        else if (level == "INFO") level_ = LogLevel::INFO;
        else if (level == "WARNING") level_ = LogLevel::WARNING;
        else if (level == "ERROR") level_ = LogLevel::ERROR;
        else if (level == "FATAL") level_ = LogLevel::FATAL;
        else level_ = LogLevel::INFO;
    }
    
    void setFile(const std::string& path) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_.is_open()) {
            file_.close();
        }
        file_.open(path, std::ios::app);
    }
    
    void setConsoleOutput(bool enable) { console_output_ = enable; }
    
    void debug(const std::string& msg) { log(LogLevel::DEBUG, msg); }
    void info(const std::string& msg) { log(LogLevel::INFO, msg); }
    void warning(const std::string& msg) { log(LogLevel::WARNING, msg); }
    void error(const std::string& msg) { log(LogLevel::ERROR, msg); }
    void fatal(const std::string& msg) { log(LogLevel::FATAL, msg); }
    
    void debugf(const char* format, ...) {
        va_list args;
        va_start(args, format);
        vlog(LogLevel::DEBUG, format, args);
        va_end(args);
    }
    
    void infof(const char* format, ...) {
        va_list args;
        va_start(args, format);
        vlog(LogLevel::INFO, format, args);
        va_end(args);
    }
    
    void warningf(const char* format, ...) {
        va_list args;
        va_start(args, format);
        vlog(LogLevel::WARNING, format, args);
        va_end(args);
    }
    
    void errorf(const char* format, ...) {
        va_list args;
        va_start(args, format);
        vlog(LogLevel::ERROR, format, args);
        va_end(args);
    }
    
private:
    Logger() : level_(LogLevel::INFO), console_output_(true) {
        auto& cfg = Config::getInstance().get().logging;
        setLevel(cfg.level);
        setFile(cfg.file);
        console_output_ = cfg.console_output;
    }
    
    ~Logger() {
        if (file_.is_open()) {
            file_.close();
        }
    }
    
    void log(LogLevel level, const std::string& msg) {
        if (level < level_) return;
        
        std::lock_guard<std::mutex> lock(mutex_);
        std::string formatted = formatMessage(level, msg);
        
        if (file_.is_open()) {
            file_ << formatted << std::endl;
            file_.flush();
        }
        
        if (console_output_) {
            if (level >= LogLevel::ERROR) {
                std::cerr << formatted << std::endl;
            } else {
                std::cout << formatted << std::endl;
            }
        }
    }
    
    void vlog(LogLevel level, const char* format, va_list args) {
        if (level < level_) return;
        
        char buffer[4096];
        vsnprintf(buffer, sizeof(buffer), format, args);
        log(level, std::string(buffer));
    }
    
    std::string formatMessage(LogLevel level, const std::string& msg) {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch()) % 1000;
        
        std::stringstream ss;
        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
        ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
        ss << " [" << levelToString(level) << "] ";
        ss << msg;
        
        return ss.str();
    }
    
    std::string levelToString(LogLevel level) {
        switch(level) {
            case LogLevel::DEBUG:   return "DEBUG";
            case LogLevel::INFO:    return "INFO";
            case LogLevel::WARNING: return "WARNING";
            case LogLevel::ERROR:   return "ERROR";
            case LogLevel::FATAL:   return "FATAL";
            default:                return "UNKNOWN";
        }
    }
    
    LogLevel level_;
    std::ofstream file_;
    std::mutex mutex_;
    bool console_output_;
};

#define LOG_DEBUG(msg) Logger::getInstance().debug(msg)
#define LOG_INFO(msg) Logger::getInstance().info(msg)
#define LOG_WARNING(msg) Logger::getInstance().warning(msg)
#define LOG_ERROR(msg) Logger::getInstance().error(msg)
#define LOG_FATAL(msg) Logger::getInstance().fatal(msg)

#define LOG_DEBUGF(fmt, ...) Logger::getInstance().debugf(fmt, ##__VA_ARGS__)
#define LOG_INFOF(fmt, ...) Logger::getInstance().infof(fmt, ##__VA_ARGS__)
#define LOG_WARNINGF(fmt, ...) Logger::getInstance().warningf(fmt, ##__VA_ARGS__)
#define LOG_ERRORF(fmt, ...) Logger::getInstance().errorf(fmt, ##__VA_ARGS__)

}

#endif