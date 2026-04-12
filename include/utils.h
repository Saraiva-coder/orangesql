#ifndef UTILS_H
#define UTILS_H

#include "types.h"
#include <string>
#include <chrono>
#include <random>
#include <thread>
#include <mutex>
#include <vector>
#include <algorithm>
#include <sstream>
#include <cstring>
#include <cstdio>

namespace orangesql {

class Timer {
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}
    
    void reset() { start_ = std::chrono::high_resolution_clock::now(); }
    
    double elapsed() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double>(end - start_).count();
    }
    
    double elapsed_ms() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start_).count();
    }
    
private:
    std::chrono::high_resolution_clock::time_point start_;
};

class Random {
public:
    static int32 getInt32(int32 min, int32 max) {
        std::uniform_int_distribution<int32> dist(min, max);
        return dist(gen);
    }
    
    static int64 getInt64(int64 min, int64 max) {
        std::uniform_int_distribution<int64> dist(min, max);
        return dist(gen);
    }
    
    static double getDouble(double min, double max) {
        std::uniform_real_distribution<double> dist(min, max);
        return dist(gen);
    }
    
    static bool getBool() {
        return getInt32(0, 1) == 1;
    }
    
    static std::string getString(size_t length) {
        static const char chars[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::string result;
        result.reserve(length);
        for (size_t i = 0; i < length; i++) {
            result.push_back(chars[getInt32(0, sizeof(chars) - 2)]);
        }
        return result;
    }
    
private:
    static thread_local std::mt19937_64 gen;
};

class StringUtils {
public:
    static std::string toUpper(const std::string& str) {
        std::string result = str;
        std::transform(result.begin(), result.end(), result.begin(), ::toupper);
        return result;
    }
    
    static std::string toLower(const std::string& str) {
        std::string result = str;
        std::transform(result.begin(), result.end(), result.begin(), ::tolower);
        return result;
    }
    
    static std::string trim(const std::string& str) {
        size_t start = str.find_first_not_of(" \t\n\r\f\v");
        if (start == std::string::npos) return "";
        size_t end = str.find_last_not_of(" \t\n\r\f\v");
        return str.substr(start, end - start + 1);
    }
    
    static std::vector<std::string> split(const std::string& str, char delimiter) {
        std::vector<std::string> tokens;
        std::istringstream ss(str);
        std::string token;
        while (std::getline(ss, token, delimiter)) {
            std::string trimmed = trim(token);
            if (!trimmed.empty()) {
                tokens.push_back(trimmed);
            }
        }
        return tokens;
    }
    
    static bool startsWith(const std::string& str, const std::string& prefix) {
        return str.size() >= prefix.size() && str.compare(0, prefix.size(), prefix) == 0;
    }
    
    static bool endsWith(const std::string& str, const std::string& suffix) {
        return str.size() >= suffix.size() && str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
    }
};

class MathUtils {
public:
    static int64 alignUp(int64 value, int64 alignment) {
        return (value + alignment - 1) & ~(alignment - 1);
    }
    
    static int64 alignDown(int64 value, int64 alignment) {
        return value & ~(alignment - 1);
    }
    
    static bool isPowerOfTwo(uint64 value) {
        return value && !(value & (value - 1));
    }
};

inline std::string formatSize(size_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB"};
    int unit = 0;
    double size = static_cast<double>(bytes);
    while (size >= 1024.0 && unit < 3) {
        size /= 1024.0;
        unit++;
    }
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%.2f %s", size, units[unit]);
    return std::string(buffer);
}

}

#endif