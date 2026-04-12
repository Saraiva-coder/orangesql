// storage/file_manager.h
#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include "../include/status.h"
#include "../include/constants.h"
#include <string>
#include <fstream>
#include <unordered_map>
#include <mutex>
#include <vector>

namespace orangesql {

class FileManager {
public:
    FileManager();
    ~FileManager();
    
    Status openFile(const std::string& filename, int& file_id);
    Status closeFile(int file_id);
    Status readPage(int file_id, int page_id, char* buffer);
    Status writePage(int file_id, int page_id, const char* buffer);
    Status allocatePage(int file_id, int& page_id);
    Status getFileSize(int file_id, size_t& size);
    bool fileExists(const std::string& filename);
    Status deleteFile(const std::string& filename);
    
private:
    struct FileHandle {
        std::fstream stream;
        std::string filename;
        size_t size;
        bool is_open;
    };
    
    std::unordered_map<int, FileHandle> files_;
    std::mutex mutex_;
    int next_file_id_;
    
    std::string getDataPath(const std::string& filename);
};

}

#endif