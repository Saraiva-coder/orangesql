// storage/file_manager.cpp
#include "file_manager.h"
#include "../include/logger.h"
#include <filesystem>
#include <cstring>

namespace fs = std::filesystem;

namespace orangesql {

FileManager::FileManager() : next_file_id_(1) {}

FileManager::~FileManager() {
    for (auto& pair : files_) {
        if (pair.second.is_open) {
            pair.second.stream.close();
        }
    }
}

Status FileManager::openFile(const std::string& filename, int& file_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string path = getDataPath(filename);
    
    for (const auto& pair : files_) {
        if (pair.second.filename == path) {
            file_id = pair.first;
            return Status::OK();
        }
    }
    
    FileHandle handle;
    handle.filename = path;
    handle.is_open = true;
    
    if (!fs::exists(path)) {
        handle.stream.open(path, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
        handle.size = 0;
    } else {
        handle.stream.open(path, std::ios::binary | std::ios::in | std::ios::out);
        handle.stream.seekg(0, std::ios::end);
        handle.size = handle.stream.tellg();
    }
    
    if (!handle.stream.is_open()) {
        return Status::Error("Cannot open file: " + filename);
    }
    
    file_id = next_file_id_++;
    files_[file_id] = std::move(handle);
    
    return Status::OK();
}

Status FileManager::closeFile(int file_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = files_.find(file_id);
    if (it == files_.end()) {
        return Status::NotFound("File not open");
    }
    
    if (it->second.is_open) {
        it->second.stream.close();
        it->second.is_open = false;
    }
    
    files_.erase(it);
    return Status::OK();
}

Status FileManager::readPage(int file_id, int page_id, char* buffer) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = files_.find(file_id);
    if (it == files_.end()) {
        return Status::NotFound("File not open");
    }
    
    FileHandle& handle = it->second;
    size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
    
    if (offset >= handle.size) {
        memset(buffer, 0, PAGE_SIZE);
        return Status::OK();
    }
    
    handle.stream.seekg(offset);
    handle.stream.read(buffer, PAGE_SIZE);
    
    if (handle.stream.gcount() != PAGE_SIZE) {
        memset(buffer + handle.stream.gcount(), 0, PAGE_SIZE - handle.stream.gcount());
    }
    
    return Status::OK();
}

Status FileManager::writePage(int file_id, int page_id, const char* buffer) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = files_.find(file_id);
    if (it == files_.end()) {
        return Status::NotFound("File not open");
    }
    
    FileHandle& handle = it->second;
    size_t offset = static_cast<size_t>(page_id) * PAGE_SIZE;
    
    handle.stream.seekp(offset);
    handle.stream.write(buffer, PAGE_SIZE);
    handle.stream.flush();
    
    if (offset + PAGE_SIZE > handle.size) {
        handle.size = offset + PAGE_SIZE;
    }
    
    return Status::OK();
}

Status FileManager::allocatePage(int file_id, int& page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = files_.find(file_id);
    if (it == files_.end()) {
        return Status::NotFound("File not open");
    }
    
    FileHandle& handle = it->second;
    page_id = static_cast<int>(handle.size / PAGE_SIZE);
    
    char buffer[PAGE_SIZE];
    memset(buffer, 0, PAGE_SIZE);
    
    handle.stream.seekp(0, std::ios::end);
    handle.stream.write(buffer, PAGE_SIZE);
    handle.stream.flush();
    handle.size += PAGE_SIZE;
    
    return Status::OK();
}

Status FileManager::getFileSize(int file_id, size_t& size) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = files_.find(file_id);
    if (it == files_.end()) {
        return Status::NotFound("File not open");
    }
    
    size = it->second.size;
    return Status::OK();
}

bool FileManager::fileExists(const std::string& filename) {
    std::string path = getDataPath(filename);
    return fs::exists(path);
}

Status FileManager::deleteFile(const std::string& filename) {
    std::string path = getDataPath(filename);
    
    for (auto& pair : files_) {
        if (pair.second.filename == path) {
            pair.second.stream.close();
        }
    }
    
    if (fs::exists(path)) {
        fs::remove(path);
    }
    
    return Status::OK();
}

std::string FileManager::getDataPath(const std::string& filename) {
    return "data/tables/" + filename;
}

}