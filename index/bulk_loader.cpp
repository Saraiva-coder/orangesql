// index/bulk_loader.cpp
#include "bulk_loader.h"
#include <cstdio>
#include <memory>

namespace orangesql {

template<typename KeyType>
BulkLoader<KeyType>::BulkLoader(BTree<KeyType>* tree)
    : tree_(tree), batch_size_(10000), sort_memory_mb_(256) {}

template<typename KeyType>
BulkLoader<KeyType>::~BulkLoader() {}

template<typename KeyType>
Status BulkLoader<KeyType>::load(const std::vector<std::pair<KeyType, uint64_t>>& data) {
    if (data.empty()) {
        return Status::OK();
    }
    
    std::vector<std::pair<KeyType, uint64_t>> sorted_data = data;
    
    if (sorted_data.size() > batch_size_) {
        externalSort(sorted_data);
    } else {
        std::sort(sorted_data.begin(), sorted_data.end());
    }
    
    return tree_->bulkLoad(sorted_data);
}

template<typename KeyType>
void BulkLoader<KeyType>::externalSort(std::vector<std::pair<KeyType, uint64_t>>& data) {
    size_t chunk_size = (sort_memory_mb_ * 1024 * 1024) / sizeof(std::pair<KeyType, uint64_t>);
    chunk_size = std::min(chunk_size, data.size());
    
    std::vector<std::string> chunk_files;
    
    for (size_t i = 0; i < data.size(); i += chunk_size) {
        size_t end = std::min(i + chunk_size, data.size());
        std::sort(data.begin() + i, data.begin() + end);
        
        char filename[256];
        snprintf(filename, sizeof(filename), "data/tmp/chunk_%zu.dat", chunk_files.size());
        chunk_files.push_back(filename);
        
        FILE* file = fopen(filename, "wb");
        for (size_t j = i; j < end; j++) {
            fwrite(&data[j], sizeof(std::pair<KeyType, uint64_t>), 1, file);
        }
        fclose(file);
    }
    
    mergeChunks(chunk_files, data);
    
    for (const auto& file : chunk_files) {
        remove(file.c_str());
    }
}

template<typename KeyType>
void BulkLoader<KeyType>::mergeChunks(const std::vector<std::string>& chunks,
                                       std::vector<std::pair<KeyType, uint64_t>>& output) {
    std::vector<FILE*> files;
    std::vector<std::pair<KeyType, uint64_t>> buffers;
    
    for (const auto& chunk : chunks) {
        FILE* file = fopen(chunk.c_str(), "rb");
        if (file) {
            files.push_back(file);
            buffers.push_back({});
            fread(&buffers.back(), sizeof(std::pair<KeyType, uint64_t>), 1, file);
        }
    }
    
    output.clear();
    
    while (true) {
        size_t best_idx = 0;
        bool has_data = false;
        
        for (size_t i = 0; i < buffers.size(); i++) {
            if (buffers[i].first != KeyType() || buffers[i].second != 0) {
                if (!has_data || buffers[i].first < buffers[best_idx].first) {
                    best_idx = i;
                    has_data = true;
                }
            }
        }
        
        if (!has_data) break;
        
        output.push_back(buffers[best_idx]);
        
        if (fread(&buffers[best_idx], sizeof(std::pair<KeyType, uint64_t>), 1, files[best_idx]) != 1) {
            buffers[best_idx] = {KeyType(), 0};
        }
    }
    
    for (auto file : files) {
        fclose(file);
    }
}

template class BulkLoader<int64_t>;
template class BulkLoader<std::string>;

}