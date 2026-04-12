// index/bulk_loader.h
#ifndef BULK_LOADER_H
#define BULK_LOADER_H

#include "btree.h"
#include "../storage/table.h"
#include <vector>
#include <algorithm>

namespace orangesql {

template<typename KeyType>
class BulkLoader {
public:
    explicit BulkLoader(BTree<KeyType>* tree);
    ~BulkLoader();
    
    Status load(const std::vector<std::pair<KeyType, uint64_t>>& data);
    Status loadFromTable(Table* table, const std::string& column);
    
    void setBatchSize(size_t batch_size) { batch_size_ = batch_size; }
    void setSortMemory(size_t memory_mb) { sort_memory_mb_ = memory_mb; }
    
private:
    BTree<KeyType>* tree_;
    size_t batch_size_;
    size_t sort_memory_mb_;
    
    void externalSort(std::vector<std::pair<KeyType, uint64_t>>& data);
    void mergeChunks(const std::vector<std::string>& chunks, 
                     std::vector<std::pair<KeyType, uint64_t>>& output);
};

}

#endif