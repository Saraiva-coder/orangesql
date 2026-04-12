// index/btree.h
#ifndef BTREE_H
#define BTREE_H

#include "../include/types.h"
#include "../include/status.h"
#include "btree_node.h"
#include "btree_cache.h"
#include <vector>
#include <memory>
#include <atomic>
#include <shared_mutex>

namespace orangesql {

template<typename KeyType>
class BTree {
public:
    explicit BTree(int order = BTREE_ORDER);
    ~BTree();
    
    Status insert(const KeyType& key, uint64_t record_id);
    Status search(const KeyType& key, std::vector<uint64_t>& record_ids);
    Status searchRange(const KeyType& start, const KeyType& end, std::vector<uint64_t>& record_ids);
    Status remove(const KeyType& key);
    Status update(const KeyType& old_key, const KeyType& new_key, uint64_t record_id);
    
    size_t size() const { return size_; }
    int height() const { return height_; }
    size_t getNodeCount() const { return node_count_; }
    
    Status bulkLoad(const std::vector<std::pair<KeyType, uint64_t>>& data);
    
    void setCacheSize(size_t cache_size);
    void enableConcurrent(bool enable);
    
private:
    int order_;
    size_t size_;
    int height_;
    size_t node_count_;
    uint64_t root_id_;
    std::unique_ptr<BTreeCache<KeyType>> cache_;
    std::shared_mutex mutex_;
    std::atomic<bool> concurrent_enabled_;
    
    Status insertInternal(BTreeNode<KeyType>* node, const KeyType& key, uint64_t record_id);
    Status splitNode(BTreeNode<KeyType>* node);
    Status findLeaf(const KeyType& key, BTreeNode<KeyType>*& leaf);
    BTreeNode<KeyType>* loadNode(uint64_t node_id);
    Status saveNode(BTreeNode<KeyType>* node);
    void deleteNode(BTreeNode<KeyType>* node);
    uint64_t allocateNodeId();
    void updateParent(BTreeNode<KeyType>* node, const KeyType& key, uint64_t child_id);
};

}

#endif