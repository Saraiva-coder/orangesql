// index/btree_node.h
#ifndef BTREE_NODE_H
#define BTREE_NODE_H

#include "../include/types.h"
#include "../include/constants.h"
#include <vector>
#include <cstdint>

namespace orangesql {

template<typename KeyType>
struct BTreeNode {
    bool is_leaf;
    std::vector<KeyType> keys;
    std::vector<uint64_t> values;
    std::vector<uint64_t> children;
    uint64_t node_id;
    uint64_t parent_id;
    uint64_t next_leaf_id;
    uint64_t prev_leaf_id;
    uint32_t level;
    uint32_t checksum;
    
    BTreeNode();
    explicit BTreeNode(uint64_t id);
    ~BTreeNode() = default;
    
    bool isFull() const { return keys.size() >= BTREE_ORDER; }
    bool isUnderflow() const { return keys.size() < BTREE_ORDER / 2; }
    size_t getSize() const { return keys.size(); }
    
    void insertKey(const KeyType& key, uint64_t value);
    void deleteKey(size_t pos);
    KeyType getKey(size_t pos) const { return keys[pos]; }
    uint64_t getValue(size_t pos) const { return values[pos]; }
    
    uint32_t computeChecksum() const;
    bool verifyChecksum() const;
};

}

#endif