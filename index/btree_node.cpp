// index/btree_node.cpp
#include "btree_node.h"
#include <cstring>
#include <algorithm>

namespace orangesql {

template<typename KeyType>
BTreeNode<KeyType>::BTreeNode() 
    : is_leaf(true), node_id(0), parent_id(0), next_leaf_id(0), prev_leaf_id(0), level(0), checksum(0) {}

template<typename KeyType>
BTreeNode<KeyType>::BTreeNode(uint64_t id) 
    : is_leaf(true), node_id(id), parent_id(0), next_leaf_id(0), prev_leaf_id(0), level(0), checksum(0) {}

template<typename KeyType>
void BTreeNode<KeyType>::insertKey(const KeyType& key, uint64_t value) {
    auto it = std::lower_bound(keys.begin(), keys.end(), key);
    size_t pos = it - keys.begin();
    
    keys.insert(it, key);
    
    if (is_leaf) {
        values.insert(values.begin() + pos, value);
    } else {
        children.insert(children.begin() + pos + 1, value);
    }
}

template<typename KeyType>
void BTreeNode<KeyType>::deleteKey(size_t pos) {
    if (pos < keys.size()) {
        keys.erase(keys.begin() + pos);
        
        if (is_leaf && pos < values.size()) {
            values.erase(values.begin() + pos);
        } else if (!is_leaf && pos + 1 < children.size()) {
            children.erase(children.begin() + pos + 1);
        }
    }
}

template<typename KeyType>
uint32_t BTreeNode<KeyType>::computeChecksum() const {
    uint32_t crc = 0;
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(&is_leaf), sizeof(is_leaf));
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(keys.data()), keys.size() * sizeof(KeyType));
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(values.data()), values.size() * sizeof(uint64_t));
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(children.data()), children.size() * sizeof(uint64_t));
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(&node_id), sizeof(node_id));
    crc = crc32(crc, reinterpret_cast<const uint8_t*>(&parent_id), sizeof(parent_id));
    return crc;
}

template<typename KeyType>
bool BTreeNode<KeyType>::verifyChecksum() const {
    return checksum == computeChecksum();
}

template struct BTreeNode<int64_t>;
template struct BTreeNode<std::string>;

}