// index/btree.cpp
#include "btree.h"
#include "../include/logger.h"
#include "../include/config.h"
#include <algorithm>
#include <cstring>
#include <chrono>

namespace orangesql {

template<typename KeyType>
BTree<KeyType>::BTree(int order) 
    : order_(order), size_(0), height_(0), node_count_(0), root_id_(1), 
      concurrent_enabled_(true) {
    
    cache_ = std::make_unique<BTreeCache<KeyType>>(CACHE_SIZE);
    
    auto* root = new BTreeNode<KeyType>();
    root->is_leaf = true;
    root->node_id = allocateNodeId();
    root->parent_id = 0;
    
    saveNode(root);
    root_id_ = root->node_id;
    node_count_++;
    
    delete root;
}

template<typename KeyType>
BTree<KeyType>::~BTree() {
    cache_->clear();
}

template<typename KeyType>
Status BTree<KeyType>::insert(const KeyType& key, uint64_t record_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto* root = loadNode(root_id_);
    if (!root) {
        return Status::Error("Failed to load root node");
    }
    
    Status status = insertInternal(root, key, record_id);
    
    if (status.ok() && root->keys.size() > static_cast<size_t>(order_)) {
        auto* new_root = new BTreeNode<KeyType>();
        new_root->is_leaf = false;
        new_root->node_id = allocateNodeId();
        new_root->children.push_back(root->node_id);
        
        root->parent_id = new_root->node_id;
        saveNode(root);
        
        splitNode(root);
        
        new_root->keys.push_back(root->keys[0]);
        saveNode(new_root);
        
        root_id_ = new_root->node_id;
        height_++;
        node_count_++;
        
        delete new_root;
    }
    
    delete root;
    return status;
}

template<typename KeyType>
Status BTree<KeyType>::insertInternal(BTreeNode<KeyType>* node, const KeyType& key, uint64_t record_id) {
    if (node->is_leaf) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        size_t pos = it - node->keys.begin();
        
        if (it != node->keys.end() && *it == key) {
            node->values.insert(node->values.begin() + pos, record_id);
        } else {
            node->keys.insert(it, key);
            node->values.insert(node->values.begin() + pos, record_id);
        }
        
        size_++;
        saveNode(node);
        return Status::OK();
    }
    
    size_t pos = std::upper_bound(node->keys.begin(), node->keys.end(), key) - node->keys.begin();
    
    auto* child = loadNode(node->children[pos]);
    if (!child) {
        return Status::Error("Failed to load child node");
    }
    
    Status status = insertInternal(child, key, record_id);
    
    if (status.ok() && child->keys.size() > static_cast<size_t>(order_)) {
        splitNode(child);
        saveNode(child);
        
        if (pos < node->keys.size()) {
            node->keys[pos] = child->keys[0];
        } else {
            node->keys.push_back(child->keys[0]);
            node->children.push_back(child->children.back());
        }
        
        saveNode(node);
    }
    
    delete child;
    return status;
}

template<typename KeyType>
Status BTree<KeyType>::splitNode(BTreeNode<KeyType>* node) {
    size_t mid = node->keys.size() / 2;
    
    auto* new_node = new BTreeNode<KeyType>();
    new_node->is_leaf = node->is_leaf;
    new_node->node_id = allocateNodeId();
    new_node->parent_id = node->parent_id;
    
    new_node->keys.assign(node->keys.begin() + mid, node->keys.end());
    node->keys.erase(node->keys.begin() + mid, node->keys.end());
    
    if (node->is_leaf) {
        new_node->values.assign(node->values.begin() + mid, node->values.end());
        node->values.erase(node->values.begin() + mid, node->values.end());
        
        new_node->next_leaf_id = node->next_leaf_id;
        node->next_leaf_id = new_node->node_id;
    } else {
        new_node->children.assign(node->children.begin() + mid + 1, node->children.end());
        node->children.erase(node->children.begin() + mid + 1, node->children.end());
        
        for (auto child_id : new_node->children) {
            auto* child = loadNode(child_id);
            if (child) {
                child->parent_id = new_node->node_id;
                saveNode(child);
                delete child;
            }
        }
    }
    
    saveNode(new_node);
    node_count_++;
    delete new_node;
    
    return Status::OK();
}

template<typename KeyType>
Status BTree<KeyType>::search(const KeyType& key, std::vector<uint64_t>& record_ids) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    BTreeNode<KeyType>* leaf = nullptr;
    Status status = findLeaf(key, leaf);
    
    if (status.ok() && leaf) {
        auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
        if (it != leaf->keys.end() && *it == key) {
            size_t pos = it - leaf->keys.begin();
            if (pos < leaf->values.size()) {
                record_ids.push_back(leaf->values[pos]);
            }
        }
        delete leaf;
    }
    
    return status;
}

template<typename KeyType>
Status BTree<KeyType>::searchRange(const KeyType& start, const KeyType& end, std::vector<uint64_t>& record_ids) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    BTreeNode<KeyType>* leaf = nullptr;
    Status status = findLeaf(start, leaf);
    
    if (status.ok() && leaf) {
        while (leaf) {
            for (size_t i = 0; i < leaf->keys.size(); i++) {
                if (leaf->keys[i] >= start && leaf->keys[i] <= end) {
                    if (i < leaf->values.size()) {
                        record_ids.push_back(leaf->values[i]);
                    }
                } else if (leaf->keys[i] > end) {
                    delete leaf;
                    return Status::OK();
                }
            }
            
            if (leaf->next_leaf_id == 0) {
                delete leaf;
                break;
            }
            
            auto* next = loadNode(leaf->next_leaf_id);
            delete leaf;
            leaf = next;
        }
    }
    
    return Status::OK();
}

template<typename KeyType>
Status BTree<KeyType>::findLeaf(const KeyType& key, BTreeNode<KeyType>*& leaf) {
    leaf = loadNode(root_id_);
    if (!leaf) {
        return Status::NotFound("Root not found");
    }
    
    while (leaf && !leaf->is_leaf) {
        size_t pos = std::upper_bound(leaf->keys.begin(), leaf->keys.end(), key) - leaf->keys.begin();
        
        if (pos >= leaf->children.size()) {
            pos = leaf->children.size() - 1;
        }
        
        auto* child = loadNode(leaf->children[pos]);
        delete leaf;
        leaf = child;
        
        if (!leaf) {
            return Status::NotFound("Node not found");
        }
    }
    
    return Status::OK();
}

template<typename KeyType>
Status BTree<KeyType>::bulkLoad(const std::vector<std::pair<KeyType, uint64_t>>& data) {
    if (data.empty()) {
        return Status::OK();
    }
    
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    std::vector<std::pair<KeyType, uint64_t>> sorted_data = data;
    std::sort(sorted_data.begin(), sorted_data.end());
    
    size_t leaf_capacity = order_;
    size_t leaf_count = (sorted_data.size() + leaf_capacity - 1) / leaf_capacity;
    
    std::vector<uint64_t> leaf_ids;
    
    for (size_t i = 0; i < leaf_count; i++) {
        auto* leaf = new BTreeNode<KeyType>();
        leaf->is_leaf = true;
        leaf->node_id = allocateNodeId();
        
        size_t start = i * leaf_capacity;
        size_t end = std::min(start + leaf_capacity, sorted_data.size());
        
        for (size_t j = start; j < end; j++) {
            leaf->keys.push_back(sorted_data[j].first);
            leaf->values.push_back(sorted_data[j].second);
        }
        
        if (i > 0) {
            leaf->prev_leaf_id = leaf_ids.back();
        }
        if (i < leaf_count - 1) {
            leaf->next_leaf_id = 0;
        }
        
        saveNode(leaf);
        leaf_ids.push_back(leaf->node_id);
        node_count_++;
        size_ += leaf->keys.size();
        
        delete leaf;
    }
    
    for (size_t i = 0; i < leaf_ids.size() - 1; i++) {
        auto* leaf = loadNode(leaf_ids[i]);
        if (leaf) {
            leaf->next_leaf_id = leaf_ids[i + 1];
            saveNode(leaf);
            delete leaf;
        }
        
        auto* next_leaf = loadNode(leaf_ids[i + 1]);
        if (next_leaf) {
            next_leaf->prev_leaf_id = leaf_ids[i];
            saveNode(next_leaf);
            delete next_leaf;
        }
    }
    
    std::vector<uint64_t> current_level = leaf_ids;
    
    while (current_level.size() > 1) {
        std::vector<uint64_t> next_level;
        size_t internal_capacity = order_;
        
        for (size_t i = 0; i < current_level.size(); i += internal_capacity) {
            auto* internal = new BTreeNode<KeyType>();
            internal->is_leaf = false;
            internal->node_id = allocateNodeId();
            
            size_t end = std::min(i + internal_capacity, current_level.size());
            
            for (size_t j = i; j < end; j++) {
                auto* child = loadNode(current_level[j]);
                if (child) {
                    internal->children.push_back(current_level[j]);
                    child->parent_id = internal->node_id;
                    saveNode(child);
                    delete child;
                    
                    if (j > i) {
                        internal->keys.push_back(child->keys[0]);
                    }
                }
            }
            
            saveNode(internal);
            next_level.push_back(internal->node_id);
            node_count_++;
            delete internal;
        }
        
        current_level = next_level;
        height_++;
    }
    
    root_id_ = current_level[0];
    
    return Status::OK();
}

template<typename KeyType>
BTreeNode<KeyType>* BTree<KeyType>::loadNode(uint64_t node_id) {
    auto* cached = cache_->get(node_id);
    if (cached) {
        return new BTreeNode<KeyType>(*cached);
    }
    
    auto* node = new BTreeNode<KeyType>();
    node->node_id = node_id;
    
    char filename[256];
    snprintf(filename, sizeof(filename), "data/indexes/node_%llu.dat", (unsigned long long)node_id);
    
    FILE* file = fopen(filename, "rb");
    if (!file) {
        delete node;
        return nullptr;
    }
    
    fread(&node->is_leaf, sizeof(bool), 1, file);
    
    uint32_t key_count;
    fread(&key_count, sizeof(uint32_t), 1, file);
    node->keys.resize(key_count);
    fread(node->keys.data(), sizeof(KeyType), key_count, file);
    
    uint32_t value_count;
    fread(&value_count, sizeof(uint32_t), 1, file);
    node->values.resize(value_count);
    fread(node->values.data(), sizeof(uint64_t), value_count, file);
    
    uint32_t child_count;
    fread(&child_count, sizeof(uint32_t), 1, file);
    node->children.resize(child_count);
    fread(node->children.data(), sizeof(uint64_t), child_count, file);
    
    fread(&node->parent_id, sizeof(uint64_t), 1, file);
    fread(&node->next_leaf_id, sizeof(uint64_t), 1, file);
    fread(&node->prev_leaf_id, sizeof(uint64_t), 1, file);
    
    fclose(file);
    
    cache_->put(node_id, node);
    
    return node;
}

template<typename KeyType>
Status BTree<KeyType>::saveNode(BTreeNode<KeyType>* node) {
    char filename[256];
    snprintf(filename, sizeof(filename), "data/indexes/node_%llu.dat", (unsigned long long)node->node_id);
    
    FILE* file = fopen(filename, "wb");
    if (!file) {
        return Status::Error("Cannot save node");
    }
    
    fwrite(&node->is_leaf, sizeof(bool), 1, file);
    
    uint32_t key_count = node->keys.size();
    fwrite(&key_count, sizeof(uint32_t), 1, file);
    fwrite(node->keys.data(), sizeof(KeyType), key_count, file);
    
    uint32_t value_count = node->values.size();
    fwrite(&value_count, sizeof(uint32_t), 1, file);
    fwrite(node->values.data(), sizeof(uint64_t), value_count, file);
    
    uint32_t child_count = node->children.size();
    fwrite(&child_count, sizeof(uint32_t), 1, file);
    fwrite(node->children.data(), sizeof(uint64_t), child_count, file);
    
    fwrite(&node->parent_id, sizeof(uint64_t), 1, file);
    fwrite(&node->next_leaf_id, sizeof(uint64_t), 1, file);
    fwrite(&node->prev_leaf_id, sizeof(uint64_t), 1, file);
    
    fclose(file);
    
    cache_->put(node->node_id, node);
    
    return Status::OK();
}

template<typename KeyType>
uint64_t BTree<KeyType>::allocateNodeId() {
    static std::atomic<uint64_t> next_id{1};
    return next_id.fetch_add(1);
}

template class BTree<int64_t>;
template class BTree<std::string>;

}