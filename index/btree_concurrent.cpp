// index/btree_concurrent.cpp
#include "btree_concurrent.h"
#include <chrono>

namespace orangesql {

template<typename KeyType>
ConcurrentBTree<KeyType>::ConcurrentBTree(int order) : btree_(order) {}

template<typename KeyType>
ConcurrentBTree<KeyType>::~ConcurrentBTree() {}

template<typename KeyType>
Status ConcurrentBTree<KeyType>::insert(const KeyType& key, uint64_t record_id) {
    latch();
    Status status = btree_.insert(key, record_id);
    unlatch();
    return status;
}

template<typename KeyType>
Status ConcurrentBTree<KeyType>::search(const KeyType& key, std::vector<uint64_t>& record_ids) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return btree_.search(key, record_ids);
}

template<typename KeyType>
Status ConcurrentBTree<KeyType>::searchRange(const KeyType& start, const KeyType& end, 
                                               std::vector<uint64_t>& record_ids) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return btree_.searchRange(start, end, record_ids);
}

template<typename KeyType>
void ConcurrentBTree<KeyType>::latch() {
    mutex_.lock();
    is_latched_ = true;
}

template<typename KeyType>
void ConcurrentBTree<KeyType>::unlatch() {
    is_latched_ = false;
    mutex_.unlock();
}

template class ConcurrentBTree<int64_t>;
template class ConcurrentBTree<std::string>;

}