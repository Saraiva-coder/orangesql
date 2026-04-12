// metadata/index_metadata.h
#ifndef INDEX_METADATA_H
#define INDEX_METADATA_H

#include "../include/types.h"
#include <string>
#include <vector>
#include <chrono>

namespace orangesql {

struct IndexMetadata {
    std::string index_name;
    std::string table_name;
    std::string column_name;
    bool is_unique;
    bool is_primary;
    std::string index_type;
    int btree_order;
    size_t total_size;
    size_t node_count;
    int height;
    double fill_factor;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point last_optimized;
    
    IndexMetadata() : is_unique(false), is_primary(false), index_type("BTREE"),
                      btree_order(128), total_size(0), node_count(0), height(0),
                      fill_factor(0.75) {}
    
    std::string serialize() const;
    static IndexMetadata deserialize(const std::string& data);
};

class IndexMetadataManager {
public:
    static IndexMetadataManager& getInstance();
    
    Status addIndex(const IndexMetadata& metadata);
    Status removeIndex(const std::string& index_name);
    IndexMetadata* getIndex(const std::string& index_name);
    std::vector<IndexMetadata> getIndexesForTable(const std::string& table_name);
    
    Status updateIndexStats(const std::string& index_name, size_t total_size,
                            size_t node_count, int height);
    Status persist();
    Status load();
    
private:
    IndexMetadataManager() = default;
    
    std::unordered_map<std::string, IndexMetadata> indexes_;
    std::shared_mutex mutex_;
};

}

#endif