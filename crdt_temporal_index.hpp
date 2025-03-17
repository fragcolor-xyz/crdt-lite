#ifndef CRDT_TEMPORAL_INDEX_HPP
#define CRDT_TEMPORAL_INDEX_HPP

#include "crdt.hpp"

namespace crdt_ext {

/**
 * TemporalIndexedCRDT extends the base CRDT with efficient temporal indexing.
 */
template <typename K, typename V, typename MergeContext = void,
          MergeRule<K, V, MergeContext> MergeRuleType = DefaultMergeRule<K, V, MergeContext>,
          ChangeComparator<K, V> ChangeComparatorType = DefaultChangeComparator<K, V>,
          typename SortFunctionType = DefaultSort,
          MapLike<K, Record<V>> MapType = CrdtMap<K, Record<V>>>
class TemporalIndexedCRDT : public CRDT<K, V, MergeContext, MergeRuleType, 
                                         ChangeComparatorType, SortFunctionType, MapType> {
private:
    // Alias for base class to make the code more readable
    using BaseClass = CRDT<K, V, MergeContext, MergeRuleType, 
                           ChangeComparatorType, SortFunctionType, MapType>;
    
    // Index mapping versions to sets of records modified at that version
    CrdtSortedMap<uint64_t, CrdtSet<K>> version_index_;
    
    // Helper to update the temporal index after a record modification
    void update_temporal_index(const K& record_id, uint64_t version) {
        version_index_[version].insert(record_id);
    }
    
public:
    // Constructor - forward all arguments to the base class
    template <typename... Args>
    TemporalIndexedCRDT(Args&&... args) : BaseClass(std::forward<Args>(args)...) {}
    
    // Method to rebuild temporal index
    void rebuild_temporal_index() {
        version_index_.clear();
        
        // Build index from current data
        for (const auto& [record_id, record] : this->get_data()) {
            for (const auto& [col_name, col_version] : record.column_versions) {
                // Add to the version index
                version_index_[col_version.local_db_version].insert(record_id);
            }
        }
    }
    
    // Standard version of get_changes_since that delegates to base class
    CrdtVector<Change<K, V>> get_changes_since(uint64_t last_db_version, 
                                             CrdtSet<CrdtNodeId> excluding = {}) const override {
        return BaseClass::get_changes_since(last_db_version, excluding);
    }
    
    // Optimized version that uses the temporal index
    CrdtVector<Change<K, V>> get_changes_since_optimized(uint64_t last_db_version, 
                                                      CrdtSet<CrdtNodeId> excluding = {}) const {
        std::cout << "Using optimized implementation" << std::endl;
        CrdtVector<Change<K, V>> changes;
        
        // If we have a parent, we need its changes too
        if (this->parent_) {
            auto parent_changes = this->parent_->get_changes_since(last_db_version);
            changes.insert(changes.end(), parent_changes.begin(), parent_changes.end());
        }
        
        // Track which records we've seen to prevent duplicate processing
        CrdtSet<K> processed_records;
        
        // Use temporal index for this instance's changes
        // Find the first version greater than last_db_version
        auto it = version_index_.lower_bound(last_db_version + 1);
        
        while (it != version_index_.end()) {
            const uint64_t version = it->first;
            const CrdtSet<K>& record_ids = it->second;
            
            // For each record modified at this version
            for (const K& record_id : record_ids) {
                // Skip records we've already processed
                if (processed_records.count(record_id) > 0) {
                    continue;
                }
                processed_records.insert(record_id);
                
                auto record_ptr = this->get_record(record_id);
                if (!record_ptr) continue;
                
                // If this record is a tombstone
                if (this->is_tombstoned(record_id)) {
                    auto col_it = record_ptr->column_versions.find("");
                    if (col_it != record_ptr->column_versions.end() && 
                        col_it->second.local_db_version > last_db_version &&
                        !excluding.contains(col_it->second.node_id)) {
                        
                        changes.emplace_back(Change<K, V>(
                            record_id, std::nullopt, std::nullopt,
                            col_it->second.col_version, col_it->second.db_version,
                            col_it->second.node_id, col_it->second.local_db_version
                        ));
                    }
                    continue;
                }
                
                // Check each column of this record
                for (const auto& [col_name, clock_info] : record_ptr->column_versions) {
                    if (clock_info.local_db_version > last_db_version &&
                        !excluding.contains(clock_info.node_id)) {
                        
                        std::optional<V> value = std::nullopt;
                        std::optional<CrdtKey> name = std::nullopt;
                        
                        if (!record_ptr->fields.empty()) {
                            auto field_it = record_ptr->fields.find(col_name);
                            if (field_it != record_ptr->fields.end()) {
                                value = field_it->second;
                            }
                            name = col_name;
                        }
                        
                        changes.emplace_back(Change<K, V>(
                            record_id, std::move(name), std::move(value),
                            clock_info.col_version, clock_info.db_version,
                            clock_info.node_id, clock_info.local_db_version
                        ));
                    }
                }
            }
            
            ++it;
        }
        
        // Perform compression to remove redundant changes
        this->compress_changes(changes);
        
        return changes;
    }
    
    // Override the variadic insert_or_update to maintain the temporal index
    template <typename... Pairs>
    void insert_or_update(const K& record_id, Pairs&&... pairs) {
        // Call the base class implementation
        uint64_t prev_version = this->clock_.current_time();
        
        BaseClass::insert_or_update(record_id, std::forward<Pairs>(pairs)...);
        
        uint64_t new_version = this->clock_.current_time();
        update_temporal_index(record_id, new_version);
    }
    
    // Override the other version of insert_or_update
    template <typename... Pairs>
    void insert_or_update(const K& record_id, uint32_t flags, Pairs&&... pairs) {
        // Call the base class implementation
        uint64_t prev_version = this->clock_.current_time();
        
        BaseClass::insert_or_update(record_id, flags, std::forward<Pairs>(pairs)...);
        
        uint64_t new_version = this->clock_.current_time();
        update_temporal_index(record_id, new_version);
    }
    
    // Override container-based versions
    template <typename Container>
    void insert_or_update_from_container(const K& record_id, Container&& fields) {
        uint64_t prev_version = this->clock_.current_time();
        
        BaseClass::insert_or_update_from_container(record_id, std::forward<Container>(fields));
        
        uint64_t new_version = this->clock_.current_time();
        update_temporal_index(record_id, new_version);
    }
    
    template <typename Container>
    void insert_or_update_from_container(const K& record_id, uint32_t flags, Container&& fields) {
        uint64_t prev_version = this->clock_.current_time();
        
        BaseClass::insert_or_update_from_container(record_id, flags, std::forward<Container>(fields));
        
        uint64_t new_version = this->clock_.current_time();
        update_temporal_index(record_id, new_version);
    }
    
    // Override delete_record to maintain temporal index
    void delete_record(const K& record_id, uint32_t flags = 0) override {
        uint64_t prev_version = this->clock_.current_time();
        
        BaseClass::delete_record(record_id, flags);
        
        uint64_t new_version = this->clock_.current_time();
        update_temporal_index(record_id, new_version);
    }
    
    // Get records modified since a given version
    CrdtVector<std::pair<K, Record<V>>> get_records_modified_since(uint64_t version) const {
        CrdtVector<std::pair<K, Record<V>>> results;
        std::set<K> seen_records;
        
        // Find versions greater than the requested version
        auto it = version_index_.lower_bound(version + 1);
        
        while (it != version_index_.end()) {
            for (const auto& record_id : it->second) {
                // Skip if already processed or tombstoned
                if (seen_records.count(record_id) > 0 || this->is_tombstoned(record_id)) {
                    continue;
                }
                
                auto record_ptr = this->get_record(record_id);
                if (record_ptr) {
                    results.emplace_back(record_id, *record_ptr);
                    seen_records.insert(record_id);
                }
            }
            ++it;
        }
        
        return results;
    }
    
    // Get recently modified records
    CrdtVector<std::pair<K, Record<V>>> get_recently_modified(size_t limit = 10) const {
        CrdtVector<std::pair<K, Record<V>>> results;
        std::set<K> seen_records;
        
        // Start from most recent versions and work backwards
        for (auto it = version_index_.rbegin(); 
             it != version_index_.rend() && results.size() < limit; 
             ++it) {
            
            for (const auto& record_id : it->second) {
                // Skip if already processed or tombstoned
                if (seen_records.count(record_id) > 0 || this->is_tombstoned(record_id)) {
                    continue;
                }
                
                auto record_ptr = this->get_record(record_id);
                if (record_ptr) {
                    results.emplace_back(record_id, *record_ptr);
                    seen_records.insert(record_id);
                    
                    if (results.size() >= limit) {
                        break;
                    }
                }
            }
        }
        
        return results;
    }
};

// Helper function to create a temporal indexed CRDT
template <typename K, typename V, typename... Args>
auto make_temporal_indexed_crdt(CrdtNodeId node_id, Args&&... args) {
    return std::make_shared<TemporalIndexedCRDT<K, V>>(node_id, std::forward<Args>(args)...);
}

} // namespace crdt_ext

#endif // CRDT_TEMPORAL_INDEX_HPP