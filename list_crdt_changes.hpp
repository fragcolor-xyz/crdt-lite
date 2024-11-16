#ifndef LIST_CRDT_CHANGES_HPP
#define LIST_CRDT_CHANGES_HPP

#include "list_crdt.hpp"

// Represents a single change in the List CRDT
template <typename T>
struct ListChange {
    ElementID id;                          // ID of the changed element
    std::optional<T> value;                // Value (nullopt if deleted)
    std::optional<ElementID> origin_left;  // Left origin at insertion
    std::optional<ElementID> origin_right; // Right origin at insertion
    uint64_t version;                      // Change version
    uint64_t db_version;                   // Database version when change occurred
    CrdtNodeId node_id;                    // Node that made the change
    uint64_t local_db_version;             // Local version when change was created
    uint32_t flags;                        // Optional flags for the change

    ListChange() = default;

    ListChange(ElementID eid, std::optional<T> val, 
              std::optional<ElementID> left, std::optional<ElementID> right,
              uint64_t ver, uint64_t db_ver, CrdtNodeId nid, 
              uint64_t local_ver = 0, uint32_t f = 0)
        : id(eid), value(std::move(val)), 
          origin_left(std::move(left)), origin_right(std::move(right)),
          version(ver), db_version(db_ver), node_id(nid),
          local_db_version(local_ver), flags(f) {}

    // Convert from ListElement to ListChange
    static ListChange from_element(const ListElement<T>& elem, 
                                 uint64_t ver, uint64_t db_ver,
                                 CrdtNodeId nid, uint64_t local_ver = 0,
                                 uint32_t flags = 0) {
        return ListChange(elem.id, elem.value, elem.origin_left, elem.origin_right,
                         ver, db_ver, nid, local_ver, flags);
    }

    // Convert to ListElement
    ListElement<T> to_element() const {
        return ListElement<T>{id, value, origin_left, origin_right};
    }
};

// Extension class that adds change tracking to ListCRDT
template <typename T>
class ListCRDTWithChanges : public ListCRDT<T> {
public:
    ListCRDTWithChanges(const CrdtNodeId& replica_id) 
        : ListCRDT<T>(replica_id), clock_() {}

    // Override insert to track changes
    void insert(uint32_t index, const T& value, CrdtVector<ListChange<T>>* changes = nullptr) {
        uint64_t db_version = clock_.tick();
        
        ElementID new_id = this->generate_id();
        std::optional<ElementID> left_origin;
        std::optional<ElementID> right_origin;

        // Get origins based on index
        const auto& visible = this->get_visible_elements();
        if (index >= visible.size()) {
            index = visible.size() - 1;
        }

        if (index == 0) {
            if (!visible.empty()) {
                right_origin = visible[0]->id;
            }
        } else if (index == visible.size() - 1) {
            if (!visible.empty()) {
                left_origin = visible[index - 1]->id;
            }
        } else {
            left_origin = visible[index - 1]->id;
            right_origin = visible[index]->id;
        }

        // Create and integrate the element
        ListElement<T> new_element{new_id, value, left_origin, right_origin};
        this->integrate(new_element);

        // Update version info
        this->update_version_info(new_id, 1, db_version, db_version);

        // Record the change if requested
        if (changes) {
            changes->emplace_back(ListChange<T>::from_element(
                new_element, 1, db_version, this->get_replica_id(), db_version));
        }
    }

    // Override delete to track changes 
    void delete_element(uint32_t index, CrdtVector<ListChange<T>>* changes = nullptr) {
        uint64_t db_version = clock_.tick();
        
        const auto& visible = this->get_visible_elements();
        if (index >= visible.size() - 1) {
            return;
        }

        ElementID target_id = visible[index]->id;
        auto element = this->get_element(target_id);
        if (!element || element->is_deleted()) {
            return;
        }

        // Create tombstone element
        ListElement<T> tombstone{target_id, std::nullopt, 
                               element->origin_left, element->origin_right};
        this->integrate(tombstone);

        // Update version info
        auto version_info = this->get_version_info(target_id);
        this->update_version_info(target_id, version_info.version + 1, db_version, db_version);

        // Record the change if requested
        if (changes) {
            changes->emplace_back(ListChange<T>::from_element(
                tombstone, version_info.version + 1, db_version, this->get_replica_id(), db_version));
        }
    }

    // Get changes since a specific version
    CrdtVector<ListChange<T>> get_changes_since(uint64_t last_db_version) const {
        CrdtVector<ListChange<T>> changes;
        
        for (const auto& elem : this->get_all_elements()) {
            // Skip root element
            if (elem.id.replica_id == 0 && elem.id.sequence == 0) {
                continue;
            }

            // Get version info for the element
            auto version_info = this->get_version_info(elem.id);
            if (version_info.local_db_version > last_db_version) {
                changes.emplace_back(ListChange<T>::from_element(
                    elem, version_info.version, version_info.db_version,
                    version_info.node_id, version_info.local_db_version));
            }
        }

        return changes;
    }

    // Apply changes from another replica
    void apply_changes(CrdtVector<ListChange<T>>&& changes) {
        for (const auto& change : changes) {
            clock_.update(change.db_version);
            this->integrate(change.to_element());
            this->update_version_info(change.id, change.version, change.db_version, change.local_db_version);
        }
    }

private:
    LogicalClock clock_;
};

// Helper function to sync two ListCRDT instances
template <typename T>
void sync_list_nodes(ListCRDTWithChanges<T>& source, 
                     ListCRDTWithChanges<T>& target,
                     uint64_t& last_db_version) {
    // Get changes from source since last sync
    auto changes = source.get_changes_since(last_db_version);
    
    // Store current last_db_version to use as minimum for next sync
    uint64_t min_version = last_db_version;

    // Update last_db_version to highest seen
    for (const auto& change : changes) {
        if (change.db_version > last_db_version) {
            last_db_version = change.db_version;
        }
    }

    // Apply changes to target
    target.apply_changes(std::move(changes));

    // Now get changes from target since min_version
    changes = target.get_changes_since(min_version);

    // Update last_db_version again if needed
    for (const auto& change : changes) {
        if (change.db_version > last_db_version) {
            last_db_version = change.db_version;
        }
    }

    // Apply target's changes back to source
    source.apply_changes(std::move(changes));
}

#endif // LIST_CRDT_CHANGES_HPP 