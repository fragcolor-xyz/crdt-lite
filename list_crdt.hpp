#ifndef LIST_CRDT_HPP
#define LIST_CRDT_HPP

#include "crdt.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <algorithm>
#include <map>
#include <set>
#include <unordered_map>
#include <cassert>
#include <functional> // For std::hash

// -----------------------------------------
// ElementID Definition
// -----------------------------------------

// Represents a unique identifier for a list element
struct ElementID {
  CrdtNodeId replica_id; // Unique identifier for the replica
  uint64_t sequence;     // Monotonically increasing sequence number

  // Comparison operator for ordering elements
  bool operator<(const ElementID &other) const {
    if (sequence != other.sequence)
      return sequence < other.sequence;
    return replica_id < other.replica_id;
  }

  // Equality operator for comparing two ElementIDs
  bool operator==(const ElementID &other) const { return sequence == other.sequence && replica_id == other.replica_id; }

  // For printing purposes
  friend std::ostream &operator<<(std::ostream &os, const ElementID &id) {
    os << "(" << id.replica_id << ", " << id.sequence << ")";
    return os;
  }
};

// Improved Hash function for ElementID to use in unordered containers
struct ElementIDHash {
  std::size_t operator()(const ElementID &id) const {
    // Using a better hash combination technique to reduce collisions
    // Example: boost::hash_combine equivalent
    std::size_t seed = std::hash<CrdtNodeId>()(id.replica_id);
    seed ^= std::hash<uint64_t>()(id.sequence) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
  }
};

// -----------------------------------------
// ListElement Definition
// -----------------------------------------

// Represents an element in the list
template <typename T> struct ListElement {
  ElementID id;                          // Unique identifier for the element
  std::optional<T> value;                // Value stored (None if tombstoned)
  std::optional<ElementID> origin_left;  // Left origin at insertion
  std::optional<ElementID> origin_right; // Right origin at insertion

  // Checks if the element is tombstoned (deleted)
  bool is_deleted() const { return !value.has_value(); }

  // For printing purposes
  friend std::ostream &operator<<(std::ostream &os, const ListElement &elem) {
    os << "ID: " << elem.id << ", ";
    if (elem.is_deleted()) {
      os << "[Deleted]";
    } else {
      os << "Value: " << elem.value.value();
    }
    os << ", Origin Left: ";
    if (elem.origin_left.has_value()) {
      os << elem.origin_left.value();
    } else {
      os << "None";
    }
    os << ", Origin Right: ";
    if (elem.origin_right.has_value()) {
      os << elem.origin_right.value();
    } else {
      os << "None";
    }
    return os;
  }
};

// -----------------------------------------
// ListElementComparator Definition
// -----------------------------------------

// Comparator for ListElements to establish a total order
template <typename T> struct ListElementComparator {
  bool operator()(const ListElement<T> &a, const ListElement<T> &b) const {
    // First, handle root elements
    bool a_is_root = (a.id.replica_id == 0 && a.id.sequence == 0);
    bool b_is_root = (b.id.replica_id == 0 && b.id.sequence == 0);

    if (a_is_root && !b_is_root)
      return true;
    if (!a_is_root && b_is_root)
      return false;
    if (a_is_root && b_is_root)
      return false; // They are the same

    // Compare origin_left
    if (a.origin_left && b.origin_left) {
      if (a.origin_left.value() != b.origin_left.value()) {
        return a.origin_left.value() < b.origin_left.value();
      }
    } else if (a.origin_left) {
      // b.origin_left is not set
      return false;
    } else if (b.origin_left) {
      // a.origin_left is not set
      return true;
    }

    // Compare origin_right
    if (a.origin_right && b.origin_right) {
      if (a.origin_right.value() != b.origin_right.value()) {
        return a.origin_right.value() < b.origin_right.value();
      }
    } else if (a.origin_right) {
      // b.origin_right is not set
      return false;
    } else if (b.origin_right) {
      // a.origin_right is not set
      return true;
    }

    // If both have the same origins, use ElementID to break the tie
    return a.id < b.id;
  }
};

// -----------------------------------------
// ListCRDT Class Definition
// -----------------------------------------

// Represents the List CRDT using optimized data structures
template <typename T> class ListCRDT {
public:
  // Constructor to initialize a new CRDT instance with a unique replica ID
  ListCRDT(const CrdtNodeId &replica_id) : replica_id_(replica_id), counter_(0) {
    assert(replica_id_ != 0 && "Replica ID 0 is reserved for the root element.");
    // Initialize with a root element to simplify origins
    ElementID root_id{0, 0}; // Use 0 as the root replica_id
    ListElement<T> root_element{root_id, std::nullopt, std::nullopt, std::nullopt};
    elements_.emplace(root_element);
    element_index_.emplace(root_id, root_element);               // Store a copy
    visible_elements_.emplace_back(&element_index_.at(root_id)); // Root is initially visible
  }

  // Inserts a value at the given index
  void insert(uint32_t index, const T &value) {
    ElementID new_id = generate_id();
    std::optional<ElementID> left_origin;
    std::optional<ElementID> right_origin;

    // Adjust index if out of bounds
    if (index > visible_elements_.size() - 1) { // Exclude root
      index = visible_elements_.size() - 1;
    }

    if (index == 0) {
      // Insert after root
      if (!visible_elements_.empty()) {
        right_origin = visible_elements_[0]->id;
      }
    } else if (index == visible_elements_.size() - 1) {
      // Insert at the end
      if (!visible_elements_.empty()) {
        left_origin = visible_elements_[index - 1]->id;
      }
    } else {
      // Insert in the middle
      left_origin = visible_elements_[index - 1]->id;
      right_origin = visible_elements_[index]->id;
    }

    // Create a new element with the given value and origins
    ListElement<T> new_element{new_id, value, left_origin, right_origin};
    integrate(new_element);
  }

  // Deletes the element at the given index by tombstoning it
  void delete_element(uint32_t index) {
    if (index >= visible_elements_.size() - 1) { // Exclude root
      std::cerr << "Delete operation failed: Index " << index << " is out of bounds.\n";
      return; // Index out of bounds, do nothing
    }

    ElementID target_id = visible_elements_[index]->id;
    auto it = find_element(target_id);
    if (it != elements_.end()) {
      if (it->is_deleted()) {
        // Already deleted
        return;
      }
      ListElement<T> updated = *it;
      updated.value.reset(); // Tombstone the element by resetting its value
      elements_.erase(it);
      elements_.emplace(updated);
      element_index_[updated.id] = updated;

      // Remove from visible_elements_
      auto ve_it = std::find_if(visible_elements_.begin(), visible_elements_.end(),
                                [&](const ListElement<T> *elem_ptr) { return elem_ptr->id == target_id; });
      if (ve_it != visible_elements_.end()) {
        visible_elements_.erase(ve_it);
      }
    }
  }

  // Merges another ListCRDT into this one
  void merge(const ListCRDT &other) {
    for (const auto &elem : other.elements_) {
      if (elem.id.replica_id == 0 && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      integrate(elem);
    }

    // Rebuild visible_elements_ after merge
    rebuild_visible_elements();
  }

  // Generates a delta containing operations not seen by the other replica
  std::pair<std::vector<ListElement<T>>, std::vector<ElementID>> generate_delta(const ListCRDT &other) const {
    std::vector<ListElement<T>> new_elements;
    std::vector<ElementID> tombstones;

    for (const auto &elem : elements_) {
      if (elem.id.replica_id == 0 && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      if (!other.has_element(elem.id)) {
        new_elements.emplace_back(elem);
        if (elem.is_deleted()) {
          tombstones.emplace_back(elem.id);
        }
      }
    }

    return {new_elements, tombstones};
  }

  // Applies a delta to this CRDT
  void apply_delta(const std::vector<ListElement<T>> &new_elements, const std::vector<ElementID> &tombstones) {
    // Apply insertions
    for (const auto &elem : new_elements) {
      if (elem.id.replica_id == 0 && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      auto it = find_element(elem.id);
      if (it == elements_.end()) {
        integrate(elem);
      } else {
        // Element already exists, possibly update tombstone
        if (elem.is_deleted()) {
          ListElement<T> updated = *it;
          updated.value.reset();
          elements_.erase(it);
          elements_.emplace(updated);
          element_index_[updated.id] = updated;

          // Remove from visible_elements_ if present
          auto ve_it = std::find_if(visible_elements_.begin(), visible_elements_.end(),
                                    [&](const ListElement<T> *ptr) { return ptr->id == elem.id; });
          if (ve_it != visible_elements_.end()) {
            visible_elements_.erase(ve_it);
          }
        }
      }
    }

    // Apply tombstones to existing elements
    for (const auto &id : tombstones) {
      auto it = find_element(id);
      if (it != elements_.end()) {
        if (it->is_deleted()) {
          continue; // Already deleted
        }
        ListElement<T> updated = *it;
        updated.value.reset();
        elements_.erase(it);
        elements_.emplace(updated);
        element_index_[updated.id] = updated;

        // Remove from visible_elements_ if present
        auto ve_it = std::find_if(visible_elements_.begin(), visible_elements_.end(),
                                  [&](const ListElement<T> *ptr) { return ptr->id == id; });
        if (ve_it != visible_elements_.end()) {
          visible_elements_.erase(ve_it);
        }
      }
    }
  }

  // Retrieves the current list as a vector of values
  std::vector<T> get_values() const {
    std::vector<T> values;
    values.reserve(visible_elements_.size());
    for (const auto &elem_ptr : visible_elements_) {
      if (elem_ptr->id.replica_id == 0 && elem_ptr->id.sequence == 0) {
        continue; // Skip the root element
      }
      if (!elem_ptr->is_deleted()) {
        values.emplace_back(elem_ptr->value.value());
      }
    }
    return values;
  }

  // Prints the current visible list for debugging
  void print_visible() const {
    for (const auto &elem_ptr : visible_elements_) {
      if (elem_ptr->id.replica_id == 0 && elem_ptr->id.sequence == 0) {
        continue; // Skip the root element
      }
      if (!elem_ptr->is_deleted()) {
        std::cout << elem_ptr->value.value() << " ";
      }
    }
    std::cout << std::endl;
  }

  // Prints all elements including tombstones for debugging
  void print_all_elements() const {
    for (const auto &elem : elements_) {
      std::cout << elem << std::endl;
    }
  }

  // Performs garbage collection by removing tombstones that are safe to delete
  void garbage_collect() {
    std::vector<ElementID> to_remove_ids;

    for (const auto &elem : elements_) {
      if (elem.is_deleted() && elem.id.replica_id != 0) {
        to_remove_ids.emplace_back(elem.id);
      }
    }

    for (const auto &id : to_remove_ids) {
      auto it = find_element(id);
      if (it != elements_.end()) {
        elements_.erase(it);
        element_index_.erase(id);
        // No need to remove from visible_elements_ since it's already tombstoned
      }
    }
  }

protected:
  // Make these methods protected so derived classes can access them
  inline ElementID generate_id() { return ElementID{replica_id_, ++counter_}; }
  
  void integrate(const ListElement<T> &new_elem) {
    auto it = elements_.find(new_elem);
    if (it != elements_.end()) {
      // Element exists, possibly update tombstone
      if (new_elem.is_deleted() && !it->is_deleted()) {
        ListElement<T> updated = *it;
        updated.value.reset();
        elements_.erase(it);
        elements_.emplace(updated);
        element_index_[updated.id] = updated;

        // Remove from visible_elements_ if present
        auto ve_it = std::find_if(visible_elements_.begin(), visible_elements_.end(),
                                  [&](const ListElement<T> *ptr) { return ptr->id == updated.id; });
        if (ve_it != visible_elements_.end()) {
          visible_elements_.erase(ve_it);
        }
      }
      return;
    }

    // Insert the new element
    elements_.emplace(new_elem);
    element_index_.emplace(new_elem.id, new_elem);

    if (!new_elem.is_deleted()) {
      // Insert into visible_elements_ maintaining order
      auto pos =
          std::lower_bound(visible_elements_.begin(), visible_elements_.end(), &new_elem,
                           [&](const ListElement<T> *a, const ListElement<T> *b) { return ListElementComparator<T>()(*a, *b); });
      visible_elements_.insert(pos, &element_index_.at(new_elem.id));
    }
  }

  // Add new protected methods needed by ListCRDTWithChanges
  const std::vector<const ListElement<T>*>& get_visible_elements() const {
    return visible_elements_;
  }

  const std::set<ListElement<T>, ListElementComparator<T>>& get_all_elements() const {
    return elements_;
  }

  const ListElement<T>* get_element(const ElementID& id) const {
    auto it = element_index_.find(id);
    if (it != element_index_.end()) {
      return &it->second;
    }
    return nullptr;
  }

  CrdtNodeId get_replica_id() const {
    return replica_id_;
  }

  struct ElementVersionInfo {
    uint64_t version;
    uint64_t db_version;
    CrdtNodeId node_id;
    uint64_t local_db_version;
  };

  ElementVersionInfo get_version_info(const ElementID& id) const {
    auto it = element_index_.find(id);
    if (it != element_index_.end()) {
      return ElementVersionInfo{
        element_versions_.count(id) ? element_versions_.at(id) : 1, // version
        db_versions_.count(id) ? db_versions_.at(id) : 0,          // db_version
        it->second.id.replica_id,                                  // node_id
        local_versions_.count(id) ? local_versions_.at(id) : 0     // local_db_version
      };
    }
    return ElementVersionInfo{1, 0, replica_id_, 0};
  }

  void update_version_info(const ElementID& id, uint64_t version, uint64_t db_version, uint64_t local_version) {
    element_versions_[id] = version;
    db_versions_[id] = db_version;
    local_versions_[id] = local_version;
  }

private:
  CrdtNodeId replica_id_;                                       // Unique identifier for the replica
  uint64_t counter_;                                            // Monotonically increasing counter for generating unique IDs
  std::set<ListElement<T>, ListElementComparator<T>> elements_; // Set of all elements (including tombstoned)
  std::unordered_map<ElementID, ListElement<T>, ElementIDHash> element_index_; // Maps ElementID to ListElement
  std::vector<const ListElement<T> *> visible_elements_;                       // Ordered list of visible elements

  // Add version tracking
  std::unordered_map<ElementID, uint64_t, ElementIDHash> element_versions_;
  std::unordered_map<ElementID, uint64_t, ElementIDHash> db_versions_;
  std::unordered_map<ElementID, uint64_t, ElementIDHash> local_versions_;

  // Checks if an element exists by its ID
  inline bool has_element(const ElementID &id) const { return element_index_.find(id) != element_index_.end(); }

  // Finds an element by its ID using the index
  typename std::set<ListElement<T>, ListElementComparator<T>>::iterator find_element(const ElementID &id) {
    auto it_map = element_index_.find(id);
    if (it_map != element_index_.end()) {
      return elements_.find(it_map->second);
    }
    return elements_.end();
  }

  typename std::set<ListElement<T>, ListElementComparator<T>>::const_iterator find_element(const ElementID &id) const {
    auto it_map = element_index_.find(id);
    if (it_map != element_index_.end()) {
      return elements_.find(it_map->second);
    }
    return elements_.end();
  }

  // Rebuilds the visible_elements_ vector after a merge
  void rebuild_visible_elements() {
    visible_elements_.clear();
    for (const auto &elem : elements_) {
      if (!elem.is_deleted()) {
        visible_elements_.emplace_back(&element_index_.at(elem.id));
      }
    }
    // Sort visible_elements_ according to the comparator
    std::sort(visible_elements_.begin(), visible_elements_.end(),
              [&](const ListElement<T> *a, const ListElement<T> *b) { return ListElementComparator<T>()(*a, *b); });
  }
};

#endif // LIST_CRDT_HPP