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

// Hash function for ElementID to use in unordered containers
struct ElementIDHash {
  std::size_t operator()(const ElementID &id) const {
    return std::hash<CrdtNodeId>()(id.replica_id) ^ std::hash<uint64_t>()(id.sequence);
  }
};

// -----------------------------------------
// ListElement Definition
// -----------------------------------------

// Represents an element in the list
template <typename T>
struct ListElement {
  ElementID id;                          // Unique identifier for the element
  std::optional<T> value;                // Value stored (None if tombstoned)
  std::optional<ElementID> origin_left;  // Left origin at insertion
  std::optional<ElementID> origin_right; // Right origin at insertion

  // Checks if the element is tombstoned (deleted)
  bool is_deleted() const { return !value.has_value(); }

  // Comparison operator for std::set ordering (needs to be consistent with ListElementComparator)
  bool operator<(const ListElement &other) const {
    // This operator is not used by std::set since a separate comparator is provided
    // It can be left undefined or implemented based on ElementID
    return id < other.id;
  }

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
template <typename T>
struct ListElementComparator {
  bool operator()(const ListElement<T> &a, const ListElement<T> &b) const {
    // Compare based on the position in the list using origins

    // Handle root element first
    bool a_is_root = (a.id.replica_id == 0 && a.id.sequence == 0);
    bool b_is_root = (b.id.replica_id == 0 && b.id.sequence == 0);

    if (a_is_root && !b_is_root)
      return true;
    if (!a_is_root && b_is_root)
      return false;
    if (a_is_root && b_is_root)
      return false; // They are the same

    // Compare origin_left
    if (a.origin_left != b.origin_left) {
      if (!a.origin_left.has_value())
        return true; // a's origin_left is None, so a is first
      if (!b.origin_left.has_value())
        return false; // b's origin_left is None, so b is first
      return a.origin_left.value() < b.origin_left.value();
    }

    // Compare origin_right
    if (a.origin_right != b.origin_right) {
      if (!a.origin_right.has_value())
        return false; // a is before
      if (!b.origin_right.has_value())
        return true; // b is before
      return a.origin_right.value() < b.origin_right.value();
    }

    // If both have the same origins, use ElementID to break the tie
    return a.id < b.id;
  }
};

// -----------------------------------------
// ListCRDT Class Definition
// -----------------------------------------

// Represents the List CRDT using CrdtSortedSet
template <typename T>
class ListCRDT {
public:
  // Constructor to initialize a new CRDT instance with a unique replica ID
  ListCRDT(const CrdtNodeId &replica_id) : replica_id_(replica_id), counter_(0) {
    // Initialize with a root element to simplify origins
    ElementID root_id{0, 0}; // Use 0 as the root replica_id
    ListElement<T> root_element{root_id, std::nullopt, std::nullopt, std::nullopt};
    elements_.insert(root_element);
    element_index_.emplace(root_id, root_element); // Store a copy
  }

  // Inserts a value at the given index
  void insert(uint32_t index, const T &value) {
    ElementID new_id = generate_id();
    std::optional<ElementID> left_origin;
    std::optional<ElementID> right_origin;

    // Retrieve visible elements (non-tombstoned)
    auto visible = get_visible_elements();
    if (index > visible.size()) {
      index = visible.size(); // Adjust index if out of bounds
    }

    if (index == 0) {
      // Insert at the beginning, right_origin is the first element
      if (!visible.empty()) {
        right_origin = visible[0].id;
      }
    } else if (index == visible.size()) {
      // Insert at the end, left_origin is the last element
      if (!visible.empty()) {
        left_origin = visible.back().id;
      }
    } else {
      // Insert in the middle
      left_origin = visible[index - 1].id;
      right_origin = visible[index].id;
    }

    // Create a new element with the given value and origins
    ListElement<T> new_element{new_id, value, left_origin, right_origin};
    integrate(new_element);
  }

  // Deletes the element at the given index by tombstoning it
  void delete_element(uint32_t index) {
    const auto &visible = get_visible_elements();
    if (index >= visible.size())
      return; // Index out of bounds, do nothing

    ElementID target_id = visible[index].id;
    auto it = find_element(target_id);
    if (it != elements_.end()) {
      ListElement<T> updated = *it;
      updated.value.reset(); // Tombstone the element by resetting its value
      elements_.erase(it);
      elements_.insert(updated);
      element_index_[target_id] = updated;
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
  }

  // Generates a delta containing operations not seen by the other replica
  std::pair<CrdtVector<ListElement<T>>, CrdtVector<ElementID>> generate_delta(const ListCRDT &other) const {
    CrdtVector<ListElement<T>> new_elements;
    CrdtVector<ElementID> tombstones;

    for (const auto &elem : elements_) {
      if (elem.id.replica_id == 0 && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      if (!other.has_element(elem.id)) {
        new_elements.push_back(elem);
        if (elem.is_deleted()) {
          tombstones.push_back(elem.id);
        }
      }
    }

    return {new_elements, tombstones};
  }

  // Applies a delta to this CRDT
  void apply_delta(const CrdtVector<ListElement<T>> &new_elements, const CrdtVector<ElementID> &tombstones) {
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
          elements_.insert(updated);
          element_index_[updated.id] = updated;
        }
      }
    }

    // Apply tombstones to existing elements
    for (const auto &id : tombstones) {
      auto it = find_element(id);
      if (it != elements_.end()) {
        ListElement<T> updated = *it;
        updated.value.reset();
        elements_.erase(it);
        elements_.insert(updated);
        element_index_[updated.id] = updated;
      }
    }
  }

  // Retrieves the current list as a vector of values
  CrdtVector<T> get_values() const {
    CrdtVector<T> values;
    for (const auto &elem : elements_) {
      if (elem.id.replica_id == 0 && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      if (!elem.is_deleted()) {
        values.push_back(elem.value.value());
      }
    }
    return values;
  }

  // Prints the current visible list for debugging
  void print_visible() const {
    for (const auto &elem : elements_) {
      if (elem.id.replica_id == 0 && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      if (!elem.is_deleted()) {
        std::cout << elem.value.value() << " ";
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
    // Remove tombstoned elements (excluding root)
    CrdtVector<ListElement<T>> to_remove;
    for (const auto &elem : elements_) {
      if (elem.is_deleted() && elem.id.replica_id != 0) {
        to_remove.push_back(elem);
      }
    }

    for (const auto &elem : to_remove) {
      elements_.erase(elem);
      element_index_.erase(elem.id);
    }
  }

private:
  CrdtNodeId replica_id_;                                                  // Unique identifier for the replica
  uint64_t counter_;                                                       // Monotonically increasing counter for generating unique IDs
  CrdtSortedSet<ListElement<T>, ListElementComparator<T>> elements_;             // Set of all elements (including tombstoned)
  CrdtMap<ElementID, ListElement<T>, ElementIDHash> element_index_;                          // Maps ElementID to ListElement

  // Generates a unique ElementID
  ElementID generate_id() { return ElementID{replica_id_, ++counter_}; }

  // Checks if an element exists by its ID
  bool has_element(const ElementID &id) const { return element_index_.find(id) != element_index_.end(); }

  // Finds an element by its ID using the index
  typename CrdtSortedSet<ListElement<T>, ListElementComparator<T>>::iterator find_element(const ElementID &id) {
    auto it_map = element_index_.find(id);
    if (it_map != element_index_.end()) {
      return elements_.find(it_map->second);
    }
    return elements_.end();
  }

  typename CrdtSortedSet<ListElement<T>, ListElementComparator<T>>::const_iterator find_element(const ElementID &id) const {
    auto it_map = element_index_.find(id);
    if (it_map != element_index_.end()) {
      return elements_.find(it_map->second);
    }
    return elements_.end();
  }

  // Retrieves visible (non-tombstoned) elements in order
  CrdtVector<ListElement<T>> get_visible_elements() const {
    CrdtVector<ListElement<T>> visible;
    for (const auto &elem : elements_) {
      if (elem.id.replica_id == 0 && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      if (!elem.is_deleted()) {
        visible.push_back(elem);
      }
    }
    return visible;
  }

  // Integrates a single element into the CRDT
  void integrate(const ListElement<T> &new_elem) {
    auto it = elements_.find(new_elem);
    if (it != elements_.end()) {
      // Element exists, possibly update tombstone
      if (new_elem.is_deleted()) {
        ListElement<T> updated = *it;
        updated.value.reset();
        elements_.erase(it);
        elements_.insert(updated);
        element_index_[updated.id] = updated;
      }
      return;
    }

    // Insert the new element
    elements_.insert(new_elem);
    element_index_.emplace(new_elem.id, new_elem);
  }
};

#endif // LIST_CRDT_HPP