#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <algorithm>
#include <map>
#include <set>
#include <unordered_map>
#include <cassert>

// Represents a unique identifier for a list element
struct ElementID {
  std::string replica_id; // Unique identifier for the replica
  uint64_t sequence;      // Monotonically increasing sequence number

  // Comparison operator for ordering elements
  bool operator<(const ElementID &other) const {
    if (sequence != other.sequence)
      return sequence < other.sequence;
    return replica_id < other.replica_id;
  }

  // Equality operator for comparing two ElementIDs
  bool operator==(const ElementID &other) const { return sequence == other.sequence && replica_id == other.replica_id; }

  // For hashing in unordered_map
  struct Hash {
    std::size_t operator()(const ElementID &id) const {
      return std::hash<std::string>()(id.replica_id) ^ std::hash<uint64_t>()(id.sequence);
    }
  };

  // For printing purposes
  friend std::ostream &operator<<(std::ostream &os, const ElementID &id) {
    os << "(" << id.replica_id << ", " << id.sequence << ")";
    return os;
  }
};

// Represents an element in the list
struct ListElement {
  ElementID id;                          // Unique identifier for the element
  std::optional<std::string> value;      // Value stored (None if tombstoned)
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

// Comparator for ListElements to establish a total order
struct ListElementComparator {
  bool operator()(const ListElement &a, const ListElement &b) const {
    // Compare based on the position in the list using origins
    if (a.origin_left != b.origin_left) {
      if (!a.origin_left.has_value())
        return true; // Root is first
      if (!b.origin_left.has_value())
        return false;
      return a.origin_left.value() < b.origin_left.value();
    }

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

// Represents the List CRDT
class ListCRDT {
public:
  // Constructor to initialize a new CRDT instance with a unique replica ID
  ListCRDT(const std::string &replica_id) : replica_id_(replica_id), counter_(0) {
    // Initialize with a root element to simplify origins
    ElementID root_id{"root", 0};
    ListElement root_element{root_id, std::nullopt, std::nullopt, std::nullopt};
    elements_.push_back(root_element);
    element_index_.emplace(root_id, 0); // Store index instead of iterator
  }

  // Inserts a value at the given index
  void insert(uint32_t index, const std::string &value) {
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
    ListElement new_element{new_id, value, left_origin, right_origin};
    integrate(new_element);
  }

  // Deletes the element at the given index by tombstoning it
  void delete_element(uint32_t index) {
    const auto &visible = get_visible_elements();
    if (index >= visible.size())
      return; // Index out of bounds, do nothing

    ElementID target_id = visible[index].id;
    size_t it = find_element(target_id);
    if (it != elements_.size()) {
      elements_[it].value.reset(); // Tombstone the element by resetting its value
    }
  }

  // Merges another ListCRDT into this one
  void merge(const ListCRDT &other) {
    // Integrate all elements from the other CRDT
    for (const auto &elem : other.elements_) {
      if (elem.id.replica_id == "root" && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      integrate(elem);
    }

    // After integration, sort all elements to establish a total order
    std::sort(elements_.begin(), elements_.end(), ListElementComparator());

    // Remove duplicates while maintaining the first occurrence
    auto last = std::unique(elements_.begin(), elements_.end(),
                            [&](const ListElement &a, const ListElement &b) -> bool { return a.id == b.id; });
    elements_.erase(last, elements_.end());

    // Rebuild the index after sorting and deduplication
    rebuild_index();
  }

  // Generates a delta containing operations not seen by the other replica
  std::pair<std::vector<ListElement>, std::vector<ElementID>> generate_delta(const ListCRDT &other) const {
    std::vector<ListElement> new_elements;
    std::vector<ElementID> tombstones;

    // Create a set of ElementIDs present in the other CRDT
    std::set<ElementID> other_ids;
    for (const auto &elem : other.elements_) {
      other_ids.insert(elem.id);
    }

    // Identify new elements and tombstones
    for (const auto &elem : elements_) {
      if (elem.id.replica_id == "root" && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      if (other_ids.find(elem.id) == other_ids.end()) {
        new_elements.push_back(elem);
        if (elem.is_deleted()) {
          tombstones.push_back(elem.id);
        }
      }
    }

    return {new_elements, tombstones};
  }

  // Applies a delta to this CRDT
  void apply_delta(const std::vector<ListElement> &new_elements, const std::vector<ElementID> &tombstones) {
    // Apply insertions
    for (const auto &elem : new_elements) {
      if (elem.id.replica_id == "root" && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      auto it = find_element(elem.id);
      if (it == elements_.size()) {
        integrate(elem);
      } else {
        // Element already exists, possibly update tombstone
        if (elem.is_deleted()) {
          elements_[it].value.reset();
        }
      }
    }

    // Apply tombstones to existing elements
    for (const auto &id : tombstones) {
      size_t it = find_element(id);
      if (it != elements_.size()) {
        elements_[it].value.reset();
      }
    }

    // After applying, sort all elements to maintain order
    std::sort(elements_.begin(), elements_.end(), ListElementComparator());

    // Remove duplicates while maintaining the first occurrence
    auto last = std::unique(elements_.begin(), elements_.end(),
                            [&](const ListElement &a, const ListElement &b) -> bool { return a.id == b.id; });
    elements_.erase(last, elements_.end());

    // Rebuild the index after sorting and deduplication
    rebuild_index();
  }

  // Retrieves the current list as a vector of strings
  std::vector<std::string> get_values() const {
    std::vector<std::string> values;
    for (const auto &elem : elements_) {
      if (elem.id.replica_id == "root" && elem.id.sequence == 0) {
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
      if (elem.id.replica_id == "root" && elem.id.sequence == 0) {
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
  // For simplicity, assumes that all replicas have seen all operations
  // In a real-world scenario, you'd track replica states to ensure safety
  void garbage_collect() {
    // Remove tombstoned elements
    elements_.erase(
        std::remove_if(elements_.begin(), elements_.end(),
                       [&](const ListElement &elem) -> bool { return elem.is_deleted() && elem.id.replica_id != "root"; }),
        elements_.end());

    // Rebuild the index after garbage collection
    rebuild_index();
  }

private:
  std::string replica_id_;            // Unique identifier for the replica
  uint64_t counter_;                  // Monotonically increasing counter for generating unique IDs
  std::vector<ListElement> elements_; // List of all elements (including tombstoned)
  std::unordered_map<ElementID, size_t, ElementID::Hash> element_index_; // Maps ElementID to index for fast lookup

  // Generates a unique ElementID
  ElementID generate_id() { return ElementID{replica_id_, ++counter_}; }

  // Finds an element by its ID using the index
  // Returns the index of the element, or elements_.size() if not found
  size_t find_element(const ElementID &id) const {
    auto it = element_index_.find(id);
    if (it != element_index_.end()) {
      return it->second;
    }
    return elements_.size();
  }

  // Retrieves visible (non-tombstoned) elements
  std::vector<ListElement> get_visible_elements() const {
    std::vector<ListElement> visible;
    for (const auto &elem : elements_) {
      if (elem.id.replica_id == "root" && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      if (!elem.is_deleted()) {
        visible.push_back(elem);
      }
    }
    return visible;
  }

  // Integrates a single element into the CRDT
  void integrate(const ListElement &new_elem) {
    // If the element already exists, update tombstone if necessary
    size_t existing_index = find_element(new_elem.id);
    if (existing_index != elements_.size()) {
      if (new_elem.is_deleted()) {
        elements_[existing_index].value.reset();
      }
      return;
    }

    // Find the correct position to insert the new element
    auto insert_pos = std::lower_bound(elements_.begin(), elements_.end(), new_elem, ListElementComparator());
    size_t index = std::distance(elements_.begin(), insert_pos);
    elements_.insert(insert_pos, new_elem);

    // Rebuild the index as inserting elements shifts indices
    rebuild_index();
  }

  // Rebuilds the element_index_ mapping
  void rebuild_index() {
    element_index_.clear();
    for (size_t i = 0; i < elements_.size(); ++i) {
      element_index_.emplace(elements_[i].id, i);
    }
  }
};