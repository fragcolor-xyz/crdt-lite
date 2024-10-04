#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <algorithm>
#include <map>
#include <set>

// Represents a unique identifier for a list element
struct ElementID {
  std::string replica_id; // Unique identifier for the replica
  uint64_t sequence;      // Monotonically increasing sequence number

  bool operator<(const ElementID &other) const {
    if (sequence != other.sequence)
      return sequence < other.sequence;
    return replica_id < other.replica_id;
  }

  bool operator==(const ElementID &other) const { return sequence == other.sequence && replica_id == other.replica_id; }

  // For printing purposes
  friend std::ostream &operator<<(std::ostream &os, const ElementID &id) {
    os << "(" << id.replica_id << ", " << id.sequence << ")";
    return os;
  }
};

// Represents an element in the list
struct ListElement {
  ElementID id;                          // Unique identifier
  std::optional<std::string> value;      // Value stored (None if tombstoned)
  std::optional<ElementID> origin_left;  // Left origin at insertion
  std::optional<ElementID> origin_right; // Right origin at insertion

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
    // If a's origin_right is b's ID, then a comes before b
    if (a.origin_right.has_value() && a.origin_right.value() == b.id) {
      return true;
    }
    // If b's origin_right is a's ID, then b comes before a
    if (b.origin_right.has_value() && b.origin_right.value() == a.id) {
      return false;
    }

    // If both have the same origin_left and origin_right, use ElementID to break the tie
    if (a.origin_left == b.origin_left && a.origin_right == b.origin_right) {
      return a.id < b.id;
    }

    // Otherwise, compare origin_left
    if (a.origin_left.has_value() && b.origin_left.has_value()) {
      if (a.origin_left.value() < b.origin_left.value()) {
        return true;
      }
      if (b.origin_left.value() < a.origin_left.value()) { // Changed from '>' to '<'
        return false;
      }
    } else if (a.origin_left.has_value()) {
      return true;
    } else if (b.origin_left.has_value()) {
      return false;
    }

    // Compare origin_right
    if (a.origin_right.has_value() && b.origin_right.has_value()) {
      if (a.origin_right.value() < b.origin_right.value()) {
        return true;
      }
      if (b.origin_right.value() < a.origin_right.value()) { // Changed from '>' to '<'
        return false;
      }
    } else if (a.origin_right.has_value()) {
      return true;
    } else if (b.origin_right.has_value()) {
      return false;
    }

    // Finally, use ElementID to break any remaining ties
    return a.id < b.id;
  }
};

// Represents the List CRDT
class ListCRDT {
public:
  // Constructor
  ListCRDT(const std::string &replica_id) : replica_id_(replica_id), counter_(0) {
    // Initialize with a root element to simplify origins
    ElementID root_id{"root", 0};
    ListElement root_element{root_id, std::nullopt, std::nullopt, std::nullopt};
    elements_.push_back(root_element);
  }

  // Inserts a value at the given index
  void insert(uint32_t index, const std::string &value) {
    ElementID new_id = generate_id();
    std::optional<ElementID> left_origin;
    std::optional<ElementID> right_origin;

    auto visible = get_visible_elements();
    if (index > 0 && index <= visible.size()) {
      // Get the ID of the element currently at (index - 1)
      left_origin = visible[index - 1].id;
    }
    if (index < visible.size()) {
      // Get the ID of the element currently at (index)
      right_origin = visible[index].id;
    }

    ListElement new_element{new_id, value, left_origin, right_origin};
    integrate(new_element);
  }

  // Deletes the element at the given index
  void delete_element(uint32_t index) {
    const auto &visible = get_visible_elements();
    if (index >= visible.size())
      return;

    ElementID target_id = visible[index].id;
    auto it = find_element(target_id);
    if (it != elements_.end()) {
      it->value.reset(); // Tombstone the element
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
      if (it == elements_.end()) {
        integrate(elem);
      } else {
        // Element already exists, possibly update tombstone
        if (elem.is_deleted()) {
          it->value.reset();
        }
      }
    }

    // Apply tombstones
    for (const auto &id : tombstones) {
      auto it = find_element(id);
      if (it != elements_.end()) {
        it->value.reset();
      }
    }

    // After applying, sort all elements to maintain order
    std::sort(elements_.begin(), elements_.end(), ListElementComparator());

    // Remove duplicates while maintaining the first occurrence
    auto last = std::unique(elements_.begin(), elements_.end(),
                            [&](const ListElement &a, const ListElement &b) -> bool { return a.id == b.id; });
    elements_.erase(last, elements_.end());
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

  // Prints the current list for debugging
  void print() const {
    for (const auto &elem : elements_) {
      if (elem.id.replica_id == "root" && elem.id.sequence == 0) {
        continue; // Skip the root element
      }
      if (elem.is_deleted()) {
        std::cout << "[Deleted] ";
      } else {
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

private:
  std::string replica_id_;
  uint64_t counter_;
  std::vector<ListElement> elements_;

  // Generates a unique ElementID
  ElementID generate_id() { return ElementID{replica_id_, ++counter_}; }

  // Finds an element by its ID
  std::vector<ListElement>::iterator find_element(const ElementID &id) {
    return std::find_if(elements_.begin(), elements_.end(), [&](const ListElement &elem) { return elem.id == id; });
  }

  std::vector<ListElement>::const_iterator find_element(const ElementID &id) const {
    return std::find_if(elements_.cbegin(), elements_.cend(), [&](const ListElement &elem) { return elem.id == id; });
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
    // If the element already exists, do nothing
    auto it = find_element(new_elem.id);
    if (it != elements_.end()) {
      // Element already exists, possibly update tombstone
      if (new_elem.is_deleted()) {
        it->value.reset();
      }
      return;
    }

    // Determine the correct position based on origins
    uint32_t pos = 0;
    if (new_elem.origin_left.has_value()) {
      pos = find_position(new_elem.origin_left.value()) + 1;
    } else {
      // If no left origin, position after root
      pos = 1;
    }

    // Insert the new element at the determined position
    elements_.insert(elements_.begin() + pos, new_elem);
  }

  // Finds the position of an ElementID in the elements vector
  uint32_t find_position(const ElementID &id) const {
    for (uint32_t i = 0; i < elements_.size(); ++i) {
      if (elements_[i].id == id) {
        return i;
      }
    }
    return elements_.size() - 1; // Default to the end
  }
};
