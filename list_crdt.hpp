#ifndef LIST_CRDT_HPP
#define LIST_CRDT_HPP

#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <optional>
#include <stdexcept>
#include <algorithm>
#include <cstdlib>
#include <cstdint>
#include <limits>
#include <cmath>
#include <fstream>

#define MAX_COMPONENT_LEVEL 100
#define BASE 16 // Base for digit values

// Type aliases for CRDT collections
template <typename T> using CrdtVector = std::vector<T>;
using CrdtString = std::string;
template <typename K, typename V> using CrdtMap = std::unordered_map<K, V>;
using CrdtNodeId = uint64_t;

/// Represents a logical clock for maintaining causality.
class LogicalClock {
public:
  LogicalClock() : time_(0) {}

  /// Increments the clock for a local event.
  uint64_t tick() { return ++time_; }

  /// Updates the clock based on a received time.
  uint64_t update(uint64_t received_time) {
    time_ = std::max(time_, received_time);
    return ++time_;
  }

  /// Sets the logical clock to a specific time.
  void set_time(uint64_t t) { time_ = t; }

  /// Retrieves the current time.
  uint64_t current_time() const { return time_; }

private:
  uint64_t time_;
};

/// Represents an identifier used in the CRDT.
struct Identifier {
  uint32_t digit;
  CrdtNodeId site_id;
  uint64_t clock;

  Identifier(uint32_t d, CrdtNodeId s, uint64_t c) : digit(d), site_id(s), clock(c) {}

  bool operator<(const Identifier &other) const {
    if (digit != other.digit)
      return digit < other.digit;
    if (site_id != other.site_id)
      return site_id < other.site_id;
    return clock < other.clock;
  }

  bool operator==(const Identifier &other) const {
    return digit == other.digit && site_id == other.site_id && clock == other.clock;
  }
};

/// Represents a unique position identifier for list elements.
struct PositionID {
  CrdtVector<Identifier> identifiers;

  PositionID() = default;
  explicit PositionID(CrdtVector<Identifier> ids) : identifiers(std::move(ids)) {}

  bool operator<(const PositionID &other) const {
    size_t min_size = std::min(identifiers.size(), other.identifiers.size());
    for (size_t i = 0; i < min_size; ++i) {
      if (identifiers[i] < other.identifiers[i])
        return true;
      if (other.identifiers[i] < identifiers[i])
        return false;
    }
    // The position with fewer identifiers is considered smaller
    return identifiers.size() < other.identifiers.size();
  }

  bool operator==(const PositionID &other) const { return identifiers == other.identifiers; }
};

// Implement std::hash for PositionID
namespace std {
template <> struct hash<PositionID> {
  size_t operator()(const PositionID &pid) const {
    size_t h = 0;
    for (const auto &id : pid.identifiers) {
      h ^= std::hash<uint32_t>{}(id.digit) + 0x9e3779b9 + (h << 6) + (h >> 2);
      h ^= std::hash<CrdtNodeId>{}(id.site_id) + 0x9e3779b9 + (h << 6) + (h >> 2);
      h ^= std::hash<uint64_t>{}(id.clock) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
  }
};
} // namespace std

/// Represents an element in the list.
template <typename V> struct ListElement {
  PositionID id;
  V value;
  bool deleted;
  uint64_t eid;       // Unique Element ID
  uint64_t timestamp; // Logical timestamp

  ListElement(PositionID pid, V val, uint64_t eid_, uint64_t timestamp_)
      : id(std::move(pid)), value(std::move(val)), deleted(false), eid(eid_), timestamp(timestamp_) {}

  // For deletions or moves
  ListElement(PositionID pid, uint64_t eid_, uint64_t timestamp_, bool is_deleted)
      : id(std::move(pid)), deleted(is_deleted), eid(eid_), timestamp(timestamp_) {}
};

/// CRDT for ordered list of data.
template <typename V> class ListCRDT {
public:
  // Constructor
  ListCRDT(CrdtNodeId node_id) : node_id_(node_id), clock_() {}

  /// Inserts or moves a value using its EID.
  void insert_or_move(uint64_t eid, const std::optional<PositionID> &pos_left, const std::optional<PositionID> &pos_right,
                      const V &value);

  /// Deletes an element by its EID.
  void erase(uint64_t eid);

  /// Merges changes from another ListCRDT.
  void merge(const std::vector<ListElement<V>> &remote_elements);

  /// Retrieves the list of elements in order.
  std::vector<V> get_elements() const;

  /// Retrieves all elements including their PositionIDs.
  std::vector<ListElement<V>> get_all_elements() const;

  /// Prints the current elements for debugging purposes.
  void print_elements() const;

  /// Generates a unique EID.
  uint64_t generate_eid();

  /// Access to elements_by_eid_ for testing purposes
  const CrdtMap<uint64_t, ListElement<V>> &get_elements_by_eid() const { return elements_by_eid_; }

  void insert_line(uint64_t eid, size_t line_number, const V &value);

  void delete_line(size_t line_number);

  void initialize_from_file(const std::string &file_path);

  void save_to_file(const std::string &file_path) const;

private:
  CrdtNodeId node_id_;
  LogicalClock clock_;

  // Map from PositionID to EID
  std::map<PositionID, uint64_t> position_to_eid_;

  // Map from EID to ListElement
  CrdtMap<uint64_t, ListElement<V>> elements_by_eid_;

  // Helper function to generate a PositionID between two PositionIDs
  PositionID generate_position_between(const std::optional<PositionID> &pos_left, const std::optional<PositionID> &pos_right);

  uint64_t next_eid_ = 1;

  std::vector<PositionID> get_positions() const {
    std::vector<PositionID> positions;
    for (const auto &pair : position_to_eid_) {
      positions.push_back(pair.first);
    }
    return positions;
  }

  std::vector<uint64_t> get_eids_in_order() const {
    std::vector<uint64_t> eids;
    for (const auto &pair : position_to_eid_) {
      eids.push_back(pair.second);
    }
    return eids;
  }
};

template <typename V>
void ListCRDT<V>::insert_or_move(uint64_t eid, const std::optional<PositionID> &pos_left,
                                 const std::optional<PositionID> &pos_right, const V &value) {
  uint64_t timestamp = clock_.tick();
  PositionID new_pos = generate_position_between(pos_left, pos_right);

  auto it = elements_by_eid_.find(eid);
  if (it != elements_by_eid_.end()) {
    // Element exists; update its position and value
    auto &elem = it->second;
    position_to_eid_.erase(elem.id); // Remove old position
    elem.id = new_pos;
    elem.value = value;
    elem.deleted = false;
    elem.timestamp = timestamp;
    position_to_eid_[new_pos] = eid;
  } else {
    // New element
    ListElement<V> elem(new_pos, value, eid, timestamp);
    elements_by_eid_.insert({eid, elem});
    position_to_eid_[new_pos] = eid;
  }
}

template <typename V> void ListCRDT<V>::erase(uint64_t eid) {
  uint64_t timestamp = clock_.tick();
  auto it = elements_by_eid_.find(eid);
  if (it != elements_by_eid_.end()) {
    auto &elem = it->second;
    elem.deleted = true;
    elem.timestamp = timestamp;
    position_to_eid_.erase(elem.id);
  }
}

template <typename V> void ListCRDT<V>::merge(const std::vector<ListElement<V>> &remote_elements) {
  for (const auto &remote_elem : remote_elements) {
    uint64_t eid = remote_elem.eid;
    clock_.update(remote_elem.timestamp);
    auto it = elements_by_eid_.find(eid);
    if (it != elements_by_eid_.end()) {
      auto &local_elem = it->second;
      if (remote_elem.timestamp > local_elem.timestamp) {
        // Accept remote change
        position_to_eid_.erase(local_elem.id);
        local_elem = remote_elem;
        if (!local_elem.deleted) {
          position_to_eid_[local_elem.id] = eid;
        }
      } else if (remote_elem.timestamp == local_elem.timestamp) {
        // Use site_id to break tie
        if (remote_elem.id.identifiers < local_elem.id.identifiers) {
          // Accept remote change
          position_to_eid_.erase(local_elem.id);
          local_elem = remote_elem;
          if (!local_elem.deleted) {
            position_to_eid_[local_elem.id] = eid;
          }
        }
      }
      // Else, keep local change
    } else {
      // New element
      elements_by_eid_.insert({eid, remote_elem});
      if (!remote_elem.deleted) {
        position_to_eid_[remote_elem.id] = eid;
      }
    }
  }
}

template <typename V> std::vector<V> ListCRDT<V>::get_elements() const {
  std::vector<V> result;
  for (const auto &pair : position_to_eid_) {
    const auto &elem = elements_by_eid_.at(pair.second);
    if (!elem.deleted) {
      result.push_back(elem.value);
    }
  }
  return result;
}

template <typename V> std::vector<ListElement<V>> ListCRDT<V>::get_all_elements() const {
  std::vector<ListElement<V>> result;
  for (const auto &pair : elements_by_eid_) {
    result.push_back(pair.second);
  }
  return result;
}

template <typename V> void ListCRDT<V>::print_elements() const {
  std::cout << "ListCRDT elements:" << std::endl;
  for (const auto &pair : position_to_eid_) {
    const auto &elem = elements_by_eid_.at(pair.second);
    if (!elem.deleted) {
      std::cout << elem.value << std::endl;
    }
  }
}

template <typename V>
PositionID ListCRDT<V>::generate_position_between(const std::optional<PositionID> &pos_left,
                                                  const std::optional<PositionID> &pos_right) {
  CrdtVector<Identifier> new_identifiers;

  const CrdtVector<Identifier> &left_ids = pos_left ? pos_left->identifiers : CrdtVector<Identifier>{Identifier(0, 0, 0)};
  const CrdtVector<Identifier> &right_ids = pos_right ? pos_right->identifiers : CrdtVector<Identifier>{Identifier(BASE, 0, 0)};

  size_t level = 0;
  while (true) {
    uint32_t left_digit = (left_ids.size() > level) ? left_ids[level].digit : 0;
    uint32_t right_digit = (right_ids.size() > level) ? right_ids[level].digit : BASE;

    // Build new_identifiers as left_ids[0..level]
    new_identifiers.assign(left_ids.begin(), left_ids.begin() + level);

    if (right_digit - left_digit > 1) {
      // There is space to allocate a new digit
      uint32_t new_digit = left_digit + ((right_digit - left_digit) / 2);
      uint64_t clock = clock_.current_time();
      new_identifiers.emplace_back(new_digit, node_id_, clock);
      break;
    } else if (left_digit == right_digit) {
      // Digits are equal, need to proceed to the next level
      if (left_ids.size() > level) {
        new_identifiers.push_back(left_ids[level]);
      } else if (right_ids.size() > level) {
        new_identifiers.push_back(right_ids[level]);
      } else {
        // Both left and right identifiers have no more digits, but digits are equal
        uint64_t clock = clock_.current_time();
        new_identifiers.emplace_back(left_digit, node_id_, clock);
        break; // We can break here since we've extended the identifier uniquely
      }
      level++;
    } else {
      // No space between left and right, need to extend identifiers
      uint64_t clock = clock_.current_time();
      new_identifiers.emplace_back(left_digit, node_id_, clock);
      level++;
    }

    // Safety check to prevent infinite loops
    if (level > MAX_COMPONENT_LEVEL) {
      throw std::runtime_error("Exceeded maximum component level in generate_position_between");
    }
  }

  return PositionID(new_identifiers);
}

template <typename V> uint64_t ListCRDT<V>::generate_eid() { return (node_id_ << 32) | next_eid_++; }

/// Overload operator<< for debugging purposes
#include <ostream>

inline std::ostream &operator<<(std::ostream &os, const Identifier &id) {
  os << "(" << id.digit << "," << id.site_id << "," << id.clock << ")";
  return os;
}

inline std::ostream &operator<<(std::ostream &os, const PositionID &pid) {
  os << "[";
  for (size_t i = 0; i < pid.identifiers.size(); ++i) {
    os << pid.identifiers[i];
    if (i + 1 < pid.identifiers.size())
      os << ",";
  }
  os << "]";
  return os;
}

template <typename V> void ListCRDT<V>::insert_line(uint64_t eid, size_t line_number, const V &value) {
  auto positions = get_positions();
  std::optional<PositionID> pos_left = std::nullopt;
  std::optional<PositionID> pos_right = std::nullopt;

  if (line_number == 0) {
    // Insert at the beginning
    if (!positions.empty()) {
      pos_right = positions[0];
    }
  } else if (line_number >= positions.size()) {
    // Insert at the end
    if (!positions.empty()) {
      pos_left = positions.back();
    }
  } else {
    // Insert between existing lines
    pos_left = positions[line_number - 1];
    pos_right = positions[line_number];
  }

  insert_or_move(eid, pos_left, pos_right, value);
}

template <typename V> void ListCRDT<V>::initialize_from_file(const std::string &file_path) {
  std::ifstream infile(file_path);
  if (!infile.is_open()) {
    throw std::runtime_error("Unable to open file: " + file_path);
  }

  std::optional<PositionID> prev_pos = std::nullopt;
  std::string line;
  while (std::getline(infile, line)) {
    uint64_t eid = generate_eid();
    insert_or_move(eid, prev_pos, std::nullopt, line);
    prev_pos = elements_by_eid_.at(eid).id;
  }
  infile.close();
}

template <typename V> void ListCRDT<V>::save_to_file(const std::string &file_path) const {
  std::ofstream outfile(file_path);
  if (!outfile.is_open()) {
    throw std::runtime_error("Unable to open file: " + file_path);
  }

  for (const auto &value : get_elements()) {
    outfile << value << "\n";
  }
  outfile.close();
}

template <typename V> void ListCRDT<V>::delete_line(size_t line_number) {
  auto eids = get_eids_in_order();
  if (line_number >= eids.size()) {
    throw std::out_of_range("Line number out of range");
  }
  uint64_t eid = eids[line_number];
  erase(eid);
}

#endif // LIST_CRDT_HPP
