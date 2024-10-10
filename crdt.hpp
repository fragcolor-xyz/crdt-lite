// crdt.hpp
#ifndef CRDT_HPP
#define CRDT_HPP

// Define this if you want to override the default collection types
// Basically define these before including this header and ensure this define is set before this header is included
// in any other files that include this file
#ifndef CRDT_COLLECTIONS_DEFINED
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <vector>

template <typename T> using CrdtVector = std::vector<T>;
using CrdtString = std::string;
template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using CrdtMap = std::unordered_map<K, V, Hash, KeyEqual>;
template <typename K, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using CrdtSet = std::unordered_set<K, Hash, KeyEqual>;
template <typename T, typename Comparator> using CrdtSortedSet = std::set<T, Comparator>;
using CrdtNodeId = uint64_t;
#endif

#include <algorithm>
#include <iostream>
#include <optional>
#include <memory>
#include <type_traits>
#include <concepts>

/// Represents a single change in the CRDT.
template <typename K, typename V> struct Change {
  K record_id;
  std::optional<CrdtString> col_name; // std::nullopt represents tombstone of the record
  std::optional<V> value;             // note std::nullopt represents deletion of the column, not the record
  uint64_t col_version;
  uint64_t db_version;
  CrdtNodeId node_id;

  // this field is useful only locally when doing things like get_changes_since
  // we record the local db_version when the change was created
  uint64_t local_db_version;

  Change() = default;

  Change(K rid, std::optional<CrdtString> cname, std::optional<V> val, uint64_t cver, uint64_t dver, CrdtNodeId nid,
         uint64_t ldb_ver = 0)
      : record_id(std::move(rid)), col_name(std::move(cname)), value(std::move(val)), col_version(cver), db_version(dver),
        node_id(nid), local_db_version(ldb_ver) {}
};

// Define a concept for a custom merge rule
template <typename Rule, typename K, typename V>
concept MergeRule = requires(Rule r, const Change<K, V> &local, const Change<K, V> &remote) {
  { r(local, remote) } -> std::convertible_to<bool>;
};

// Default merge rule (current behavior)
template <typename K, typename V> struct DefaultMergeRule {
  constexpr bool operator()(const Change<K, V> &local, const Change<K, V> &remote) const {
    if (remote.col_version > local.col_version) {
      return true;
    } else if (remote.col_version < local.col_version) {
      return false;
    } else {
      if (remote.db_version > local.db_version) {
        return true;
      } else if (remote.db_version < local.db_version) {
        return false;
      } else {
        return (remote.node_id > local.node_id);
      }
    }
  }
};

// Define a concept for a custom change comparator
template <typename Comparator, typename K, typename V>
concept ChangeComparator = requires(Comparator c, const Change<K, V> &a, const Change<K, V> &b) {
  { c(a, b) } -> std::convertible_to<bool>;
};

// Default change comparator (current behavior)
template <typename K, typename V> struct DefaultChangeComparator {
  constexpr bool operator()(const Change<K, V> &a, const Change<K, V> &b) const {
    if (a.record_id != b.record_id)
      return a.record_id < b.record_id;
    if (a.col_name.has_value() != b.col_name.has_value())
      return b.col_name.has_value(); // Deletions (nullopt) come last for each record
    if (a.col_name != b.col_name)
      return a.col_name < b.col_name;
    if (a.col_version != b.col_version)
      return a.col_version > b.col_version;
    if (a.db_version != b.db_version)
      return a.db_version > b.db_version;
    if (a.node_id != b.node_id)
      return a.node_id > b.node_id;
    return false; // Consider equal if all fields match
  }
};

/// Represents a default sort function.
struct DefaultSort {
  template <typename Iterator, typename Comparator>
  constexpr void operator()(Iterator begin, Iterator end, Comparator comp) const {
    std::sort(begin, end, comp);
  }
};

/// Represents a logical clock for maintaining causality.
class LogicalClock {
public:
  LogicalClock() : time_(0) {}

  /// Increments the clock for a local event.
  constexpr uint64_t tick() { return ++time_; }

  /// Updates the clock based on a received time.
  constexpr uint64_t update(uint64_t received_time) {
    time_ = std::max(time_, received_time);
    return ++time_;
  }

  /// Sets the logical clock to a specific time.
  constexpr void set_time(uint64_t t) { time_ = t; }

  /// Retrieves the current time.
  constexpr uint64_t current_time() const { return time_; }

private:
  uint64_t time_;
};

/// Represents the version information for a column.
struct ColumnVersion {
  uint64_t col_version;
  uint64_t db_version;
  CrdtNodeId node_id;

  // this field is useful only locally when doing things like get_changes_since
  // we record the local db_version when the change was created
  uint64_t local_db_version;

  constexpr ColumnVersion(uint64_t c, uint64_t d, CrdtNodeId n, uint64_t ldb_ver = 0)
      : col_version(c), db_version(d), node_id(n), local_db_version(ldb_ver) {}
};

/// Represents a record in the CRDT.
template <typename V> struct Record {
  CrdtMap<CrdtString, V> fields;
  CrdtMap<CrdtString, ColumnVersion> column_versions;

  Record() = default;

  constexpr Record(CrdtMap<CrdtString, V> &&f, CrdtMap<CrdtString, ColumnVersion> &&cv)
      : fields(std::move(f)), column_versions(std::move(cv)) {}
};

// Free function to compare two Record<V> instances
template <typename V> constexpr bool operator==(const Record<V> &lhs, const Record<V> &rhs) {
  // Compare fields
  if (lhs.fields.size() != rhs.fields.size())
    return false;
  for (const auto &[key, value] : lhs.fields) {
    auto it = rhs.fields.find(key);
    if (it == rhs.fields.end() || it->second != value)
      return false;
  }

  // We don't care about column_versions, as those will be different for each node

  return true;
}

/// Represents the CRDT structure, generic over key (`K`) and value (`V`) types.
template <typename K, typename V, MergeRule<K, V> MergeRuleType = DefaultMergeRule<K, V>,
          ChangeComparator<K, V> ChangeComparatorType = DefaultChangeComparator<K, V>, typename SortFunctionType = DefaultSort>
class CRDT : public std::enable_shared_from_this<CRDT<K, V, MergeRuleType, ChangeComparatorType, SortFunctionType>> {
public:
  // Create a new empty CRDT
  // Complexity: O(1)
  CRDT(CrdtNodeId node_id, std::shared_ptr<CRDT<K, V, MergeRuleType, ChangeComparatorType, SortFunctionType>> parent = nullptr,
       MergeRuleType merge_rule = MergeRuleType(), ChangeComparatorType change_comparator = ChangeComparatorType(),
       SortFunctionType sort_func = SortFunctionType())
      : node_id_(node_id), clock_(), data_(), tombstones_(), parent_(parent), merge_rule_(std::move(merge_rule)),
        change_comparator_(std::move(change_comparator)), sort_func_(std::move(sort_func)) {
    if (parent_) {
      // Set clock to parent's clock
      clock_ = parent_->clock_;
      // Capture the base version from the parent
      base_version_ = parent_->clock_.current_time();
    } else {
      base_version_ = 0; // No parent, base_version_ is 0
    }
  }

  /// Create a CRDT from a list of changes (e.g., loaded from disk).
  ///
  /// # Arguments
  ///
  /// * `node_id` - The unique identifier for this CRDT node.
  /// * `changes` - A list of changes to apply to reconstruct the CRDT state.
  ///
  /// Complexity: O(n), where n is the number of changes
  constexpr CRDT(CrdtNodeId node_id, CrdtVector<Change<K, V>> &&changes) : node_id_(node_id), clock_(), data_(), tombstones_() {
    apply_changes(std::move(changes));
  }

  /// Resets the CRDT to a state as if it was constructed with the given changes.
  ///
  /// # Arguments
  ///
  /// * `changes` - A list of changes to apply to reconstruct the CRDT state.
  ///
  /// Complexity: O(n), where n is the number of changes
  constexpr void reset(CrdtVector<Change<K, V>> &&changes) {
    // Clear existing data
    data_.clear();
    tombstones_.clear();

    // Reset the logical clock
    clock_ = LogicalClock();

    apply_changes(std::move(changes));
  }

  /// Reverts all changes made by this CRDT since it was created from the parent.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the inverse changes needed to undo the child's changes.
  ///
  /// # Complexity
  ///
  /// O(c), where c is the number of changes since `base_version_`
  constexpr CrdtVector<Change<K, V>> revert() {
    if (!parent_) {
      throw std::runtime_error("Cannot revert without a parent CRDT.");
    }

    // Step 1: Retrieve all changes made by the child since base_version_
    CrdtVector<Change<K, V>> child_changes = this->get_changes_since(base_version_);

    // Step 2: Generate inverse changes using the parent CRDT
    return invert_changes(child_changes, *parent_);
  }

  /// Computes the difference between this CRDT and another CRDT.
  ///
  /// # Arguments
  ///
  /// * `other` - The CRDT to compare against.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the changes needed to transform this CRDT into the other CRDT.
  ///
  /// # Complexity
  ///
  /// O(c), where c is the number of changes since the common ancestor
  constexpr CrdtVector<Change<K, V>> diff(const CRDT<K, V, MergeRuleType, ChangeComparatorType, SortFunctionType> &other) const {
    // Find the common ancestor (lowest common db_version)
    uint64_t common_version = std::min(clock_.current_time(), other.clock_.current_time());

    // Get changes from this CRDT since the common ancestor
    CrdtVector<Change<K, V>> this_changes = this->get_changes_since(common_version);

    // Get changes from the other CRDT since the common ancestor
    CrdtVector<Change<K, V>> other_changes = other.get_changes_since(common_version);

    // Invert the changes from this CRDT
    CrdtVector<Change<K, V>> inverted_this_changes = invert_changes(this_changes, other);

    // Combine the inverted changes from this CRDT with the changes from the other CRDT
    CrdtVector<Change<K, V>> diff_changes;
    diff_changes.reserve(inverted_this_changes.size() + other_changes.size());
    diff_changes.insert(diff_changes.end(), inverted_this_changes.begin(), inverted_this_changes.end());
    diff_changes.insert(diff_changes.end(), other_changes.begin(), other_changes.end());

    // Compress the changes to remove redundant operations
    compress_changes(diff_changes);

    return diff_changes;
  }

  /// Inserts a new record or updates an existing record in the CRDT.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `fields` - A hashmap of field names to their values.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the changes made, or void if ReturnChanges is false.
  ///
  /// Complexity: O(m), where m is the number of fields in the input
  template <bool ReturnChanges = true>
  constexpr std::conditional_t<ReturnChanges, CrdtVector<Change<K, V>>, void> insert_or_update(const K &record_id,
                                                                                               CrdtMap<CrdtString, V> &&fields) {
    CrdtVector<Change<K, V>> changes;
    uint64_t db_version = clock_.tick();

    // Check if the record is tombstoned
    if (is_record_tombstoned(record_id)) {
      if constexpr (ReturnChanges) {
        return changes;
      } else {
        return;
      }
    }

    Record<V> &record = get_or_create_record_unchecked(record_id);

    for (auto &[col_name, value] : fields) {
      uint64_t col_version;
      auto col_it = record.column_versions.find(col_name);
      if (col_it != record.column_versions.end()) {
        col_version = ++col_it->second.col_version;
        col_it->second.db_version = db_version;
        col_it->second.node_id = node_id_;
        col_it->second.local_db_version = db_version;
      } else {
        col_version = 1;
        record.column_versions.emplace(col_name, ColumnVersion(col_version, db_version, node_id_, db_version));
      }

      if constexpr (ReturnChanges) {
        record.fields[col_name] = value;
        changes.emplace_back(
            Change<K, V>(record_id, std::move(col_name), std::move(value), col_version, db_version, node_id_, db_version));
      } else {
        record.fields[std::move(col_name)] = std::move(value);
      }
    }

    if constexpr (ReturnChanges) {
      return changes;
    }
  }

  /// Deletes a record by marking it as tombstoned.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the deletion, or void if ReturnChanges is false.
  ///
  /// Complexity: O(1)
  template <bool ReturnChanges = true>
  std::conditional_t<ReturnChanges, CrdtVector<Change<K, V>>, void> delete_record(const K &record_id) {
    CrdtVector<Change<K, V>> changes;
    if (is_record_tombstoned(record_id)) {
      if constexpr (ReturnChanges) {
        return changes;
      } else {
        return;
      }
    }

    uint64_t db_version = clock_.tick();

    // Mark as tombstone and remove data
    tombstones_.emplace(record_id);
    data_.erase(record_id);

    // Insert deletion clock info
    CrdtMap<CrdtString, ColumnVersion> deletion_clock;
    deletion_clock.emplace("__deleted__", ColumnVersion(1, db_version, node_id_, db_version));

    // Store deletion info in the data map
    data_.emplace(record_id, Record<V>(CrdtMap<CrdtString, V>(), std::move(deletion_clock)));

    if constexpr (ReturnChanges) {
      changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt, 1, db_version, node_id_, db_version));
      return changes;
    }
  }

  /// Retrieves all changes since a given `last_db_version`.
  ///
  /// # Arguments
  ///
  /// * `last_db_version` - The database version to retrieve changes since.
  ///
  /// # Returns
  ///
  /// A vector of changes.
  ///
  /// Complexity: O(n * m), where n is the number of records and m is the average number of columns per record
  CrdtVector<Change<K, V>> get_changes_since(uint64_t last_db_version, CrdtSet<CrdtNodeId> excluding = {}) const {
    CrdtVector<Change<K, V>> changes;

    // Get changes from parent
    if (parent_) {
      auto parent_changes = parent_->get_changes_since(last_db_version);
      changes.insert(changes.end(), parent_changes.begin(), parent_changes.end());
    }

    for (const auto &[record_id, record] : data_) {
      for (const auto &[col_name, clock_info] : record.column_versions) {
        if (clock_info.local_db_version > last_db_version && !excluding.contains(clock_info.node_id)) {
          std::optional<V> value = std::nullopt;
          std::optional<CrdtString> name = std::nullopt;
          if (col_name != "__deleted__") {
            auto field_it = record.fields.find(col_name);
            if (field_it != record.fields.end()) {
              value = field_it->second;
            }
            name = col_name;
          }
          changes.emplace_back(Change<K, V>(record_id, std::move(name), std::move(value), clock_info.col_version,
                                            clock_info.db_version, clock_info.node_id, clock_info.local_db_version));
        }
      }
    }

    if (parent_) {
      // Since we merge from the parent, we need to also run a compression pass
      // to remove changes that have been overwritten by top level changes
      // since we compare at first by col_version, it's fine even if our db_version is lower
      // since we merge from the parent, we know that the changes are applied in order and col_version should always be increasing
      compress_changes(changes);
    }

    return changes;
  }

  /// Merges a set of incoming changes into the CRDT.
  ///
  /// # Arguments
  ///
  /// * `changes` - A vector of changes to merge.
  ///
  /// # Returns
  ///
  /// If `ReturnAcceptedChanges` is `true`, returns a vector of accepted changes.
  /// Otherwise, returns `void`.
  ///
  /// Complexity: O(c), where c is the number of changes to merge
  template <bool ReturnAcceptedChanges = false>
  std::conditional_t<ReturnAcceptedChanges, CrdtVector<Change<K, V>>, void> merge_changes(CrdtVector<Change<K, V>> &&changes,
                                                                                          bool ignore_parent = false) {
    CrdtVector<Change<K, V>> accepted_changes;

    if (changes.empty()) {
      if constexpr (ReturnAcceptedChanges) {
        return accepted_changes;
      } else {
        return;
      }
    }

    for (auto &&change : changes) {
      const K &record_id = change.record_id;
      std::optional<CrdtString> col_name = std::move(change.col_name);
      uint64_t remote_col_version = change.col_version;
      uint64_t remote_db_version = change.db_version;
      CrdtNodeId remote_node_id = change.node_id;
      std::optional<V> remote_value = std::move(change.value);

      // Always update the logical clock to maintain causal consistency,
      // prevent clock drift, and ensure accurate conflict resolution.
      // This reflects the node's knowledge of global progress, even for
      // non-accepted changes.
      uint64_t new_local_db_version = clock_.update(remote_db_version);

      // Retrieve local column version information
      const Record<V> *record_ptr = get_record_ptr(record_id, ignore_parent);
      const ColumnVersion *local_col_info = nullptr;
      if (record_ptr != nullptr) {
        auto col_it = record_ptr->column_versions.find(col_name ? *col_name : "__deleted__");
        if (col_it != record_ptr->column_versions.end()) {
          local_col_info = &col_it->second;
        }
      }

      // Determine whether to accept the remote change
      bool should_accept = false;

      if (local_col_info == nullptr) {
        // No local version exists; accept the remote change
        should_accept = true;
      } else {
        Change<K, V> local_change(record_id, col_name ? *col_name : "__deleted__", std::nullopt, local_col_info->col_version,
                                  local_col_info->db_version, local_col_info->node_id);
        should_accept = merge_rule_(local_change, change);
      }

      if (should_accept) {
        if (!col_name) {
          // Handle deletion
          tombstones_.emplace(record_id);
          data_.erase(record_id);

          // Update deletion clock info
          CrdtMap<CrdtString, ColumnVersion> deletion_clock;
          deletion_clock.emplace("__deleted__",
                                 ColumnVersion(remote_col_version, remote_db_version, remote_node_id, new_local_db_version));

          // Store deletion info in the data map
          data_.emplace(record_id, Record<V>(CrdtMap<CrdtString, V>(), std::move(deletion_clock)));

          if constexpr (ReturnAcceptedChanges) {
            accepted_changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt, remote_col_version,
                                                       remote_db_version, remote_node_id, new_local_db_version));
          }
        } else if (!is_record_tombstoned(record_id, ignore_parent)) {
          // Handle insertion or update
          Record<V> &record = get_or_create_record_unchecked(record_id, ignore_parent);

          // Update field value
          if (remote_value.has_value()) {
            if constexpr (ReturnAcceptedChanges) {
              record.fields[*col_name] = *remote_value;
            } else {
              record.fields[*col_name] = std::move(*remote_value);
            }
          } else {
            // If remote_value is std::nullopt, remove the field
            record.fields.erase(*col_name);
          }

          // Update the column version info
          if constexpr (ReturnAcceptedChanges) {
            record.column_versions.insert_or_assign(
                *col_name, ColumnVersion(remote_col_version, remote_db_version, remote_node_id, new_local_db_version));
            accepted_changes.emplace_back(Change<K, V>(record_id, std::move(col_name), std::move(remote_value),
                                                       remote_col_version, remote_db_version, remote_node_id,
                                                       new_local_db_version));
          } else {
            record.column_versions.insert_or_assign(
                std::move(*col_name), ColumnVersion(remote_col_version, remote_db_version, remote_node_id, new_local_db_version));
          }
        }
      }
    }

    if constexpr (ReturnAcceptedChanges) {
      return accepted_changes;
    }
  }

  /// Compresses a vector of changes in-place by removing redundant changes that overwrite each other.
  ///
  /// # Arguments
  ///
  /// * `changes` - A vector of changes to compress (will be modified in-place).
  ///
  /// Complexity: O(n log n), where n is the number of changes
  template <bool Sorted = false> static void compress_changes(CrdtVector<Change<K, V>> &changes) {
    if (changes.empty())
      return;

    auto new_end = compress_changes<Sorted>(changes.begin(), changes.end());
    changes.erase(new_end, changes.end());
  }

  /// Compresses a range of changes by removing redundant changes that overwrite each other.
  ///
  /// # Arguments
  ///
  /// * `begin` - Iterator to the beginning of the range.
  /// * `end` - Iterator to the end of the range.
  ///
  /// # Returns
  ///
  /// Iterator to the new end of the range after compression.
  ///
  /// Complexity: O(n log n), where n is the number of changes
  template <bool Sorted = false, typename Iterator> static Iterator compress_changes(Iterator begin, Iterator end) {
    if (begin == end)
      return end;

    if constexpr (!Sorted) {
      // Sort changes using the custom ChangeComparator
      SortFunctionType()(begin, end, ChangeComparatorType());
    }

    // Use two-pointer technique to compress in-place
    Iterator write = begin;
    for (Iterator read = std::next(begin); read != end; ++read) {
      if (read->record_id != write->record_id) {
        // New record, always keep it
        ++write;
        if (write != read) {
          *write = std::move(*read);
        }
      } else if (!read->col_name.has_value() && write->col_name.has_value()) {
        // Current read is a deletion, keep it and skip all previous changes for this record
        *write = std::move(*read);
      } else if (read->col_name != write->col_name) {
        // New column for the same record
        ++write;
        if (write != read) {
          *write = std::move(*read);
        }
      }
      // Else: same record and column, keep the existing one (which is the most recent due to sorting)
    }

    return std::next(write);
  }

/// Prints the current data and tombstones for debugging purposes.
///
/// Complexity: O(n * m), where n is the number of records and m is the average number of fields per record
#ifndef NDEBUG
  constexpr void print_data() const {
    std::cout << "Node " << node_id_ << " Data:" << std::endl;
    for (const auto &[record_id, record] : data_) {
      if (tombstones_.find(record_id) != tombstones_.end()) {
        continue; // Skip tombstoned records
      }
      std::cout << "ID: ";
      print_value(record_id);
      std::cout << std::endl;
      for (const auto &[key, value] : record.fields) {
        std::cout << "  ";
        print_value(key);
        std::cout << ": ";
        print_value(value);
        std::cout << std::endl;
      }
    }
    std::cout << "Tombstones: ";
    for (const auto &tid : tombstones_) {
      print_value(tid);
      std::cout << " ";
    }
    std::cout << std::endl << std::endl;
  }
#else
  constexpr void print_data() const {}
#endif

  // Accessors for testing
  // Complexity: O(1)
  constexpr const LogicalClock &get_clock() const { return clock_; }

  // Updated get_data() method
  constexpr CrdtMap<K, Record<V>> get_data() const {
    if (!parent_) {
      return data_;
    }

    CrdtMap<K, Record<V>> combined_data = parent_->get_data();
    for (const auto &[key, record] : data_) {
      combined_data[key] = record;
    }
    return combined_data;
  }

  /// Retrieves a pointer to a record if it exists, or nullptr if it doesn't.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `ignore_parent` - If true, only checks the current CRDT instance, ignoring the parent.
  ///
  /// # Returns
  ///
  /// A pointer to the Record<V> if found, or nullptr if not found.
  ///
  /// Complexity: O(1) average case for hash table lookup
  constexpr Record<V> *get_record(const K &record_id, bool ignore_parent = false) {
    return get_record_ptr(record_id, ignore_parent);
  }

  constexpr const Record<V> *get_record(const K &record_id, bool ignore_parent = false) const {
    return get_record_ptr(record_id, ignore_parent);
  }

  // Add this public method to the CRDT class
  /// Checks if a record is tombstoned.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `ignore_parent` - If true, only checks the current CRDT instance, ignoring the parent.
  ///
  /// # Returns
  ///
  /// True if the record is tombstoned, false otherwise.
  ///
  /// Complexity: O(1) average case for hash table lookup
  constexpr bool is_tombstoned(const K &record_id, bool ignore_parent = false) const {
    return is_record_tombstoned(record_id, ignore_parent);
  }

  // Add this constructor to the CRDT class
  CRDT(const CRDT &other)
      : node_id_(other.node_id_), clock_(other.clock_), data_(other.data_), tombstones_(other.tombstones_),
        parent_(other.parent_), base_version_(other.base_version_), merge_rule_(other.merge_rule_),
        change_comparator_(other.change_comparator_), sort_func_(other.sort_func_) {
    // Note: This creates a shallow copy of the parent pointer
  }

  CRDT &operator=(const CRDT &other) {
    if (this != &other) {
      node_id_ = other.node_id_;
      clock_ = other.clock_;
      data_ = other.data_;
      tombstones_ = other.tombstones_;
      parent_ = other.parent_;
      base_version_ = other.base_version_;
      merge_rule_ = other.merge_rule_;
      change_comparator_ = other.change_comparator_;
      sort_func_ = other.sort_func_;
    }
    return *this;
  }

private:
  CrdtNodeId node_id_;
  LogicalClock clock_;
  CrdtMap<K, Record<V>> data_;
  CrdtSet<K> tombstones_;

  // our clock won't be shared with the parent
  // we optionally allow to merge from the parent or push to the parent
  std::shared_ptr<CRDT<K, V, MergeRuleType, ChangeComparatorType, SortFunctionType>> parent_;
  uint64_t base_version_; // Tracks the parent's db_version at the time of child creation
  MergeRuleType merge_rule_;
  ChangeComparatorType change_comparator_;
  SortFunctionType sort_func_;

  // Helper function to print values
  template <typename T> static void print_value(const T &value) {
    if constexpr (std::is_same_v<T, std::string> || std::is_arithmetic_v<T>) {
      std::cout << value;
    } else {
      std::cout << "[non-printable]";
    }
  }

  /// Applies a list of changes to reconstruct the CRDT state.
  ///
  /// # Arguments
  ///
  /// * `changes` - A list of changes to apply.
  ///
  /// Complexity: O(n), where n is the number of changes
  void apply_changes(CrdtVector<Change<K, V>> &&changes) {
    // Determine the maximum db_version from the changes
    uint64_t max_db_version = 0;
    for (const auto &change : changes) {
      if (change.db_version > max_db_version) {
        max_db_version = change.db_version;
      }
    }

    // Set the logical clock to the maximum db_version
    clock_.set_time(max_db_version);

    // Apply each change to reconstruct the CRDT state
    for (auto &&change : changes) {
      const K &record_id = change.record_id;
      std::optional<CrdtString> col_name = std::move(change.col_name);
      uint64_t remote_col_version = change.col_version;
      uint64_t remote_db_version = change.db_version;
      CrdtNodeId remote_node_id = change.node_id;
      uint64_t remote_local_db_version = change.local_db_version;
      std::optional<V> remote_value = std::move(change.value);

      if (!col_name.has_value()) {
        // Handle deletion
        tombstones_.emplace(record_id);
        data_.erase(record_id);

        // Insert deletion clock info
        CrdtMap<CrdtString, ColumnVersion> deletion_clock;
        deletion_clock.emplace("__deleted__",
                               ColumnVersion(remote_col_version, remote_db_version, remote_node_id, remote_local_db_version));

        // Store deletion info in the data map
        data_.emplace(record_id, Record<V>(CrdtMap<CrdtString, V>(), std::move(deletion_clock)));
      } else {
        if (!is_record_tombstoned(record_id)) {
          // Handle insertion or update
          Record<V> &record = get_or_create_record_unchecked(record_id);

          // Insert or update the field value
          if (remote_value.has_value()) {
            record.fields[*col_name] = std::move(remote_value.value());
          }

          // Update the column version info
          record.column_versions.insert_or_assign(std::move(*col_name), ColumnVersion(remote_col_version, remote_db_version,
                                                                                      remote_node_id, remote_local_db_version));
        }
      }
    }
  }

  constexpr bool is_record_tombstoned(const K &record_id, bool ignore_parent = false) const {
    if (tombstones_.find(record_id) != tombstones_.end()) {
      return true;
    }
    if (parent_ && !ignore_parent) {
      return parent_->is_record_tombstoned(record_id);
    }
    return false;
  }

  // Notice that this will not check if the record is tombstoned! Such check should be done by the caller
  constexpr Record<V> &get_or_create_record_unchecked(const K &record_id, bool ignore_parent = false) {
    auto [it, inserted] = data_.try_emplace(record_id);
    if (inserted && parent_ && !ignore_parent) {
      if (auto parent_record = parent_->get_record_ptr(record_id)) {
        it->second = *parent_record;
      }
    }
    return it->second;
  }

  constexpr Record<V> *get_record_ptr(const K &record_id, bool ignore_parent = false) {
    auto it = data_.find(record_id);
    if (it != data_.end()) {
      return &(it->second);
    }
    if (ignore_parent) {
      return nullptr;
    } else {
      return parent_ ? parent_->get_record_ptr(record_id) : nullptr;
    }
  }

  constexpr const Record<V> *get_record_ptr(const K &record_id, bool ignore_parent = false) const {
    auto it = data_.find(record_id);
    if (it != data_.end()) {
      return &(it->second);
    }
    if (ignore_parent) {
      return nullptr;
    } else {
      return parent_ ? parent_->get_record_ptr(record_id) : nullptr;
    }
  }

  /// Generates inverse changes for a given set of changes based on a reference CRDT state.
  ///
  /// # Arguments
  ///
  /// * `changes` - A vector of changes to invert.
  /// * `reference_crdt` - A reference CRDT to use as the base state for inversion.
  ///
  /// # Returns
  ///
  /// A vector of inverse `Change` objects.
  CrdtVector<Change<K, V>>
  invert_changes(const CrdtVector<Change<K, V>> &changes,
                 const CRDT<K, V, MergeRuleType, ChangeComparatorType, SortFunctionType> &reference_crdt) const {
    CrdtVector<Change<K, V>> inverse_changes;

    for (const auto &change : changes) {
      const K &record_id = change.record_id;
      const std::optional<CrdtString> &col_name = change.col_name;

      if (!col_name.has_value()) {
        // The change was a record deletion (tombstone)
        // To revert, restore the record's state from the reference CRDT
        auto record_ptr = reference_crdt.get_record(record_id);
        if (record_ptr) {
          // Restore all fields from the record, sorted by db_version
          std::vector<std::pair<CrdtString, V>> sorted_fields(record_ptr->fields.begin(), record_ptr->fields.end());
          std::sort(sorted_fields.begin(), sorted_fields.end(), [&](const auto &a, const auto &b) {
            return record_ptr->column_versions.at(a.first).db_version < record_ptr->column_versions.at(b.first).db_version;
          });
          for (const auto &[ref_col, ref_val] : sorted_fields) {
            inverse_changes.emplace_back(Change<K, V>(record_id, ref_col, ref_val,
                                                      record_ptr->column_versions.at(ref_col).col_version,
                                                      record_ptr->column_versions.at(ref_col).db_version, node_id_,
                                                      record_ptr->column_versions.at(ref_col).local_db_version));
          }
        }
      } else {
        // The change was an insertion or update of a column
        CrdtString col = *col_name;
        auto record_ptr = reference_crdt.get_record(record_id);
        if (record_ptr) {
          auto field_it = record_ptr->fields.find(col);
          if (field_it != record_ptr->fields.end()) {
            // The record has a value for this column in the reference; set it back to the reference's value
            inverse_changes.emplace_back(Change<K, V>(
                record_id, col, field_it->second, record_ptr->column_versions.at(col).col_version,
                record_ptr->column_versions.at(col).db_version, node_id_, record_ptr->column_versions.at(col).local_db_version));
          } else {
            // The record does not have this column in the reference; delete it to revert
            inverse_changes.emplace_back(Change<K, V>(record_id, col,
                                                      std::nullopt, // Indicates deletion
                                                      0,            // Column version 0 signifies deletion
                                                      clock_.current_time(), node_id_));
          }
        } else {
          // The record does not exist in the reference; remove the entire record to revert
          inverse_changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt,
                                                    0, // Column version 0 signifies a tombstone
                                                    clock_.current_time(), node_id_));
        }
      }
    }

    return inverse_changes;
  }
};

/// Synchronizes two CRDT nodes.
/// Retrieves changes from the source since last_db_version and merges them into
/// the target. Updates last_db_version to prevent reprocessing the same
/// changes.
///
/// Complexity: O(c + m), where c is the number of changes since last_db_version,
/// and m is the complexity of merge_changes
template <typename K, typename V, MergeRule<K, V> MergeRuleType = DefaultMergeRule<K, V>,
          ChangeComparator<K, V> ChangeComparatorType = DefaultChangeComparator<K, V>, typename SortFunctionType = DefaultSort>
constexpr void sync_nodes(CRDT<K, V, MergeRuleType, ChangeComparatorType, SortFunctionType> &source,
                          CRDT<K, V, MergeRuleType, ChangeComparatorType, SortFunctionType> &target, uint64_t &last_db_version) {
  auto changes = source.get_changes_since(last_db_version);

  // Update last_db_version to the current max db_version in source
  uint64_t max_version = last_db_version;
  for (const auto &change : changes) {
    if (change.db_version > max_version) {
      max_version = change.db_version;
    }
  }
  if (max_version > last_db_version) {
    last_db_version = max_version;
  }

  target.merge_changes(std::move(changes));
}

#endif // CRDT_HPP