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
#include <vector>

template <typename T> using CrdtVector = std::vector<T>;

using CrdtString = std::string;

template <typename K, typename V> using CrdtMap = std::unordered_map<K, V>;

template <typename K> using CrdtSet = std::unordered_set<K>;

using CrdtNodeId = uint64_t;
#endif

#include <algorithm>
#include <iostream>
#include <optional>
#include <memory>

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

/// Represents the version information for a column.
struct ColumnVersion {
  uint64_t col_version;
  uint64_t db_version;
  CrdtNodeId node_id;

  ColumnVersion(uint64_t c, uint64_t d, CrdtNodeId n) : col_version(c), db_version(d), node_id(n) {}
};

/// Represents a record in the CRDT.
template <typename V> struct Record {
  CrdtMap<CrdtString, V> fields;
  CrdtMap<CrdtString, ColumnVersion> column_versions;

  Record() = default;

  Record(CrdtMap<CrdtString, V> &&f, CrdtMap<CrdtString, ColumnVersion> &&cv)
      : fields(std::move(f)), column_versions(std::move(cv)) {}
};

// Free function to compare two Record<V> instances
template <typename V> bool operator==(const Record<V> &lhs, const Record<V> &rhs) {
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

/// Represents a single change in the CRDT.
template <typename K, typename V> struct Change {
  K record_id;
  std::optional<CrdtString> col_name; // std::nullopt represents tombstone of the record
  std::optional<V> value;             // note std::nullopt represents deletion of the column, not the record
  uint64_t col_version;
  uint64_t db_version;
  CrdtNodeId node_id;

  Change() = default;

  Change(K rid, std::optional<CrdtString> cname, std::optional<V> val, uint64_t cver, uint64_t dver, CrdtNodeId nid)
      : record_id(std::move(rid)), col_name(std::move(cname)), value(std::move(val)), col_version(cver), db_version(dver),
        node_id(nid) {}
};

/// Represents the CRDT structure, generic over key (`K`) and value (`V`) types.
template <typename K, typename V> class CRDT : public std::enable_shared_from_this<CRDT<K, V>> {
public:
  // Create a new empty CRDT
  // Complexity: O(1)
  CRDT(CrdtNodeId node_id, std::shared_ptr<CRDT<K, V>> parent = nullptr)
      : node_id_(node_id), clock_(), data_(), tombstones_(), parent_(parent) {
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
  CRDT(CrdtNodeId node_id, CrdtVector<Change<K, V>> &&changes) : node_id_(node_id), clock_(), data_(), tombstones_() {
    apply_changes(std::move(changes));
  }

  /// Resets the CRDT to a state as if it was constructed with the given changes.
  ///
  /// # Arguments
  ///
  /// * `changes` - A list of changes to apply to reconstruct the CRDT state.
  ///
  /// Complexity: O(n), where n is the number of changes
  void reset(CrdtVector<Change<K, V>> &&changes) {
    // Clear existing data
    data_.clear();
    tombstones_.clear();

    // Reset the logical clock
    clock_ = LogicalClock();

    apply_changes(std::move(changes));
  }

  /// Generates inverse changes for a given set of changes based on the parent state.
  ///
  /// # Arguments
  ///
  /// * `changes` - A vector of changes to invert.
  ///
  /// # Returns
  ///
  /// A vector of inverse `Change` objects.
  /// Generates inverse changes for a given set of changes based on the specified source (parent or top-level CRDT).
  ///
  /// # Arguments
  ///
  /// * `changes` - A vector of changes to invert.
  ///
  /// # Returns
  ///
  /// A vector of inverse `Change` objects.
  template <bool UseParent = false> CrdtVector<Change<K, V>> invert_changes(const CrdtVector<Change<K, V>> &changes) {
    CrdtVector<Change<K, V>> inverse_changes;

    for (const auto &change : changes) {
      const K &record_id = change.record_id;
      const std::optional<CrdtString> &col_name = change.col_name;
      const std::optional<V> &value = change.value;

      if (!col_name.has_value()) {
        // The change was a record deletion (tombstone)
        // To revert, restore the record's state from the specified source
        auto record_ptr = UseParent ? parent_->get_record_ptr(record_id) : get_record_ptr(record_id);
        if (record_ptr) {
          // Restore all fields from the record
          for (const auto &[parent_col, parent_val] : record_ptr->fields) {
            inverse_changes.emplace_back(Change<K, V>(record_id, parent_col, parent_val,
                                                      record_ptr->column_versions.at(parent_col).col_version,
                                                      record_ptr->column_versions.at(parent_col).db_version, node_id_));
          }
          // Remove the tombstone
          inverse_changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt,
                                                    0, // Column version 0 signifies removal of tombstone
                                                    clock_.current_time(), node_id_));
        }
      } else {
        // The change was an insertion or update of a column
        CrdtString col = *col_name;
        auto record_ptr = UseParent ? parent_->get_record_ptr(record_id) : get_record_ptr(record_id);
        if (record_ptr) {
          auto field_it = record_ptr->fields.find(col);
          if (field_it != record_ptr->fields.end()) {
            // The record has a value for this column; set it back to the record's value
            inverse_changes.emplace_back(Change<K, V>(record_id, col, field_it->second,
                                                      record_ptr->column_versions.at(col).col_version,
                                                      record_ptr->column_versions.at(col).db_version, node_id_));
          } else {
            // The record does not have this column; delete it to revert
            inverse_changes.emplace_back(Change<K, V>(record_id, col, std::nullopt, // Indicates deletion
                                                      0,                            // Column version 0 signifies deletion
                                                      clock_.current_time(), node_id_));
          }
        } else {
          // The record does not have the record; remove the entire record to revert
          inverse_changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt,
                                                    0, // Column version 0 signifies a tombstone
                                                    clock_.current_time(), node_id_));
        }
      }
    }

    return inverse_changes;
  }

  /// Reverts all changes made by this CRDT since it was created from the parent.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the inverse changes needed to undo the child’s changes.
  ///
  /// # Complexity
  ///
  /// O(c), where c is the number of changes since `base_version_`
  CrdtVector<Change<K, V>> revert() {
    if (!parent_) {
      throw std::runtime_error("Cannot revert without a parent CRDT.");
    }

    // Step 1: Retrieve all changes made by the child since base_version_
    CrdtVector<Change<K, V>> child_changes = this->get_changes_since(base_version_);

    // Step 2: Generate inverse changes using the generalized function
    CrdtVector<Change<K, V>> inverse_changes = invert_changes<true>(child_changes);

    return inverse_changes;
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
  std::conditional_t<ReturnChanges, CrdtVector<Change<K, V>>, void> insert_or_update(const K &record_id,
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

    for (const auto &[col_name, value] : fields) {
      uint64_t col_version;
      auto col_it = record.column_versions.find(col_name);
      if (col_it != record.column_versions.end()) {
        col_version = ++col_it->second.col_version;
        col_it->second.db_version = db_version;
        col_it->second.node_id = node_id_;
      } else {
        col_version = 1;
        record.column_versions.emplace(col_name, ColumnVersion(col_version, db_version, node_id_));
      }

      if constexpr (ReturnChanges) {
        record.fields[col_name] = value;
        changes.emplace_back(Change<K, V>(record_id, std::move(col_name), std::move(value), col_version, db_version, node_id_));
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
    deletion_clock.emplace("__deleted__", ColumnVersion(1, db_version, node_id_));

    // Store deletion info in the data map
    data_.emplace(record_id, Record<V>(CrdtMap<CrdtString, V>(), std::move(deletion_clock)));

    if constexpr (ReturnChanges) {
      changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt, 1, db_version, node_id_));
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
  CrdtVector<Change<K, V>> get_changes_since(uint64_t last_db_version) const {
    CrdtVector<Change<K, V>> changes;

    // get changes from parent
    if (parent_) {
      auto parent_changes = parent_->get_changes_since(last_db_version);
      changes.insert(changes.end(), parent_changes.begin(), parent_changes.end());
    }

    for (const auto &[record_id, record] : data_) {
      for (const auto &[col_name, clock_info] : record.column_versions) {
        if (clock_info.db_version > last_db_version) {
          std::optional<V> value = std::nullopt;
          if (col_name != "__deleted__") {
            auto field_it = record.fields.find(col_name);
            if (field_it != record.fields.end()) {
              value = field_it->second;
            }
          }
          changes.emplace_back(
              Change<K, V>(record_id, col_name, value, clock_info.col_version, clock_info.db_version, clock_info.node_id));
        }
      }
    }

    if (parent_) {
      // since we merge from the parent, we need to also run a compression pass
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
    CrdtVector<Change<K, V>> accepted_changes; // Will be optimized away if ReturnAcceptedChanges is false

    if (changes.empty()) {
      if constexpr (ReturnAcceptedChanges) {
        return accepted_changes;
      } else {
        return;
      }
    }

    auto new_db_version = clock_.tick();

    for (auto &&change : changes) {
      const K &record_id = change.record_id;
      std::optional<CrdtString> col_name = std::move(change.col_name);
      uint64_t remote_col_version = change.col_version;
      uint64_t remote_db_version = change.db_version;
      CrdtNodeId remote_node_id = change.node_id;
      std::optional<V> remote_value = std::move(change.value);

      if (remote_db_version >= new_db_version) {
        // If the remote db_version is greater than the new db_version, we need to update the local db_version
        new_db_version = clock_.update(remote_db_version);
      }

      // Retrieve local column version information
      auto record_ptr = get_record_ptr(record_id, ignore_parent);
      ColumnVersion *local_col_info = nullptr;
      if (record_ptr != nullptr) {
        auto col_it = record_ptr->column_versions.find(col_name ? col_name.value() : "__deleted__");
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
        const ColumnVersion &local = *local_col_info;

        if (remote_col_version > local.col_version) {
          // Remote change is newer; accept it
          should_accept = true;
        } else if (remote_col_version < local.col_version) {
          // Remote change is older; reject it
          should_accept = false;
        } else {
          // col_version is equal; use db_version as the next tiebreaker
          if (remote_db_version > local.db_version) {
            should_accept = true;
          } else if (remote_db_version < local.db_version) {
            should_accept = false;
          } else {
            // db_version is equal; use node_id for final tiebreaking
            should_accept = (remote_node_id > local.node_id);
          }
        }
      }

      if (should_accept) {
        if (!col_name.has_value()) {
          // Handle deletion
          tombstones_.emplace(record_id);
          data_.erase(record_id);

          // Update deletion clock info
          CrdtMap<CrdtString, ColumnVersion> deletion_clock;
          deletion_clock.emplace("__deleted__", ColumnVersion(remote_col_version, new_db_version, remote_node_id));

          // Store deletion info in the data map
          data_.emplace(record_id, Record<V>(CrdtMap<CrdtString, V>(), std::move(deletion_clock)));

          if constexpr (ReturnAcceptedChanges) {
            accepted_changes.emplace_back(
                Change<K, V>(record_id, std::nullopt, std::nullopt, remote_col_version, new_db_version, remote_node_id));
          }
        } else if (!is_record_tombstoned(record_id, ignore_parent)) {
          // Handle insertion or update
          Record<V> &record = get_or_create_record_unchecked(record_id, ignore_parent);

          // Update field value
          if (remote_value.has_value() && col_name.has_value()) {
            // move if ReturnAcceptedChanges is false, otherwise copy
            if constexpr (!ReturnAcceptedChanges) {
              record.fields[col_name.value()] = std::move(remote_value.value());
            } else {
              record.fields[col_name.value()] = remote_value.value();
            }
          } else {
            // If remote_value is std::nullopt, remove the field
            record.fields.erase(col_name ? col_name.value() : "__deleted__");
          }

          if constexpr (ReturnAcceptedChanges) {
            // Update the column version info
            record.column_versions.insert_or_assign(col_name ? col_name.value() : "__deleted__",
                                                    ColumnVersion(remote_col_version, new_db_version, remote_node_id));

            accepted_changes.emplace_back(Change<K, V>(record_id, std::move(col_name), std::move(remote_value),
                                                       remote_col_version, new_db_version, remote_node_id));
          } else {
            // Update the column version info
            record.column_versions.insert_or_assign(col_name ? std::move(col_name.value()) : "__deleted__",
                                                    ColumnVersion(remote_col_version, new_db_version, remote_node_id));
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
  /// # Returns
  ///
  /// The number of changes remaining after compression.
  ///
  /// Complexity: O(n log n), where n is the number of changes
  static void compress_changes(CrdtVector<Change<K, V>> &changes) {
    if (changes.empty())
      return;

    // Sort changes based on record_id, col_name (with nullopt last), and then by precedence rules
    std::sort(changes.begin(), changes.end(), [](const Change<K, V> &a, const Change<K, V> &b) {
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
      return a.node_id > b.node_id;
    });

    // Use two-pointer technique to compress in-place
    size_t write = 0;
    for (size_t read = 1; read < changes.size(); ++read) {
      if (changes[read].record_id != changes[write].record_id) {
        // New record, always keep it
        ++write;
        if (write != read) {
          changes[write] = std::move(changes[read]);
        }
      } else if (!changes[read].col_name.has_value()) {
        // Current read is a deletion, keep it and skip all previous changes for this record
        write = read;
      } else if (changes[read].col_name != changes[write].col_name) {
        // New column for the same record
        ++write;
        if (write != read) {
          changes[write] = std::move(changes[read]);
        }
      }
      // Else: same record and column, keep the existing one (which is the most recent due to sorting)
    }

    changes.resize(write + 1);
  }

  /// Prints the current data and tombstones for debugging purposes.
  ///
  /// Complexity: O(n * m), where n is the number of records and m is the average number of fields per record
  void print_data() const {
    std::cout << "Node " << node_id_ << " Data:" << std::endl;
    for (const auto &[record_id, record] : data_) {
      if (tombstones_.find(record_id) != tombstones_.end()) {
        continue; // Skip tombstoned records
      }
      std::cout << "ID: " << record_id << std::endl;
      for (const auto &[key, value] : record.fields) {
        std::cout << "  " << key << ": " << value << std::endl;
      }
    }
    std::cout << "Tombstones: ";
    for (const auto &tid : tombstones_) {
      std::cout << tid << " ";
    }
    std::cout << std::endl << std::endl;
  }

  // Accessors for testing
  // Complexity: O(1)
  const LogicalClock &get_clock() const { return clock_; }

  // Updated get_data() method
  CrdtMap<K, Record<V>> get_data() const {
    if (!parent_) {
      return data_;
    }

    CrdtMap<K, Record<V>> combined_data = parent_->get_data();
    for (const auto &[key, record] : data_) {
      combined_data[key] = record;
    }
    return combined_data;
  }

private:
  CrdtNodeId node_id_;
  LogicalClock clock_;
  CrdtMap<K, Record<V>> data_;
  CrdtSet<K> tombstones_;

  // our clock won't be shared with the parent
  // we optionally allow to merge from the parent or push to the parent
  std::shared_ptr<CRDT<K, V>> parent_;
  uint64_t base_version_; // Tracks the parent’s db_version at the time of child creation

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
      std::optional<V> remote_value = std::move(change.value);

      if (!col_name.has_value()) {
        // Handle deletion
        tombstones_.emplace(record_id);
        data_.erase(record_id);

        // Insert deletion clock info
        CrdtMap<CrdtString, ColumnVersion> deletion_clock;
        deletion_clock.emplace("__deleted__", ColumnVersion(remote_col_version, remote_db_version, remote_node_id));

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
          record.column_versions.insert_or_assign(std::move(*col_name),
                                                  ColumnVersion(remote_col_version, remote_db_version, remote_node_id));
        }
      }
    }
  }

  bool is_record_tombstoned(const K &record_id, bool ignore_parent = false) const {
    if (tombstones_.find(record_id) != tombstones_.end()) {
      return true;
    }
    if (parent_ && !ignore_parent) {
      return parent_->is_record_tombstoned(record_id);
    }
    return false;
  }

  // Notice that this will not check if the record is tombstoned! Such check should be done by the caller
  Record<V> &get_or_create_record_unchecked(const K &record_id, bool ignore_parent = false) {
    auto [it, inserted] = data_.try_emplace(record_id);
    if (inserted && parent_ && !ignore_parent) {
      if (auto parent_record = parent_->get_record_ptr(record_id)) {
        it->second = *parent_record;
      }
    }
    return it->second;
  }

  Record<V> *get_record_ptr(const K &record_id, bool ignore_parent = false) {
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
};

/// Synchronizes two CRDT nodes.
/// Retrieves changes from the source since last_db_version and merges them into
/// the target. Updates last_db_version to prevent reprocessing the same
/// changes.
///
/// Complexity: O(c + m), where c is the number of changes since last_db_version,
/// and m is the complexity of merge_changes
template <typename K, typename V> void sync_nodes(CRDT<K, V> &source, CRDT<K, V> &target, uint64_t &last_db_version) {
  auto changes = source.get_changes_since(last_db_version);

  // Update last_db_version to the current max db_version in source
  uint64_t max_version = 0;
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