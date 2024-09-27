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
#include <cassert>
#include <iostream>
#include <optional>

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
  uint64_t seq;

  ColumnVersion(uint64_t c, uint64_t d, CrdtNodeId n, uint64_t s) : col_version(c), db_version(d), node_id(n), seq(s) {}
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

  // Compare column_versions
  if (lhs.column_versions.size() != rhs.column_versions.size())
    return false;
  for (const auto &[key, value] : lhs.column_versions) {
    auto it = rhs.column_versions.find(key);
    if (it == rhs.column_versions.end() || it->second.col_version != value.col_version ||
        it->second.db_version != value.db_version || it->second.node_id != value.node_id || it->second.seq != value.seq)
      return false;
  }

  return true;
}

/// Represents a single change in the CRDT.
template <typename K, typename V> struct Change {
  K record_id;
  CrdtString col_name;
  std::optional<V> value; // std::nullopt represents deletion
  uint64_t col_version;
  uint64_t db_version;
  CrdtNodeId node_id;
  uint64_t seq;

  Change() = default;

  Change(K rid, CrdtString cname, std::optional<V> val, uint64_t cver, uint64_t dver, CrdtNodeId nid, uint64_t s)
      : record_id(std::move(rid)), col_name(std::move(cname)), value(std::move(val)), col_version(cver), db_version(dver),
        node_id(nid), seq(s) {}
};

/// Represents the CRDT structure, generic over key (`K`) and value (`V`) types.
template <typename K, typename V> class CRDT {
public:
  // Create a new empty CRDT
  CRDT(CrdtNodeId node_id) : node_id_(node_id), clock_(), data_(), tombstones_() {}

  /// Create a CRDT from a list of changes (e.g., loaded from disk).
  ///
  /// # Arguments
  ///
  /// * `node_id` - The unique identifier for this CRDT node.
  /// * `changes` - A list of changes to apply to reconstruct the CRDT state.
  CRDT(CrdtNodeId node_id, CrdtVector<Change<K, V>> &&changes) : node_id_(node_id), clock_(), data_(), tombstones_() {
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
      CrdtString col_name = std::move(change.col_name);
      uint64_t remote_col_version = change.col_version;
      uint64_t remote_db_version = change.db_version;
      CrdtNodeId remote_node_id = change.node_id;
      uint64_t remote_seq = change.seq;
      std::optional<V> remote_value = std::move(change.value);

      if (col_name == "__deleted__") {
        // Handle deletion
        tombstones_.emplace(record_id);
        data_.erase(record_id);

        // Insert deletion clock info
        CrdtMap<CrdtString, ColumnVersion> deletion_clock;
        deletion_clock.emplace("__deleted__", ColumnVersion(remote_col_version, remote_db_version, remote_node_id, remote_seq));

        // Store deletion info in the data map
        data_.emplace(record_id, Record<V>(CrdtMap<CrdtString, V>(), std::move(deletion_clock)));
      } else {
        if (tombstones_.find(record_id) == tombstones_.end()) {
          // Handle insertion or update
          Record<V> &record = data_[record_id]; // Inserts default if not exists

          // Insert or update the field value
          if (remote_value.has_value()) {
            record.fields[col_name] = std::move(remote_value.value());
          }

          // Update the column version info
          record.column_versions.insert_or_assign(
              std::move(col_name), ColumnVersion(remote_col_version, remote_db_version, remote_node_id, remote_seq));
        }
      }
    }
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
  template <bool ReturnChanges = true>
  std::conditional_t<ReturnChanges, CrdtVector<Change<K, V>>, void> insert_or_update(const K &record_id,
                                                                                     CrdtMap<CrdtString, V> &&fields) {
    CrdtVector<Change<K, V>> changes;
    uint64_t db_version = clock_.tick();

    // Check if the record is tombstoned
    if (tombstones_.find(record_id) != tombstones_.end()) {
      std::cout << "Insert/Update ignored: Record " << record_id << " is deleted (tombstoned)." << std::endl;
      if constexpr (ReturnChanges) {
        return changes;
      } else {
        return;
      }
    }

    bool is_new_record = data_.find(record_id) == data_.end();
    Record<V> &record = is_new_record ? data_[record_id] : data_.at(record_id);

    for (const auto &[col_name, value] : fields) {
      uint64_t col_version, seq;
      if (is_new_record) {
        col_version = 1;
        seq = 0;
        record.column_versions.emplace(col_name, ColumnVersion(col_version, db_version, node_id_, seq));
      } else {
        auto col_it = record.column_versions.find(col_name);
        if (col_it != record.column_versions.end()) {
          col_version = ++col_it->second.col_version;
          seq = ++col_it->second.seq;
          col_it->second.db_version = db_version;
          col_it->second.node_id = node_id_;
        } else {
          col_version = 1;
          seq = 0;
          record.column_versions.emplace(col_name, ColumnVersion(col_version, db_version, node_id_, seq));
        }
      }

      if constexpr (ReturnChanges) {
        record.fields[col_name] = value;
        changes.emplace_back(
            Change<K, V>(record_id, std::move(col_name), std::move(value), col_version, db_version, node_id_, seq));
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
  template <bool ReturnChanges = true>
  std::conditional_t<ReturnChanges, CrdtVector<Change<K, V>>, void> delete_record(const K &record_id) {
    CrdtVector<Change<K, V>> changes;
    if (tombstones_.find(record_id) != tombstones_.end()) {
      std::cout << "Delete ignored: Record " << record_id << " is already deleted (tombstoned)." << std::endl;
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
    deletion_clock.emplace("__deleted__", ColumnVersion(1, db_version, node_id_, 0));

    // Store deletion info in the data map
    data_.emplace(record_id, Record<V>(CrdtMap<CrdtString, V>(), std::move(deletion_clock)));

    if constexpr (ReturnChanges) {
      changes.emplace_back(Change<K, V>(record_id, "__deleted__", std::nullopt, 1, db_version, node_id_, 0));
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
  CrdtVector<Change<K, V>> get_changes_since(uint64_t last_db_version) const {
    CrdtVector<Change<K, V>> changes;

    for (const auto &[record_id, record] : data_) {
      for (const auto &[col_name, clock_info] : record.column_versions) {
        if (clock_info.db_version >= last_db_version) {
          std::optional<V> value = std::nullopt;
          if (col_name != "__deleted__") {
            auto field_it = record.fields.find(col_name);
            if (field_it != record.fields.end()) {
              value = field_it->second;
            }
          }
          changes.emplace_back(Change<K, V>(record_id, col_name, value, clock_info.col_version, clock_info.db_version,
                                            clock_info.node_id, clock_info.seq));
        }
      }
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
  template <bool ReturnAcceptedChanges = false>
  std::conditional_t<ReturnAcceptedChanges, CrdtVector<Change<K, V>>, void> merge_changes(CrdtVector<Change<K, V>> &&changes) {
    CrdtVector<Change<K, V>> accepted_changes; // Will be optimized away if ReturnAcceptedChanges is false

    for (auto &&change : changes) {
      const K &record_id = change.record_id;
      CrdtString col_name = std::move(change.col_name);
      uint64_t remote_col_version = change.col_version;
      uint64_t remote_db_version = change.db_version;
      CrdtNodeId remote_node_id = change.node_id;
      uint64_t remote_seq = change.seq;
      std::optional<V> remote_value = std::move(change.value);

      // Update logical clock with the received db_version
      clock_.update(remote_db_version);

      // Retrieve local column version information
      auto record_it = data_.find(record_id);
      ColumnVersion *local_col_info = nullptr;
      if (record_it != data_.end()) {
        auto col_it = record_it->second.column_versions.find(col_name);
        if (col_it != record_it->second.column_versions.end()) {
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
            // db_version is equal; use node_id and seq for tiebreaking
            if (remote_node_id > local.node_id) {
              should_accept = true;
            } else if (remote_node_id < local.node_id) {
              should_accept = false;
            } else {
              // node_id is equal; compare seq
              should_accept = (remote_seq > local.seq);
            }
          }
        }
      }

      if (should_accept) {
        if (col_name == "__deleted__") {
          // Handle deletion
          tombstones_.emplace(record_id);
          data_.erase(record_id);

          // Update deletion clock info
          CrdtMap<CrdtString, ColumnVersion> deletion_clock;
          deletion_clock.emplace("__deleted__", ColumnVersion(remote_col_version, remote_db_version, remote_node_id, remote_seq));

          // Store deletion info in the data map
          data_.emplace(record_id, Record<V>(CrdtMap<CrdtString, V>(), std::move(deletion_clock)));
        } else if (tombstones_.find(record_id) == tombstones_.end()) {
          // Handle insertion or update if the record is not tombstoned
          Record<V> &record = data_[record_id]; // Inserts default if not exists

          // Update field value
          if (remote_value.has_value()) {
            // move if ReturnAcceptedChanges is false, otherwise copy
            if constexpr (!ReturnAcceptedChanges) {
              record.fields[col_name] = std::move(remote_value.value());
            } else {
              record.fields[col_name] = remote_value.value();
            }
          } else {
            // If remote_value is std::nullopt, remove the field
            record.fields.erase(col_name);
          }

          if constexpr (ReturnAcceptedChanges) {
            // Update the column version info
            record.column_versions.insert_or_assign(
                col_name, ColumnVersion(remote_col_version, remote_db_version, remote_node_id, remote_seq));

            accepted_changes.emplace_back(Change<K, V>(record_id, std::move(col_name), std::move(remote_value),
                                                       remote_col_version, remote_db_version, remote_node_id, remote_seq));
          } else {
            // Update the column version info
            record.column_versions.insert_or_assign(
                std::move(col_name), ColumnVersion(remote_col_version, remote_db_version, remote_node_id, remote_seq));
          }
        }
      }
    }

    if constexpr (ReturnAcceptedChanges) {
      return accepted_changes;
    }
  }

  /// Prints the current data and tombstones for debugging purposes.
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
  const LogicalClock &get_clock() const { return clock_; }

  const CrdtMap<K, Record<V>> &get_data() const { return data_; }

private:
  CrdtNodeId node_id_;
  LogicalClock clock_;
  CrdtMap<K, Record<V>> data_;
  CrdtSet<K> tombstones_;
};

/// Synchronizes two CRDT nodes.
/// Retrieves changes from the source since last_db_version and merges them into
/// the target. Updates last_db_version to prevent reprocessing the same
/// changes.
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
