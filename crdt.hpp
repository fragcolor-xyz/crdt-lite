// crdt.hpp
#ifndef CRDT_HPP
#define CRDT_HPP

#include <algorithm>
#include <cassert>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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

  /// Retrieves the current time.
  uint64_t current_time() const { return time_; }

private:
  uint64_t time_;
};

/// Represents the version information for a column.
struct ColumnVersion {
  uint64_t col_version;
  uint64_t db_version;
  uint64_t site_id;
  uint64_t seq;

  ColumnVersion(uint64_t c = 0, uint64_t d = 0, uint64_t s = 0, uint64_t se = 0)
      : col_version(c), db_version(d), site_id(s), seq(se) {}
};

/// Represents a record in the CRDT.
template <typename V> struct Record {
  std::unordered_map<std::string, V> fields;
  std::unordered_map<std::string, ColumnVersion> column_versions;

  Record() = default;

  Record(std::unordered_map<std::string, V> &&f, std::unordered_map<std::string, ColumnVersion> &&cv)
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
        it->second.db_version != value.db_version || it->second.site_id != value.site_id || it->second.seq != value.seq)
      return false;
  }

  return true;
}

/// Represents a single change in the CRDT.
template <typename K, typename V> struct Change {
  K record_id;
  std::string col_name;
  std::optional<V> value; // std::nullopt represents deletion
  uint64_t col_version;
  uint64_t db_version;
  uint64_t site_id;
  uint64_t seq;

  Change() = default;

  Change(K rid, std::string cname, std::optional<V> val, uint64_t cver, uint64_t dver, uint64_t sid, uint64_t s)
      : record_id(std::move(rid)), col_name(std::move(cname)), value(std::move(val)), col_version(cver), db_version(dver),
        site_id(sid), seq(s) {}
};

/// Represents the CRDT structure, generic over key (`K`) and value (`V`) types.
template <typename K, typename V> class CRDT {
public:
  CRDT(uint64_t node_id) : node_id_(node_id), clock_(), data_(), tombstones_() {}

  /// Inserts a new record or updates an existing record in the CRDT.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `fields` - A hashmap of field names to their values.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the changes made.
  std::vector<Change<K, V>> insert_or_update(const K &record_id, const std::unordered_map<std::string, V> &fields) {
    std::vector<Change<K, V>> changes;
    uint64_t db_version = clock_.tick();

    // Check if the record is tombstoned
    if (tombstones_.find(record_id) != tombstones_.end()) {
      std::cout << "Insert/Update ignored: Record " << record_id << " is deleted (tombstoned)." << std::endl;
      return changes; // No changes to return
    }

    bool is_new_record = data_.find(record_id) == data_.end();

    if (is_new_record) {
      // Initialize column versions
      std::unordered_map<std::string, ColumnVersion> column_versions;
      for (const auto &[col_name, _] : fields) {
        column_versions.emplace(col_name, ColumnVersion(1, db_version, node_id_, 0));
        // Create a Change object for each field inserted
        changes.emplace_back(Change<K, V>(record_id, col_name, fields.at(col_name), 1, db_version, node_id_, 0));
      }

      // Insert the record
      Record<V> record;
      record.fields = fields;
      record.column_versions = std::move(column_versions);
      data_.emplace(record_id, std::move(record));
    } else {
      // Update existing record
      Record<V> &record = data_.at(record_id);
      for (const auto &[col_name, value] : fields) {
        // Update the value
        record.fields[col_name] = value;

        // Update the clock for this column
        auto col_it = record.column_versions.find(col_name);
        if (col_it != record.column_versions.end()) {
          col_it->second.col_version += 1;
          col_it->second.db_version = db_version;
          col_it->second.seq += 1;
          col_it->second.site_id = node_id_;
        } else {
          // If the column does not exist, initialize it
          record.column_versions.emplace(col_name, ColumnVersion(1, db_version, node_id_, 0));
        }

        // Create a Change object for each field updated
        changes.emplace_back(Change<K, V>(record_id, col_name, value, record.column_versions[col_name].col_version, db_version,
                                          node_id_, record.column_versions[col_name].seq));
      }
    }

    return changes;
  }

  /// Deletes a record by marking it as tombstoned.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the deletion.
  std::vector<Change<K, V>> delete_record(const K &record_id) {
    std::vector<Change<K, V>> changes;

    if (tombstones_.find(record_id) != tombstones_.end()) {
      std::cout << "Delete ignored: Record " << record_id << " is already deleted (tombstoned)." << std::endl;
      return changes; // No changes to return
    }

    uint64_t db_version = clock_.tick();

    // Mark as tombstone
    tombstones_.emplace(record_id);

    // Remove data
    data_.erase(record_id);

    // Insert deletion clock info
    std::unordered_map<std::string, ColumnVersion> deletion_clock;
    deletion_clock.emplace("__deleted__", ColumnVersion(1, db_version, node_id_, 0));

    // Store deletion info in the data map
    data_.emplace(record_id, Record<V>(std::unordered_map<std::string, V>(), std::move(deletion_clock)));

    // Create a Change object for deletion
    changes.emplace_back(Change<K, V>(record_id, "__deleted__", std::nullopt, 1, db_version, node_id_, 0));

    return changes;
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
  std::vector<Change<K, V>> get_changes_since(uint64_t last_db_version) const {
    std::vector<Change<K, V>> changes;

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
                                            clock_info.site_id, clock_info.seq));
        }
      }
    }

    return changes;
  }

  /// Merges a set of incoming changes into the CRDT.
  ///
  /// # Arguments
  ///
  /// * `changes` - A slice of changes to merge.
  void merge_changes(const std::vector<Change<K, V>> &changes) {
    for (const auto &change : changes) {
      const K &record_id = change.record_id;
      const std::string &col_name = change.col_name;
      uint64_t remote_col_version = change.col_version;
      uint64_t remote_db_version = change.db_version;
      uint64_t remote_site_id = change.site_id;
      uint64_t remote_seq = change.seq;
      std::optional<V> remote_value = change.value;

      // Update logical clock
      clock_.update(remote_db_version);

      // Retrieve local column info
      auto record_it = data_.find(record_id);
      std::optional<ColumnVersion> local_col_info = std::nullopt;
      if (record_it != data_.end()) {
        auto col_it = record_it->second.column_versions.find(col_name);
        if (col_it != record_it->second.column_versions.end()) {
          local_col_info = col_it->second;
        }
      }

      // Determine if we should accept the remote change
      bool should_accept = false;
      if (!local_col_info.has_value()) {
        should_accept = true;
      } else {
        const ColumnVersion &local = local_col_info.value();
        if (remote_col_version > local.col_version) {
          should_accept = true;
        } else if (remote_col_version == local.col_version) {
          if (col_name == "__deleted__" && change.col_name != "__deleted__") {
            should_accept = true;
          } else if (change.col_name == "__deleted__" && col_name != "__deleted__") {
            should_accept = false;
          } else if (change.col_name == "__deleted__" && col_name == "__deleted__") {
            if (remote_site_id > local.site_id) {
              should_accept = true;
            } else if (remote_site_id == local.site_id) {
              should_accept = (remote_seq > local.seq);
            } else {
              should_accept = false;
            }
          } else {
            if (remote_site_id > local.site_id) {
              should_accept = true;
            } else if (remote_site_id == local.site_id) {
              should_accept = (remote_seq > local.seq);
            } else {
              should_accept = false;
            }
          }
        }
      }

      if (should_accept) {
        if (col_name == "__deleted__") {
          // Handle deletion
          tombstones_.emplace(record_id);
          data_.erase(record_id);

          // Insert deletion clock info
          std::unordered_map<std::string, ColumnVersion> deletion_clock;
          deletion_clock.emplace("__deleted__", ColumnVersion(remote_col_version, remote_db_version, remote_site_id, remote_seq));

          // Store deletion info in the data map
          data_.emplace(record_id, Record<V>(std::unordered_map<std::string, V>(), std::move(deletion_clock)));
        } else if (tombstones_.find(record_id) == tombstones_.end()) {
          // Handle insertion or update only if the record is not tombstoned
          Record<V> &record = data_[record_id]; // Inserts default if not exists

          // Insert or update the field value
          if (remote_value.has_value()) {
            record.fields[col_name] = remote_value.value();
          }

          // Update the column version info
          record.column_versions[col_name] = ColumnVersion(remote_col_version, remote_db_version, remote_site_id, remote_seq);
        }
      }
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

  const std::unordered_map<K, Record<V>> &get_data() const { return data_; }

private:
  uint64_t node_id_;
  LogicalClock clock_;
  std::unordered_map<K, Record<V>> data_;
  std::unordered_set<K> tombstones_;
};

/// Synchronizes two CRDT nodes.
/// Retrieves changes from the source since last_db_version and merges them into
/// the target. Updates last_db_version to prevent reprocessing the same
/// changes.
template <typename K, typename V> void sync_nodes(CRDT<K, V> &source, CRDT<K, V> &target, uint64_t &last_db_version) {
  auto changes = source.get_changes_since(last_db_version);
  target.merge_changes(changes);
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
}

/// Represents the state of a node, tracking the last integrated db_version.
template <typename K, typename V> struct NodeState {
  CRDT<K, V> crdt;
  uint64_t last_db_version;

  NodeState(uint64_t node_id) : crdt(node_id), last_db_version(0) {}

  void sync_from(const CRDT<K, V> &source_crdt) {
    auto changes = source_crdt.get_changes_since(last_db_version);
    crdt.merge_changes(changes);
    // Update last_db_version to the current max db_version in source_crdt
    uint64_t max_version = 0;
    for (const auto &change : changes) {
      if (change.db_version > max_version) {
        max_version = change.db_version;
      }
    }
    if (max_version > last_db_version) {
      last_db_version = max_version;
    }
  }
};

#endif // CRDT_HPP
