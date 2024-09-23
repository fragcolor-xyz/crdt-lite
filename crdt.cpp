#include <algorithm>
#include <cassert>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Helper function to generate unique IDs (simulating UUIDs)
std::string generate_uuid() {
  static uint64_t counter = 0;
  return "uuid-" + std::to_string(++counter);
}

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

  Record(std::unordered_map<std::string, V> &&f,
         std::unordered_map<std::string, ColumnVersion> &&cv)
      : fields(std::move(f)), column_versions(std::move(cv)) {}
};

// Free function to compare two Record<V> instances
template <typename V>
bool operator==(const Record<V> &lhs, const Record<V> &rhs) {
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
    if (it == rhs.column_versions.end() ||
        it->second.col_version != value.col_version ||
        it->second.db_version != value.db_version ||
        it->second.site_id != value.site_id || it->second.seq != value.seq)
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

  Change(K rid, std::string cname, std::optional<V> val, uint64_t cver,
         uint64_t dver, uint64_t sid, uint64_t s)
      : record_id(std::move(rid)), col_name(std::move(cname)),
        value(std::move(val)), col_version(cver), db_version(dver),
        site_id(sid), seq(s) {}
};

/// Represents the CRDT structure, generic over key (`K`) and value (`V`) types.
template <typename K, typename V> class CRDT {
public:
  CRDT(uint64_t node_id)
      : node_id_(node_id), clock_(), data_(), tombstones_() {}

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
  std::vector<Change<K, V>>
  insert_or_update(const K &record_id,
                   const std::unordered_map<std::string, V> &fields) {
    std::vector<Change<K, V>> changes;
    uint64_t db_version = clock_.tick();

    // Check if the record is tombstoned
    if (tombstones_.find(record_id) != tombstones_.end()) {
      std::cout << "Insert/Update ignored: Record " << record_id
                << " is deleted (tombstoned)." << std::endl;
      return changes; // No changes to return
    }

    bool is_new_record = data_.find(record_id) == data_.end();

    if (is_new_record) {
      // Initialize column versions
      std::unordered_map<std::string, ColumnVersion> column_versions;
      for (const auto &[col_name, _] : fields) {
        column_versions.emplace(col_name,
                                ColumnVersion(1, db_version, node_id_, 0));
        // Create a Change object for each field inserted
        changes.emplace_back(Change<K, V>(record_id, col_name,
                                          fields.at(col_name), 1, db_version,
                                          node_id_, 0));
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
          record.column_versions.emplace(
              col_name, ColumnVersion(1, db_version, node_id_, 0));
        }

        // Create a Change object for each field updated
        changes.emplace_back(Change<K, V>(
            record_id, col_name, value,
            record.column_versions[col_name].col_version, db_version, node_id_,
            record.column_versions[col_name].seq));
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
      std::cout << "Delete ignored: Record " << record_id
                << " is already deleted (tombstoned)." << std::endl;
      return changes; // No changes to return
    }

    uint64_t db_version = clock_.tick();

    // Mark as tombstone
    tombstones_.emplace(record_id);

    // Remove data
    data_.erase(record_id);

    // Insert deletion clock info
    std::unordered_map<std::string, ColumnVersion> deletion_clock;
    deletion_clock.emplace("__deleted__",
                           ColumnVersion(1, db_version, node_id_, 0));

    // Store deletion info in the data map
    data_.emplace(record_id, Record<V>(std::unordered_map<std::string, V>(),
                                       std::move(deletion_clock)));

    // Create a Change object for deletion
    changes.emplace_back(Change<K, V>(record_id, "__deleted__", std::nullopt, 1,
                                      db_version, node_id_, 0));

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
          changes.emplace_back(Change<K, V>(
              record_id, col_name, value, clock_info.col_version,
              clock_info.db_version, clock_info.site_id, clock_info.seq));
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
          } else if (change.col_name == "__deleted__" &&
                     col_name != "__deleted__") {
            should_accept = false;
          } else if (change.col_name == "__deleted__" &&
                     col_name == "__deleted__") {
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
          deletion_clock.emplace("__deleted__",
                                 ColumnVersion(remote_col_version,
                                               remote_db_version,
                                               remote_site_id, remote_seq));

          // Store deletion info in the data map
          data_.emplace(record_id,
                        Record<V>(std::unordered_map<std::string, V>(),
                                  std::move(deletion_clock)));
        } else if (tombstones_.find(record_id) == tombstones_.end()) {
          // Handle insertion or update only if the record is not tombstoned
          Record<V> &record = data_[record_id]; // Inserts default if not exists

          // Insert or update the field value
          if (remote_value.has_value()) {
            record.fields[col_name] = remote_value.value();
          }

          // Update the column version info
          record.column_versions[col_name] =
              ColumnVersion(remote_col_version, remote_db_version,
                            remote_site_id, remote_seq);
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
template <typename K, typename V>
void sync_nodes(CRDT<K, V> &source, CRDT<K, V> &target,
                uint64_t &last_db_version) {
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

/// Simple assertion helper
void assert_true(bool condition, const std::string &message) {
  if (!condition) {
    std::cerr << "Assertion failed: " << message << std::endl;
    exit(1);
  }
}

int main() {
  // Test Case: Basic Insert and Merge using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Node1 inserts a record
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields1 = {
        {"id", record_id},
        {"form_id", generate_uuid()},
        {"tag", "Node1Tag"},
        {"created_at", "2023-10-01T12:00:00Z"},
        {"created_by", "User1"}};
    auto changes1 = node1.insert_or_update(record_id, fields1);

    // Node2 inserts the same record with different data
    std::unordered_map<std::string, std::string> fields2 = {
        {"id", record_id},
        {"form_id", fields1.at("form_id")},
        {"tag", "Node2Tag"},
        {"created_at", "2023-10-01T12:05:00Z"},
        {"created_by", "User2"}};
    auto changes2 = node2.insert_or_update(record_id, fields2);

    // Merge node2's changes into node1
    node1.merge_changes(changes2);

    // Merge node1's changes into node2
    node2.merge_changes(changes1);

    // Both nodes should resolve the conflict and have the same data
    assert_true(node1.get_data() == node2.get_data(),
                "Basic Insert and Merge: Data mismatch");
    assert_true(node1.get_data().at(record_id).fields.at("tag") == "Node2Tag",
                "Basic Insert and Merge: Tag should be 'Node2Tag'");
    assert_true(node1.get_data().at(record_id).fields.at("created_by") ==
                    "User2",
                "Basic Insert and Merge: created_by should be 'User2'");
    std::cout << "Test 'Basic Insert and Merge' passed." << std::endl;
  }

  // Test Case: Updates with Conflicts using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Insert a shared record
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields = {
        {"id", record_id}, {"tag", "InitialTag"}};
    auto changes_init1 = node1.insert_or_update(record_id, fields);
    auto changes_init2 = node2.insert_or_update(record_id, fields);

    // Merge initial inserts
    node1.merge_changes(changes_init2);
    node2.merge_changes(changes_init1);

    // Node1 updates 'tag'
    std::unordered_map<std::string, std::string> updates1 = {
        {"tag", "Node1UpdatedTag"}};
    auto change_update1 = node1.insert_or_update(record_id, updates1);

    // Node2 updates 'tag'
    std::unordered_map<std::string, std::string> updates2 = {
        {"tag", "Node2UpdatedTag"}};
    auto change_update2 = node2.insert_or_update(record_id, updates2);

    // Merge changes
    node1.merge_changes(change_update2);
    node2.merge_changes(change_update1);

    // Conflict resolved based on site_id (Node2 has higher site_id)
    assert_true(node1.get_data().at(record_id).fields.at("tag") ==
                    "Node2UpdatedTag",
                "Updates with Conflicts: Tag resolution mismatch");
    assert_true(node1.get_data() == node2.get_data(),
                "Updates with Conflicts: Data mismatch");
    std::cout << "Test 'Updates with Conflicts' passed." << std::endl;
  }

  // Test Case: Delete and Merge using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Insert and sync a record
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields = {
        {"id", record_id}, {"tag", "ToBeDeleted"}};
    auto changes_init = node1.insert_or_update(record_id, fields);

    // Merge to node2
    node2.merge_changes(changes_init);

    // Node1 deletes the record
    auto changes_delete = node1.delete_record(record_id);

    // Merge the deletion to node2
    node2.merge_changes(changes_delete);

    // Both nodes should reflect the deletion
    assert_true(node1.get_data().at(record_id).fields.empty(),
                "Delete and Merge: Node1 should have empty fields");
    assert_true(node2.get_data().at(record_id).fields.empty(),
                "Delete and Merge: Node2 should have empty fields");
    assert_true(
        node1.get_data().at(record_id).column_versions.find("__deleted__") !=
            node1.get_data().at(record_id).column_versions.end(),
        "Delete and Merge: Node1 should have '__deleted__' column version");
    assert_true(
        node2.get_data().at(record_id).column_versions.find("__deleted__") !=
            node2.get_data().at(record_id).column_versions.end(),
        "Delete and Merge: Node2 should have '__deleted__' column version");
    std::cout << "Test 'Delete and Merge' passed." << std::endl;
  }

  // Test Case: Tombstone Handling using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Insert a record and delete it on node1
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields = {
        {"id", record_id}, {"tag", "Temporary"}};
    auto changes_insert = node1.insert_or_update(record_id, fields);
    auto changes_delete = node1.delete_record(record_id);

    // Merge changes to node2
    node2.merge_changes(changes_insert);
    node2.merge_changes(changes_delete);

    // Node2 tries to insert the same record
    auto changes_attempt_insert = node2.insert_or_update(record_id, fields);

    // Merge changes back to node1
    node1.merge_changes(changes_attempt_insert);

    // Node2 should respect the tombstone
    assert_true(node2.get_data().at(record_id).fields.empty(),
                "Tombstone Handling: Node2 should have empty fields");
    assert_true(
        node2.get_data().at(record_id).column_versions.find("__deleted__") !=
            node2.get_data().at(record_id).column_versions.end(),
        "Tombstone Handling: Node2 should have '__deleted__' column version");
    std::cout << "Test 'Tombstone Handling' passed." << std::endl;
  }

  // Test Case: Conflict Resolution with site_id and seq using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Both nodes insert a record with the same id
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields1 = {
        {"id", record_id}, {"tag", "Node1Tag"}};
    std::unordered_map<std::string, std::string> fields2 = {
        {"id", record_id}, {"tag", "Node2Tag"}};
    auto changes1 = node1.insert_or_update(record_id, fields1);
    auto changes2 = node2.insert_or_update(record_id, fields2);

    // Merge changes
    node1.merge_changes(changes2);
    node2.merge_changes(changes1);

    // Both nodes update the 'tag' field multiple times
    std::unordered_map<std::string, std::string> updates1 = {
        {"tag", "Node1Tag1"}};
    auto changes_update1 = node1.insert_or_update(record_id, updates1);

    updates1 = {{"tag", "Node1Tag2"}};
    auto changes_update2 = node1.insert_or_update(record_id, updates1);

    std::unordered_map<std::string, std::string> updates2 = {
        {"tag", "Node2Tag1"}};
    auto changes_update3 = node2.insert_or_update(record_id, updates2);

    updates2 = {{"tag", "Node2Tag2"}};
    auto changes_update4 = node2.insert_or_update(record_id, updates2);

    // Merge changes
    node1.merge_changes(changes_update4);
    node2.merge_changes(changes_update2);
    node2.merge_changes(changes_update1);
    node1.merge_changes(changes_update3);

    // Since node2 has a higher site_id, its latest update should prevail
    std::string expected_tag = "Node2Tag2";

    assert_true(node1.get_data().at(record_id).fields.at("tag") == expected_tag,
                "Conflict Resolution: Tag resolution mismatch");
    assert_true(node1.get_data() == node2.get_data(),
                "Conflict Resolution: Data mismatch");
    std::cout << "Test 'Conflict Resolution with site_id and seq' passed."
              << std::endl;
  }

  // Test Case: Logical Clock Update using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Node1 inserts a record
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields = {{"id", record_id},
                                                           {"tag", "Node1Tag"}};
    auto changes_insert = node1.insert_or_update(record_id, fields);

    // Node2 receives the change
    node2.merge_changes(changes_insert);

    // Node2's clock should be updated
    assert_true(node2.get_clock().current_time() > 0,
                "Logical Clock Update: Node2 clock should be greater than 0");
    assert_true(node2.get_clock().current_time() >=
                    node1.get_clock().current_time(),
                "Logical Clock Update: Node2 clock should be >= Node1 clock");
    std::cout << "Test 'Logical Clock Update' passed." << std::endl;
  }

  // Test Case: Merge without Conflicts using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Node1 inserts a record
    std::string record_id1 = generate_uuid();
    std::unordered_map<std::string, std::string> fields1 = {
        {"id", record_id1}, {"tag", "Node1Record"}};
    auto changes1 = node1.insert_or_update(record_id1, fields1);

    // Node2 inserts a different record
    std::string record_id2 = generate_uuid();
    std::unordered_map<std::string, std::string> fields2 = {
        {"id", record_id2}, {"tag", "Node2Record"}};
    auto changes2 = node2.insert_or_update(record_id2, fields2);

    // Merge changes
    node1.merge_changes(changes2);
    node2.merge_changes(changes1);

    // Both nodes should have both records
    assert_true(node1.get_data().find(record_id1) != node1.get_data().end(),
                "Merge without Conflicts: Node1 should contain record_id1");
    assert_true(node1.get_data().find(record_id2) != node1.get_data().end(),
                "Merge without Conflicts: Node1 should contain record_id2");
    assert_true(node2.get_data().find(record_id1) != node2.get_data().end(),
                "Merge without Conflicts: Node2 should contain record_id1");
    assert_true(node2.get_data().find(record_id2) != node2.get_data().end(),
                "Merge without Conflicts: Node2 should contain record_id2");
    assert_true(
        node1.get_data() == node2.get_data(),
        "Merge without Conflicts: Data mismatch between Node1 and Node2");
    std::cout << "Test 'Merge without Conflicts' passed." << std::endl;
  }

  // Test Case: Multiple Merges using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Node1 inserts a record
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields = {
        {"id", record_id}, {"tag", "InitialTag"}};
    auto changes_init = node1.insert_or_update(record_id, fields);

    // Merge to node2
    node2.merge_changes(changes_init);

    // Node2 updates the record
    std::unordered_map<std::string, std::string> updates2 = {
        {"tag", "UpdatedByNode2"}};
    auto changes_update2 = node2.insert_or_update(record_id, updates2);

    // Node1 updates the record
    std::unordered_map<std::string, std::string> updates1 = {
        {"tag", "UpdatedByNode1"}};
    auto changes_update1 = node1.insert_or_update(record_id, updates1);

    // Merge changes
    node1.merge_changes(changes_update2);
    node2.merge_changes(changes_update1);

    // Since node2 has a higher site_id, its latest update should prevail
    std::string expected_tag = "UpdatedByNode2";

    assert_true(node1.get_data().at(record_id).fields.at("tag") == expected_tag,
                "Multiple Merges: Tag resolution mismatch");
    assert_true(node1.get_data() == node2.get_data(),
                "Multiple Merges: Data mismatch between Node1 and Node2");
    std::cout << "Test 'Multiple Merges' passed." << std::endl;
  }

  // Test Case: Inserting After Deletion using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Node1 inserts and deletes a record
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields = {
        {"id", record_id}, {"tag", "Temporary"}};
    auto changes_insert = node1.insert_or_update(record_id, fields);
    auto changes_delete = node1.delete_record(record_id);

    // Merge deletion to node2
    node2.merge_changes(changes_insert);
    node2.merge_changes(changes_delete);

    // Node2 tries to insert the same record
    auto changes_attempt_insert = node2.insert_or_update(record_id, fields);

    // Merge changes back to node1
    node1.merge_changes(changes_attempt_insert);

    // The deletion should prevail
    assert_true(node1.get_data().at(record_id).fields.empty(),
                "Inserting After Deletion: Node1 should have empty fields");
    assert_true(node2.get_data().at(record_id).fields.empty(),
                "Inserting After Deletion: Node2 should have empty fields");
    assert_true(
        node1.get_data().at(record_id).column_versions.find("__deleted__") !=
            node1.get_data().at(record_id).column_versions.end(),
        "Inserting After Deletion: Node1 should have '__deleted__' column "
        "version");
    assert_true(
        node2.get_data().at(record_id).column_versions.find("__deleted__") !=
            node2.get_data().at(record_id).column_versions.end(),
        "Inserting After Deletion: Node2 should have '__deleted__' column "
        "version");
    std::cout << "Test 'Inserting After Deletion' passed." << std::endl;
  }

  // Test Case: Offline Changes Then Merge using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Both nodes start with an empty state

    // Node1 inserts a record
    std::string record_id1 = generate_uuid();
    std::unordered_map<std::string, std::string> fields1 = {
        {"id", record_id1}, {"tag", "Node1Tag"}};
    auto changes1 = node1.insert_or_update(record_id1, fields1);

    // Node2 is offline and inserts a different record
    std::string record_id2 = generate_uuid();
    std::unordered_map<std::string, std::string> fields2 = {
        {"id", record_id2}, {"tag", "Node2Tag"}};
    auto changes2 = node2.insert_or_update(record_id2, fields2);

    // Now, node2 comes online and merges changes from node1
    uint64_t last_db_version_node2 = 0;
    sync_nodes(node1, node2, last_db_version_node2);

    // Similarly, node1 merges changes from node2
    uint64_t last_db_version_node1 = 0;
    sync_nodes(node2, node1, last_db_version_node1);

    // Both nodes should now have both records
    assert_true(node1.get_data().find(record_id1) != node1.get_data().end(),
                "Offline Changes Then Merge: Node1 should contain record_id1");
    assert_true(node1.get_data().find(record_id2) != node1.get_data().end(),
                "Offline Changes Then Merge: Node1 should contain record_id2");
    assert_true(node2.get_data().find(record_id1) != node2.get_data().end(),
                "Offline Changes Then Merge: Node2 should contain record_id1");
    assert_true(node2.get_data().find(record_id2) != node2.get_data().end(),
                "Offline Changes Then Merge: Node2 should contain record_id2");
    assert_true(
        node1.get_data() == node2.get_data(),
        "Offline Changes Then Merge: Data mismatch between Node1 and Node2");
    std::cout << "Test 'Offline Changes Then Merge' passed." << std::endl;
  }

  // Test Case: Conflicting Updates with Different Last DB Versions using
  // insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Both nodes insert the same record
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields1 = {
        {"id", record_id}, {"tag", "InitialTag"}};
    std::unordered_map<std::string, std::string> fields2 = {
        {"id", record_id}, {"tag", "InitialTag"}};
    auto changes_init1 = node1.insert_or_update(record_id, fields1);
    auto changes_init2 = node2.insert_or_update(record_id, fields2);

    // Merge initial inserts
    node1.merge_changes(changes_init2);
    node2.merge_changes(changes_init1);

    // Node1 updates 'tag' twice
    std::unordered_map<std::string, std::string> updates_node1 = {
        {"tag", "Node1Tag1"}};
    auto changes_node1_update1 =
        node1.insert_or_update(record_id, updates_node1);

    updates_node1 = {{"tag", "Node1Tag2"}};
    auto changes_node1_update2 =
        node1.insert_or_update(record_id, updates_node1);

    // Node2 updates 'tag' once
    std::unordered_map<std::string, std::string> updates_node2 = {
        {"tag", "Node2Tag1"}};
    auto changes_node2_update1 =
        node2.insert_or_update(record_id, updates_node2);

    // Merge node1's changes into node2
    node2.merge_changes(changes_node1_update1);
    node2.merge_changes(changes_node1_update2);

    // Merge node2's changes into node1
    node1.merge_changes(changes_node2_update1);

    // The 'tag' should reflect the latest update based on db_version and
    // site_id Assuming node1 has a higher db_version due to two updates
    std::string final_tag = "Node1Tag2";

    assert_true(node1.get_data().at(record_id).fields.at("tag") == final_tag,
                "Conflicting Updates: Final tag should be 'Node1Tag2'");
    assert_true(node2.get_data().at(record_id).fields.at("tag") == final_tag,
                "Conflicting Updates: Final tag should be 'Node1Tag2'");
    assert_true(node1.get_data() == node2.get_data(),
                "Conflicting Updates: Data mismatch between Node1 and Node2");
    std::cout
        << "Test 'Conflicting Updates with Different Last DB Versions' passed."
        << std::endl;
  }

  // Test Case: Clock Synchronization After Merges using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);
    CRDT<std::string, std::string> node3(3);

    // Merge trackers
    uint64_t last_db_version_node1 = 0;
    uint64_t last_db_version_node2 = 0;
    uint64_t last_db_version_node3 = 0;

    // Node1 inserts a record
    std::string record_id1 = generate_uuid();
    std::unordered_map<std::string, std::string> fields1 = {
        {"id", record_id1}, {"tag", "Node1Tag"}};
    auto changes1 = node1.insert_or_update(record_id1, fields1);

    // Node2 inserts another record
    std::string record_id2 = generate_uuid();
    std::unordered_map<std::string, std::string> fields2 = {
        {"id", record_id2}, {"tag", "Node2Tag"}};
    auto changes2 = node2.insert_or_update(record_id2, fields2);

    // Node3 inserts a third record
    std::string record_id3 = generate_uuid();
    std::unordered_map<std::string, std::string> fields3 = {
        {"id", record_id3}, {"tag", "Node3Tag"}};
    auto changes3 = node3.insert_or_update(record_id3, fields3);

    // First round of merges
    // Merge node1's changes into node2 and node3
    sync_nodes(node1, node2, last_db_version_node2);
    sync_nodes(node1, node3, last_db_version_node3);

    // Merge node2's changes into node1 and node3
    sync_nodes(node2, node1, last_db_version_node1);
    sync_nodes(node2, node3, last_db_version_node3);

    // Merge node3's changes into node1 and node2
    sync_nodes(node3, node1, last_db_version_node1);
    sync_nodes(node3, node2, last_db_version_node2);

    // All nodes should have all three records
    assert_true(node1.get_data() == node2.get_data(),
                "Clock Synchronization: Node1 and Node2 data mismatch");
    assert_true(node2.get_data() == node3.get_data(),
                "Clock Synchronization: Node2 and Node3 data mismatch");
    assert_true(node1.get_data() == node3.get_data(),
                "Clock Synchronization: Node1 and Node3 data mismatch");

    // Check that logical clocks are properly updated
    uint64_t min_expected_clock_value = 3; // At least 3 inserts happened
    assert_true(node1.get_clock().current_time() >= min_expected_clock_value,
                "Clock Synchronization: Node1 clock too low");
    assert_true(node2.get_clock().current_time() >= min_expected_clock_value,
                "Clock Synchronization: Node2 clock too low");
    assert_true(node3.get_clock().current_time() >= min_expected_clock_value,
                "Clock Synchronization: Node3 clock too low");

    // Capture max clock before another round of merges
    uint64_t max_clock_before_merge = std::max(
        {node1.get_clock().current_time(), node2.get_clock().current_time(),
         node3.get_clock().current_time()});

    // Perform another round of merges
    sync_nodes(node1, node2, last_db_version_node2);
    sync_nodes(node2, node3, last_db_version_node3);
    sync_nodes(node3, node1, last_db_version_node1);

    // Check that clocks have been updated after merges
    assert_true(node1.get_clock().current_time() > max_clock_before_merge,
                "Clock Synchronization: Node1 clock did not update");
    assert_true(node2.get_clock().current_time() > max_clock_before_merge,
                "Clock Synchronization: Node2 clock did not update");
    assert_true(node3.get_clock().current_time() > max_clock_before_merge,
                "Clock Synchronization: Node3 clock did not update");

    // Since clocks don't need to be identical, we don't assert equality
    std::cout << "Test 'Clock Synchronization After Merges' passed."
              << std::endl;
  }

  // Test Case: Atomic Sync Per Transaction using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Node1 inserts a record
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields = {
        {"id", record_id}, {"tag", "InitialTag"}};
    auto changes_node1 = node1.insert_or_update(record_id, fields);

    // Sync immediately after the transaction
    node2.merge_changes(changes_node1);

    // Verify synchronization
    assert_true(node2.get_data().find(record_id) != node2.get_data().end(),
                "Atomic Sync: Node2 should contain the inserted record");
    assert_true(node2.get_data().at(record_id).fields.at("tag") == "InitialTag",
                "Atomic Sync: Tag should be 'InitialTag'");
    std::cout << "Test 'Atomic Sync Per Transaction' passed." << std::endl;
  }

  // Test Case: Concurrent Updates using insert_or_update
  {
    CRDT<std::string, std::string> node1(1);
    CRDT<std::string, std::string> node2(2);

    // Insert a record on node1
    std::string record_id = generate_uuid();
    std::unordered_map<std::string, std::string> fields = {
        {"id", record_id}, {"tag", "InitialTag"}};
    auto changes_insert = node1.insert_or_update(record_id, fields);

    // Merge to node2
    node2.merge_changes(changes_insert);

    // Concurrently update 'tag' on both nodes
    std::unordered_map<std::string, std::string> updates_node1 = {
        {"tag", "Node1TagUpdate"}};
    auto changes_update1 = node1.insert_or_update(record_id, updates_node1);

    std::unordered_map<std::string, std::string> updates_node2 = {
        {"tag", "Node2TagUpdate"}};
    auto changes_update2 = node2.insert_or_update(record_id, updates_node2);

    // Merge changes
    node1.merge_changes(changes_update2);
    node2.merge_changes(changes_update1);

    // Conflict resolution based on site_id (Node2 has higher site_id)
    std::string expected_tag = "Node2TagUpdate";

    assert_true(node1.get_data().at(record_id).fields.at("tag") == expected_tag,
                "Concurrent Updates: Tag should be 'Node2TagUpdate'");
    assert_true(node2.get_data().at(record_id).fields.at("tag") == expected_tag,
                "Concurrent Updates: Tag should be 'Node2TagUpdate'");
    std::cout << "Test 'Concurrent Updates' passed." << std::endl;
  }

  std::cout << "All tests passed successfully!" << std::endl;
  return 0;
}
