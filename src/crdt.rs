// crdt.rs

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

/// Represents a logical clock for maintaining causality.
#[derive(Debug, Clone)]
struct LogicalClock {
  time: u64,
}

impl LogicalClock {
  /// Creates a new LogicalClock starting at time 0.
  fn new() -> Self {
    LogicalClock { time: 0 }
  }

  /// Increments the clock for a local event.
  fn tick(&mut self) -> u64 {
    self.time += 1;
    self.time
  }

  /// Updates the clock based on a received time.
  fn update(&mut self, received_time: u64) -> u64 {
    self.time = std::cmp::max(self.time, received_time);
    self.time += 1;
    self.time
  }

  /// Retrieves the current time.
  fn current_time(&self) -> u64 {
    self.time
  }
}

/// Represents the version information for a column.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnVersion {
  col_version: u64,
  db_version: u64,
  site_id: u64,
  seq: u64,
}

impl ColumnVersion {
  /// Creates a new ColumnVersion.
  fn new(col_version: u64, db_version: u64, site_id: u64, seq: u64) -> Self {
    ColumnVersion {
      col_version,
      db_version,
      site_id,
      seq,
    }
  }
}

/// Represents a record in the CRDT.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record<V> {
  fields: HashMap<String, V>,
  column_versions: HashMap<String, ColumnVersion>,
}

impl<V> Record<V> {
  /// Creates a new Record.
  fn new(fields: HashMap<String, V>, column_versions: HashMap<String, ColumnVersion>) -> Self {
    Record {
      fields,
      column_versions,
    }
  }
}

/// Represents the CRDT structure, generic over key (`K`) and value (`V`) types.
#[derive(Debug, Clone)]
struct CRDT<K, V>
where
  K: Eq + Hash + Clone + Debug,
{
  node_id: u64,
  clock: LogicalClock,
  data: HashMap<K, Record<V>>,
  tombstones: HashSet<K>,
}

impl<K, V> CRDT<K, V>
where
  K: Eq + Hash + Clone + Debug,
  V: Clone + Debug,
{
  /// Creates a new CRDT instance.
  fn new(node_id: u64) -> Self {
    CRDT {
      node_id,
      clock: LogicalClock::new(),
      data: HashMap::new(),
      tombstones: HashSet::new(),
    }
  }

  /// Inserts a new record into the CRDT.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `fields` - A hashmap of field names to their values.
  fn insert(&mut self, record_id: K, fields: HashMap<String, V>) {
    // Prevent re-insertion if the record is already tombstoned
    if self.tombstones.contains(&record_id) {
      println!(
        "Insert ignored: Record {:?} is already deleted (tombstoned).",
        record_id
      );
      return;
    }

    let db_version = self.clock.tick();

    // Initialize column versions
    let mut column_versions = HashMap::new();
    for (col_name, _) in &fields {
      column_versions.insert(
        col_name.clone(),
        ColumnVersion::new(1, db_version, self.node_id, 0),
      );
    }

    // Insert the record
    let record = Record::new(fields, column_versions);
    self.data.insert(record_id.clone(), record);
  }

  /// Updates an existing record's fields.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `updates` - A hashmap of field names to their new values.
  fn update(&mut self, record_id: &K, updates: HashMap<String, V>) {
    if self.tombstones.contains(record_id) {
      println!(
        "Update ignored: Record {:?} is deleted (tombstoned).",
        record_id
      );
      return;
    }

    if let Some(record) = self.data.get_mut(record_id) {
      let db_version = self.clock.tick();

      for (col_name, value) in updates {
        // Update the value
        record.fields.insert(col_name.clone(), value);

        // Update the clock for this column
        let col_info = record.column_versions.get_mut(&col_name).unwrap();
        col_info.col_version += 1;
        col_info.db_version = db_version;
        col_info.seq += 1;
        col_info.site_id = self.node_id;
      }
    } else {
      println!("Update ignored: Record {:?} does not exist.", record_id);
    }
  }

  /// Deletes a record by marking it as tombstoned.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  fn delete(&mut self, record_id: &K) {
    if self.tombstones.contains(record_id) {
      println!(
        "Delete ignored: Record {:?} is already deleted (tombstoned).",
        record_id
      );
      return;
    }

    let db_version = self.clock.tick();

    // Mark as tombstone
    self.tombstones.insert(record_id.clone());

    // Remove data
    self.data.remove(record_id);

    // Insert deletion clock info
    let mut deletion_clock = HashMap::new();
    deletion_clock.insert(
      "__deleted__".to_string(),
      ColumnVersion::new(1, db_version, self.node_id, 0),
    );

    // Store deletion info in a separate structure
    self.data.insert(
      record_id.clone(),
      Record::new(HashMap::new(), deletion_clock),
    );
  }

  /// Retrieves all changes since a given `last_db_version`.
  ///
  /// # Arguments
  ///
  /// * `last_db_version` - The database version to retrieve changes since.
  ///
  /// # Returns
  ///
  /// A vector of changes represented as tuples.
  fn get_changes_since(&self, last_db_version: u64) -> Vec<Change<K, V>> {
    let mut changes = Vec::new();

    for (record_id, columns) in &self.data {
      for (col_name, clock_info) in columns.column_versions.iter() {
        if clock_info.db_version > last_db_version {
          let value = if col_name != "__deleted__" {
            self
              .data
              .get(record_id)
              .and_then(|r| r.fields.get(col_name))
              .cloned()
          } else {
            None
          };

          changes.push(Change {
            record_id: record_id.clone(),
            col_name: col_name.clone(),
            value,
            col_version: clock_info.col_version,
            db_version: clock_info.db_version,
            site_id: clock_info.site_id,
            seq: clock_info.seq,
          });
        }
      }
    }

    changes
  }

  /// Merges a set of incoming changes into the CRDT.
  ///
  /// # Arguments
  ///
  /// * `changes` - A slice of changes to merge.
  fn merge_changes(&mut self, changes: &[Change<K, V>]) {
    for change in changes {
      let record_id = &change.record_id;
      let col_name = &change.col_name;
      let remote_col_version = change.col_version;
      let remote_db_version = change.db_version;
      let remote_site_id = change.site_id;
      let remote_seq = change.seq;
      let remote_value = change.value.clone();

      // Update logical clock
      self.clock.update(remote_db_version);

      // Retrieve local column info
      let local_col_info = self
        .data
        .get(record_id)
        .and_then(|r| r.column_versions.get(col_name))
        .cloned();

      // Determine if we should accept the remote change
      let should_accept = match local_col_info {
        None => true,
        Some(ref local) => {
          if remote_col_version > local.col_version {
            true
          } else if remote_col_version == local.col_version {
            // Prioritize deletions over inserts/updates
            if col_name == "__deleted__" && change.col_name != "__deleted__" {
              true
            } else if change.col_name == "__deleted__" && col_name != "__deleted__" {
              false
            } else if change.col_name == "__deleted__" && col_name == "__deleted__" {
              // If both are deletions, use site_id and seq as tie-breakers
              if remote_site_id > local.site_id {
                true
              } else if remote_site_id == local.site_id {
                remote_seq > local.seq
              } else {
                false
              }
            } else {
              // Tie-breaker using site ID and seq
              if remote_site_id > local.site_id {
                true
              } else if remote_site_id == local.site_id {
                remote_seq > local.seq
              } else {
                false
              }
            }
          } else {
            false
          }
        }
      };

      if should_accept {
        if col_name == "__deleted__" {
          // Handle deletion
          self.tombstones.insert(record_id.clone());
          self.data.remove(record_id);
          // Insert deletion clock info
          let mut deletion_clock = HashMap::new();
          deletion_clock.insert(
            "__deleted__".to_string(),
            ColumnVersion::new(
              remote_col_version,
              remote_db_version,
              remote_site_id,
              remote_seq,
            ),
          );

          // Store deletion info in a separate structure
          self.data.insert(
            record_id.clone(),
            Record::new(HashMap::new(), deletion_clock),
          );
        } else if !self.tombstones.contains(record_id) {
          // Handle insertion or update only if the record is not tombstoned
          let record = self
            .data
            .entry(record_id.clone())
            .or_insert_with(|| Record {
              fields: HashMap::new(),
              column_versions: HashMap::new(),
            });

          // Insert or update the field value
          if let Some(val) = remote_value.clone() {
            record.fields.insert(col_name.clone(), val);
          }

          // Update the column version info
          record.column_versions.insert(
            col_name.clone(),
            ColumnVersion::new(
              remote_col_version,
              remote_db_version,
              remote_site_id,
              remote_seq,
            ),
          );
        }
      }
    }
  }

  /// Prints the current data and tombstones for debugging purposes.
  fn print_data(&self) {
    println!("Node {} Data:", self.node_id);
    for (record_id, record) in &self.data {
      if self.tombstones.contains(record_id) {
        continue; // Skip tombstoned records
      }
      println!("ID: {:?}", record_id);
      for (key, value) in &record.fields {
        println!("  {}: {:?}", key, value);
      }
    }
    println!("Tombstones: {:?}", self.tombstones);
    println!();
  }
}

/// Represents a single change in the CRDT.
#[derive(Debug, Clone)]
struct Change<K, V> {
  record_id: K,
  col_name: String,
  value: Option<V>,
  col_version: u64,
  db_version: u64,
  site_id: u64,
  seq: u64,
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::collections::HashMap;
  use uuid::Uuid;

  /// Helper function to create a unique UUID string.
  fn new_uuid() -> String {
    Uuid::new_v4().to_string()
  }

  #[test]
  fn test_basic_insert_and_merge() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Node1 inserts a record
    let record_id = new_uuid();
    let mut fields1 = HashMap::new();
    fields1.insert("id".to_string(), record_id.clone());
    fields1.insert("form_id".to_string(), new_uuid());
    fields1.insert("tag".to_string(), "Node1Tag".to_string());
    fields1.insert("created_at".to_string(), "2023-10-01T12:00:00Z".to_string());
    fields1.insert("created_by".to_string(), "User1".to_string());

    node1.insert(record_id.clone(), fields1);

    // Node2 inserts the same record with different data
    let mut fields2 = HashMap::new();
    fields2.insert("id".to_string(), record_id.clone());
    fields2.insert(
      "form_id".to_string(),
      node1
        .data
        .get(&record_id)
        .unwrap()
        .fields
        .get("form_id")
        .unwrap()
        .clone(),
    );
    fields2.insert("tag".to_string(), "Node2Tag".to_string());
    fields2.insert("created_at".to_string(), "2023-10-01T12:05:00Z".to_string());
    fields2.insert("created_by".to_string(), "User2".to_string());

    node2.insert(record_id.clone(), fields2);

    // Merge node2 into node1
    let changes_from_node2 = node2.get_changes_since(0);
    node1.merge_changes(&changes_from_node2);

    // Merge node1 into node2
    let changes_from_node1 = node1.get_changes_since(0);
    node2.merge_changes(&changes_from_node1);

    // Both nodes should resolve the conflict and have the same data
    assert_eq!(node1.data, node2.data);
    assert_eq!(
      node1
        .data
        .get(&record_id)
        .unwrap()
        .fields
        .get("tag")
        .unwrap(),
      "Node2Tag"
    );
    assert_eq!(
      node1
        .data
        .get(&record_id)
        .unwrap()
        .fields
        .get("created_by")
        .unwrap(),
      "User2"
    );
  }

  #[test]
  fn test_updates_with_conflicts() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Insert a shared record
    let record_id = new_uuid();
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), record_id.clone());
    fields.insert("tag".to_string(), "InitialTag".to_string());

    node1.insert(record_id.clone(), fields.clone());
    node2.insert(record_id.clone(), fields.clone());

    // Node1 updates 'tag'
    let mut updates1 = HashMap::new();
    updates1.insert("tag".to_string(), "Node1UpdatedTag".to_string());
    node1.update(&record_id, updates1);

    // Node2 updates 'tag'
    let mut updates2 = HashMap::new();
    updates2.insert("tag".to_string(), "Node2UpdatedTag".to_string());
    node2.update(&record_id, updates2);

    // Merge changes
    let changes_from_node1 = node1.get_changes_since(0);
    node2.merge_changes(&changes_from_node1);
    let changes_from_node2 = node2.get_changes_since(0);
    node1.merge_changes(&changes_from_node2);

    // Conflict resolved
    // Since col_versions are equal, tie-breaker is site_id (Node2 has higher site_id)
    let expected_tag = if node2.node_id > node1.node_id {
      "Node2UpdatedTag"
    } else {
      "Node1UpdatedTag"
    };

    assert_eq!(
      node1
        .data
        .get(&record_id)
        .unwrap()
        .fields
        .get("tag")
        .unwrap(),
      expected_tag
    );
    assert_eq!(node1.data, node2.data);
  }

  #[test]
  fn test_delete_and_merge() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Insert and sync a record
    let record_id = new_uuid();
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), record_id.clone());
    fields.insert("tag".to_string(), "ToBeDeleted".to_string());

    node1.insert(record_id.clone(), fields.clone());

    // Merge to node2
    let changes = node1.get_changes_since(0);
    node2.merge_changes(&changes);

    // Node1 deletes the record
    node1.delete(&record_id);

    // Merge the deletion to node2
    let deletion_changes = node1.get_changes_since(0);
    node2.merge_changes(&deletion_changes);

    // Both nodes should reflect the deletion
    assert!(node1.data.get(&record_id).unwrap().fields.is_empty());
    assert!(node2.data.get(&record_id).unwrap().fields.is_empty());
    assert!(node1.tombstones.contains(&record_id));
    assert!(node2.tombstones.contains(&record_id));
  }

  #[test]
  fn test_tombstone_handling() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Insert a record and delete it on node1
    let record_id = new_uuid();
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), record_id.clone());
    fields.insert("tag".to_string(), "Temporary".to_string());

    node1.insert(record_id.clone(), fields.clone());
    node1.delete(&record_id);

    // Node2 inserts the same record
    node2.insert(record_id.clone(), fields.clone());

    // Merge changes
    let changes_from_node1 = node1.get_changes_since(0);
    node2.merge_changes(&changes_from_node1);

    // Node2 should respect the tombstone
    assert!(node2.data.get(&record_id).unwrap().fields.is_empty());
    assert!(node2.tombstones.contains(&record_id));
  }

  #[test]
  fn test_conflict_resolution_with_site_id_and_seq() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Both nodes insert a record with the same id
    let record_id = new_uuid();
    let mut fields1 = HashMap::new();
    fields1.insert("id".to_string(), record_id.clone());
    fields1.insert("tag".to_string(), "Node1Tag".to_string());

    let mut fields2 = HashMap::new();
    fields2.insert("id".to_string(), record_id.clone());
    fields2.insert("tag".to_string(), "Node2Tag".to_string());

    node1.insert(record_id.clone(), fields1.clone());
    node2.insert(record_id.clone(), fields2.clone());

    // Both nodes update the 'tag' field multiple times
    let mut updates1 = HashMap::new();
    updates1.insert("tag".to_string(), "Node1Tag1".to_string());
    node1.update(&record_id, updates1.clone());

    updates1.insert("tag".to_string(), "Node1Tag2".to_string());
    node1.update(&record_id, updates1.clone());

    let mut updates2 = HashMap::new();
    updates2.insert("tag".to_string(), "Node2Tag1".to_string());
    node2.update(&record_id, updates2.clone());

    updates2.insert("tag".to_string(), "Node2Tag2".to_string());
    node2.update(&record_id, updates2.clone());

    // Merge changes
    let changes_from_node1 = node1.get_changes_since(0);
    node2.merge_changes(&changes_from_node1);
    let changes_from_node2 = node2.get_changes_since(0);
    node1.merge_changes(&changes_from_node2);

    // The node with the higher site_id and seq should win
    let expected_tag = if node2.node_id > node1.node_id {
      "Node2Tag2"
    } else {
      "Node1Tag2"
    };

    assert_eq!(
      node1
        .data
        .get(&record_id)
        .unwrap()
        .fields
        .get("tag")
        .unwrap(),
      expected_tag
    );
    assert_eq!(node1.data, node2.data);
  }

  #[test]
  fn test_logical_clock_update() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Node1 inserts a record
    let record_id = new_uuid();
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), record_id.clone());
    fields.insert("tag".to_string(), "Node1Tag".to_string());

    node1.insert(record_id.clone(), fields.clone());

    // Node2 receives the change
    let changes = node1.get_changes_since(0);
    node2.merge_changes(&changes);

    // Node2's clock should be updated
    assert!(node2.clock.current_time() > 0);
    assert!(node2.clock.current_time() >= node1.clock.current_time());
  }

  #[test]
  fn test_merge_without_conflicts() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Node1 inserts a record
    let record_id1 = new_uuid();
    let mut fields1 = HashMap::new();
    fields1.insert("id".to_string(), record_id1.clone());
    fields1.insert("tag".to_string(), "Node1Record".to_string());

    node1.insert(record_id1.clone(), fields1.clone());

    // Node2 inserts a different record
    let record_id2 = new_uuid();
    let mut fields2 = HashMap::new();
    fields2.insert("id".to_string(), record_id2.clone());
    fields2.insert("tag".to_string(), "Node2Record".to_string());

    node2.insert(record_id2.clone(), fields2.clone());

    // Merge changes
    let changes_from_node1 = node1.get_changes_since(0);
    node2.merge_changes(&changes_from_node1);

    let changes_from_node2 = node2.get_changes_since(0);
    node1.merge_changes(&changes_from_node2);

    // Both nodes should have both records
    assert!(node1.data.contains_key(&record_id1));
    assert!(node1.data.contains_key(&record_id2));
    assert!(node2.data.contains_key(&record_id1));
    assert!(node2.data.contains_key(&record_id2));
    assert_eq!(node1.data, node2.data);
  }

  #[test]
  fn test_multiple_merges() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Node1 inserts a record
    let record_id = new_uuid();
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), record_id.clone());
    fields.insert("tag".to_string(), "InitialTag".to_string());

    node1.insert(record_id.clone(), fields.clone());

    // Merge to node2
    let changes = node1.get_changes_since(0);
    node2.merge_changes(&changes);

    // Node2 updates the record
    let mut updates2 = HashMap::new();
    updates2.insert("tag".to_string(), "UpdatedByNode2".to_string());
    node2.update(&record_id, updates2.clone());

    // Node1 updates the record
    let mut updates1 = HashMap::new();
    updates1.insert("tag".to_string(), "UpdatedByNode1".to_string());
    node1.update(&record_id, updates1.clone());

    // Merge changes
    let changes_from_node2 = node2.get_changes_since(0);
    node1.merge_changes(&changes_from_node2);
    let changes_from_node1 = node1.get_changes_since(0);
    node2.merge_changes(&changes_from_node1);

    // Conflict resolved
    let expected_tag = if node2.node_id > node1.node_id {
      "UpdatedByNode2"
    } else {
      "UpdatedByNode1"
    };

    assert_eq!(
      node1
        .data
        .get(&record_id)
        .unwrap()
        .fields
        .get("tag")
        .unwrap(),
      expected_tag
    );
    assert_eq!(node1.data, node2.data);
  }

  #[test]
  fn test_inserting_after_deletion() {
    // Initialize two nodes
    let mut node1: CRDT<String, String> = CRDT::new(1);
    let mut node2: CRDT<String, String> = CRDT::new(2);

    // Node1 inserts and deletes a record
    let record_id = new_uuid();
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), record_id.clone());
    fields.insert("tag".to_string(), "Temporary".to_string());

    node1.insert(record_id.clone(), fields.clone());
    node1.delete(&record_id);

    // Merge deletion to node2
    let changes_from_node1 = node1.get_changes_since(0);
    node2.merge_changes(&changes_from_node1);

    // Node2 tries to insert the same record
    node2.insert(record_id.clone(), fields.clone());

    // Merge changes
    let changes_from_node2 = node2.get_changes_since(0);
    node1.merge_changes(&changes_from_node2);

    // The deletion should prevail
    assert!(node1.data.get(&record_id).unwrap().fields.is_empty());
    assert!(node2.data.get(&record_id).unwrap().fields.is_empty());
    assert!(node1.tombstones.contains(&record_id));
    assert!(node2.tombstones.contains(&record_id));
  }
}
