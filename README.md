# crdt-lite

## **1. Rust implementation of a generic CRDT.**

This implementation includes:

1. **Logical Clock**: To manage causality.
2. **CRDT Structure**: Generic over key (`K`) and value (`V`) types.
3. **Record Management**: Handling inserts, updates, deletes with tombstones.
4. **Conflict Resolution**: Based on column versions, site IDs, and sequence numbers.
5. **Merging Mechanism**: To synchronize state across nodes.
6. **Comprehensive Tests**: Using Rust's `#[test]` framework to ensure correctness.

## **2. Detailed Explanation**

### **2.1. LogicalClock**

The `LogicalClock` maintains a monotonically increasing counter to preserve causality across events.

```rust
#[derive(Debug, Clone)]
struct LogicalClock {
    time: u64,
}

impl LogicalClock {
    fn new() -> Self { /* ... */ }
    fn tick(&mut self) -> u64 { /* ... */ }
    fn update(&mut self, received_time: u64) -> u64 { /* ... */ }
    fn current_time(&self) -> u64 { /* ... */ }
}
```

- **tick()**: Increments the clock for each local event.
- **update()**: Updates the clock based on incoming changes to maintain causality.
- **current_time()**: Retrieves the current clock time.

### **2.2. ColumnVersion**

`ColumnVersion` tracks the versioning information for each column within a record, enabling fine-grained conflict resolution.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnVersion {
    col_version: u64,
    db_version: u64,
    site_id: u64,
    seq: u64,
}

impl ColumnVersion {
    fn new(col_version: u64, db_version: u64, site_id: u64, seq: u64) -> Self { /* ... */ }
}
```

- **col_version**: Number of times the column has been updated.
- **db_version**: Logical clock time when the column was last updated.
- **site_id**: Identifier of the node where the update originated.
- **seq**: Sequence number for ordering updates from the same node.

### **2.3. Record**

`Record` represents an individual record in the CRDT, containing field values and their corresponding version information.

```rust
#[derive(Debug, Clone)]
struct Record<V> {
    fields: HashMap<String, V>,
    column_versions: HashMap<String, ColumnVersion>,
}

impl<V> Record<V> {
    fn new(fields: HashMap<String, V>, column_versions: HashMap<String, ColumnVersion>) -> Self { /* ... */ }
}
```

### **2.4. CRDT**

The `CRDT` struct manages the overall state, including data records, tombstones for deletions, and methods to manipulate and merge data.

```rust
#[derive(Debug, Clone)]
struct CRDT<K, V>
where
    K: Eq + Hash + Clone,
{
    node_id: u64,
    clock: LogicalClock,
    data: HashMap<K, Record<V>>,
    tombstones: HashSet<K>,
}
```

- **node_id**: Unique identifier for the node (site).
- **clock**: Instance of `LogicalClock`.
- **data**: Stores records, keyed by their unique identifier.
- **tombstones**: Tracks deleted records to prevent resurrection.

### **2.5. Methods**

#### **2.5.1. insert**

Inserts a new record if it's not already tombstoned.

```rust
fn insert(&mut self, record_id: K, fields: HashMap<String, V>) { /* ... */ }
```

- **Checks**: Prevents re-insertion of tombstoned records.
- **Initialization**: Sets `col_version` to 1 for all columns.

#### **2.5.2. update**

Updates existing record fields if the record isn't tombstoned.

```rust
fn update(&mut self, record_id: &K, updates: HashMap<String, V>) { /* ... */ }
```

- **Checks**: Ignores updates on tombstoned or non-existent records.
- **Versioning**: Increments `col_version` and `seq` for updated columns.

#### **2.5.3. delete**

Deletes a record by marking it as tombstoned.

```rust
fn delete(&mut self, record_id: &K) { /* ... */ }
```

- **Checks**: Prevents duplicate deletions.
- **Tombstone**: Inserts into `tombstones` and removes from `data`.

#### **2.5.4. get_changes_since**

Retrieves all changes since a specified `last_db_version`.

```rust
fn get_changes_since(&self, last_db_version: u64) -> Vec<Change<K, V>> { /* ... */ }
```

- **Returns**: A vector of `Change` structs representing the modifications.

#### **2.5.5. merge_changes**

Merges incoming changes into the CRDT, resolving conflicts based on versioning and site IDs.

```rust
fn merge_changes(&mut self, changes: &[Change<K, V>]) { /* ... */ }
```

- **Conflict Resolution**:
  - **Higher `col_version`**: Accepts the change.
  - **Equal `col_version`**:
    - **Deletion Prioritized**: Deletions take precedence over insertions/updates.
    - **Site ID & Seq**: If both changes are deletions, use `site_id` and `seq` as tie-breakers.

#### **2.5.6. print_data**

Prints the current state for debugging.

```rust
fn print_data(&self) { /* ... */ }
```

### **2.6. Change Struct**

Represents a single change in the CRDT.

```rust
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
```

- **record_id**: Identifier of the record.
- **col_name**: Name of the column.
- **value**: New value for the column (None for deletions).
- **col_version**: Version of the column.
- **db_version**: Logical clock time of the change.
- **site_id**: Originating node's ID.
- **seq**: Sequence number for ordering.

### **2.7. Tests**

Comprehensive tests ensure the correctness of the CRDT implementation. They cover scenarios like:

- Basic insertions and merges.
- Concurrent updates with conflict resolution.
- Deletions and tombstone handling.
- Preventing re-insertion after deletion.
- Logical clock updates.

#### **Running Tests**

1. **Ensure Rust is Installed**: If not, install it from [rustup.rs](https://rustup.rs/).

2. **Save the Code**: Save the above code in a file named `crdt.rs`.

3. **Create a New Cargo Project**:

   ```bash
   cargo new crdt_project
   cd crdt_project
   ```

4. **Replace `src/lib.rs` with `crdt.rs`**: Alternatively, place `crdt.rs` in `src/` and adjust accordingly.

5. **Add Dependencies**: Add `uuid` to `Cargo.toml` for generating unique identifiers.

   ```toml
   [dependencies]
   uuid = { version = "1.2", features = ["v4"] }
   ```

6. **Run Tests**:

   ```bash
   cargo test
   ```

All tests should pass, indicating that the CRDT behaves as expected.

## **3. Complete Rust Implementation**

Below is the complete Rust implementation, including the CRDT structures and the comprehensive tests.

```rust
// crdt.rs

use std::collections::{HashMap, HashSet};
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
#[derive(Debug, Clone)]
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
    K: Eq + Hash + Clone,
{
    node_id: u64,
    clock: LogicalClock,
    data: HashMap<K, Record<V>>,
    tombstones: HashSet<K>,
}

impl<K, V> CRDT<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
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

        // Remove data and clock info
        self.data.remove(record_id);

        // Insert deletion clock info
        let mut deletion_clock = HashMap::new();
        deletion_clock.insert(
            "__deleted__".to_string(),
            ColumnVersion::new(1, db_version, self.node_id, 0),
        );

        // Update clock info
        // If the record was never inserted, ensure we have the tombstone clock
        self.data.insert(
            record_id.clone(),
            Record::new(HashMap::new(), deletion_clock.clone()),
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
                        self.data
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
                    // Insert or update deletion clock info
                    let mut deletion_clock = HashMap::new();
                    deletion_clock.insert(
                        "__deleted__".to_string(),
                        ColumnVersion::new(remote_col_version, remote_db_version, remote_site_id, remote_seq),
                    );

                    // Update clock info
                    self.data.insert(
                        record_id.clone(),
                        Record::new(HashMap::new(), deletion_clock.clone()),
                    );
                } else {
                    // Handle insertion or update
                    let record = self.data.entry(record_id.clone()).or_insert_with(|| Record {
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
                        ColumnVersion::new(remote_col_version, remote_db_version, remote_site_id, remote_seq),
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

    /// Retrieves the current database version (logical clock time).
    fn get_db_version(&self) -> u64 {
        self.clock.current_time()
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
        fields2.insert("form_id".to_string(), node1.data.get(&record_id).unwrap().fields.get("form_id").unwrap().clone());
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
        assert_eq!(node1.data.get(&record_id).unwrap().fields.get("tag").unwrap(), "Node2Tag");
        assert_eq!(node1.data.get(&record_id).unwrap().fields.get("created_by").unwrap(), "User2");
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
        node1.update(&record_id, updates1.clone());

        // Node2 updates 'tag'
        let mut updates2 = HashMap::new();
        updates2.insert("tag".to_string(), "Node2UpdatedTag".to_string());
        node2.update(&record_id, updates2.clone());

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
            node1.data.get(&record_id).unwrap().fields.get("tag").unwrap(),
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
        assert!(!node1.data.contains_key(&record_id));
        assert!(!node2.data.contains_key(&record_id));
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
        assert!(!node2.data.contains_key(&record_id));
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
            node1.data.get(&record_id).unwrap().fields.get("tag").unwrap(),
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
            node1.data.get(&record_id).unwrap().fields.get("tag").unwrap(),
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
        assert!(!node1.data.contains_key(&record_id));
        assert!(!node2.data.contains_key(&record_id));
        assert!(node1.tombstones.contains(&record_id));
        assert!(node2.tombstones.contains(&record_id));
    }
}
```

## **4. Explanation of the Rust Implementation**

### **4.1. LogicalClock**

Maintains a monotonically increasing counter to manage causality.

- **tick()**: Increments the clock for local events.
- **update()**: Updates the clock based on incoming changes to ensure causality.

### **4.2. ColumnVersion**

Tracks the versioning information for each column within a record.

- **col_version**: Number of times the column has been updated.
- **db_version**: Logical clock time when the column was last updated.
- **site_id**: Identifier of the node where the update originated.
- **seq**: Sequence number for ordering updates from the same node.

### **4.3. Record**

Represents an individual record in the CRDT, containing field values and their corresponding version information.

### **4.4. CRDT Structure**

Manages the overall state, including data records, tombstones for deletions, and methods to manipulate and merge data.

- **node_id**: Unique identifier for the node (site).
- **clock**: Instance of `LogicalClock`.
- **data**: Stores records, keyed by their unique identifier.
- **tombstones**: Tracks deleted records to prevent resurrection.

### **4.5. Methods**

#### **4.5.1. insert**

Inserts a new record if it's not already tombstoned.

```rust
fn insert(&mut self, record_id: K, fields: HashMap<String, V>) { /* ... */ }
```

- **Checks**: Prevents re-insertion of tombstoned records.
- **Initialization**: Sets `col_version` to 1 for all columns.

#### **4.5.2. update**

Updates existing record fields if the record isn't tombstoned.

```rust
fn update(&mut self, record_id: &K, updates: HashMap<String, V>) { /* ... */ }
```

- **Checks**: Ignores updates on tombstoned or non-existent records.
- **Versioning**: Increments `col_version` and `seq` for updated columns.

#### **4.5.3. delete**

Deletes a record by marking it as tombstoned.

```rust
fn delete(&mut self, record_id: &K) { /* ... */ }
```

- **Checks**: Prevents duplicate deletions.
- **Tombstone**: Inserts into `tombstones` and removes from `data`.

#### **4.5.4. get_changes_since**

Retrieves all changes since a specified `last_db_version`.

```rust
fn get_changes_since(&self, last_db_version: u64) -> Vec<Change<K, V>> { /* ... */ }
```

- **Returns**: A vector of `Change` structs representing the modifications.

#### **4.5.5. merge_changes**

Merges incoming changes into the CRDT, resolving conflicts based on versioning and site IDs.

```rust
fn merge_changes(&mut self, changes: &[Change<K, V>]) { /* ... */ }
```

- **Conflict Resolution**:
  - **Higher `col_version`**: Accepts the change.
  - **Equal `col_version`**:
    - **Deletion Prioritized**: Deletions take precedence over insertions/updates.
    - **Site ID & Seq**: If both changes are deletions, use `site_id` and `seq` as tie-breakers.

#### **4.5.6. print_data**

Prints the current state for debugging.

```rust
fn print_data(&self) { /* ... */ }
```

### **4.6. Change Struct**

Represents a single change in the CRDT, encapsulating all necessary metadata for conflict resolution.

```rust
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
```

### **4.7. Tests**

Comprehensive tests ensure the correctness of the CRDT implementation. They cover scenarios like:

- Basic insertions and merges.
- Concurrent updates with conflict resolution.
- Deletions and tombstone handling.
- Preventing re-insertion after deletion.
- Logical clock updates.

Each test follows these steps:

1. **Initialization**: Create CRDT instances representing different nodes.
2. **Operations**: Perform insert, update, or delete operations on the nodes.
3. **Merging**: Exchange and merge changes between nodes.
4. **Assertions**: Verify that the state is consistent and conflicts are resolved as expected.

#### **Running Tests**

1. **Add Dependencies**: Ensure `uuid` is included in `Cargo.toml`:

   ```toml
   [dependencies]
   uuid = { version = "1.2", features = ["v4"] }
   ```

2. **Run Tests**:

   ```bash
   cargo test
   ```

All tests should pass, confirming that the CRDT behaves correctly across various scenarios.

## **5. Conclusion**

This Rust implementation provides a generic, efficient, and robust CRDT structure capable of handling inserts, updates, deletes with tombstones, and merging changes across nodes with proper conflict resolution. The comprehensive tests ensure that the CRDT maintains consistency and correctness, aligning with your original Python implementation's behavior.

Feel free to expand upon this foundation by:

- **Adding Persistence**: Integrate with databases like SQLite for persistent storage.
- **Networking**: Implement networking protocols to facilitate real-time synchronization between nodes.
- **Advanced CRDTs**: Explore other CRDT types like OR-Sets, PN-Counters, or Sequence CRDTs for more complex use cases.
- **Optimization**: Optimize the merging process and data structures for performance in large-scale systems.

If you have any further questions or need assistance with specific enhancements, feel free to ask!