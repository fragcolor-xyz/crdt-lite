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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
