# CRDT-Lite

> [!WARNING]
> This project is in early development and not intended for production use.

A lightweight implementation of Conflict-free Replicated Data Types (CRDTs) in both **Rust** and **C++**. CRDT-Lite provides generic CRDT structures for building distributed systems requiring eventual consistency.

CRDT-Lite is currently being used in [Formabble](https://formabble.com), a collaborative game engine, and will be integrated into a derived product that we will announce soon.

## What's Inside

This library includes two CRDT implementations:

1. **Column-Based CRDT** (Rust: `src/lib.rs`, C++: `crdt.hpp`) - Generic key-value store with fine-grained column-level conflict resolution
2. **Text CRDT** (C++: `text_crdt.hpp`) - Line-based collaborative text editor with fractional positioning

Both Rust and C++ implementations share the same core algorithms and maintain API compatibility.

## Features

### Column-Based CRDT

- ✅ **Generic over key/value types** - Use any type that meets the requirements
- ✅ **Fine-grained conflict resolution** - Column-level (field-level) versioning
- ✅ **Last-Write-Wins semantics** - Deterministic conflict resolution using logical clocks
- ✅ **Tombstone-based deletion** - Proper handling of deleted records across nodes
- ✅ **Parent-child hierarchies** - Support for layered CRDT structures
- ✅ **Custom merge rules** - Implement your own conflict resolution strategies
- ✅ **Change compression** - Optimized transmission by removing redundant changes
- ✅ **Efficient sync** - Track version boundaries to skip unchanged records

### Text CRDT

- ✅ **Line-based editing** - Collaborative text with line granularity
- ✅ **Fractional positioning** - Infinite density between any two lines
- ✅ **Multiple merge strategies** - Last-Write-Wins and Both-Writes-Win support
- ✅ **Dual indexing** - Fast position-ordered iteration and ID-based lookup
- ✅ **Conflict detection** - Preserve or resolve concurrent edits
- ✅ **Change streaming** - Real-time collaboration support

### Persistence Layer (Rust)

- ✅ **Write-Ahead Log (WAL)** - Durable operation log with automatic rotation
- ✅ **MessagePack Snapshots** - Schema-evolution-friendly format (add fields without breaking old snapshots)
- ✅ **Incremental Snapshots** - 95% I/O reduction by storing only changed records
- ✅ **Optional Compression** - zstd compression for 50-70% additional size reduction
- ✅ **Crash Recovery** - Automatic recovery from snapshot + WAL replay
- ✅ **Hook System** - Callbacks for post-operation, snapshot, and WAL sealing events
- ✅ **Batch Collector** - Accumulate changes for efficient network broadcasting
- ✅ **Upload Tracking** - Track uploaded snapshots/WAL segments for safe cleanup
- ✅ **Auto-cleanup** - Configurable retention policies to prevent disk bloat
- ✅ **Windows Support** - Proper file handle management for strict locking

### Platform Support

- ✅ **`no_std` compatible** - Works in embedded systems and other `no_std` environments (requires `alloc`)
- ✅ **Zero-cost abstractions** - Uses hashbrown (same HashMap as std) in `no_std` mode
- ✅ **Optional std** - std is enabled by default for backwards compatibility

#### Using in `no_std` Environments

The Rust implementation supports `no_std` environments with allocator support.

**Requirements:**
- **`alloc` crate required**: This library needs an allocator for `Vec`, `HashMap`, `Arc`, etc.
- **Example setup**:
  ```rust
  #![no_std]
  extern crate alloc;

  use crdt_lite::CRDT;
  // ... use as normal
  ```

**Cargo.toml configuration:**

```toml
[dependencies]
# For no_std with basic CRDT functionality (requires alloc feature)
crdt-lite = { version = "0.5", default-features = false, features = ["alloc"] }

# For no_std with JSON serialization
crdt-lite = { version = "0.5", default-features = false, features = ["alloc", "json"] }

# For no_std with binary serialization (bincode)
crdt-lite = { version = "0.5", default-features = false, features = ["alloc", "binary"] }

# For standard environments (default, uses std::collections::HashMap)
crdt-lite = { version = "0.5", features = ["json"] }
```

**Implementation Notes:**
- `no_std` mode uses `hashbrown::HashMap`, which is the same underlying implementation that `std::collections::HashMap` uses (identical performance)
- The `std` feature is enabled by default for backwards compatibility
- The `alloc` feature is required for `no_std` environments and pulls in `hashbrown` only when needed
- When using std (default), `hashbrown` is not compiled, reducing dependency bloat
- Binary serialization uses `bincode` 2.0 which has full `no_std` support

### NodeId Type Configuration

By default, `NodeId` is `u64`. For applications using UUID-based node identifiers, enable the `node-id-u128` feature:

```toml
[dependencies]
crdt-lite = { version = "0.5", features = ["node-id-u128"] }
```

**Why u128?**
- `u64` provides 2^64 (~18 quintillion) unique IDs
- With random UUID generation, birthday paradox makes collisions likely after ~4 billion IDs
- `u128` provides 2^128 unique IDs, eliminating collision concerns for UUID-based systems
- C++ implementation allows customizing `CrdtNodeId` via preprocessor (see `crdt.hpp`)

## Quick Start

### Rust Implementation

```bash
cargo add crdt-lite  # (when published)
# or add to Cargo.toml: crdt-lite = "0.5"
```

```rust
use crdt_lite::{CRDT, DefaultMergeRule};

// Create two CRDT nodes
let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

// Node 1: Insert data
let changes1 = node1.insert_or_update(
    &"user1".to_string(),
    vec![
        ("name".to_string(), "Alice".to_string()),
        ("age".to_string(), "30".to_string()),
    ],
);

// Node 2: Insert conflicting data
let changes2 = node2.insert_or_update(
    &"user1".to_string(),
    vec![
        ("name".to_string(), "Bob".to_string()),
        ("age".to_string(), "25".to_string()),
    ],
);

// Merge changes bidirectionally
let merge_rule = DefaultMergeRule;
node1.merge_changes(changes2, &merge_rule);
node2.merge_changes(changes1, &merge_rule);

// Both nodes converge to same state (node2 wins due to higher node_id)
assert_eq!(node1.get_data(), node2.get_data());
```

**Run tests:**
```bash
cargo test
```

### C++ Implementation

**Compile and run:**
```bash
# Column CRDT tests
g++ -std=c++20 -o crdt tests.cpp && ./crdt

# Text CRDT tests
g++ -std=c++20 -o text_crdt test_text_crdt.cpp && ./text_crdt
```

**Column CRDT example:**
```cpp
#include "crdt.hpp"

CRDT<std::string, std::string> node1(1);
CRDT<std::string, std::string> node2(2);

// Node 1: Insert data
std::vector<Change<std::string, std::string>> changes1;
node1.insert_or_update("user1", changes1,
    std::make_pair("name", "Alice"),
    std::make_pair("age", "30")
);

// Node 2: Insert conflicting data
std::vector<Change<std::string, std::string>> changes2;
node2.insert_or_update("user1", changes2,
    std::make_pair("name", "Bob"),
    std::make_pair("age", "25")
);

// Merge changes bidirectionally
node1.merge_changes(std::move(changes2));
node2.merge_changes(std::move(changes1));

// Both nodes converge
assert(node1.get_data() == node2.get_data());
```

**Text CRDT example:**
```cpp
#include "text_crdt.hpp"

TextCRDT<std::string> doc1(1);
TextCRDT<std::string> doc2(2);

// Both nodes insert lines concurrently
auto id1 = doc1.insert_line_at_end("Line from Node 1");
auto id2 = doc2.insert_line_at_end("Line from Node 2");

// Sync changes
uint64_t sync_version = 0;
auto changes1 = doc1.get_changes_since(sync_version);
auto changes2 = doc2.get_changes_since(sync_version);

doc2.merge_changes(changes1);
doc1.merge_changes(changes2);

// Both nodes have both lines
assert(doc1.line_count() == 2);
assert(doc2.line_count() == 2);
```

## Core Concepts

### Column-Based Design

Records are stored as maps of columns (field names) to values. Each column has independent version tracking:

```rust
// Rust
pub struct Record<C, V> {
  pub fields: HashMap<C, V>,
  pub column_versions: HashMap<C, ColumnVersion>,
  // Version boundaries for efficient sync
  pub lowest_local_db_version: u64,
  pub highest_local_db_version: u64,
}
```

**Why column-based?**
- Conflicts resolved per-field, not per-record
- Only changed columns need syncing
- Natural fit for structured data (forms, database records)

### Conflict Resolution

Conflicts are resolved deterministically using a three-tier comparison:

1. **Column Version** (per-field edit counter) - Higher wins
2. **Database Version** (global logical clock) - Higher wins
3. **Node ID** (unique node identifier) - Higher wins (tie-breaker)

This ordering ensures:
- **Field-level granularity**: Each field resolves independently
- **Causal ordering**: Logical clocks prevent out-of-order updates
- **Determinism**: All nodes converge to identical state

### Logical Clocks

Maintains causality using Lamport-style logical clocks:

```rust
impl LogicalClock {
  // Local operation
  pub fn tick(&mut self) -> u64 {
    self.time += 1;
    self.time
  }

  // Receive remote change
  pub fn update(&mut self, received_time: u64) -> u64 {
    self.time = self.time.max(received_time);
    self.time += 1;
    self.time
  }
}
```

**Important:** Always update clock on merge, even for rejected changes (prevents clock drift).

### Tombstone-Based Deletion

Deleted records are marked with tombstones rather than immediately removed:

```rust
pub struct TombstoneInfo {
  pub db_version: u64,
  pub node_id: NodeId,
  pub local_db_version: u64,
}
```

**⚠️ Critical: Tombstone Management**

Tombstones accumulate indefinitely unless compacted. To prevent memory exhaustion:

1. Track which versions have been acknowledged by **ALL nodes**
2. Call `compact_tombstones(min_acknowledged_version)` periodically
3. **Never compact early** - deleted records will reappear on nodes that haven't seen the deletion yet (zombie records)

### Field Deletion

Individual fields can be deleted from records without removing the entire record:

```rust
// Rust
let change = crdt.delete_field(&record_id, &"email".to_string());
```

```cpp
// C++
std::vector<Change<std::string, std::string>> changes;
bool success = crdt.delete_field(record_id, "email", changes);
```

**How it works:**
- Field is removed from `record.fields` map
- `ColumnVersion` entry **remains** in `record.column_versions` (acts as implicit field tombstone)
- Syncs to other nodes as `Change { col_name: Some("field"), value: None }`
- Field deletion is versioned like field updates (increments `col_version`)

**Distinguished from null values:**
- **Field deletion**: Field removed entirely from map
- **Null value**: Field exists in map with null/None value (if V = Option<T>)

### Fractional Positioning (Text CRDT)

Each line in the text CRDT has a position defined by a path of integers:

```cpp
struct FractionalPosition {
  std::vector<uint64_t> path;

  // Examples:
  // [10000]           - First line
  // [10000, 5000]     - Between first and second level
  // [20000]           - After [10000]
};
```

**Properties:**
- Total ordering with infinite density
- Can always insert between any two positions
- Automatically extends depth when space runs out

## Persistence Layer

The Rust implementation includes an optional persistence layer with WAL (Write-Ahead Log) and snapshots for durability and crash recovery.

### Quick Start

```toml
[dependencies]
# Basic persistence with bincode (legacy)
crdt-lite = { version = "0.5", features = ["persist"] }

# MessagePack persistence with schema evolution support
crdt-lite = { version = "0.5", features = ["persist-msgpack"] }

# With optional compression (50-70% additional size reduction)
crdt-lite = { version = "0.5", features = ["persist-compressed"] }
```

```rust
use crdt_lite::persist::{PersistedCRDT, PersistConfig};
use std::path::PathBuf;

// Open or create a persisted CRDT (uses MessagePack by default)
let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    PathBuf::from("./data"),
    1, // node_id
    PersistConfig::default(),
)?;

// Use like a normal CRDT - all operations are automatically persisted
pcrdt.insert_or_update(
    &"user1".to_string(),
    vec![
        ("name".to_string(), "Alice".to_string()),
        ("email".to_string(), "alice@example.com".to_string()),
    ],
)?;

// Changes are persisted to WAL and automatically recovered on crash
```

### Configuration

```rust
use crdt_lite::persist::{PersistConfig, SnapshotFormat};

let config = PersistConfig {
    // Snapshot creation
    snapshot_threshold: 1000,          // Create snapshot every 1000 changes
    snapshot_interval_secs: Some(300), // Or every 5 minutes (for low-activity nodes)

    // MessagePack + Incremental Snapshots (NEW in v0.6.0)
    snapshot_format: SnapshotFormat::MessagePack, // Default: schema evolution support
    enable_incremental_snapshots: true,            // Default: 95% I/O reduction
    full_snapshot_interval: 10,                    // Full snapshot every 10 incrementals
    enable_compression: false,                     // Optional: further 50-70% reduction

    // Cleanup
    auto_cleanup_snapshots: Some(3), // Keep 3 most recent snapshots
    max_batch_size: Some(10000),     // Auto-flush batch at 10k changes
};

let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    PathBuf::from("./data"),
    1,
    config,
)?;
```

**Why MessagePack + Incremental Snapshots?**
- **Schema Evolution**: Add new fields to your structs without breaking old snapshots (use `#[serde(default)]`)
- **95% I/O Reduction**: Only changed records are saved in incremental snapshots
- **Backwards Compatible**: Automatically falls back to bincode for old snapshot files
- **Optimal Recovery**: Loads latest full snapshot + applies incremental updates

### Hook System

The persistence layer provides three types of hooks for integration with backup systems, network layers, etc:

#### Post-Operation Hook

Called after changes are written to WAL (before fsync). Use for broadcasting changes to other nodes:

```rust
use std::sync::mpsc::Sender;

let (tx, rx) = std::sync::mpsc::channel();

pcrdt.add_post_hook(Box::new(move |changes| {
    // Send to network layer for broadcast
    let _ = tx.send(changes.to_vec());
}));
```

#### Snapshot Hook

Called after a snapshot is created and sealed. Use for uploading to cloud storage:

```rust
pcrdt.add_snapshot_hook(Box::new(move |snapshot_path| {
    let path = snapshot_path.clone();

    // Spawn async upload task
    tokio::spawn(async move {
        let data = tokio::fs::read(&path).await.unwrap();
        // Upload to R2, S3, etc.
        // r2.put(format!("snapshots/{}", path.file_name()?), data).await?;

        // Mark as uploaded for safe cleanup
        // pcrdt.lock().unwrap().mark_snapshot_uploaded(path);
    });
}));
```

#### WAL Segment Hook

Called after a WAL segment is sealed (rotated). Use for archival:

```rust
pcrdt.add_wal_segment_hook(Box::new(move |segment_path| {
    let path = segment_path.clone();

    tokio::spawn(async move {
        let data = tokio::fs::read(&path).await.unwrap();
        // Archive to cloud storage
        // r2.put(format!("wal/{}", path.file_name()?), data).await?;
    });
}));
```

### Batch Collector

Accumulate changes for efficient network broadcasting:

```rust
// Perform multiple operations
pcrdt.insert_or_update(&"user1".to_string(), fields1)?;
pcrdt.insert_or_update(&"user2".to_string(), fields2)?;

// Collect all changes since last call
let batch = pcrdt.take_batch();

// Broadcast to other nodes
broadcast_to_peers(batch);
```

**Auto-flush protection:** By default, the batch is cleared when it reaches 10,000 changes to prevent OOM. Call `take_batch()` regularly to avoid losing changes.

### Upload Tracking and Cleanup

For cloud backup workflows, track which files have been uploaded before deleting them:

```rust
// After successful upload
pcrdt.mark_snapshot_uploaded(snapshot_path);
pcrdt.mark_wal_segment_uploaded(segment_path);

// Cleanup only uploaded files (safe - won't lose data)
pcrdt.cleanup_old_snapshots(2, true)?; // Keep 2, require uploaded
pcrdt.cleanup_old_wal_segments(5, true)?; // Keep 5, require uploaded

// Or cleanup all old files (after snapshot creation)
pcrdt.cleanup_old_snapshots(2, false)?; // Unconditional cleanup
```

### Crash Recovery

Recovery is automatic on `open()`:

```rust
// After crash, simply open again
let pcrdt = PersistedCRDT::<String, String, String>::open(
    PathBuf::from("./data"),
    1,
    PersistConfig::default(),
)?;

// All data is recovered from snapshot + WAL replay
```

### Durability Guarantees

**Design choice:** WAL writes are buffered by the OS but NOT fsynced per-operation for CRDT performance:

| Failure Type | Data Loss | Why |
|--------------|-----------|-----|
| Process crash | None | OS page cache survives process termination |
| Kernel panic | ~0-30s | Depends on kernel writeback timing (typically 30s) |
| Power failure | Up to `snapshot_threshold` ops | Unflushed WAL + page cache lost |

**What this means:**
- ✅ **Process crashes**: Fully recoverable (most common failure mode)
- ✅ **System crashes**: Usually recoverable (kernel flushes dirty pages every ~30s)
- ⚠️ **Power failures**: May lose recent operations not yet in a snapshot
- ✅ **Distributed safety**: Changes broadcast to peers before local fsync (network-wide convergence maintained)

**Snapshots provide:**
- **Guaranteed persistence** at a point in time (fsynced to disk)
- **Bounded recovery time** (don't replay thousands of WAL operations)
- **WAL compaction** (can delete old segments after snapshot)

This design prioritizes CRDT convergence and performance over single-node durability. For stronger local durability:
- Reduce `snapshot_threshold` (e.g., 10-50 for single-node deployments)
- Use UPS/battery-backed storage for power failure protection
- Accept that process crashes are already safe (page cache preserved)

### Tombstone Compaction

After all nodes acknowledge a version, compact tombstones to reclaim memory:

```rust
// Track acknowledgments from all nodes
let min_acknowledged = get_min_ack_from_all_nodes();

// Atomically compact tombstones and cleanup WAL
pcrdt.compact_tombstones(min_acknowledged)?;
```

**Critical:** Only compact after ALL nodes acknowledge the version, or deleted records will reappear (zombie records).

### Examples

See working examples in the repository:
- `examples/persistence_example.rs` - Basic persistence with hooks
- `examples/r2_backup_example.rs` - Cloud backup workflow with R2

Run with:
```bash
cargo run --example persistence_example --features persist
cargo run --example r2_backup_example --features persist
```

## Synchronization

### Basic Sync Protocol

```rust
// Rust
let changes = node1.get_changes_since(last_db_version);
node2.merge_changes(changes, &DefaultMergeRule);

// Optionally exclude changes from specific nodes
let excluding = HashSet::from([node2_id]);
let changes = node1.get_changes_since_excluding(last_db_version, &excluding);
```

```cpp
// C++
auto changes = node1.get_changes_since(last_db_version);
node2.merge_changes(std::move(changes));

// Or use the helper function
uint64_t last_sync = 0;
sync_nodes(node1, node2, last_sync);
```

### Change Compression

When syncing with parent-child CRDTs or after accumulating many changes:

```rust
// Rust
CRDT::<String, String, String>::compress_changes(&mut changes);
```

```cpp
// C++
CRDT<K, V>::compress_changes(changes);
```

This removes redundant operations (O(n log n)):
- Superseded field updates (same record+column, older version)
- Field updates replaced by record deletions

## Advanced Features

### Parent-Child Hierarchies

Create temporary overlays or transaction isolation:

```rust
// Rust
use std::sync::Arc;

let parent = Arc::new(CRDT::<String, String, String>::new(1, None));
let child = CRDT::new(2, Some(parent.clone()));

// Child sees parent data but maintains separate modifications
```

```cpp
// C++
auto parent = std::make_shared<CRDT<K, V>>(1);
CRDT<K, V> child(2, parent);

// Generate inverse changes to undo child's work
auto revert_changes = child.revert();

// Compute difference between two CRDTs
auto diff = child.diff(other_crdt);
```

### Custom Merge Rules

```rust
// Rust
struct CustomMergeRule;

impl<K, C, V> MergeRule<K, C, V> for CustomMergeRule {
    fn should_accept(
        &self,
        local_col: u64, local_db: u64, local_node: NodeId,
        remote_col: u64, remote_db: u64, remote_node: NodeId,
    ) -> bool {
        // Custom logic here
        remote_col > local_col
    }
}

crdt.merge_changes(changes, &CustomMergeRule);
```

```cpp
// C++
template <typename K, typename V, typename Context = void>
struct CustomMergeRule {
  constexpr bool operator()(
      const Change<K, V> &local,
      const Change<K, V> &remote) const {
    // Custom logic here
    return remote.col_version > local.col_version;
  }
};

crdt.merge_changes<false, void, CustomMergeRule<K, V>>(
    std::move(changes), false, CustomMergeRule<K, V>()
);
```

### Text CRDT Merge Strategies

**Last-Write-Wins (default):**
```cpp
doc.merge_changes(remote_changes);
```

**Both-Writes-Win (preserve conflicts):**
```cpp
BothWritesWinMergeRule<std::string, std::string> bww;
doc.merge_changes_with_rule(remote_changes, bww);

// Check for conflicts
auto line = doc.get_line(line_id);
if (line->has_conflicts()) {
  auto all_versions = line->get_all_versions();
  // Display all concurrent edits to user
}
```

**⚠️ Auto-Merge (EXPERIMENTAL - DO NOT USE):**
The `AutoMergingTextRule` is currently broken and violates CRDT convergence guarantees. See `CLAUDE.md` for details.

## Security and DoS Protection

### Trust Model

⚠️ **This is a data structure library**, not a complete distributed system. Security must be implemented at higher layers:

- No authentication (accepts any changes)
- No authorization (no access control)
- Assumes all nodes are non-malicious

### DoS Mitigation Strategies

1. **Tombstone Accumulation**
   - Track tombstone count: `crdt.tombstone_count()`
   - Set application-level limits
   - Compact periodically after all nodes acknowledge a version

2. **Resource Exhaustion**
   - Implement rate limiting on operations
   - Validate and limit key/value sizes
   - Set maximum records/columns per record

3. **Clock Manipulation**
   - Malicious nodes can set high `db_version` to win all conflicts
   - Use authenticated logical clocks in production

### Production Recommendations

1. **Network Layer**: Use TLS/encryption for change transmission
2. **Authentication**: Verify node identity (HMAC, digital signatures)
3. **Rate Limiting**: Per-node operation limits
4. **Input Validation**: Sanitize and limit sizes
5. **Monitoring**: Track tombstone growth and memory usage
6. **Thread Safety**: Use `Arc<Mutex<CRDT>>` in Rust, external locks in C++

## Performance Characteristics

### Time Complexity

| Operation | Average Case | Notes |
|-----------|-------------|-------|
| `insert_or_update` | O(n) | n = number of fields |
| `delete_field` | O(1) | HashMap field removal |
| `delete_record` | O(1) | HashMap record removal |
| `merge_changes` | O(c) | c = number of changes |
| `get_changes_since` | O(r × f) | r = records, f = fields (optimized with version bounds) |
| `compress_changes` | O(n log n) | Uses unstable sort for better performance |
| `compact_tombstones` | O(t) | t = number of tombstones |

### Memory Efficiency

- HashMap-based storage: O(1) average case lookups
- Version boundaries: Skip unchanged records during sync
- Change compression: Remove redundant operations
- Tombstone compaction: Prevent unbounded growth

## Limitations

- **Thread Safety**: Not thread-safe; external synchronization required
- **Network Transport**: Not included; implement your own sync protocol
- **Text CRDT**: Auto-merge feature is incomplete (use BWW instead)
- **No Encryption**: Implement at application/network layer

## Migration Between Languages

| C++ | Rust |
|-----|------|
| `CRDT<K, V> crdt(node_id);` | `let mut crdt = CRDT::<K, C, V>::new(node_id, None);` |
| `crdt.insert_or_update(id, changes, pair1, pair2);` | `let changes = crdt.insert_or_update(&id, vec![pair1, pair2]);` |
| `crdt.delete_field(id, "field", changes);` | `if let Some(change) = crdt.delete_field(&id, &"field") { ... }` |
| `crdt.delete_record(id, changes);` | `if let Some(change) = crdt.delete_record(&id) { ... }` |
| `crdt.merge_changes(std::move(changes));` | `crdt.merge_changes(changes, &DefaultMergeRule);` |
| `auto* record = crdt.get_record(id);` | `let record = crdt.get_record(&id);` |

## Documentation

- **`CLAUDE.md`** - Comprehensive technical documentation for developers (and Claude!)
- See inline documentation in source files

## Future Enhancements

### Planned
- [ ] Tombstone garbage collection improvements
- [ ] Custom merge functions for specialized use cases
- [ ] Text CRDT: Fix auto-merge algorithm
- [ ] Text CRDT: Implement move operations
- [ ] Text CRDT: Position rebalancing

### Rust-Specific Possibilities
- [ ] `async` support for network operations
- [ ] Optional resource limits (max records, max tombstones)

### Completed
- [x] `serde` integration for serialization (v0.2.0)
- [x] WebAssembly support via `no_std` + `alloc` (v0.2.0)
- [x] Persistence layer with WAL and snapshots (v0.5.0)
- [x] Hook system for post-operation, snapshot, and WAL events (v0.5.0)
- [x] Batch collector for efficient network broadcasting (v0.5.0)
- [x] MessagePack snapshots with schema evolution support (v0.6.0)
- [x] Incremental snapshots for 95% I/O reduction (v0.6.0)
- [x] Optional zstd compression for snapshots (v0.6.0)

## Testing

```bash
# Rust - All tests
cargo test

# Rust - Persistence layer tests (bincode)
cargo test --features persist

# Rust - MessagePack + incremental snapshots tests
cargo test --features persist-msgpack

# Rust - With compression
cargo test --features persist-compressed

# C++ - Column CRDT
g++ -std=c++20 tests.cpp -o tests && ./tests

# C++ - Text CRDT
g++ -std=c++20 test_text_crdt.cpp -o test_text_crdt && ./test_text_crdt
```

## Contributing

Contributions are welcome! Please ensure:

**Rust:**
- All tests pass (`cargo test`)
- Code is formatted (`cargo fmt`)
- No clippy warnings (`cargo clippy`)
- Maintain feature parity with C++

**C++:**
- Requires C++20 compatible compiler
- All tests pass
- Maintain feature parity with Rust

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Related Projects

- **[crdt-sqlite](https://github.com/sinkingsugar/crdt-sqlite)** - SQLite wrapper with CRDT synchronization
  - Persistent storage for CRDT-backed applications
  - Automatic change tracking via SQLite triggers
  - Normal SQL INSERT/UPDATE/DELETE syntax (no special APIs required)
  - Cross-platform support (Linux, macOS, Windows)
  - Use crdt-lite for: in-memory state, game sync, real-time collaboration
  - Use crdt-sqlite for: persistent storage, database-backed apps, SQLite integration

## References

### CRDT Theory
- [A comprehensive study of CRDTs - Shapiro et al.](https://hal.inria.fr/inria-00555588)
- [Conflict-free Replicated Data Types](https://arxiv.org/abs/1805.06358)

### Text CRDTs
- [LSEQ: Adaptive Structure for Sequences](https://hal.archives-ouvertes.fr/hal-00921633)
- [Logoot: Scalable Optimistic Replication](https://hal.inria.fr/inria-00432368)
- [Fractional Indexing - Figma](https://www.figma.com/blog/realtime-editing-of-ordered-sequences/)

### Logical Clocks
- [Time, Clocks, and Ordering - Lamport](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)

---

**CRDT-Lite** offers a streamlined approach to conflict-free replicated data types, balancing simplicity and efficiency. By focusing on fine-grained conflict resolution and deterministic merge semantics, CRDT-Lite is well-suited for applications requiring scalability and low overhead in distributed environments.
