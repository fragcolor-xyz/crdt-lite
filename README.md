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
crdt-lite = { version = "0.2", default-features = false, features = ["alloc"] }

# For no_std with JSON serialization
crdt-lite = { version = "0.2", default-features = false, features = ["alloc", "json"] }

# For no_std with binary serialization (bincode)
crdt-lite = { version = "0.2", default-features = false, features = ["alloc", "binary"] }

# For standard environments (default, uses std::collections::HashMap)
crdt-lite = { version = "0.2", features = ["json"] }
```

**Implementation Notes:**
- `no_std` mode uses `hashbrown::HashMap`, which is the same underlying implementation that `std::collections::HashMap` uses (identical performance)
- The `std` feature is enabled by default for backwards compatibility
- The `alloc` feature is required for `no_std` environments and pulls in `hashbrown` only when needed
- When using std (default), `hashbrown` is not compiled, reducing dependency bloat
- Binary serialization uses `bincode` 2.0 which has full `no_std` support

## Quick Start

### Rust Implementation

```bash
cargo add crdt-lite  # (when published)
# or add to Cargo.toml: crdt-lite = "0.1"
```

```rust
use crdt_lite::{CRDT, DefaultMergeRule};

// Create two CRDT nodes
let mut node1: CRDT<String, String> = CRDT::new(1, None);
let mut node2: CRDT<String, String> = CRDT::new(2, None);

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
pub struct Record<V> {
  pub fields: HashMap<ColumnKey, V>,
  pub column_versions: HashMap<ColumnKey, ColumnVersion>,
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
CRDT::<String, String>::compress_changes(&mut changes);
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

let parent = Arc::new(CRDT::<String, String>::new(1, None));
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

impl<K, V> MergeRule<K, V> for CustomMergeRule {
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
| `delete_record` | O(1) | HashMap removal |
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
| `CRDT<K, V> crdt(node_id);` | `let mut crdt = CRDT::<K, V>::new(node_id, None);` |
| `crdt.insert_or_update(id, changes, pair1, pair2);` | `let changes = crdt.insert_or_update(&id, vec![pair1, pair2]);` |
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

## Testing

```bash
# Rust
cargo test

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
