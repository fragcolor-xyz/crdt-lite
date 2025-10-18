# crdt-lite

A lightweight, column-based CRDT (Conflict-free Replicated Data Type) implementation with both Rust and C++ versions.

## Project Overview

This library provides generic CRDTs for building distributed, eventually-consistent data structures. The project includes:

1. **Column-based CRDT** (`src/lib.rs`, `crdt.hpp`) - Generic key-value store with column-level conflict resolution
2. **Text CRDT** (`text_crdt.hpp`) - Line-based collaborative text editor with fractional positioning

Both implementations support last-write-wins (LWW) semantics, tombstone-based deletion, logical clocks for causality tracking, and customizable merge rules.

## Architecture

### Dual Implementation Strategy

- **Rust** (`src/lib.rs`): Primary implementation with strong type safety, memory safety guarantees
- **C++** (`crdt.hpp`, `text_crdt.hpp`): Header-only template library for high-performance, zero-cost abstractions

Both implementations maintain API compatibility and use the same core algorithms.

### Core Concepts

#### 1. Column-Based Design

Records are stored as maps of columns (field names) to values. Each column has independent version tracking:

```rust
// Rust
pub struct Record<V> {
  pub fields: HashMap<ColumnKey, V>,
  pub column_versions: HashMap<ColumnKey, ColumnVersion>,
  pub lowest_local_db_version: u64,
  pub highest_local_db_version: u64,
}
```

```cpp
// C++
template <typename V> struct Record {
  CrdtMap<CrdtKey, V> fields;
  CrdtMap<CrdtKey, ColumnVersion> column_versions;
  uint64_t lowest_local_db_version = UINT64_MAX;
  uint64_t highest_local_db_version = 0;
};
```

**Why column-based?**
- Fine-grained conflict resolution (conflicts resolved per-field, not per-record)
- Efficient sync (only changed columns need syncing)
- Natural fit for structured data (forms, database records)

#### 2. Version Tracking

Each change is tracked with both column-level and global version information:

```rust
pub struct ColumnVersion {
  pub col_version: u64,      // Per-column version (increments on each edit)
  pub db_version: u64,       // Global logical clock at creation time
  pub node_id: NodeId,       // Which node made this change
  pub local_db_version: u64, // Local clock when applied (for sync)
}
```

**Conflict Resolution Priority:**
1. Column version (higher wins)
2. DB version (higher wins)
3. Node ID (higher wins as deterministic tie-breaker)

#### 3. Logical Clocks

Maintains causality using Lamport-style logical clocks:

```rust
impl LogicalClock {
  pub fn tick(&mut self) -> u64 {
    self.time += 1;
    self.time
  }

  pub fn update(&mut self, received_time: u64) -> u64 {
    self.time = self.time.max(received_time);
    self.time += 1;
    self.time
  }
}
```

**Important:** Always update clock on merge, even for rejected changes (prevents clock drift, maintains causal consistency).

#### 4. Tombstone-Based Deletion

Deleted records are marked with tombstones rather than immediately removed:

```rust
pub struct TombstoneInfo {
  pub db_version: u64,
  pub node_id: NodeId,
  pub local_db_version: u64,
}
```

**Tombstone Management:**
- Tombstones accumulate indefinitely unless compacted
- **CRITICAL:** Only compact after ALL nodes acknowledge a version
- Early compaction causes deleted records to reappear (zombie records)
- See `compact_tombstones()` docs in src/lib.rs:1002-1024

**DoS Protection:**
- Tombstones can grow unbounded → memory exhaustion
- Implement application-level rate limiting
- Track acknowledged versions across all nodes
- Call `compact_tombstones(min_acknowledged_version)` periodically

#### 5. Parent-Child Hierarchies

CRDTs can have parent-child relationships for layered state:

```rust
pub struct CRDT<K, V> {
  parent: Option<Arc<CRDT<K, V>>>,
  base_version: u64,  // Parent's version at child creation
  // ...
}
```

**Use cases:**
- Temporary overlays (preview changes without committing)
- Transaction isolation
- Branching workflows

**Operations:**
- `revert()`: Generate inverse changes to undo child modifications
- `diff()`: Compute changes between two CRDTs
- Child sees parent data but maintains separate modifications

## Column-Based CRDT Details

### Key Operations

#### Insert/Update
```rust
// Rust
let changes = crdt.insert_or_update(&record_id, fields);
```

```cpp
// C++
std::vector<Change<K, V>> changes;
crdt.insert_or_update(record_id, changes,
  std::make_pair("field1", value1),
  std::make_pair("field2", value2)
);
```

- Increments column versions for modified fields
- Ticks global logical clock
- Returns changes for propagation to other nodes
- Fails silently if record is tombstoned

#### Delete
```rust
// Rust
let change = crdt.delete_record(&record_id);
```

```cpp
// C++
std::vector<Change<K, V>> changes;
crdt.delete_record(record_id, changes);
```

- Removes record from active data
- Adds tombstone entry
- Tombstone prevents future inserts/updates to this record

#### Merge
```rust
// Rust
let accepted = crdt.merge_changes(incoming_changes, &merge_rule);
```

```cpp
// C++
// LWW (default)
crdt.merge_changes(std::move(changes));

// Custom merge rule with context
crdt.merge_changes<true, MyContext, MyMergeRule>(
  std::move(changes), ignore_parent, merge_rule, context
);
```

- Updates logical clock for ALL changes (accepted or rejected)
- Applies merge rule to resolve conflicts
- Returns accepted changes (optional)

### Sync Protocol

```rust
// Get changes since last sync
let changes = node1.get_changes_since(last_db_version);

// Optionally exclude changes from specific nodes
let excluding = HashSet::from([node2_id]);
let changes = node1.get_changes_since_excluding(last_db_version, &excluding);

// Apply to remote node
node2.merge_changes(changes, &DefaultMergeRule);

// Update sync version
last_db_version = node1.get_clock().current_time();
```

**Optimization:** Records track `lowest_local_db_version` and `highest_local_db_version` to skip records that haven't changed.

### Change Compression

When parent-child CRDTs are involved, changes may contain redundant operations:

```rust
CRDT::compress_changes(&mut changes);
```

**What it does:**
1. Sorts changes by (record_id, col_name, version) using `DefaultChangeComparator`
2. Removes superseded changes (same record+column, older version)
3. Simplifies deletions (tombstone replaces all prior field updates)

**Algorithm:** Two-pointer in-place compression (O(n log n) due to sort)

## Text CRDT Details (`text_crdt.hpp`)

Line-based collaborative text editor with fractional positioning.

### Fractional Positioning

Each line has a `FractionalPosition` - a path of integers defining total ordering:

```cpp
struct FractionalPosition {
  std::vector<uint64_t> path;

  // Examples:
  // [10000]           - First line
  // [10000, 5000]     - Between [10000] and [10000, 10000]
  // [20000]           - After [10000]
};
```

**Properties:**
- Total ordering with spaceship operator (`<=>`)
- Infinite density: can always insert between any two positions
- Automatically extends depth when space runs out

**Position Generation:**
```cpp
// Insert at start
FractionalPosition::before(existing_first);

// Insert at end
FractionalPosition::after(existing_last);

// Insert between two lines
FractionalPosition::between(line_a_pos, line_b_pos);
```

**Algorithm for `between(a, b)`:**
1. Find first index where paths differ
2. If room at that level (b[i] - a[i] > 1), use midpoint
3. Otherwise, extend a's path to next level
4. Handles prefix cases (one path is prefix of other)

### Line Data Structure

```cpp
template <typename K, typename V = std::string>
struct LineData {
  K id;                           // Stable UUID identity (never changes)
  FractionalPosition position;    // Current position (can change with moves)
  V content;                      // Line content
  VersionInfo version;            // Version metadata

  // For Both-Writes-Win
  std::optional<std::vector<ConflictVersion<V>>> conflicts;

  // For auto-merge (3-way merge)
  std::optional<V> base_content;
};
```

### Operations

```cpp
TextCRDT<std::string> doc(node_id);

// Insert operations
auto id1 = doc.insert_line_at_start("First line");
auto id2 = doc.insert_line_at_end("Last line");
auto id3 = doc.insert_line_after(id1, "After first");
auto id4 = doc.insert_line_before(id2, "Before last");

// Edit and delete
doc.edit_line(id1, "Modified content");
doc.delete_line(id2);

// Query
auto lines = doc.get_all_lines();  // In document order
std::string text = doc.to_text("\n");
```

### Merge Rules

#### 1. Last-Write-Wins (Default)

```cpp
// Automatic with default merge_changes()
doc.merge_changes(remote_changes);
```

Conflict resolution:
1. Compare `line_version` (per-line edit counter)
2. Compare `db_version` (global logical clock)
3. Compare `node_id` (deterministic tie-breaker)

#### 2. Both-Writes-Win (BWW)

Preserves all concurrent edits as conflicts:

```cpp
BothWritesWinMergeRule<std::string, std::string> bww;
doc.merge_changes_with_rule(remote_changes, bww);

// Check for conflicts
auto line = doc.get_line(line_id);
if (line->has_conflicts()) {
  auto all_versions = line->get_all_versions();
  // Display all concurrent versions to user
}
```

**Detection:** Concurrent edits have same `line_version` but different `node_id`.

**Storage:**
- Primary content: displayed version (deterministic winner by node_id)
- `conflicts` field: vector of all concurrent versions
- Sorted deterministically for convergence

#### 3. Auto-Merge (BROKEN - DO NOT USE)

**⚠️ CRITICAL: AutoMergingTextRule is INCOMPLETE and BUGGY**

**Location:** text_crdt.hpp:966-1118

**Intended behavior:**
- 3-way merge for non-overlapping concurrent edits
- Track base content before local edit
- Compute word-level diffs from base
- Merge non-overlapping diffs automatically
- Fall back to BWW for overlapping changes

**Example that SHOULD work:**
```
Base:   "The quick brown fox"
Local:  "The FAST brown fox"   (changed word 1)
Remote: "The quick brown CAT"  (changed word 3)
Merge:  "The FAST brown CAT"   (both changes applied!)
```

**KNOWN BUGS:**

1. **Broken diff merge** (text_crdt.hpp:888-900)
   - `merge_diffs()` naively concatenates diffs
   - Produces duplicate KEEP operations
   - Doesn't resolve conflicts between DELETE/INSERT and KEEP
   - **Result:** Incorrect merge output

2. **Incorrect apply_diff** (text_crdt.hpp:848-885)
   - Doesn't handle merged diffs with overlapping positions
   - Applies operations in wrong order
   - **Result:** Merged content is wrong

3. **Non-deterministic convergence**
   - Two nodes merging same concurrent edits may diverge
   - **VIOLATES CRDT CONVERGENCE GUARANTEE**
   - Currently falls back to BWW for most cases (hiding the bug)

**To fix:**
- Implement proper diff merge that deduplicates KEEP operations
- Resolve DELETE/INSERT vs KEEP conflicts correctly
- Ensure apply_diff handles merged diffs properly
- Add comprehensive tests for all edge cases
- Verify deterministic convergence

**Current workaround:** Use `BothWritesWinMergeRule` instead.

### Change Streaming

Operations can populate a change vector for immediate propagation:

```cpp
std::vector<TextChange<std::string, std::string>> changes;

doc.insert_line_at_end("Line 1", &changes);
doc.edit_line(id, "Modified", &changes);
doc.delete_line(id, &changes);

// Stream to remote node
remote_doc.merge_changes(changes);
```

**Use case:** Real-time collaboration (send changes as they happen).

### Dual Indexing

```cpp
std::map<FractionalPosition, K> position_to_id_;  // Ordered iteration
std::unordered_map<K, LineData<K, V>> lines_;    // Fast lookup by ID
```

**Benefits:**
- O(log n) position-ordered iteration (for `get_all_lines()`)
- O(1) line lookup by ID
- Both indices stay synchronized on insert/delete/move

## Testing

### Column CRDT Tests (`tests.cpp`)

- Basic insert/merge
- Concurrent updates with conflicts
- Delete and tombstone propagation
- Multi-node sync
- Parent-child operations
- Custom merge rules
- Change compression

Run:
```bash
g++ -std=c++20 tests.cpp -o tests && ./tests
```

### Text CRDT Tests (`test_text_crdt.cpp`)

**Test categories:**

1. **FractionalPosition Tests**
   - Basic operations (first, before, after)
   - Between with room vs adjacent
   - Prefix handling
   - Repeated bisection (stress test)

2. **Basic Operations**
   - Insert at start/end/after/before
   - Edit and delete
   - Index consistency

3. **Distributed CRDT Tests**
   - Basic sync
   - Concurrent insertions
   - Concurrent edits (LWW and BWW)
   - Delete propagation
   - Three-way sync
   - Delete vs edit conflicts
   - Multi-node concurrent edits

4. **Auto-Merge Tests**
   - Non-overlapping edits (DISABLED - auto-merge broken)
   - Overlapping edits (fallback to BWW)

5. **Change Streaming Tests**
   - Insert/edit/delete streaming
   - Live sync scenario

6. **Text Output Tests**
   - `to_text()` with custom separators
   - `get_all_lines()` ordering

Run:
```bash
g++ -std=c++20 test_text_crdt.cpp -o test_text_crdt && ./test_text_crdt
```

### Rust Tests (`src/lib.rs`)

Located at bottom of lib.rs (lines 1097-1190):

- Logical clock
- Tombstone storage
- Basic insert
- Delete record
- Merge changes with conflicts

Run:
```bash
cargo test
```

## Working with the Codebase

### Adding New Features

1. **Choose implementation:** Start with Rust or C++ based on your needs
2. **Update both:** Keep Rust and C++ APIs in sync
3. **Add tests:** All new features need test coverage in both languages
4. **Document:** Update this file with architecture decisions

### Common Pitfalls

1. **Forgetting to update clock on merge**
   - ALWAYS call `clock.update(remote_db_version)` even for rejected changes
   - Prevents clock drift and maintains causality

2. **Early tombstone compaction**
   - Only compact when ALL nodes have acknowledged the version
   - Otherwise deleted records reappear (zombie records)

3. **Ignoring parent in queries**
   - `get_record_ptr()` checks parent by default
   - Use `ignore_parent=true` carefully

4. **Not handling tombstoned records**
   - Always check `is_tombstoned()` before operations
   - Insert/update on tombstoned records silently fails

5. **Assuming stable iteration order**
   - HashMap iteration order is not stable
   - Use sorted structures or `compress_changes()` for determinism

### Custom Collection Types (C++)

The C++ implementation allows custom collection types:

```cpp
#define CRDT_COLLECTIONS_DEFINED

// Define your custom types
template <typename T> using CrdtVector = MyCustomVector<T>;
template <typename K, typename V> using CrdtMap = MyCustomMap<K, V>;

#include "crdt.hpp"
```

**Use cases:**
- Custom allocators
- Embedded systems with specialized containers
- Performance tuning

### Performance Considerations

1. **Sync frequency vs overhead**
   - More frequent sync = better collaboration experience
   - But more overhead (network, merge processing)
   - Use version bounds to filter unchanged records

2. **Change compression**
   - Compresses redundant changes (O(n log n))
   - Essential for parent-child CRDTs
   - Optional for simple peer-to-peer sync

3. **Tombstone growth**
   - Grows linearly with deletions
   - Can cause memory issues over time
   - Compact periodically (coordinate across all nodes)

4. **Text CRDT position depth**
   - Fractional positions extend depth when space runs out
   - Deep paths (10+ levels) can impact performance
   - Consider rebalancing positions periodically (not implemented)

## Security and DoS Protection

### Logical Clock Overflow

- Uses `u64` for version numbers
- Overflow after 2^64 operations (extremely unlikely)
- Applications with extreme longevity should monitor clock values

### Tombstone Accumulation DoS

**Attack vector:** Malicious node creates many records, then deletes them

**Mitigations:**
1. Track tombstone count: `crdt.tombstone_count()`
2. Set application-level tombstone limits
3. Reject operations when limit exceeded
4. Implement rate limiting on delete operations
5. Coordinate regular compaction across all trusted nodes

### Resource Exhaustion

**Attack vectors:**
- Unbounded record creation
- Extremely large field values
- Excessive column count per record

**Mitigations:**
- Application-level rate limiting
- Size limits on values
- Maximum columns per record
- Maximum total records

## Known Issues and TODOs

### Text CRDT (`text_crdt.hpp`)

- [ ] **CRITICAL:** Fix AutoMergingTextRule diff merge algorithm (lines 888-900, 1069-1076)
- [ ] **CRITICAL:** Fix apply_diff to handle merged diffs correctly (lines 848-885)
- [ ] Implement move operations (line position changes)
- [ ] Add position rebalancing to prevent deep paths
- [ ] Implement proper UUID generation (currently timestamp-based, line 580-592)

### Column CRDT

- [ ] Add batch operations for efficiency
- [ ] Implement schema validation hooks
- [ ] Add support for complex value types (nested structures)
- [ ] Optimize tombstone storage (consider Bloom filters)

### Both

- [ ] Add benchmarks
- [ ] Implement wire protocol for efficient serialization
- [ ] Add support for encrypted CRDTs
- [ ] Network partition tolerance testing

## References

### CRDT Theory
- [A comprehensive study of Convergent and Commutative Replicated Data Types](https://hal.inria.fr/inria-00555588)
- [Conflict-free Replicated Data Types (CRDTs) - Shapiro et al.](https://arxiv.org/abs/1805.06358)

### Text CRDTs
- [LSEQ: an Adaptive Structure for Sequences in Distributed Collaborative Editing](https://hal.archives-ouvertes.fr/hal-00921633)
- [Logoot: A Scalable Optimistic Replication Algorithm for Collaborative Editing](https://hal.inria.fr/inria-00432368)
- [Fractional Indexing](https://www.figma.com/blog/realtime-editing-of-ordered-sequences/)

### Logical Clocks
- [Time, Clocks, and the Ordering of Events in a Distributed System - Lamport](https://lamport.azurewebsites.net/pubs/time-clocks.pdf)

## License

See LICENSE file in repository root.
