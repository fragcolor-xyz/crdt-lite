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

## Persistence Layer (Rust)

The Rust implementation includes an optional persistence layer (`src/persist.rs`) with Write-Ahead Log (WAL), snapshots, and crash recovery. As of v0.6.0, it supports **MessagePack** format with **incremental snapshots** for schema evolution and 95% I/O reduction.

### Features

1. **Write-Ahead Log (WAL)** - Append-only change log with automatic rotation
2. **MessagePack Snapshots** - Schema-evolution-friendly serialization (v0.6.0+)
3. **Incremental Snapshots** - Only changed records since last full snapshot (v0.6.0+)
4. **Optional Compression** - zstd compression for 50-70% additional size reduction
5. **Hook System** - Callbacks for post-operation, snapshot, and WAL segment sealing
6. **Crash Recovery** - Automatic recovery from snapshot + WAL replay
7. **Backwards Compatible** - Falls back to legacy bincode snapshots

### Quick Start

**Cargo.toml:**
```toml
[dependencies]
# MessagePack with schema evolution support (recommended)
crdt-lite = { version = "0.6", features = ["persist-msgpack"] }

# With optional compression
crdt-lite = { version = "0.6", features = ["persist-compressed"] }

# Legacy bincode (no schema evolution)
crdt-lite = { version = "0.6", features = ["persist"] }
```

**Basic Usage:**
```rust
use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};
use std::path::PathBuf;

// Open or create persisted CRDT (uses MessagePack by default)
let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    PathBuf::from("./data"),
    1, // node_id
    PersistConfig::default(),
)?;

// All operations are automatically persisted
let changes = pcrdt.insert_or_update(
    &"user1".to_string(),
    vec![
        ("name".to_string(), "Alice".to_string()),
        ("email".to_string(), "alice@example.com".to_string()),
    ],
)?;

// Broadcast changes to other nodes
// network.broadcast(changes);
```

### Configuration

```rust
use crdt_lite::persist::{PersistConfig, SnapshotFormat};

let config = PersistConfig {
    // Snapshot triggers
    snapshot_threshold: 1000,          // Create snapshot every 1000 changes
    snapshot_interval_secs: Some(300), // Or every 5 minutes (whichever comes first)

    // MessagePack + Incremental Snapshots (NEW in v0.6.0)
    snapshot_format: SnapshotFormat::MessagePack, // Default: schema evolution support
    enable_incremental_snapshots: true,            // Default: 95% I/O reduction
    full_snapshot_interval: 10,                    // Full snapshot every 10 incrementals
    enable_compression: false,                     // Optional: zstd compression

    // Cleanup policies
    auto_cleanup_snapshots: Some(3), // Keep 3 most recent snapshots
    max_batch_size: Some(10000),     // Auto-flush batch collector at 10k changes
};

let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    PathBuf::from("./data"),
    1,
    config,
)?;
```

### Schema Evolution with MessagePack

**The Problem:** Adding fields to your CRDT structs breaks old snapshots with bincode:
```rust
// Old version
#[derive(Serialize, Deserialize)]
struct MyValue {
    name: String,
}

// New version - bincode fails to load old snapshots!
#[derive(Serialize, Deserialize)]
struct MyValue {
    name: String,
    email: String, // NEW FIELD
}
```

**The Solution:** Use MessagePack with `#[serde(default)]`:
```rust
// New version - MessagePack loads old snapshots successfully!
#[derive(Serialize, Deserialize)]
struct MyValue {
    name: String,
    #[serde(default)]
    email: String, // Defaults to "" for old snapshots
}

// Or with Option
#[derive(Serialize, Deserialize)]
struct MyValue {
    name: String,
    #[serde(default)]
    email: Option<String>, // Defaults to None for old snapshots
}
```

**How it works:**
- **bincode** uses length-prefixed encoding that hardcodes field count → schema changes break
- **MessagePack** uses self-describing format → missing fields use `Default::default()`
- Overhead: MessagePack is ~27% larger than bincode, but incremental snapshots more than compensate

### Incremental Snapshots

**The Problem:** With 10,000 records and 1% daily change rate:
- Full snapshots: Write all 10,000 records every time → 151 MB/day
- Only 100 records actually changed!

**The Solution:** Incremental snapshots only store changed records:
```
Snapshot sequence:
1. Full snapshot (baseline)           → 349 KB (all 10K records)
2. Incremental (100 changed)          → 3 KB (99.1% smaller!)
3. Incremental (100 changed)          → 3 KB
...
10. Incremental (100 changed)         → 3 KB
11. Full snapshot (new baseline)      → 349 KB
```

**Results:**
- Per-snapshot: 99.1% size reduction (349 KB → 3 KB)
- Daily I/O: 95% reduction (151 MB → 6 MB)
- Recovery: Fast (load 1 full + apply up to 9 incrementals)

**File naming:**
```
data/
  snapshot_full_000001.msgpack     # Full snapshot (baseline)
  snapshot_incr_000002_base_000001.msgpack  # Incremental #1
  snapshot_incr_000003_base_000001.msgpack  # Incremental #2
  ...
  snapshot_incr_000010_base_000001.msgpack  # Incremental #9
  snapshot_full_000011.msgpack     # New full snapshot (new baseline)
```

**Configuration:**
```rust
let config = PersistConfig {
    enable_incremental_snapshots: true, // Enable incrementals (default: true)
    full_snapshot_interval: 10,         // Full every 10 incrementals (default: 10)
    // Balance: lower = faster recovery, higher = more I/O savings
    ..Default::default()
};
```

**Recovery process:**
1. Find latest full snapshot (e.g., `snapshot_full_000001.msgpack`)
2. Find all incrementals building on it (e.g., `snapshot_incr_*_base_000001.msgpack`)
3. Load full snapshot as baseline
4. Apply incremental snapshots in order
5. Replay WAL files for changes after last snapshot

**Memory considerations:**
- Each incremental snapshot is loaded entirely into memory during recovery
- Rule of thumb: ~1.5 MB per 1,000 changed records
- With default `full_snapshot_interval: 10`, max memory ~15 MB per recovery
- **If memory is constrained:** Reduce `full_snapshot_interval` or `snapshot_threshold`
- **If incremental snapshots exceed 10 MB:** Your snapshot frequency is too low

### Hook System

The persistence layer provides three hook types for integration with cloud storage, network layers, etc.

#### 1. Post-Operation Hook

Called after changes are written to WAL (before fsync). Use for broadcasting to network:

```rust
use std::sync::mpsc::Sender;

let (tx, rx) = std::sync::mpsc::channel();

pcrdt.add_post_hook(Box::new(move |changes| {
    // Non-blocking send to network layer
    let _ = tx.send(changes.to_vec());
}));

// In another thread, read from rx and broadcast
```

**Timing:**
- ✅ Changes applied to in-memory CRDT
- ✅ Changes written to WAL file
- ✅ WAL buffer flushed to OS page cache
- ❌ NOT yet fsynced (happens during snapshot)

**Why before fsync?** For distributed CRDTs, minimizing network propagation delay is more important than local durability. If this node crashes before fsync, peers have the data and this node recovers from them on restart.

#### 2. Snapshot Hook

Called after a snapshot has been created and sealed. Use for uploading to cloud storage:

```rust
pcrdt.add_snapshot_hook(Box::new(move |snapshot_path, db_version| {
    let path = snapshot_path.clone();

    // Spawn async upload task
    tokio::spawn(async move {
        let data = tokio::fs::read(&path).await.unwrap();

        // Upload to R2, S3, etc.
        // let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("snapshot");
        // r2.put(format!("snapshots/{}", filename), data).await?;

        // Mark as uploaded for safe cleanup
        // pcrdt.lock().unwrap().mark_snapshot_uploaded(path);

        println!("Uploaded snapshot at db_version {}", db_version);
    });
}));
```

**New in v0.6.0:** Hook receives `db_version` parameter (CRDT logical clock at snapshot time) for tracking.

#### 3. WAL Segment Hook

Called after a WAL segment is sealed (rotated). Use for archival:

```rust
pcrdt.add_wal_segment_hook(Box::new(move |segment_path| {
    let path = segment_path.clone();

    tokio::spawn(async move {
        let data = tokio::fs::read(&path).await.unwrap();
        // Archive to cloud storage
        // let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("wal");
        // r2.put(format!("wal/{}", filename), data).await?;
    });
}));
```

### Compression

**Optional zstd compression** provides 50-70% additional size reduction at minimal CPU cost:

```toml
[dependencies]
crdt-lite = { version = "0.6", features = ["persist-compressed"] }
```

```rust
let config = PersistConfig {
    enable_compression: true, // Enable zstd compression
    ..Default::default()
};
```

**Tradeoffs:**
- Compression: 50-70% additional size reduction
- CPU: ~1-2ms per snapshot (negligible for most workloads)
- Recovery: Automatic decompression (transparent)

**Example:** 10K records
- Uncompressed full: 349 KB
- Compressed full: ~100-150 KB
- Compressed incremental: ~1-1.5 KB (from 3 KB)

### Durability Guarantees

**Design choice:** WAL writes are buffered by OS but NOT fsynced per-operation for CRDT performance:

| Failure Type | Data Loss | Why |
|--------------|-----------|-----|
| Process crash | **None** | OS page cache survives process termination |
| Kernel panic | **~0-30s** | Depends on kernel writeback (typically 30s) |
| Power failure | **Up to snapshot_threshold ops** | Unflushed WAL + page cache lost |

**What this means:**
- ✅ **Process crashes**: Fully recoverable (most common failure mode)
- ✅ **System crashes**: Usually recoverable (kernel flushes pages every ~30s)
- ⚠️ **Power failures**: May lose recent operations not yet in a snapshot
- ✅ **Distributed safety**: Changes broadcast to peers before local fsync (convergence maintained)

**For stronger local durability:**
- Reduce `snapshot_threshold` (e.g., 10-50 for single-node deployments)
- Use UPS/battery-backed storage
- Set `snapshot_interval_secs` to 60s (snapshots every minute)

**What snapshots provide:**
- Guaranteed persistence at a point in time (fsynced)
- Bounded recovery time (don't replay thousands of WAL ops)
- WAL compaction (can delete old segments)

### Batch Collector

Accumulate changes for efficient network broadcasting:

```rust
// Perform operations
pcrdt.insert_or_update(&"user1".to_string(), fields1)?;
pcrdt.insert_or_update(&"user2".to_string(), fields2)?;
pcrdt.delete_record(&"user3".to_string())?;

// Collect all changes since last take_batch()
let batch = pcrdt.take_batch();

// Broadcast to network
network.broadcast_to_peers(batch);
```

**Auto-flush protection:** By default, batch is cleared at 10,000 changes to prevent OOM. Call `take_batch()` regularly (every 100-1000 ops or every 1-10 seconds) to avoid silently losing changes.

### Upload Tracking and Cleanup

For cloud backup workflows, track which files have been uploaded before deleting:

```rust
// In snapshot hook, after successful upload
pcrdt.mark_snapshot_uploaded(snapshot_path);

// In WAL segment hook, after successful upload
pcrdt.mark_wal_segment_uploaded(segment_path);

// Cleanup only uploaded files (safe - won't lose data)
pcrdt.cleanup_old_snapshots(2, true)?;  // Keep 2, require uploaded
pcrdt.cleanup_old_wal_segments(5, true)?; // Keep 5, require uploaded
```

**Cleanup modes:**
- `require_uploaded: true` - Only delete files marked as uploaded (safe for cloud backup)
- `require_uploaded: false` - Delete all old files unconditionally (used by auto-cleanup)

### Crash Recovery

Recovery is automatic on `open()`:

```rust
// After crash, simply open again
let pcrdt = PersistedCRDT::<String, String, String>::open(
    PathBuf::from("./data"),
    1,
    PersistConfig::default(),
)?;

// All data recovered from:
// 1. Latest full snapshot (MessagePack or bincode)
// 2. Incremental snapshots (if any)
// 3. WAL replay (changes after last snapshot)
```

**Recovery process (MessagePack):**
1. Discover all snapshots in directory
2. Find latest full snapshot (e.g., `snapshot_full_000001.msgpack`)
3. Find all incrementals building on it (e.g., `snapshot_incr_*_base_000001.msgpack`)
4. Load full snapshot
5. Apply incrementals in version order
6. Replay WAL files
7. Resume operations

**Recovery time:**
- Full snapshot load: O(n) where n = record count
- Incremental apply: O(m) where m = changed records (typically <1% of n)
- WAL replay: O(c) where c = changes since last snapshot

### Tombstone Compaction

Deleted records accumulate as tombstones. Compact periodically after all nodes acknowledge a version:

```rust
// Track minimum acknowledged version from all nodes
let min_acknowledged = track_acks_from_all_nodes();

// Atomically compact tombstones, create snapshot, delete old WAL
pcrdt.compact_tombstones(min_acknowledged)?;
```

**⚠️ CRITICAL:** Only compact after ALL nodes acknowledge the version, or deleted records will reappear (zombie records). This is because:
1. Compaction removes tombstones from memory
2. Snapshot captures the compacted state
3. Old WAL segments (containing deletion operations) are deleted
4. If a node that hasn't seen the deletion syncs with you, the deleted record has no tombstone to reject it

### Migration from Bincode to MessagePack

**Automatic fallback:** The persistence layer automatically detects and loads legacy bincode snapshots. On next snapshot, it creates MessagePack snapshots. Both formats coexist during migration.

**Manual migration:**
```rust
// 1. Load with bincode support
let config = PersistConfig {
    snapshot_format: SnapshotFormat::MessagePack, // Target format
    ..Default::default()
};
let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    PathBuf::from("./data"),
    node_id,
    config,
)?;

// 2. Force a snapshot (will use MessagePack)
pcrdt.snapshot()?;

// 3. Old bincode snapshots can now be deleted manually
```

### Performance Characteristics

**Snapshot creation:**
- Full MessagePack: ~2-5ms per 10K records (includes serialization + write + fsync)
- Incremental MessagePack: ~0.1-0.5ms per 100 changed records (99% faster)
- Compression: +1-2ms (negligible)

**Recovery:**
- Full snapshot load: ~5-10ms per 10K records
- Incremental apply: ~0.5-1ms per 100 changed records
- WAL replay: ~0.01ms per change

**Disk usage (10K records, typical data):**
- Full snapshot (bincode): ~270 KB
- Full snapshot (MessagePack): ~349 KB (+27%)
- Incremental snapshot: ~3 KB (99% smaller than full)
- With compression: ~100-150 KB full, ~1 KB incremental

**Daily I/O (10K records, 1% change rate):**
- Old (10 full snapshots): 151 MB
- New (1 full + 9 incrementals): 6 MB (95% reduction)

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
