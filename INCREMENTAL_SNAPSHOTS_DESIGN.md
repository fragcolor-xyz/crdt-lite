# Incremental Snapshots for CRDT Persistence

**Question**: Can we use incremental snapshots with MessagePack to reduce snapshot overhead?

**Answer**: YES! This is a much bigger win than zero-copy serialization.

## The Problem with Full Snapshots

Current persist.rs approach:
```rust
// Every 1000 changes or 5 minutes
snapshot() {
    serialize(entire_crdt);  // 10,000 records = 635 KB
    write_to_disk();
}
```

**Issues:**
- Serializes unchanged records repeatedly
- Snapshot size grows linearly with CRDT size
- I/O cost scales with total records, not changed records

**Example scenario:**
```
CRDT with 10,000 records:
- User updates 10 records
- Snapshot threshold reached
- Serialize all 10,000 records (635 KB)
- Only 10 records (6 KB) actually changed!

Wasted: 629 KB serialization + I/O
```

## Incremental Snapshot Strategy

### Concept

```rust
// Full snapshot (baseline)
snapshot_0000.msgpack:  635 KB, covers versions 0-1000

// Incremental snapshots (deltas)
snapshot_0001.msgpack:   12 KB, covers versions 1001-2000
snapshot_0002.msgpack:    8 KB, covers versions 2001-3000
snapshot_0003.msgpack:   15 KB, covers versions 3001-4000

// Recovery: Load base + apply all incrementals
total_load = 635 KB + 12 KB + 8 KB + 15 KB = 670 KB
```

### Implementation Design

```rust
// Snapshot metadata
#[derive(Serialize, Deserialize)]
struct SnapshotMetadata {
    snapshot_type: SnapshotType,
    base_version: u64,        // What version this builds on
    max_version: u64,         // Highest version in this snapshot
    record_count: usize,
    tombstone_count: usize,
    created_at: u64,
}

#[derive(Serialize, Deserialize)]
enum SnapshotType {
    Full,                     // Complete CRDT state
    Incremental { since: u64 }, // Only changes since version
}

// Incremental snapshot structure
#[derive(Serialize, Deserialize)]
struct IncrementalSnapshot<K, C, V> {
    metadata: SnapshotMetadata,

    // Only records that changed
    changed_records: HashMap<K, Record<C, V>>,

    // New tombstones
    new_tombstones: HashMap<K, TombstoneInfo>,

    // Updated clock
    clock: LogicalClock,
}
```

### CRDT Tracking Support

**Good news**: CRDT already tracks what we need!

```rust
// In lib.rs:349-360
pub struct Record<C, V> {
    pub fields: HashMap<C, V>,
    pub column_versions: HashMap<C, ColumnVersion>,
    pub lowest_local_db_version: u64,  // ← First change
    pub highest_local_db_version: u64, // ← Last change
}
```

**Getting changed records:**

```rust
impl<K, C, V> CRDT<K, C, V> {
    pub fn get_changed_records_since(&self, version: u64) -> HashMap<K, Record<C, V>> {
        self.data
            .iter()
            .filter(|(_, record)| record.highest_local_db_version > version)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn get_new_tombstones_since(&self, version: u64) -> HashMap<K, TombstoneInfo> {
        self.tombstones
            .iter()
            .filter(|(_, info)| info.local_db_version > version)
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}
```

## Snapshot Strategy Options

### Option 1: Simple Fixed Interval

```rust
// Full snapshot every N incrementals
if (snapshot_count % 10 == 0) {
    create_full_snapshot();
} else {
    create_incremental_snapshot(last_snapshot_version);
}
```

**Pros:** Simple
**Cons:** Fixed ratio may not be optimal

### Option 2: Size-Based Trigger

```rust
// Create full snapshot when incrementals exceed threshold
total_incremental_size = sum(incremental_sizes);
if (total_incremental_size > full_snapshot_size * 0.5) {
    create_full_snapshot();
} else {
    create_incremental_snapshot();
}
```

**Pros:** Adapts to change patterns
**Cons:** More complex

### Option 3: Time + Size Hybrid

```rust
// Full snapshot daily, incrementals in between
if (time_since_full > 24_hours) {
    create_full_snapshot();
} else if (changes_since_snapshot > threshold) {
    create_incremental_snapshot();
}
```

**Pros:** Balances recovery time with space
**Cons:** More state to track

## Recovery Process

```rust
impl PersistedCRDT {
    fn recover(base_path: &Path) -> Result<CRDT> {
        // 1. Find latest full snapshot
        let full_snapshot = find_latest_full_snapshot(base_path)?;
        let mut crdt = load_full_snapshot(&full_snapshot)?;
        let base_version = full_snapshot.max_version;

        // 2. Find all incremental snapshots after base
        let incrementals = find_incremental_snapshots(base_path, base_version)?;

        // 3. Apply incrementals in order
        for incremental in incrementals {
            apply_incremental(&mut crdt, incremental)?;
        }

        // 4. Replay WAL as usual
        replay_wal(&mut crdt, base_path)?;

        Ok(crdt)
    }

    fn apply_incremental(&mut self, incremental: IncrementalSnapshot) {
        // Merge changed records
        for (k, record) in incremental.changed_records {
            self.crdt.data.insert(k, record);
        }

        // Add new tombstones
        for (k, info) in incremental.new_tombstones {
            self.crdt.tombstones.insert_or_assign(k, info);
        }

        // Update clock
        self.crdt.clock.set_time(incremental.clock.current_time());
    }
}
```

## MessagePack Advantages for Incremental Snapshots

### 1. Self-Describing Format

Each snapshot is a complete, standalone MessagePack document:
```
snapshot_0000.msgpack:  {type: "full", version: 1000, data: {...}}
snapshot_0001.msgpack:  {type: "incremental", since: 1000, version: 2000, data: {...}}
```

No need for complex indexing - each file is independently readable.

### 2. Schema Evolution

Can add fields to snapshot format without breaking old snapshots:
```rust
// V1 snapshots
struct Snapshot {
    metadata: SnapshotMetadata,
    records: HashMap<K, Record>,
}

// V2 snapshots (add compression metadata)
struct Snapshot {
    metadata: SnapshotMetadata,
    records: HashMap<K, Record>,
    #[serde(default)]
    compression: Option<CompressionInfo>,  // Old snapshots: None
}
```

### 3. Efficient Partial Serialization

MessagePack efficiently handles small maps:
```rust
// Full snapshot: 10,000 records
serialize({records: 10_000 entries}) → 635 KB

// Incremental: 10 changed records
serialize({records: 10 entries}) → 2 KB

// Size is proportional to changed data!
```

## Performance Analysis

### Scenario: Large CRDT with Low Change Rate

```
CRDT: 100,000 records (6.3 MB full snapshot)
Change rate: 100 records/hour
Snapshot interval: 1 hour

Traditional approach:
- Every hour: serialize 100,000 records (6.3 MB)
- Daily: 24 × 6.3 MB = 151 MB written

Incremental approach (1 full/day + incrementals):
- Full snapshot: 1 × 6.3 MB = 6.3 MB
- Incrementals: 23 × 20 KB = 460 KB
- Daily: 6.3 MB + 0.46 MB = 6.76 MB written

Savings: 151 MB → 6.76 MB = 95% reduction!
```

### Recovery Time

```
Traditional (single full snapshot):
- Read: 6.3 MB
- Deserialize: 6.3 MB
- Total: ~100 ms

Incremental (1 full + 23 incrementals):
- Read: 6.3 MB + 460 KB = 6.76 MB
- Deserialize: 6.76 MB
- Apply 23 incrementals: ~10 ms
- Total: ~110 ms

Overhead: 10% slower recovery, but 95% less I/O during operation
```

## Implementation Plan

### Phase 1: Add Incremental Support to CRDT

```rust
// src/lib.rs
impl<K, C, V> CRDT<K, C, V> {
    pub fn get_changed_since(&self, version: u64) -> (
        HashMap<K, Record<C, V>>,
        HashMap<K, TombstoneInfo>
    ) {
        let records = self.get_changed_records_since(version);
        let tombstones = self.get_new_tombstones_since(version);
        (records, tombstones)
    }
}
```

### Phase 2: Add Snapshot Types to persist.rs

```rust
// src/persist.rs
enum SnapshotType {
    Full,
    Incremental { since_version: u64 },
}

impl PersistedCRDT {
    fn create_snapshot_with_type(&mut self, snapshot_type: SnapshotType) -> Result<()> {
        match snapshot_type {
            SnapshotType::Full => self.create_full_snapshot(),
            SnapshotType::Incremental { since_version } => {
                self.create_incremental_snapshot(since_version)
            }
        }
    }
}
```

### Phase 3: Update Snapshot Logic

```rust
impl PersistedCRDT {
    fn check_auto_snapshot(&mut self) -> Result<()> {
        if self.should_create_full_snapshot() {
            self.create_full_snapshot()?;
        } else if self.should_create_incremental_snapshot() {
            self.create_incremental_snapshot(self.last_snapshot_version)?;
        }
        Ok(())
    }

    fn should_create_full_snapshot(&self) -> bool {
        // Full snapshot every 10 incrementals or 24 hours
        self.incremental_count >= 10 ||
        self.time_since_full_snapshot() > Duration::from_hours(24)
    }
}
```

### Phase 4: Update Recovery

```rust
fn recover(base_path: &Path) -> Result<CRDT> {
    // Find all snapshots (full + incrementals)
    let snapshots = discover_snapshots(base_path)?;

    // Load base full snapshot
    let full = snapshots.iter()
        .filter(|s| s.is_full())
        .max_by_key(|s| s.version)?;

    let mut crdt = load_snapshot(full)?;

    // Apply incrementals in order
    for incremental in snapshots.iter()
        .filter(|s| s.is_incremental() && s.base_version >= full.version)
        .sorted_by_key(|s| s.version) {

        apply_incremental(&mut crdt, incremental)?;
    }

    // Replay WAL as usual
    replay_wal(&mut crdt, base_path)?;

    Ok(crdt)
}
```

## Benefits Summary

| Metric | Full Snapshots | Incremental Snapshots |
|--------|----------------|----------------------|
| **Snapshot I/O** | 6.3 MB/hour | 20 KB/hour (99% less!) |
| **Daily I/O** | 151 MB | 6.76 MB (95% less!) |
| **Recovery time** | 100 ms | 110 ms (10% slower) |
| **Disk usage** | Same | Same (after compaction) |
| **Complexity** | Low | Medium |

## Recommendation

**YES, implement incremental snapshots with MessagePack!**

This is a **much better optimization** than zero-copy:
- ✅ **95% reduction in snapshot I/O** for typical workloads
- ✅ **Faster normal operations** (less serialization)
- ✅ **Only 10% slower recovery** (acceptable trade-off)
- ✅ **MessagePack handles it naturally** (self-describing format)
- ✅ **CRDT already tracks versions** (implementation is straightforward)

**Priority order for optimizations:**
1. 🥇 **Incremental snapshots** (95% I/O reduction)
2. 🥈 **Compression** (50-70% size reduction)
3. 🥉 **Schema evolution** (avoid data loss - CRITICAL)
4. ❌ Zero-copy (0.5% improvement - not worth it)

## Next Steps

1. Switch to MessagePack (schema evolution)
2. Implement incremental snapshots (huge I/O savings)
3. Add optional compression (zstd on top of MessagePack)
4. Profile and optimize from there

**Files to modify:**
- `src/lib.rs`: Add `get_changed_since()` methods
- `src/persist.rs`: Add snapshot type enum, incremental logic
- `src/persist/wal.rs`: No changes needed (WAL works same way)
