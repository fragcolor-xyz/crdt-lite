# Schema Evolution Investigation for persist.rs

**Date**: 2025-11-10
**Investigated by**: Claude
**Branch**: `claude/investigate-persist-crdt-schema-changes-011CUzBAfhzsEegKXmUzWTft`

## Executive Summary

**Current Status**: ❌ **BREAKING** - Adding new fields to CRDT structures will break loading of old snapshots and WAL files.

**Root Cause**: persist.rs uses `bincode` v2.0.1 for serialization, which does not support schema evolution (adding/removing fields) even with `#[serde(default)]` attributes.

## Investigation Details

### Current Serialization Approach

1. **Snapshots** (`persist.rs:716-719`)
   - Uses `CRDT::to_bytes()` → `bincode::serde::encode_to_vec()`
   - Serializes entire CRDT state: `CRDT<K, C, V>`
   - No version metadata in file

2. **WAL (Write-Ahead Log)** (`persist/wal.rs:88-107`)
   - Uses `bincode::serde::encode_to_vec()` for each `Change<K, C, V>`
   - Length-prefixed binary format
   - No version metadata per change

3. **Recovery** (`persist.rs:625-669`)
   - Loads latest snapshot via `CRDT::from_bytes()` → `bincode::serde::decode_from_slice()`
   - Replays all WAL files via `read_wal_file()` → `bincode::serde::decode_from_slice()`
   - Fails immediately if deserialization encounters unexpected structure

### Key Structures Affected

```rust
// lib.rs:349-360
pub struct Record<C, V> {
    pub fields: HashMap<C, V>,
    pub column_versions: HashMap<C, ColumnVersion>,
    pub lowest_local_db_version: u64,
    pub highest_local_db_version: u64,
}

// lib.rs:137-160
pub struct Change<K, C, V> {
    pub record_id: K,
    pub col_name: Option<C>,
    pub value: Option<V>,
    pub col_version: u64,
    pub db_version: u64,
    pub node_id: NodeId,
    pub local_db_version: u64,
    pub flags: u32,  // ← Added in recent version
}

// lib.rs:522-538
pub struct CRDT<K, C, V> {
    node_id: NodeId,
    clock: LogicalClock,
    data: HashMap<K, Record<C, V>>,
    tombstones: TombstoneStorage<K>,
    base_version: u64,
}
```

### Bincode Limitations

Tested in `examples/test_bincode_behavior.rs`:

```
Test: Deserialize old data with new struct
Result: FAIL - UnexpectedEnd { additional: 1 }

Even with #[serde(default)], bincode v2.0.1 rejects:
  ✗ Adding fields
  ✗ Removing fields
  ✗ Reordering fields

Bincode is designed for exact struct matching, not evolution.
```

### Schema Change Scenarios

| Scenario | Current Behavior | Impact |
|----------|------------------|--------|
| Add field to `Record<C, V>` | ❌ Deserialization fails | Cannot load old snapshots |
| Add field to `Change<K, C, V>` | ❌ Deserialization fails | Cannot replay old WAL files |
| Remove field | ❌ Deserialization fails | Extra data in old files causes error |
| Rename field | ❌ Deserialization fails | Field name mismatch |
| Change field type | ❌ Deserialization fails | Type mismatch |

## Real-World Impact

### Example: Adding a field to `Record`

```rust
// Developer adds a new field to Record
pub struct Record<C, V> {
    pub fields: HashMap<C, V>,
    pub column_versions: HashMap<C, ColumnVersion>,
    pub lowest_local_db_version: u64,
    pub highest_local_db_version: u64,
    pub last_modified_timestamp: u64,  // NEW FIELD
}
```

**What happens:**
1. User upgrades to new version
2. Application tries to load existing snapshot/WAL
3. `PersistedCRDT::open()` calls `CRDT::from_bytes()`
4. Bincode encounters struct with only 4 fields, expects 5
5. Returns `DecodeError`: `UnexpectedEnd { additional: 1 }`
6. Application fails to start
7. **User loses access to their data**

### Workaround (Manual Migration)

Users would need to:
1. Keep old version of application running
2. Export data via `get_changes_since(0)`
3. Upgrade application
4. Delete old snapshot/WAL files
5. Re-import all changes
6. **Requires downtime and manual intervention**

## Test Results

Three test programs created to demonstrate the issue:

### 1. `test_schema_evolution.rs`
- Creates snapshot with current schema
- Analyzes compatibility scenarios
- Documents which changes would break

### 2. `test_schema_evolution_failure.rs`
- Simulates adding field to `Record`
- Shows exact error: `UnexpectedEnd { additional: 1 }`
- Confirms `#[serde(default)]` does NOT help

### 3. `test_bincode_behavior.rs`
- Tests bincode v2.0.1 schema evolution capabilities
- Confirms bincode cannot handle struct changes
- Explains bincode's design philosophy

**Run tests:**
```bash
cargo run --example test_schema_evolution --features persist
cargo run --example test_schema_evolution_failure --features persist
cargo run --example test_bincode_behavior --features binary
```

## Recommendations

### Option 1: Switch to Self-Describing Format (Recommended)

**Replace bincode with JSON or MessagePack**

Pros:
- ✅ `#[serde(default)]` works as expected
- ✅ Can add optional fields safely
- ✅ Can remove deprecated fields
- ✅ Better for long-term storage
- ✅ Easier debugging (JSON is human-readable)

Cons:
- ❌ Larger file sizes (10-50% increase)
- ❌ Slightly slower (5-10% performance impact)
- ❌ Breaking change (requires migration)

**Implementation:**
```rust
// In lib.rs, add alongside binary feature:
#[cfg(feature = "json")]
pub fn to_json_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(self)
}

#[cfg(feature = "json")]
pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
    serde_json::from_slice(bytes)
}
```

### Option 2: Implement Versioned Enums

**Wrap structs in version enums**

```rust
#[derive(Serialize, Deserialize)]
enum VersionedRecord<C, V> {
    V1(RecordV1<C, V>),
    V2(RecordV2<C, V>),
}

#[derive(Serialize, Deserialize)]
pub struct RecordV1<C, V> {
    pub fields: HashMap<C, V>,
    pub column_versions: HashMap<C, ColumnVersion>,
    pub lowest_local_db_version: u64,
    pub highest_local_db_version: u64,
}

#[derive(Serialize, Deserialize)]
pub struct RecordV2<C, V> {
    pub fields: HashMap<C, V>,
    pub column_versions: HashMap<C, ColumnVersion>,
    pub lowest_local_db_version: u64,
    pub highest_local_db_version: u64,
    pub last_modified_timestamp: u64,
}

impl<C, V> VersionedRecord<C, V> {
    fn migrate_to_latest(self) -> RecordV2<C, V> {
        match self {
            VersionedRecord::V1(v1) => RecordV2 {
                fields: v1.fields,
                column_versions: v1.column_versions,
                lowest_local_db_version: v1.lowest_local_db_version,
                highest_local_db_version: v1.highest_local_db_version,
                last_modified_timestamp: 0, // Default value
            },
            VersionedRecord::V2(v2) => v2,
        }
    }
}
```

Pros:
- ✅ Keeps bincode (compact format)
- ✅ Explicit migration logic
- ✅ Can handle complex transformations

Cons:
- ❌ More boilerplate code
- ❌ Must maintain all old versions
- ❌ Still requires manual version bumps

### Option 3: Add Schema Version Field

**Add version metadata to detect incompatible schemas**

```rust
pub struct Record<C, V> {
    #[serde(default)]
    schema_version: u32,  // 0 if missing (old snapshot)
    pub fields: HashMap<C, V>,
    pub column_versions: HashMap<C, ColumnVersion>,
    pub lowest_local_db_version: u64,
    pub highest_local_db_version: u64,
    // Future fields go here
}
```

**Problem**: This STILL breaks with bincode! Old snapshots don't have this field, so adding it causes deserialization to fail.

**Solution**: Must be combined with Option 1 (switch format) or Option 2 (versioned enums).

### Option 4: Document as "Not Supported"

**Accept limitation and document it clearly**

Add to documentation:
```
## Schema Stability

⚠️ WARNING: The persist.rs serialization format is UNSTABLE.

Adding, removing, or modifying fields in CRDT structures will break
compatibility with existing snapshots and WAL files.

If you need to upgrade to a version with schema changes:
1. Export data: `crdt.get_changes_since(0)`
2. Delete old persistence files
3. Upgrade application
4. Re-import changes

This is a known limitation of the bincode serialization format.
Consider using in-memory CRDTs only for production use.
```

Pros:
- ✅ No code changes needed
- ✅ Honest about limitations

Cons:
- ❌ Poor user experience
- ❌ Not suitable for production
- ❌ Data migration is painful

## Recommended Action Plan

### Immediate (Current Release)

1. **Document the limitation** in README and rustdocs
   - Add warning to `persist` module documentation
   - Clarify that schema is not stable
   - Provide migration instructions

2. **Add schema version detection**
   - Add `SCHEMA_VERSION` constant to persist.rs
   - Write version to snapshot metadata file (separate from data)
   - Check version on load, fail fast with clear error

### Short Term (Next Release)

3. **Add JSON persistence option**
   - New feature flag: `persist-json`
   - Uses `serde_json` instead of `bincode`
   - Document as "recommended for production"
   - Provide migration tool: `bincode_to_json`

4. **Add all `#[serde(default)]` attributes**
   - Even though bincode doesn't use them
   - Prepares for JSON format
   - Makes future migration easier

### Long Term (Future)

5. **Deprecate bincode for persistence**
   - Mark `persist` feature as deprecated
   - Recommend `persist-json` instead
   - Eventually remove bincode option

6. **Implement proper schema evolution**
   - Version enums for breaking changes
   - Migration functions between versions
   - Automatic upgrade on load

## Conclusion

**The current persist.rs implementation has a critical limitation**: it cannot handle schema changes without breaking existing data files.

**Primary recommendation**: Switch from bincode to a self-describing format (JSON or MessagePack) that supports `#[serde(default)]` and field addition/removal.

**Immediate action**: Document this limitation clearly so users are not surprised by data loss during upgrades.

## Appendix: File Locations

- Snapshot creation: `src/persist.rs:712-764`
- Snapshot loading: `src/persist.rs:625-669`
- WAL writing: `src/persist/wal.rs:85-108`
- WAL reading: `src/persist/wal.rs:175-212`
- CRDT serialization: `src/lib.rs:1286-1312`
- Test programs: `examples/test_schema_*.rs`

## Related Issues

None found in the repository yet. This investigation was prompted by:
> "Investigate how persist.rs deals when the CRDT schema has changed
> (more new fields added, no removals) and loading a wal/snapshot
> without those fields (older versions)"
