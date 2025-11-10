# Schema Evolution Investigation - Summary

## Question
How does persist.rs handle CRDT schema changes (adding new fields, no removals) when loading WAL/snapshot files from older versions?

## Answer
**It doesn't.** Adding fields will cause deserialization to fail with `UnexpectedEnd` error.

## Root Cause
persist.rs uses **bincode v2.0.1** for serialization, which:
- Encodes exact field count into binary format
- Requires perfect struct matching
- Does NOT support `#[serde(default)]` for adding fields
- Designed for speed, not schema evolution

## Impact

```rust
// Developer adds a field
pub struct Record<C, V> {
    pub fields: HashMap<C, V>,
    pub column_versions: HashMap<C, ColumnVersion>,
    pub lowest_local_db_version: u64,
    pub highest_local_db_version: u64,
    pub new_field: u64,  // ← Adding this breaks everything
}
```

**Result**:
- ❌ Cannot load old snapshots
- ❌ Cannot replay old WAL files
- ❌ Users lose data access
- ❌ Requires manual migration

## Test Evidence

Created 3 test programs:

```bash
# Run these to see the failures:
cargo run --example test_schema_evolution --features persist
cargo run --example test_schema_evolution_failure --features persist
cargo run --example test_bincode_behavior --features binary
```

**Output shows**: `UnexpectedEnd { additional: 1 }` when trying to deserialize old data with new struct.

## What Breaks

| Change | Result |
|--------|--------|
| Add field to `Record<C, V>` | ❌ BREAKS |
| Add field to `Change<K, C, V>` | ❌ BREAKS |
| Add field to `CRDT<K, C, V>` | ❌ BREAKS |
| Remove field | ❌ BREAKS |
| Rename field | ❌ BREAKS |
| Change field type | ❌ BREAKS |
| Reorder fields | ❌ BREAKS |

## Recommendations

### Immediate
1. **Document the limitation** - Add warning to docs
2. **Version detection** - Fail fast with clear error message

### Short-term
3. **Add JSON option** - New `persist-json` feature using `serde_json`
4. **Migration tool** - Convert bincode → JSON

### Long-term
5. **Deprecate bincode** - JSON is safer for persistence
6. **Schema versioning** - Versioned enums + migrations

## Files Changed

- `SCHEMA_EVOLUTION_INVESTIGATION.md` - Full investigation report (400+ lines)
- `examples/test_schema_evolution.rs` - Overview test
- `examples/test_schema_evolution_failure.rs` - Failure demonstration
- `examples/test_bincode_behavior.rs` - Format limitations

## Conclusion

**persist.rs has no schema evolution support.** Any struct changes will break existing data files. This is a fundamental limitation of bincode that cannot be worked around without switching serialization formats.

**Recommended**: Switch to JSON/MessagePack for `persist` feature.
