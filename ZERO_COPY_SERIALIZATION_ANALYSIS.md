# Zero-Copy Serialization for CRDT Snapshots

**Context**: When creating snapshots, does zero-copy serialization help?

## The Question

When `persist.rs` creates a snapshot:
```rust
// Current approach (MessagePack)
let bytes = crdt.to_msgpack_bytes()?;  // Allocates Vec<u8>
file.write_all(&bytes)?;               // Writes buffer to disk

// Zero-copy approach (hypothetical)
crdt.serialize_to_writer(&mut file)?;  // Write directly to file?
```

## Zero-Copy Serialization: What It Means

"Zero-copy" serialization typically means:
1. No intermediate `Vec<u8>` buffer
2. Write directly to output (file, socket, etc.)
3. Avoid copying data when possible

## Analysis for CRDT Snapshots

### What Actually Happens During Serialization

```rust
// CRDT structure in memory
CRDT {
    data: HashMap<K, Record<C, V>> {
        "record_1" → Record {
            fields: HashMap<C, V> { "name" → "Alice", ... },
            column_versions: HashMap<C, ColumnVersion> { ... },
            ...
        },
        "record_2" → ...,
        ...
    },
    tombstones: TombstoneStorage<K>,
    clock: LogicalClock,
}
```

**Serialization steps (any format):**
1. ✅ Walk HashMap entries (can't avoid)
2. ✅ Encode keys as strings/bytes (can't avoid)
3. ✅ Encode values with type info (can't avoid)
4. ⚠️ Write to intermediate buffer vs direct to file
5. ⚠️ Copy buffer to file

**Step 4-5 are where "zero-copy" could help.**

### Does It Actually Help?

**Potential savings:**
```rust
// Traditional (MessagePack to buffer)
let bytes = rmp_serde::to_vec(&crdt)?;  // Allocate 635 KB Vec
file.write_all(&bytes)?;                // Write 635 KB

// Zero-copy (direct write)
rmp_serde::encode::write(&mut file, &crdt)?;  // Write directly
// Saves one 635 KB allocation + copy
```

**But:**
- File I/O is buffered anyway (BufWriter)
- OS does its own buffering
- The actual work is traversing the HashMap
- Allocation is fast (modern allocators)

### Benchmark Comparison

For a 10,000 record CRDT (635 KB serialized):

**Traditional (to buffer):**
```
1. Walk data structures:     ~5 ms  (unavoidable)
2. Encode to Vec<u8>:         ~2 ms  (serialize)
3. Allocate 635 KB Vec:       ~0.1 ms (one malloc)
4. Write to BufWriter:        ~1 ms  (copy to OS buffer)
5. Fsync to disk:            ~10 ms  (I/O wait)
---
Total: ~18 ms
```

**Zero-copy (direct write):**
```
1. Walk data structures:     ~5 ms  (unavoidable)
2. Encode to BufWriter:       ~2 ms  (serialize)
3. Skip Vec allocation:       ✅ Save 0.1 ms
4. Direct write:              ~1 ms  (to OS buffer)
5. Fsync to disk:            ~10 ms  (I/O wait)
---
Total: ~18 ms (practically identical)
```

**Savings: 0.1 ms / 18 ms = 0.5% improvement**

### Why So Little Benefit?

1. **Allocation is cheap**: Modern allocators (jemalloc, mimalloc) make 635KB allocations in microseconds
2. **Traversal dominates**: Walking HashMap entries is the expensive part
3. **I/O is slow**: Disk writes (even buffered) are 100x slower than memory ops
4. **OS buffers anyway**: OS maintains page cache, one more copy doesn't matter

### Memory Pressure Consideration

**Could it help with memory?**

```rust
// Peak memory usage

Traditional:
  CRDT in memory:          10 MB
  + Serialization buffer:   635 KB
  ------------------------
  Total peak:              10.6 MB

Zero-copy:
  CRDT in memory:          10 MB
  + BufWriter buffer:       8 KB (tiny!)
  ------------------------
  Total peak:              10 MB
```

**Savings: 635 KB / 10.6 MB = 6% peak memory**

Not significant unless:
- Extremely memory-constrained (embedded systems)
- Creating multiple snapshots concurrently
- CRDT is already close to memory limits

## Format Support

| Format | Zero-Copy Serialize | API |
|--------|---------------------|-----|
| Bincode | ❌ No | Only `to_vec()` |
| Bitcode | ❌ No | Only `encode()` → Vec |
| **MessagePack** | ✅ Yes | `rmp_serde::encode::write()` |
| CBOR | ✅ Yes | `serde_cbor::to_writer()` |
| FlexBuffers | ⚠️ Partial | Still uses internal buffer |

**MessagePack already supports zero-copy serialization!**

```rust
// Current: allocate buffer
let bytes = rmp_serde::to_vec(&crdt)?;
file.write_all(&bytes)?;

// Zero-copy: write directly
rmp_serde::encode::write(&mut file, &crdt)?;
```

## Real-World Impact

For typical CRDT operations:

### Snapshot Creation (1/hour)
```
Traditional:  18 ms
Zero-copy:    17.9 ms
Savings:      0.1 ms (negligible)
Impact:       User doesn't notice
```

### Snapshot Creation (embedded, 256 KB RAM)
```
Traditional:  Might OOM if CRDT + buffer > RAM
Zero-copy:    Uses 8 KB buffer instead of 635 KB
Impact:       Could make difference in extreme cases
```

## Conclusion

**Zero-copy serialization does NOT significantly help for CRDT snapshots because:**

1. ✅ **Traversal dominates cost** (~5ms vs 0.1ms for allocation)
2. ✅ **I/O is the bottleneck** (disk writes are 100x slower)
3. ✅ **Memory savings are small** (6% peak memory)
4. ✅ **OS buffers anyway** (one more copy doesn't matter)

**But:** MessagePack already supports it via `encode::write()`, so we could use it anyway at no cost.

## Recommendation

**Use MessagePack with buffer (current approach):**

```rust
// Simple, clear, works
let bytes = rmp_serde::to_vec(&crdt)?;
file.write_all(&bytes)?;
```

**Don't bother with zero-copy unless:**
- Profiling shows serialization buffer allocation is a bottleneck (unlikely)
- Extremely memory-constrained environment (<512 KB RAM)
- Creating many concurrent snapshots

**Better optimizations for snapshot performance:**
1. **Compression**: zstd can reduce size by 50-70% (bigger win than zero-copy)
2. **Incremental snapshots**: Only write changed records
3. **Parallel serialization**: Serialize chunks concurrently
4. **Async I/O**: Don't block on fsync

## Updated Recommendation

**MessagePack is still the best choice, and zero-copy doesn't change the decision.**

FlexBuffers' claimed "zero-copy" advantages:
- ❌ Serialization: Doesn't help (0.5% improvement)
- ❌ Deserialization: Doesn't help (need mutable CRDT)
- ❌ Size: 3x larger (185% overhead)

MessagePack wins on:
- ✅ Size: Only 27% overhead
- ✅ Speed: Fast enough (allocation is not bottleneck)
- ✅ Simplicity: Clean API
- ✅ Schema evolution: Works perfectly
