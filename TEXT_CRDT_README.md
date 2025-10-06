# Text CRDT Implementation

A line-based Conflict-free Replicated Data Type (CRDT) for collaborative text editing, designed for simplicity and strong eventual consistency.

## Overview

This implementation provides a **line-based text CRDT** that avoids the complexity of character-level CRDTs while still enabling distributed collaborative editing. Each line is treated as an atomic unit with:

- **Fractional positioning** - Lines can be inserted between any two existing lines
- **UUID identity** - Each line has a stable unique identifier
- **Version tracking** - Tracks causality with logical clocks
- **Configurable merge strategies** - Choose between LWW, BWW, or experimental auto-merge

## Architecture

### Core Components

1. **FractionalPosition** (`text_crdt.hpp:70-128`)
   - Array-based fractional indexing (`std::vector<uint64_t>`)
   - Infinite density - can always insert between any two positions
   - Smart `between()` algorithm minimizes depth growth
   - Handles position collisions deterministically

2. **TextCRDT<K, V>** (`text_crdt.hpp:230-651`)
   - Dual indexing for performance:
     - `position_to_id_` - Ordered map for iteration
     - `lines_` - Hash map for fast lookup by UUID
   - Logical clock for causality tracking
   - Tombstone-based deletion
   - Change tracking for efficient sync
   - **Optional change streaming** - All modification operations accept optional `std::vector<TextChange>*` for live sync
   - **Text output** - `to_text()` for full document string, `get_all_lines()` for structured access

3. **LineData** (`text_crdt.hpp:156-186`)
   - UUID identity (template parameter K)
   - FractionalPosition for ordering
   - Content (template parameter V, typically `std::string`)
   - Version metadata (line_version + db_version + node_id)
   - Optional conflict storage for BWW
   - Optional base_content for auto-merge

### Merge Strategies

#### 1. Last-Write-Wins (LWW) - Default ✅
```cpp
node.merge_changes(changes);
```
- Deterministic conflict resolution
- Higher line_version wins
- Falls back to db_version, then node_id
- **Status**: Production-ready

#### 2. Both-Writes-Win (BWW) ✅
```cpp
BothWritesWinMergeRule<K, V> bww;
node.merge_changes_with_rule(changes, bww);
```
- Preserves all concurrent edits
- Stores conflicts for user resolution
- Deterministic ordering (by version)
- **Status**: Production-ready

#### 3. Auto-Merge ⚠️ EXPERIMENTAL
```cpp
AutoMergingTextRule<K> auto_merge;
node.merge_changes_with_rule(changes, auto_merge);
```
- Attempts 3-way merge using base content
- Word-level diff for non-overlapping changes
- Falls back to BWW on conflicts
- **Status**: BROKEN - Do not use (see Known Issues)

## Usage Example

### Basic Usage with Initial Sync

```cpp
#include "text_crdt.hpp"

// Create two nodes
TextCRDT<std::string> node1(1);
TextCRDT<std::string> node2(2);

// Node1 creates document
auto id1 = node1.insert_line_at_end("Hello world");
auto id2 = node1.insert_line_at_end("Goodbye world");

// Get full text
std::string text = node1.to_text();  // "Hello world\nGoodbye world"

// Initial sync to node2 using get_changes_since()
uint64_t last_sync = 0;
auto changes = node1.get_changes_since(last_sync);
node2.merge_changes(changes);

// Both nodes edit concurrently
node1.edit_line(id1, "Hello CRDT");
node2.edit_line(id1, "Hello distributed systems");

// Sync changes bidirectionally
changes = node1.get_changes_since(last_sync);
node2.merge_changes(changes);  // LWW: node2's edit wins (higher node_id)

// Or use BWW to preserve both
BothWritesWinMergeRule<std::string, std::string> bww;
node1.merge_changes_with_rule(node2.get_changes_since(0), bww);
// Now node1 has conflicts - both versions preserved
```

### Live Sync with Change Streaming

For real-time collaboration after initial sync, use change streaming to avoid expensive `get_changes_since()` calls:

```cpp
TextCRDT<std::string> node1(1);
TextCRDT<std::string> node2(2);

// Container to collect changes
std::vector<TextChange<std::string, std::string>> changes;

// All operations support optional change streaming
auto id1 = node1.insert_line_at_end("Line 1", &changes);
auto id2 = node1.insert_line_at_end("Line 2", &changes);
node1.edit_line(id1, "Modified Line 1", &changes);
node1.delete_line(id2, &changes);

// Stream changes to node2 immediately
node2.merge_changes(changes);

// Both nodes are now in sync without calling get_changes_since()
assert(node1.to_text() == node2.to_text());
```

## Testing

Run the comprehensive test suite:

```bash
clang++ -std=c++20 -O2 -o test-text-crdt test_text_crdt.cpp && ./test-text-crdt
```

**Test Coverage** (30 tests):
- ✅ FractionalPosition edge cases (5 tests)
- ✅ Basic operations (insert, edit, delete) (6 tests)
- ✅ Advanced operations (interleaved insertions, consistency) (3 tests)
- ✅ Distributed CRDT (sync, concurrent edits, conflicts) (8 tests)
- ✅ Change streaming (insert, edit, delete, live sync) (4 tests)
- ✅ Text output (to_text, get_all_lines) (2 tests)
- ⚠️ Auto-merge (2 tests, non-overlapping disabled due to bugs)

## Known Issues

### AutoMergingTextRule - EXPERIMENTAL/BROKEN ⚠️

The auto-merge functionality has critical bugs and should NOT be used:

1. **Diff merge algorithm is broken** - `merge_diffs()` naively concatenates diffs instead of properly combining them, causing duplicate KEEP operations

2. **Non-deterministic convergence** - Two nodes merging the same edits may produce different results, **violating CRDT convergence guarantees**

3. **apply_diff() is incorrect** - Doesn't properly handle merged diffs with overlapping positions

**Recommended**: Use `BothWritesWinMergeRule` instead, which correctly preserves all concurrent edits for user resolution.

**To fix**: Implement proper diff merge algorithm that deduplicates operations and resolves position conflicts correctly.

## Design Decisions

### Why line-based?
- Simpler than character-level CRDTs (no tombstone interleaving)
- More intuitive for code/document editing
- Better performance for typical edit patterns
- Still supports fine-grained merging with configurable strategies

### Why fractional positioning?
- Infinite density (always room to insert)
- No global coordination needed
- Deterministic ordering
- Simpler than tree-based approaches (LSEQ, RGA)

### Why dual indexing?
- Fast iteration in document order (via `position_to_id_`)
- Fast lookup/edit by UUID (via `lines_`)
- Small memory overhead for significant performance gain

### Why store base_content?
- Enables true 3-way merge (when auto-merge is fixed)
- Allows detecting non-overlapping concurrent edits
- Minimal overhead (only stored during concurrent editing)

## Performance Characteristics

- **Insert**: O(log n) - Binary search in position map
- **Edit**: O(1) - Hash lookup by UUID
- **Delete**: O(log n) - Remove from position map
- **Iterate**: O(n) - Walk ordered position map
- **Sync**: O(changes) - Only send incremental changes
- **Position depth**: O(log insertions between same pair) - Grows slowly

## Future Work

1. **Fix AutoMergingTextRule**:
   - Proper diff merge algorithm
   - Comprehensive test coverage
   - Verify convergence in all scenarios

2. **Configurable granularity**:
   - Sentence-level splitting
   - Paragraph-level splitting
   - Custom delimiter support

3. **Compression**:
   - Tombstone garbage collection
   - Position path compression/rebalancing
   - Change history compaction

4. **Move operations**:
   - Efficient line reordering
   - Concurrent move conflict resolution

5. **Persistence**:
   - Serialization format
   - Incremental checkpoints
   - Storage backend integration

## References

- Original design discussion: See `Crdt-lite Code Discussion.md`
- Fractional indexing: Inspired by Figma's approach
- CRDT theory: [CRDTs: An Update or, Just a Future in Depth](https://hal.inria.fr/hal-00932833/document)
- 3-way merge: Git's merge algorithm

## License

See parent directory LICENSE file.
