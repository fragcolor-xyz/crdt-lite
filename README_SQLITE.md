# CRDT-SQLite Wrapper

A lightweight wrapper that adds CRDT (Conflict-free Replicated Data Type) synchronization to SQLite databases. This allows SQLite tables to be synchronized across multiple nodes with automatic conflict resolution.

## Features

- ✅ **Automatic change tracking** - Uses SQLite update hooks, no triggers needed
- ✅ **Transparent synchronization** - Use standard SQL, changes tracked automatically
- ✅ **Column-level conflicts** - Fine-grained conflict resolution per field
- ✅ **Last-write-wins** - Deterministic conflict resolution with column/db/node version
- ✅ **Tombstone-based deletion** - Proper deletion tracking and synchronization
- ✅ **Transaction support** - ACID guarantees with automatic rollback
- ✅ **Type preservation** - Maintains SQLite type affinity (INTEGER, REAL, TEXT, BLOB)
- ✅ **Custom PRIMARY KEY support** - uint128_t IDs via lookaside table (distributed UUIDs)
- ✅ **Automatic schema migrations** - ALTER TABLE ADD COLUMN auto-detected and tracked
- ✅ **Security hardened** - SQL injection prevention, safe table name validation
- ✅ **Header-only CRDT core** - Easy to integrate

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/fragcolor-xyz/crdt-lite.git
cd crdt-lite

# Build with CMake
mkdir build && cd build
cmake .. -DBUILD_SQLITE_WRAPPER=ON
cmake --build .

# Run tests
ctest --output-on-failure
```

### Basic Usage

```cpp
#include "crdt_sqlite.hpp"

// Create database with unique node ID
CRDTSQLite db("myapp.db", 1);

// Create a regular SQLite table
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");

// Enable CRDT synchronization
db.enable_crdt("users");

// Use normal SQL - changes are tracked automatically
db.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");
db.execute("UPDATE users SET email = 'alice@newdomain.com' WHERE name = 'Alice'");

// Get changes since last sync (version 0 = all changes)
auto changes = db.get_changes_since(0);

// Send changes to other nodes...
// ... receive changes from other nodes ...

// Merge changes from another node
db.merge_changes(remote_changes);
```

## Advanced Features

### Distributed UUIDs with uint128_t

For globally unique IDs across nodes without coordination, use `uint128_t` instead of auto-increment:

```cpp
// Define ID type before including header
#define CRDT_RECORD_ID_TYPE __uint128_t
#include "crdt_sqlite.hpp"
#include "record_id_types.hpp"

CRDTSQLite db("myapp.db", 1);

// Create table with BLOB primary key
db.execute("CREATE TABLE users (id BLOB PRIMARY KEY, name TEXT, email TEXT)");
db.enable_crdt("users");

// Generate distributed UUID (node_id in high 64 bits, timestamp in low 64 bits)
uint128_t user_id = RecordIdTraits<uint128_t>::generate_with_node(1);

// Insert with explicit ID
sqlite3_stmt *stmt = db.prepare("INSERT INTO users (id, name, email) VALUES (?, ?, ?)");
RecordIdTraits<uint128_t>::bind_to_sqlite(stmt, 1, user_id);
sqlite3_bind_text(stmt, 2, "Alice", -1, SQLITE_STATIC);
sqlite3_bind_text(stmt, 3, "alice@example.com", -1, SQLITE_STATIC);
sqlite3_step(stmt);
sqlite3_finalize(stmt);

// Updates and deletes work automatically!
db.execute("UPDATE users SET email = 'new@email.com' WHERE id = ?");  // Bind uint128_t
db.execute("DELETE FROM users WHERE id = ?");  // Tracked via lookaside table
```

**How it works:**
- Shadow tables use `BLOB` type for `record_id` column
- Lookaside table (`_crdt_users_lookaside`) maps SQLite `rowid` → `uint128_t` ID
- DELETE operations query lookaside (main table row is already gone)
- Fully portable - no special SQLite compilation flags needed

**ID Format:**
```
[64 bits: node_id][64 bits: timestamp + random]
```

### Automatic Schema Migrations

ALTER TABLE ADD COLUMN is automatically detected and tracked:

```cpp
CRDTSQLite db("myapp.db", 1);
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
db.enable_crdt("users");

// Insert some data
db.execute("INSERT INTO users (name) VALUES ('Alice')");

// Later... add a new column
db.execute("ALTER TABLE users ADD COLUMN age INTEGER");
// ✅ Schema automatically refreshed!
// ✅ Column types updated in _crdt_users_types
// ✅ New column tracked immediately

// Insert with new column
db.execute("INSERT INTO users (name, age) VALUES ('Bob', 30)");

// Sync to other nodes
auto changes = db.get_changes_since(0);
// ✅ Both 'name' and 'age' changes included
remote_db.merge_changes(changes);
// ✅ Works even if remote node already has 'age' column
```

**How it works:**
- `sqlite3_set_authorizer()` detects `SQLITE_ALTER_TABLE` operations
- `refresh_schema()` called automatically after ALTER TABLE executes
- Column types re-scanned via `PRAGMA table_info()`
- Metadata tables updated with new columns

**Supported:**
- ✅ `ALTER TABLE ADD COLUMN` - Fully automatic

**Not supported:**
- ❌ `ALTER TABLE DROP COLUMN` - Breaks existing sync data
- ❌ `ALTER TABLE RENAME COLUMN` - Would need versions table migration
- ❌ `ALTER TABLE RENAME TABLE` - Would need all shadow tables renamed

For unsupported operations, handle manually or use `refresh_schema()` after migration.

## Architecture

### Shadow Tables

For each CRDT-enabled table `foo`, shadow tables are created:

```
_crdt_foo_versions    - Column version tracking
_crdt_foo_tombstones  - Deletion tracking
_crdt_foo_clock       - Logical clock
_crdt_foo_types       - Column type information
_crdt_foo_lookaside   - rowid → ID mapping (only for uint128_t)
```

**Note:** Lookaside table only created when using `uint128_t` record IDs.

### How It Works

```
┌─────────────────────────────────────────────────────┐
│  Application SQL                                    │
│  INSERT INTO users VALUES (...)                     │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────┐
│  SQLite Update Hook (C API)                         │
│  - Captures: INSERT, UPDATE, DELETE                 │
│  - Buffers changes in transaction                   │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────┐
│  On Transaction Commit                              │
│  1. Tick logical clock                              │
│  2. Create Change objects for each modified column  │
│  3. Update CRDT<int64_t, string> in-memory          │
│  4. Write to shadow tables                          │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────┐
│  Sync API                                           │
│  - get_changes_since() → vector<Change>             │
│  - merge_changes() → applies to SQLite + CRDT       │
└─────────────────────────────────────────────────────┘
```

### Change Structure

```cpp
struct Change<K, C, V> {
  K record_id;              // SQLite rowid
  std::optional<C> col_name; // Column name (nullopt = deletion)
  std::optional<V> value;    // Column value (nullopt = field deletion)
  uint64_t col_version;      // Per-column version counter
  uint64_t db_version;       // Global logical clock
  CrdtNodeId node_id;        // Node that created this change
  uint64_t local_db_version; // Local clock when applied
  uint32_t flags;            // Optional metadata
};
```

## API Reference

### CRDTSQLite Class

#### Constructor

```cpp
CRDTSQLite(const char *path, CrdtNodeId node_id)
```

- `path`: Path to SQLite database file
- `node_id`: Unique identifier for this node (must be globally unique)

#### enable_crdt

```cpp
void enable_crdt(const std::string &table_name)
```

Enables CRDT synchronization for a table. Must be called after table creation.

**Requirements:**
- Table must exist
- Table must have a `rowid` (standard SQLite behavior)
- Can only enable one table per CRDTSQLite instance currently

#### execute

```cpp
void execute(const char *sql)
```

Executes SQL statement(s). Changes to CRDT-enabled tables are tracked automatically.

#### get_changes_since

```cpp
std::vector<Change<int64_t, std::string>> get_changes_since(uint64_t last_db_version)
```

Retrieves all changes since a given logical clock version.

**Parameters:**
- `last_db_version`: Clock version (use 0 for all changes)

**Returns:** Vector of changes sorted and compressed

#### get_changes_since_excluding

```cpp
std::vector<Change<int64_t, std::string>> get_changes_since_excluding(
  uint64_t last_db_version,
  const CrdtSet<CrdtNodeId> &excluding
)
```

Like `get_changes_since` but excludes changes from specific nodes.

**Use case:** Avoid echoing changes back to their originator during sync.

#### merge_changes

```cpp
std::vector<Change<int64_t, std::string>> merge_changes(
  std::vector<Change<int64_t, std::string>> changes
)
```

Merges changes from another node.

**Returns:** Vector of accepted changes (those that won conflict resolution)

**Side effects:**
- Updates logical clock
- Modifies SQLite table
- Updates shadow tables

#### compact_tombstones

```cpp
size_t compact_tombstones(uint64_t min_acknowledged_version)
```

Removes tombstones older than specified version.

**⚠️ CRITICAL:** Only call this when **ALL nodes** have acknowledged the version!

**Parameters:**
- `min_acknowledged_version`: Minimum version acknowledged by all nodes

**Returns:** Number of tombstones removed

#### refresh_schema

```cpp
void refresh_schema()
```

Manually refresh schema metadata after ALTER TABLE operations.

**When to use:**
- If you execute ALTER TABLE via raw SQLite API (bypassing `CRDTSQLite::execute()`)
- Normally called automatically by `execute()` when ALTER TABLE is detected

**What it does:**
- Re-scans column types via `PRAGMA table_info()`
- Updates `_crdt_<table>_types` with current schema

#### Other Methods

```cpp
size_t tombstone_count() const;
uint64_t get_clock() const;
sqlite3 *get_db(); // Get raw SQLite handle
```

## Conflict Resolution

### Last-Write-Wins (LWW) Semantics

Conflicts are resolved using three-level comparison:

```cpp
bool should_accept_remote(local, remote) {
  if (remote.col_version > local.col_version) return true;   // 1. Column version
  if (remote.col_version < local.col_version) return false;

  if (remote.db_version > local.db_version) return true;     // 2. DB version
  if (remote.db_version < local.db_version) return false;

  return (remote.node_id > local.node_id);                   // 3. Node ID (tiebreaker)
}
```

### Example: Concurrent Updates

```
Node 1 (node_id=1)                    Node 2 (node_id=2)
─────────────────                    ─────────────────
UPDATE users                         UPDATE users
SET name = 'Alice'                   SET name = 'Bob'
WHERE rowid = 1                      WHERE rowid = 1

col_version = 2                      col_version = 2
db_version = 5                       db_version = 5
node_id = 1                          node_id = 2
```

After bidirectional sync:
- Both nodes converge to `name = 'Bob'`
- Node 2 wins (higher node_id)

### Column-Level Granularity

Conflicts are resolved **per column**, not per row:

```
Node 1: UPDATE users SET name = 'Alice', age = 30 WHERE rowid = 1
Node 2: UPDATE users SET email = 'bob@x.com' WHERE rowid = 1

After sync:
- name = 'Alice'  (from Node 1)
- age = 30        (from Node 1)
- email = 'bob@x.com'  (from Node 2)
```

## Synchronization Protocol

### Two-Node Sync

```cpp
// Node 1 and Node 2

// Track last synchronized version per remote node
uint64_t last_sync_from_node2 = 0;

// Periodic sync:
while (true) {
  // 1. Get changes since last sync, excluding echoes
  CrdtSet<CrdtNodeId> excluding;
  excluding.insert(node2_id);
  auto changes = node1.get_changes_since_excluding(last_sync_from_node2, excluding);

  // 2. Send to Node 2
  send_to_node2(changes);

  // 3. Update sync version
  last_sync_from_node2 = node1.get_clock();

  // 4. Receive changes from Node 2
  auto remote_changes = receive_from_node2();

  // 5. Merge
  node1.merge_changes(remote_changes);

  sleep(sync_interval);
}
```

### Multi-Node Sync

For N nodes, maintain a version vector:

```cpp
struct SyncState {
  std::unordered_map<CrdtNodeId, uint64_t> last_sync_version;
};

void sync_with_node(CRDTSQLite &local, CRDTSQLite &remote,
                   SyncState &state, CrdtNodeId remote_id) {
  uint64_t last_version = state.last_sync_version[remote_id];

  // Get new changes
  auto changes = local.get_changes_since(last_version);

  // Send to remote
  send_to(remote_id, changes);

  // Update sync state
  state.last_sync_version[remote_id] = local.get_clock();

  // Receive and merge
  auto remote_changes = receive_from(remote_id);
  local.merge_changes(remote_changes);
}
```

## Transaction Handling

### Automatic Buffering

Changes are buffered during transactions and flushed on commit:

```cpp
db.execute("BEGIN TRANSACTION");
db.execute("INSERT INTO users (name) VALUES ('Alice')");
db.execute("INSERT INTO users (name) VALUES ('Bob')");
db.execute("COMMIT");  // Changes flushed to CRDT here
```

### Rollback

On rollback, buffered changes are discarded:

```cpp
db.execute("BEGIN TRANSACTION");
db.execute("INSERT INTO users (name) VALUES ('Charlie')");
db.execute("ROLLBACK");  // Changes discarded, clock unchanged
```

## Type Handling

### SQLite Type Affinity

SQLite uses dynamic typing with type affinity. The wrapper preserves types:

| SQLite Type | Stored As | Example |
|-------------|-----------|---------|
| NULL | "NULL" string | NULL |
| INTEGER | Decimal string | "42", "-123" |
| REAL | Decimal string (17 digits precision) | "3.14159265358979323" |
| TEXT | UTF-8 string | "Hello, world!" |
| BLOB | Hex string (prefixed "BLOB:") | "BLOB:48656c6c6f" |

### Example: Type Reconstruction

```cpp
// Original: INSERT INTO users (id, score) VALUES (1, 3.14)
// Change stored as:
Change {
  record_id = 1,
  col_name = "score",
  value = "3.14159",      // Stored as string
  ...
}

// On merge:
// 1. Read column type from _crdt_users_types: REAL
// 2. Convert string -> double: 3.14159
// 3. Bind to SQLite as REAL
```

## Performance Considerations

### Storage Overhead

For a table with 10 columns and 1000 rows:
- Main table: 1000 rows
- Shadow versions: ~10,000 rows (1000 × 10)
- **Overhead ratio: ~10x metadata**

### Operation Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| INSERT | O(n) | n = number of columns |
| UPDATE | O(m) | m = number of modified columns |
| DELETE | O(1) | Just adds tombstone |
| SELECT | O(1) | No overhead on reads |
| get_changes_since | O(c) | c = number of changes |
| merge_changes | O(c) | c = number of changes |
| Sync (full) | O(c + m) | c = changes, m = merge cost |

### Optimization Tips

1. **Sync frequently** - Smaller changesets = faster sync
2. **Exclude echo nodes** - Use `get_changes_since_excluding()`
3. **Batch operations** - Use transactions for multiple changes
4. **Compact tombstones** - Periodically after all nodes acknowledge
5. **Limit columns** - Fewer columns = less metadata

## Limitations

### Current Limitations

1. **Single table per instance** - One CRDTSQLite instance tracks one table
   - **Workaround:** Create multiple instances

2. **No SQL transactions = CRDT transactions** - SQLite ROLLBACK doesn't undo CRDT operations
   - **Workaround:** Changes are buffered and discarded on rollback

3. **No foreign keys across CRDT-enabled tables** - Referential integrity not synchronized
   - **Workaround:** Enforce at application level

4. **Limited ALTER TABLE support** - Only ADD COLUMN is automatically tracked
   - ✅ **ADD COLUMN:** Fully supported with automatic schema refresh
   - ❌ **DROP COLUMN:** Not supported (no clean way to handle in CRDT)
   - ❌ **RENAME COLUMN:** Not supported (would need to update versions table)
   - ❌ **RENAME TABLE:** Not supported (would need to rename all shadow tables)
   - **Note:** ADD COLUMN is the most common migration operation

5. **WITHOUT ROWID tables not supported** - These lack SQLite's internal rowid
   - **Workaround:** Use regular tables (most tables have rowid by default)
   - **Note:** For distributed UUIDs, use uint128_t ID with lookaside table (see below)

### Eventual Consistency vs. Strong Consistency

This library provides **eventual consistency**:
- ✅ All nodes converge to same state
- ✅ No coordination required
- ❌ Not immediate consistency
- ❌ No ACID across nodes

**Not suitable for:**
- Bank account balances (use transactions)
- Inventory counts (use operational transforms)
- Monotonic counters (use counter CRDTs)

**Perfect for:**
- User profiles
- Settings/preferences
- Collaborative documents
- Offline-first apps
- Multi-device sync

## Security and DoS Protection

### Tombstone Accumulation

**Attack vector:** Malicious node creates and deletes many records

**Mitigation:**
```cpp
// Monitor tombstone count
if (db.tombstone_count() > MAX_TOMBSTONES) {
  // Rate limit or reject operations
}

// Coordinate compaction with all nodes
uint64_t min_ack_version = get_min_acknowledged_version_from_all_nodes();
db.compact_tombstones(min_ack_version);
```

### Resource Limits

Implement application-level limits:
```cpp
const size_t MAX_RECORDS = 100000;
const size_t MAX_TOMBSTONES = 10000;
const size_t MAX_SYNC_CHANGES = 5000;

if (count_records() > MAX_RECORDS) {
  throw std::runtime_error("Record limit exceeded");
}
```

### Clock Overflow

The logical clock uses `uint64_t`:
- Max value: 2^64 - 1 = 18,446,744,073,709,551,615
- At 1 million ops/sec: ~584,000 years until overflow
- **Conclusion:** Not a practical concern

## Testing

### Running Tests

```bash
cd build
ctest --output-on-failure
```

### Test Coverage

The test suite includes:

**int64_t tests (16 tests):**
- ✅ Basic initialization
- ✅ INSERT/UPDATE/DELETE operations
- ✅ Two-node synchronization
- ✅ Concurrent updates (conflict resolution)
- ✅ Delete synchronization
- ✅ Multiple columns
- ✅ Tombstone compaction
- ✅ Transaction commit/rollback
- ✅ Persistence (reload from disk)
- ✅ Node exclusion in sync
- ✅ NULL value handling
- ✅ Integer type preservation
- ✅ ALTER TABLE ADD COLUMN with sync

**uint128_t tests (4 tests):**
- ✅ Basic uint128_t ID operations
- ✅ Two-node sync with distributed IDs
- ✅ Collision resistance (20,000 IDs)
- ✅ DELETE sync with lookaside table

### Example Test

```cpp
// Test concurrent updates with conflict resolution
CRDTSQLite db1("test1.db", 1);
CRDTSQLite db2("test2.db", 2);

// Same schema
db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
db2.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

db1.enable_crdt("users");
db2.enable_crdt("users");

// Concurrent updates
db1.execute("INSERT INTO users (rowid, name) VALUES (1, 'Alice')");
db2.execute("INSERT INTO users (rowid, name) VALUES (1, 'Bob')");

// Bidirectional sync
auto changes1 = db1.get_changes_since(0);
auto changes2 = db2.get_changes_since(0);
db1.merge_changes(changes2);
db2.merge_changes(changes1);

// Both converge to 'Bob' (node 2 wins)
assert(get_value(db1, "users", "name", 1) == "Bob");
assert(get_value(db2, "users", "name", 1) == "Bob");
```

## Comparison to Alternatives

| Solution | Approach | Pros | Cons |
|----------|----------|------|------|
| **CRDT-SQLite (this)** | Wrapper library | ✅ No SQLite patching<br>✅ Easy to integrate<br>✅ Column-level conflicts | ❌ Shadow table overhead<br>❌ Single table per instance |
| **cr-sqlite** | SQLite extension | ✅ Multi-table<br>✅ SQL API | ❌ Extension loading<br>❌ Complex to build |
| **LiteSync** | Trigger-based | ✅ Pure SQL<br>✅ No C++ | ❌ Trigger overhead<br>❌ Manual setup |
| **Custom replication** | Log shipping | ✅ Full control | ❌ No conflict resolution<br>❌ Complex |

## Roadmap

### Recently Completed ✅

- ✅ **Schema migration helpers** - ALTER TABLE ADD COLUMN auto-detected
- ✅ **Distributed UUID support** - uint128_t with lookaside table
- ✅ **Security hardening** - SQL injection prevention, table name validation

### Planned Features

- [ ] Multiple table support
- [ ] JSON serialization for changes
- [ ] Wire protocol for efficient sync
- [ ] Custom merge rules (already available in core CRDT, needs SQLite wrapper)
- [ ] Replication monitoring/metrics
- [ ] Delta compression for sync
- [ ] Partial replica (filter by predicates)

### Contributions Welcome

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

See [LICENSE](LICENSE) file.

## References

- [CRDT Theory](https://crdt.tech/)
- [SQLite Hooks](https://www.sqlite.org/c3ref/update_hook.html)
- [Lamport Clocks](https://en.wikipedia.org/wiki/Lamport_timestamp)
- [Original crdt.hpp](crdt.hpp)
- [CLAUDE.md](CLAUDE.md) - Full CRDT architecture documentation
