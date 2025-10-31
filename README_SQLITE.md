# CRDT-SQLite: Conflict-Free Replicated SQLite Database

A high-performance CRDT wrapper for SQLite that enables automatic multi-node synchronization with last-write-wins conflict resolution.

## Features

- 🚀 **High performance**: Hybrid trigger + async processing (benchmarks pending)
- 💾 **Crash-safe**: Survives power loss between commit and metadata update
- 🔄 **Automatic sync**: Changes tracked transparently using triggers
- 🎯 **Column-level conflicts**: Fine-grained conflict resolution per field
- 🌍 **Cross-platform**: Linux, macOS, Windows (with custom SQLite build)
- 📦 **Zero code changes**: Existing SQL apps work without modification
- ✏️ **Normal SQL writes**: Unlike cr-sqlite, no virtual tables = normal INSERT/UPDATE/DELETE works!

## Quick Start

```cpp
#include "crdt_sqlite.hpp"

// Create database with unique node ID
CRDTSQLite db("myapp.db", 1);

// Create table and enable CRDT
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
db.enable_crdt("users");

// Use normal SQL - changes are tracked automatically by triggers!
// You can use execute() wrapper (convenient)...
db.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");

// ...or raw SQLite APIs (also works!)
sqlite3_exec(db.get_db(), "UPDATE users SET email = 'new@example.com' WHERE name = 'Alice'",
             nullptr, nullptr, nullptr);

// Get changes since last sync
auto changes = db.get_changes_since(0);

// Send to other nodes...
// Then merge incoming changes
db.merge_changes(remote_changes);
```

## Architecture: Triggers + WAL Hook

### Design Philosophy

CRDT-SQLite uses a **hybrid trigger + WAL hook** architecture that combines crash safety with high performance:

```
┌─────────────────────────────────────────────────┐
│  User: INSERT INTO users (name) VALUES ('Bob')  │
└─────────────────────────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  SQLite Trigger Fires  │
         │  (INSERT/UPDATE/DELETE)│
         └────────────────────────┘
                      │
                      ▼
  ┌────────────────────────────────────────┐
  │  Trigger: INSERT INTO _pending         │
  │    (operation, record_id)              │
  │  ✓ Fast (just one INSERT)              │
  │  ✓ Transactional (auto-rollback)       │
  └────────────────────────────────────────┘
                      │
                      ▼ COMMIT happens
         ┌────────────────────────┐
         │  WAL Checkpoint         │
         │  Locks RELEASED         │
         └────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  wal_hook() fires       │
         │  (AFTER commit)         │
         └────────────────────────┘
                      │
                      ▼
  ┌────────────────────────────────────────┐
  │  process_pending_changes()              │
  │  1. Read _pending table                 │
  │  2. Query current row values            │
  │  3. Update _versions shadow table       │
  │  4. Increment _clock                    │
  │  5. Delete from _pending                │
  │  ✓ NO locks held (wal_hook is safe!)   │
  └────────────────────────────────────────┘
```

### Performance Comparison

| Approach | Lock Duration | Overhead | Crash Safe | Expected Performance |
|----------|---------------|----------|------------|---------------------|
| **cr-sqlite** (pure triggers) | Long | High | Yes | Baseline |
| **update_hook + vector** | Short | Low | ❌ No | Fast but unsafe |
| **Ours** (triggers + wal_hook) | Short | Medium | ✅ Yes | **TBD** (benchmarks needed) |

**Hypothesis**: Should be significantly faster than cr-sqlite because:
- Triggers only INSERT into `_pending` (minimal work during write)
- Heavy metadata updates happen in wal_hook AFTER locks released
- Shorter critical section = less contention

**Note**: Formal benchmarks comparing against cr-sqlite are planned but not yet completed.

**Key Insight**: The `wal_hook` callback fires **AFTER** commit with all locks released, making `prepare()`/`step()` calls 100% safe.

## Comparison with cr-sqlite

| Feature | **CRDT-SQLite** (Ours) | cr-sqlite |
|---------|------------------------|-----------|
| **Write API** | ✅ Normal SQL (INSERT/UPDATE/DELETE) | ❌ Virtual tables (special functions required) |
| **Read API** | ✅ Normal SELECT | ✅ Normal SELECT |
| **Architecture** | Triggers + wal_hook | Virtual tables + triggers |
| **Learning curve** | Low (just SQL) | Higher (learn cr-sqlite API) |
| **Existing code** | Works unchanged | Requires rewrite |
| **Performance** | TBD (benchmarks pending) | Established baseline |

**Key advantage:** Our trigger-based approach means you write normal SQL - no special APIs, no virtual tables, no code changes. Just enable CRDT on a table and keep writing SQL like you always have.

## Shadow Tables

When you call `enable_crdt("users")`, four shadow tables are created:

### 1. `_crdt_users_versions`
Tracks the version of each column:
```sql
CREATE TABLE _crdt_users_versions (
  record_id INTEGER,
  col_name TEXT,
  col_version INTEGER,   -- Per-column edit counter
  db_version INTEGER,    -- Global logical clock
  node_id INTEGER,       -- Which node made this change
  local_db_version INTEGER,  -- Local clock when applied
  PRIMARY KEY (record_id, col_name)
);
```

### 2. `_crdt_users_tombstones`
Tracks deleted records:
```sql
CREATE TABLE _crdt_users_tombstones (
  record_id INTEGER PRIMARY KEY,
  db_version INTEGER,
  node_id INTEGER,
  local_db_version INTEGER
);
```

### 3. `_crdt_users_clock`
Logical clock for causality tracking:
```sql
CREATE TABLE _crdt_users_clock (
  time INTEGER PRIMARY KEY
);
```

### 4. `_crdt_users_pending`
Temporary table for tracking changes within transactions:
```sql
CREATE TABLE _crdt_users_pending (
  operation INTEGER,
  record_id INTEGER,
  PRIMARY KEY (operation, record_id)
);
```

## API Reference

### Constructor

```cpp
CRDTSQLite(const char *path, CrdtNodeId node_id);
```

- **path**: Path to SQLite database file
- **node_id**: Unique identifier for this node

### Core Methods

#### `enable_crdt(const std::string &table_name)`
Enables CRDT synchronization for a table. Creates triggers that automatically track changes.

**Schema Change Support:**
- ✅ ALTER TABLE ADD COLUMN - fully automatic
- ❌ DROP TABLE - blocked
- ⚠️ RENAME TABLE - not blocked but WILL BREAK
- ⚠️ DROP/RENAME COLUMN - not supported

#### `execute(const char *sql)` **[Optional Convenience Wrapper]**
Executes SQL with exception-based error handling and automatic schema refresh.

**What it does:**
- Wraps `sqlite3_exec()` with exception throwing
- Auto-calls `refresh_schema()` after ALTER TABLE

**Alternative: Use Raw SQLite APIs**

Since triggers and `wal_hook` handle everything automatically, you can use raw SQLite APIs directly:

```cpp
CRDTSQLite db("myapp.db", 1);
db.enable_crdt("users");

// Option 1: Use execute() wrapper (convenience)
db.execute("INSERT INTO users (name) VALUES ('Alice')");

// Option 2: Use raw sqlite3_exec() (also works!)
sqlite3_exec(db.get_db(), "INSERT INTO users (name) VALUES ('Bob')",
             nullptr, nullptr, nullptr);

// Option 3: Use prepare/step (already exposed)
sqlite3_stmt *stmt = db.prepare("INSERT INTO users (name) VALUES (?)");
sqlite3_bind_text(stmt, 1, "Charlie", -1, SQLITE_STATIC);
sqlite3_step(stmt);
sqlite3_finalize(stmt);

// All three options work! Triggers fire automatically.
// Only caveat: Call refresh_schema() manually after ALTER TABLE (typically during migrations)
```

#### `refresh_schema()`
Refreshes internal column metadata after schema changes.

**When to call:**
- After ALTER TABLE ADD COLUMN (typically at end of migration)
- Only needed if using raw SQLite APIs instead of `execute()`

#### `prepare(const char *sql)`
Prepares a SQL statement for parameterized queries. Returns `sqlite3_stmt*` (caller must finalize).

#### `get_db()`
Returns raw `sqlite3*` handle for direct SQLite API access.

#### `get_changes_since(uint64_t last_db_version, size_t max_changes = 0)`
Gets all changes since a given version.

#### `merge_changes(std::vector<Change<...>> changes)`
Merges changes from another node using LWW conflict resolution.

#### `compact_tombstones(uint64_t min_acknowledged_version)`
Removes old tombstones.

⚠️ **CRITICAL**: Only call when ALL nodes have acknowledged the version!

## Conflict Resolution

### Last-Write-Wins (LWW)

Conflicts are resolved per-column using three-way comparison:

1. **Column version** (higher wins)
2. **DB version** (higher wins if column versions equal)  
3. **Node ID** (deterministic tie-breaker)

Example:
```cpp
// Node 1 updates email
db1.execute("UPDATE users SET email = 'alice@foo.com' WHERE id = 1");

// Node 2 updates name (concurrent)
db2.execute("UPDATE users SET name = 'Alice Smith' WHERE id = 1");

// After sync: BOTH changes are kept!
// Different columns don't conflict!
```

## Threading Model

⚠️ **CRDTSQLite is NOT thread-safe**

**Safe usage:**
- One instance per thread (each with own database connection)
- Protect ALL access with external mutex
- We use `SQLITE_OPEN_FULLMUTEX` for proper mutex protection

## Windows Platform Notes

Windows CI builds SQLite from source with explicit threading flags:

```powershell
# Download SQLite amalgamation
Invoke-WebRequest -Uri "https://www.sqlite.org/2024/sqlite-amalgamation-3470200.zip"

# Compile with FULLMUTEX threading
cl /c /O2 /DSQLITE_THREADSAFE=1 sqlite3.c
lib /OUT:sqlite3.lib sqlite3.obj
```

**Why?** vcpkg SQLite may use different default threading modes, causing mutex assertion crashes.

## See Also

- [Main README](README.md) - Column-based CRDT (header-only)
- [Text CRDT](text_crdt.hpp) - Line-based collaborative text editor
- [CLAUDE.md](CLAUDE.md) - Full architecture documentation

## License

See LICENSE file in repository root.
