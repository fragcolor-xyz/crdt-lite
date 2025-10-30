# Using UUIDs with CRDT-SQLite

## The Problem with rowid

The current `crdt_sqlite.hpp` implementation uses SQLite's automatic `rowid` (int64) as the record identifier. This works fine for single-node databases, but **causes ID collisions in distributed systems**:

```
Node 1: INSERT INTO users (name) VALUES ('Alice')  -- rowid=1 auto-assigned
Node 2: INSERT INTO users (name) VALUES ('Bob')    -- rowid=1 auto-assigned

After sync: COLLISION! Both records have ID=1
```

## The Solution: UUID Primary Keys

For distributed CRDTs, you need **globally unique identifiers**. UUIDs are perfect for this.

## Architecture Options

### Option 1: String-Based CRDT (Recommended for New Projects)

Modify `CRDTSQLite` to use `CRDT<std::string, std::string>` instead of `CRDT<int64_t, std::string>`:

```cpp
// In crdt_sqlite.hpp, change:
CRDT<int64_t, std::string> crdt_;  // OLD

// To:
CRDT<std::string, std::string> crdt_;  // NEW - supports UUIDs
```

**Schema:**
```sql
CREATE TABLE users (
  id TEXT PRIMARY KEY,  -- UUID as TEXT
  name TEXT,
  email TEXT
);
```

**Usage:**
```cpp
CRDTSQLiteUUID db("app.db", 1);
db.enable_crdt("users");

// Generate UUID before insert
std::string uuid = db.generate_uuid();  // "550e8400-e29b-41d4-a716-446655440000"

// Insert with explicit UUID
std::string sql = "INSERT INTO users (id, name, email) VALUES ('" + uuid +
                  "', 'Alice', 'alice@example.com')";
db.execute(sql.c_str());

// Sync works perfectly - no ID collisions!
```

### Option 2: Hybrid Approach (Works with Existing Code)

Keep using int64 rowid internally, but map to UUIDs at application level:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,  -- SQLite rowid
  uuid TEXT UNIQUE NOT NULL,  -- Application-level UUID
  name TEXT,
  email TEXT
);

CREATE INDEX idx_users_uuid ON users(uuid);
```

**Application layer:**
```cpp
class UUIDMapper {
  std::unordered_map<std::string, int64_t> uuid_to_rowid_;
  std::unordered_map<int64_t, std::string> rowid_to_uuid_;

public:
  std::string insert_with_uuid(CRDTSQLite &db, const std::string &uuid,
                                const std::string &name) {
    // Insert with UUID column
    std::string sql = "INSERT INTO users (uuid, name) VALUES ('" + uuid +
                      "', '" + name + "')";
    db.execute(sql.c_str());

    // Get the rowid that was assigned
    int64_t rowid = sqlite3_last_insert_rowid(db.get_db());

    // Map UUID <-> rowid
    uuid_to_rowid_[uuid] = rowid;
    rowid_to_uuid_[rowid] = uuid;

    return uuid;
  }

  // When syncing, translate rowid changes to UUID
  std::vector<UUIDChange> translate(const std::vector<Change<int64_t, string>> &changes) {
    std::vector<UUIDChange> uuid_changes;
    for (const auto &c : changes) {
      UUIDChange uc;
      uc.uuid = rowid_to_uuid_[c.record_id];  // Translate
      uc.col_name = c.col_name;
      uc.value = c.value;
      // ... copy other fields
      uuid_changes.push_back(uc);
    }
    return uuid_changes;
  }
};
```

**Pros:**
- ✅ No changes to `crdt_sqlite.cpp`
- ✅ Works with existing implementation

**Cons:**
- ❌ Extra mapping layer
- ❌ Need to sync mapping table
- ❌ More complex

### Option 3: Use u128 Node IDs with ROWID Encoding (Clever Hack)

Encode node ID into the rowid to make it globally unique:

```cpp
// Use high 48 bits for node ID, low 16 bits for local sequence
int64_t generate_global_rowid(uint64_t node_id, uint16_t local_seq) {
  return (node_id << 16) | local_seq;
}

// Node 1: rowids like 0x0001000000000001, 0x0001000000000002, ...
// Node 2: rowids like 0x0002000000000001, 0x0002000000000002, ...
// No collisions!
```

**Pros:**
- ✅ No schema changes
- ✅ No UUID generation overhead
- ✅ Still uses int64

**Cons:**
- ❌ Limited to 2^48 nodes and 2^16 records per node
- ❌ Requires coordination of node IDs
- ❌ Not standard UUIDs

## UUID Generation Libraries

### C++

**libuuid** (Linux/Unix):
```cpp
#include <uuid/uuid.h>

std::string generate_uuid() {
  uuid_t uuid;
  uuid_generate(uuid);

  char uuid_str[37];
  uuid_unparse(uuid, uuid_str);

  return std::string(uuid_str);
}
```

**stduuid** (Cross-platform, header-only):
```cpp
#include <uuid.h>

std::string generate_uuid() {
  std::random_device rd;
  auto seed_data = std::array<int, std::mt19937::state_size>{};
  std::generate(std::begin(seed_data), std::end(seed_data), std::ref(rd));
  std::seed_seq seq(std::begin(seed_data), std::end(seed_data));
  std::mt19937 generator(seq);

  uuids::uuid_random_generator gen{generator};
  uuids::uuid id = gen();

  return uuids::to_string(id);
}
```

**Boost.UUID**:
```cpp
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

std::string generate_uuid() {
  boost::uuids::random_generator gen;
  boost::uuids::uuid id = gen();
  return boost::uuids::to_string(id);
}
```

### Rust

```rust
use uuid::Uuid;

fn generate_uuid() -> String {
  Uuid::new_v4().to_string()
}
```

## Recommended Implementation

For a **production-ready UUID-based CRDT-SQLite**:

1. **Template the Record ID Type**
   ```cpp
   template <typename RecordId = int64_t>
   class CRDTSQLite {
     CRDT<RecordId, std::string> crdt_;
     // ...
   };

   // Usage:
   CRDTSQLite<std::string> uuid_db("app.db", 1);  // UUID keys
   CRDTSQLite<int64_t> local_db("local.db", 1);   // rowid keys
   ```

2. **Provide UUID Utilities**
   ```cpp
   class UUIDColumn {
     std::string column_name_;

   public:
     UUIDColumn(const std::string &col) : column_name_(col) {}

     std::string generate() {
       // Use libuuid, Boost, or stduuid
     }

     void bind(sqlite3_stmt *stmt, int index, const std::string &uuid) {
       sqlite3_bind_text(stmt, index, uuid.c_str(), -1, SQLITE_TRANSIENT);
     }
   };
   ```

3. **Schema Constraints**
   ```sql
   CREATE TABLE users (
     id TEXT PRIMARY KEY CHECK(length(id) = 36),  -- Validate UUID format
     name TEXT NOT NULL,
     email TEXT UNIQUE
   );
   ```

4. **Sync Protocol**
   ```cpp
   // Changes now include UUID record IDs
   auto changes = uuid_db.get_changes_since(0);
   // changes[0].record_id is "550e8400-e29b-41d4-a716-446655440000"

   // Send to another node - no collision possible!
   send_to_peer(changes);
   ```

## Migration Path

If you have an existing int64-based database and want to migrate to UUIDs:

```sql
-- Step 1: Add UUID column
ALTER TABLE users ADD COLUMN uuid TEXT;

-- Step 2: Generate UUIDs for existing rows
UPDATE users SET uuid = lower(hex(randomblob(16)));

-- Step 3: Make UUID primary key in new table
CREATE TABLE users_new (
  uuid TEXT PRIMARY KEY,
  name TEXT,
  email TEXT
);

INSERT INTO users_new SELECT uuid, name, email FROM users;

-- Step 4: Drop old table
DROP TABLE users;
ALTER TABLE users_new RENAME TO users;

-- Step 5: Migrate shadow tables similarly
```

## Node ID Considerations

The CRDT also uses node IDs. With the `node-id-u128` feature, you can use UUIDs for nodes too:

```rust
// In Cargo.toml:
crdt-lite = { version = "0.4", features = ["node-id-u128"] }
```

```rust
use uuid::Uuid;

let node_uuid = Uuid::new_v4().as_u128();
let crdt = CRDT::new(node_uuid, None);
```

This ensures both record IDs and node IDs are globally unique UUIDs!

## Performance Considerations

| Aspect | int64 rowid | UUID (TEXT) |
|--------|-------------|-------------|
| Storage | 8 bytes | 36 bytes (or 16 as BLOB) |
| Index size | Smaller | Larger (4-5x) |
| Insert speed | Faster | Slightly slower |
| Comparison | Faster | Slower |
| Collision risk | **HIGH (distributed)** | **NONE** |
| Global uniqueness | ❌ | ✅ |

**For distributed CRDTs, UUIDs are essential** despite the overhead.

## Conclusion

**Current implementation**: Works great for single-node or centralized systems using rowid.

**For distributed systems**: You MUST use UUIDs or similar globally unique identifiers to avoid record ID collisions.

**Recommended approach**: Template `CRDTSQLite` on the RecordId type, defaulting to `int64_t` for backward compatibility but supporting `std::string` for UUIDs.

Example PR to make this change would involve:
1. Template class on `RecordId` type
2. Add UUID generation utilities
3. Update documentation with UUID examples
4. Add UUID-based tests

This is a great enhancement for production use!
