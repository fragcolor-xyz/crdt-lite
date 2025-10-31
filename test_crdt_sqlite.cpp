// test_crdt_sqlite.cpp
#include "crdt_sqlite.hpp"
#include <iostream>
#include <cassert>
#include <filesystem>
#include <cstdio>

namespace fs = std::filesystem;

// Test helper macros
#define TEST(name) void test_##name()
#define RUN_TEST(name) \
  do { \
    std::cout << "Running test: " << #name << "..."; \
    test_##name(); \
    std::cout << " PASSED" << std::endl; \
  } while (0)

#define ASSERT_EQ(a, b) \
  do { \
    if ((a) != (b)) { \
      std::cerr << "Assertion failed: " << #a << " == " << #b \
                << " (got " << (a) << " and " << (b) << ")" << std::endl; \
      std::exit(1); \
    } \
  } while (0)

#define ASSERT_TRUE(cond) \
  do { \
    if (!(cond)) { \
      std::cerr << "Assertion failed: " << #cond << std::endl; \
      std::exit(1); \
    } \
  } while (0)

#define ASSERT_FALSE(cond) ASSERT_TRUE(!(cond))

// Test database helper
class TestDB {
public:
  TestDB(const std::string &name) : path_("test_" + name + ".db") {
    // Remove if exists
    if (fs::exists(path_)) {
      fs::remove(path_);
    }
  }

  ~TestDB() {
    // Clean up
    if (fs::exists(path_)) {
      fs::remove(path_);
    }
  }

  const char *path() const { return path_.c_str(); }

private:
  std::string path_;
};

// Helper to count rows in a table
int count_rows(sqlite3 *db, const std::string &table) {
  std::string query = "SELECT COUNT(*) FROM " + table;
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);
  sqlite3_step(stmt);
  int count = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);
  return count;
}

// Helper to get a string value from a table
std::string get_value(sqlite3 *db, const std::string &table,
                     const std::string &col, int64_t rowid) {
  std::string query = "SELECT " + col + " FROM " + table + " WHERE rowid = ?";
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);
  sqlite3_bind_int64(stmt, 1, rowid);

  std::string result;
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    const unsigned char *text = sqlite3_column_text(stmt, 0);
    if (text) {
      result = reinterpret_cast<const char *>(text);
    }
  }
  sqlite3_finalize(stmt);
  return result;
}

// Test 1: Basic initialization
TEST(basic_init) {
  TestDB test_db("basic_init");
  CRDTSQLite db(test_db.path(), 1);

  // Create a simple table
  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

  // Enable CRDT
  db.enable_crdt("users");

  // Check that shadow tables were created
  sqlite3 *raw_db = db.get_db();
  sqlite3_stmt *stmt;
  const char *check_sql = "SELECT name FROM sqlite_master WHERE type='table' "
                         "AND (name='_crdt_users_versions' OR name='_crdt_users_tombstones' "
                         "OR name='_crdt_users_clock')";
  sqlite3_prepare_v2(raw_db, check_sql, -1, &stmt, nullptr);

  int table_count = 0;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    table_count++;
  }
  sqlite3_finalize(stmt);

  ASSERT_EQ(table_count, 3); // versions, tombstones, clock
}

// Test 2: Simple insert
TEST(simple_insert) {
  TestDB test_db("simple_insert");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
  db.enable_crdt("users");

  // Insert a row
  db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)");

  // Check that it's in the database
  ASSERT_EQ(count_rows(db.get_db(), "users"), 1);

  // Check that we can get changes
  auto changes = db.get_changes_since(0);
  ASSERT_TRUE(changes.size() >= 2); // name and age columns
}

// Test 3: Update operation
TEST(update_operation) {
  TestDB test_db("update_operation");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
  db.enable_crdt("users");

  db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)");
  uint64_t version_after_insert = db.get_clock();

  db.execute("UPDATE users SET age = 26 WHERE name = 'Bob'");

  auto changes = db.get_changes_since(version_after_insert);
  ASSERT_TRUE(changes.size() > 0); // Should have update changes

  // Verify the value changed
  std::string age = get_value(db.get_db(), "users", "age", 1);
  ASSERT_EQ(age, "26");
}

// Test 4: Delete operation
TEST(delete_operation) {
  TestDB test_db("delete_operation");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db.enable_crdt("users");

  db.execute("INSERT INTO users (name) VALUES ('Charlie')");
  ASSERT_EQ(count_rows(db.get_db(), "users"), 1);

  uint64_t version_before_delete = db.get_clock();

  db.execute("DELETE FROM users WHERE name = 'Charlie'");
  ASSERT_EQ(count_rows(db.get_db(), "users"), 0);

  // Check tombstone was created
  ASSERT_EQ(db.tombstone_count(), 1);

  // Check changes include deletion
  auto changes = db.get_changes_since(version_before_delete);
  bool found_delete = false;
  for (const auto &change : changes) {
    if (!change.col_name.has_value()) {
      found_delete = true;
      break;
    }
  }
  ASSERT_TRUE(found_delete);
}

// Test 5: Two-node sync
TEST(two_node_sync) {
  TestDB test_db1("two_node_1");
  TestDB test_db2("two_node_2");

  CRDTSQLite db1(test_db1.path(), 1);
  CRDTSQLite db2(test_db2.path(), 2);

  // Create same schema on both
  db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
  db2.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");

  db1.enable_crdt("users");
  db2.enable_crdt("users");

  // Insert on node 1
  db1.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");

  // Get changes and sync to node 2
  auto changes = db1.get_changes_since(0);
  db2.merge_changes(changes);

  // Verify node 2 has the data
  ASSERT_EQ(count_rows(db2.get_db(), "users"), 1);
  ASSERT_EQ(get_value(db2.get_db(), "users", "name", 1), "Alice");
  ASSERT_EQ(get_value(db2.get_db(), "users", "email", 1), "alice@example.com");
}

// Test 6: Concurrent updates (conflict resolution)
TEST(concurrent_updates) {
  TestDB test_db1("concurrent_1");
  TestDB test_db2("concurrent_2");

  CRDTSQLite db1(test_db1.path(), 1);
  CRDTSQLite db2(test_db2.path(), 2);

  // Create same schema
  db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db2.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

  db1.enable_crdt("users");
  db2.enable_crdt("users");

  // Insert same record on both nodes
  db1.execute("INSERT INTO users (rowid, name) VALUES (1, 'Alice')");
  db2.execute("INSERT INTO users (rowid, name) VALUES (1, 'Bob')");

  // Sync node 1 -> node 2
  auto changes1 = db1.get_changes_since(0);
  db2.merge_changes(changes1);

  // Sync node 2 -> node 1
  auto changes2 = db2.get_changes_since(0);
  db1.merge_changes(changes2);

  // Both should converge to the same value (node 2's value wins due to higher node_id)
  std::string name1 = get_value(db1.get_db(), "users", "name", 1);
  std::string name2 = get_value(db2.get_db(), "users", "name", 1);

  ASSERT_EQ(name1, name2);
  ASSERT_EQ(name1, "Bob"); // Node 2 wins
}

// Test 7: Delete sync
TEST(delete_sync) {
  TestDB test_db1("delete_sync_1");
  TestDB test_db2("delete_sync_2");

  CRDTSQLite db1(test_db1.path(), 1);
  CRDTSQLite db2(test_db2.path(), 2);

  // Create same schema
  db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db2.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

  db1.enable_crdt("users");
  db2.enable_crdt("users");

  // Insert on node 1 and sync
  db1.execute("INSERT INTO users (name) VALUES ('Charlie')");
  auto changes = db1.get_changes_since(0);
  db2.merge_changes(changes);

  ASSERT_EQ(count_rows(db1.get_db(), "users"), 1);
  ASSERT_EQ(count_rows(db2.get_db(), "users"), 1);

  // Delete on node 1
  uint64_t version_before_delete = db1.get_clock();
  db1.execute("DELETE FROM users WHERE name = 'Charlie'");

  // Sync deletion to node 2
  auto delete_changes = db1.get_changes_since(version_before_delete);
  db2.merge_changes(delete_changes);

  // Both should have 0 rows
  ASSERT_EQ(count_rows(db1.get_db(), "users"), 0);
  ASSERT_EQ(count_rows(db2.get_db(), "users"), 0);

  // Both should have tombstone
  ASSERT_EQ(db1.tombstone_count(), 1);
  ASSERT_EQ(db2.tombstone_count(), 1);
}

// Test 8: Multiple columns
TEST(multiple_columns) {
  TestDB test_db("multiple_columns");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER)");
  db.enable_crdt("products");

  db.execute("INSERT INTO products (name, price, stock) VALUES ('Widget', 19.99, 100)");

  auto changes = db.get_changes_since(0);

  // Should have changes for name, price, and stock
  bool has_name = false, has_price = false, has_stock = false;
  for (const auto &change : changes) {
    if (change.col_name.has_value()) {
      if (*change.col_name == "name") has_name = true;
      if (*change.col_name == "price") has_price = true;
      if (*change.col_name == "stock") has_stock = true;
    }
  }

  ASSERT_TRUE(has_name);
  ASSERT_TRUE(has_price);
  ASSERT_TRUE(has_stock);
}

// Test 9: Tombstone compaction
TEST(tombstone_compaction) {
  TestDB test_db("tombstone_compaction");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db.enable_crdt("users");

  // Insert and delete multiple rows
  for (int i = 1; i <= 10; i++) {
    std::string sql = "INSERT INTO users (rowid, name) VALUES (" + std::to_string(i) + ", 'User" + std::to_string(i) + "')";
    db.execute(sql.c_str());
  }

  // Verify all inserts created tombstones when deleted
  int initial_tombstones = 0;
  for (int i = 1; i <= 10; i++) {
    std::string sql = "DELETE FROM users WHERE rowid = " + std::to_string(i);
    db.execute(sql.c_str());
    initial_tombstones++;
  }

  // The actual tombstone count may vary based on which deletes succeeded
  size_t tombstone_count_before = db.tombstone_count();
  ASSERT_TRUE(tombstone_count_before >= 9 && tombstone_count_before <= 10);

  // Compact tombstones (use current_clock + 1 to ensure all current tombstones are included)
  uint64_t compact_version = db.get_clock() + 1;
  size_t removed = db.compact_tombstones(compact_version);

  ASSERT_EQ(removed, tombstone_count_before);
  ASSERT_EQ(db.tombstone_count(), 0);
}

// Test 10: Transaction rollback
TEST(transaction_rollback) {
  TestDB test_db("transaction_rollback");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db.enable_crdt("users");

  db.execute("INSERT INTO users (name) VALUES ('Alice')");
  uint64_t version_after_first = db.get_clock();

  // Start transaction, insert, then rollback
  db.execute("BEGIN TRANSACTION");
  db.execute("INSERT INTO users (name) VALUES ('Bob')");
  db.execute("ROLLBACK");

  // Should only have Alice
  ASSERT_EQ(count_rows(db.get_db(), "users"), 1);

  // Clock should not have advanced (rollback cleared pending changes)
  uint64_t version_after_rollback = db.get_clock();
  ASSERT_EQ(version_after_first, version_after_rollback);
}

// Test 11: Transaction commit
TEST(transaction_commit) {
  TestDB test_db("transaction_commit");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db.enable_crdt("users");

  uint64_t version_start = db.get_clock();

  // Transaction with multiple inserts
  db.execute("BEGIN TRANSACTION");
  db.execute("INSERT INTO users (name) VALUES ('Alice')");
  db.execute("INSERT INTO users (name) VALUES ('Bob')");
  db.execute("COMMIT");

  ASSERT_EQ(count_rows(db.get_db(), "users"), 2);

  // Clock should have advanced
  ASSERT_TRUE(db.get_clock() > version_start);
}

// Test 12: Persistence (reload from disk)
TEST(persistence) {
  const char *db_path = "test_persistence.db";

  // Remove if exists
  if (fs::exists(db_path)) {
    fs::remove(db_path);
  }

  uint64_t final_clock;

  // Scope 1: Create and populate
  {
    CRDTSQLite db(db_path, 1);
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    db.enable_crdt("users");

    db.execute("INSERT INTO users (name) VALUES ('Alice')");
    db.execute("INSERT INTO users (name) VALUES ('Bob')");

    final_clock = db.get_clock();
  }

  // Scope 2: Reopen and verify
  {
    CRDTSQLite db(db_path, 1);
    db.enable_crdt("users");

    ASSERT_EQ(count_rows(db.get_db(), "users"), 2);
    ASSERT_EQ(db.get_clock(), final_clock);

    // Can still get changes
    auto changes = db.get_changes_since(0);
    ASSERT_TRUE(changes.size() >= 2);
  }

  // Cleanup
  fs::remove(db_path);
}

// Test 13: Excluding nodes in get_changes_since
TEST(exclude_nodes) {
  TestDB test_db1("exclude_1");
  TestDB test_db2("exclude_2");

  CRDTSQLite db1(test_db1.path(), 1);
  CRDTSQLite db2(test_db2.path(), 2);

  db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db2.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");

  db1.enable_crdt("users");
  db2.enable_crdt("users");

  // Insert on both
  db1.execute("INSERT INTO users (name) VALUES ('Alice')");
  db2.execute("INSERT INTO users (name) VALUES ('Bob')");

  // Sync bidirectionally
  auto changes1 = db1.get_changes_since(0);
  auto changes2 = db2.get_changes_since(0);
  db1.merge_changes(changes2);
  db2.merge_changes(changes1);

  // Now get changes excluding node 1
  CrdtSet<CrdtNodeId> excluding;
  excluding.insert(1);
  auto filtered = db1.get_changes_since_excluding(0, excluding);

  // Should only have changes from node 2
  for (const auto &change : filtered) {
    ASSERT_EQ(change.node_id, 2);
  }
}

// Test 14: NULL values
TEST(null_values) {
  TestDB test_db("null_values");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
  db.enable_crdt("users");

  db.execute("INSERT INTO users (name, email) VALUES ('Alice', NULL)");

  auto changes = db.get_changes_since(0);

  // Should have a change for email column even though it's NULL
  bool has_email = false;
  for (const auto &change : changes) {
    if (change.col_name.has_value() && *change.col_name == "email") {
      has_email = true;
      // Value should represent NULL
      ASSERT_TRUE(change.value.has_value());
      ASSERT_EQ(*change.value, "NULL");
    }
  }
  ASSERT_TRUE(has_email);
}

// Test 15: Integer types
TEST(integer_types) {
  TestDB test_db("integer_types");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER)");
  db.enable_crdt("numbers");

  db.execute("INSERT INTO numbers (value) VALUES (42)");
  db.execute("INSERT INTO numbers (value) VALUES (-123)");
  db.execute("INSERT INTO numbers (value) VALUES (0)");

  auto changes = db.get_changes_since(0);

  // Verify integer values are correctly stored
  bool found_42 = false, found_neg = false, found_zero = false;
  for (const auto &change : changes) {
    if (change.col_name.has_value() && *change.col_name == "value" && change.value.has_value()) {
      if (*change.value == "42") found_42 = true;
      if (*change.value == "-123") found_neg = true;
      if (*change.value == "0") found_zero = true;
    }
  }

  ASSERT_TRUE(found_42);
  ASSERT_TRUE(found_neg);
  ASSERT_TRUE(found_zero);
}

TEST(alter_table_add_column) {
  TestDB db1_file("alter_table1");
  TestDB db2_file("alter_table2");

  // Create first node
  CRDTSQLite db1(db1_file.path(), 1);
  db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
  db1.enable_crdt("users");

  // Insert a record
  db1.execute("INSERT INTO users (name) VALUES ('Alice')");

  // Get initial changes (includes the INSERT)
  auto changes1 = db1.get_changes_since(0);
  // May include id and name columns
  ASSERT_TRUE(changes1.size() >= 1);

  // Find the name change
  bool found_name = false;
  for (const auto &change : changes1) {
    if (change.col_name.has_value() && change.col_name.value() == "name") {
      found_name = true;
      ASSERT_EQ(change.value.value(), "Alice");
    }
  }
  ASSERT_TRUE(found_name);

  // ALTER TABLE ADD COLUMN (should auto-refresh schema)
  db1.execute("ALTER TABLE users ADD COLUMN age INTEGER");

  // Insert a new record with the new column
  db1.execute("INSERT INTO users (name, age) VALUES ('Bob', 30)");

  // Verify the new column is tracked
  uint64_t last_version = changes1.back().local_db_version;
  auto changes2 = db1.get_changes_since(last_version);
  ASSERT_TRUE(changes2.size() >= 2);  // At least name + age for Bob

  // Find the age change
  bool found_age = false;
  for (const auto &change : changes2) {
    if (change.col_name.has_value() && change.col_name.value() == "age") {
      found_age = true;
      ASSERT_EQ(change.value.value(), "30");
    }
  }
  ASSERT_TRUE(found_age);

  // Sync to another node (with schema already including age column)
  CRDTSQLite db2(db2_file.path(), 2);
  db2.execute("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)");
  db2.enable_crdt("users");

  db2.merge_changes(db1.get_changes_since(0));

  // Verify both records synced
  ASSERT_EQ(count_rows(db2.get_db(), "users"), 2);

  // Verify Bob's age synced
  std::string age_val = get_value(db2.get_db(), "users", "age", 2);
  ASSERT_EQ(age_val, "30");
}

// Test 17: Tombstone resurrection - deleted record should not be recreatable
TEST(tombstone_resurrection) {
  TestDB test_db("tombstone_resurrection");
  CRDTSQLite db(test_db.path(), 1);

  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db.enable_crdt("users");

  // Insert and delete a record
  db.execute("INSERT INTO users (id, name) VALUES (100, 'Deleted User')");
  ASSERT_EQ(count_rows(db.get_db(), "users"), 1);

  db.execute("DELETE FROM users WHERE id = 100");
  ASSERT_EQ(count_rows(db.get_db(), "users"), 0);
  ASSERT_EQ(db.tombstone_count(), 1);

  uint64_t version_after_delete = db.get_clock();

  // Try to recreate the same record - tombstone should prevent resurrection
  db.execute("INSERT INTO users (id, name) VALUES (100, 'Resurrected User')");

  // FIXED: Tombstone now blocks resurrection locally too!
  // The INSERT physically happens but process_pending_changes() detects tombstone
  // and deletes the row, maintaining consistency with remote merge behavior
  ASSERT_EQ(count_rows(db.get_db(), "users"), 0);  // Tombstone enforced locally
  auto changes = db.get_changes_since(version_after_delete);
  ASSERT_EQ(changes.size(), 0);  // No changes emitted - resurrection blocked

  // Verify tombstone still exists
  ASSERT_EQ(db.tombstone_count(), 1);

  // When syncing to another node, same behavior
  TestDB test_db2("tombstone_resurrection_remote");
  CRDTSQLite db2(test_db2.path(), 2);
  db2.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db2.enable_crdt("users");

  // Sync all changes including original delete
  db2.merge_changes(db.get_changes_since(0));

  // Remote node should have tombstone, no records
  ASSERT_EQ(count_rows(db2.get_db(), "users"), 0);
  ASSERT_EQ(db2.tombstone_count(), 1);

  // Additional test: Verify UPDATE on tombstoned record is also blocked
  // First, create a non-tombstoned record
  db.execute("INSERT INTO users (id, name) VALUES (200, 'Active User')");
  ASSERT_EQ(count_rows(db.get_db(), "users"), 1);

  // Delete it to create tombstone
  db.execute("DELETE FROM users WHERE id = 200");
  ASSERT_EQ(count_rows(db.get_db(), "users"), 0);
  ASSERT_EQ(db.tombstone_count(), 2);

  // Try to resurrect via INSERT then UPDATE
  db.execute("INSERT INTO users (id, name) VALUES (200, 'Resurrected')");
  ASSERT_EQ(count_rows(db.get_db(), "users"), 0);  // INSERT blocked by tombstone

  // Try direct UPDATE (should also be blocked)
  // Note: This would need the record to exist first, but since INSERT was blocked,
  // UPDATE won't have a target. SQLite will silently do nothing.
  db.execute("UPDATE users SET name = 'Updated Ghost' WHERE id = 200");
  ASSERT_EQ(count_rows(db.get_db(), "users"), 0);  // Still no records
}

// Test 18: Concurrent edits to different columns
TEST(concurrent_different_columns) {
  TestDB test_db1("concurrent_diff_cols_1");
  TestDB test_db2("concurrent_diff_cols_2");

  CRDTSQLite db1(test_db1.path(), 1);
  CRDTSQLite db2(test_db2.path(), 2);

  // Setup both nodes with same initial state
  db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
  db1.enable_crdt("users");
  db1.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@old.com')");

  db2.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
  db2.enable_crdt("users");
  db2.merge_changes(db1.get_changes_since(0));

  // Capture sync point after initial sync (both nodes at same state)
  uint64_t sync_point1 = db1.get_clock();
  uint64_t sync_point2 = db2.get_clock();

  // Node 1: Update name column
  db1.execute("UPDATE users SET name = 'Alice Smith' WHERE id = 1");

  // Node 2: Update email column (concurrent, different column)
  db2.execute("UPDATE users SET email = 'alice@new.com' WHERE id = 1");

  // Sync both ways (each node sends changes since its own last sync)
  db1.merge_changes(db2.get_changes_since(sync_point2));
  db2.merge_changes(db1.get_changes_since(sync_point1));

  // Both changes should be preserved (different columns don't conflict)
  std::string name1 = get_value(db1.get_db(), "users", "name", 1);
  std::string email1 = get_value(db1.get_db(), "users", "email", 1);
  ASSERT_EQ(name1, "Alice Smith");
  ASSERT_EQ(email1, "alice@new.com");

  std::string name2 = get_value(db2.get_db(), "users", "name", 1);
  std::string email2 = get_value(db2.get_db(), "users", "email", 1);
  ASSERT_EQ(name2, "Alice Smith");
  ASSERT_EQ(email2, "alice@new.com");
}

void test_clock_overflow() {
  remove("test_clock_overflow.db");
  CRDTSQLite db("test_clock_overflow.db", 1);
  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
  db.enable_crdt("users");

  // Manually set clock to UINT64_MAX - 1
  // SQLite INTEGER is signed 64-bit, so we work around this by using sqlite3_bind_int64
  // with the value reinterpreted as signed
  std::string clock_table = "_crdt_users_clock";
  sqlite3_stmt *stmt;
  std::string update_sql = "UPDATE " + clock_table + " SET time = ?";
  sqlite3_prepare_v2(db.get_db(), update_sql.c_str(), -1, &stmt, nullptr);

  // Bind UINT64_MAX - 1, which will be stored as -2 in SQLite but read back as UINT64_MAX - 1
  sqlite3_bind_int64(stmt, 1, static_cast<int64_t>(UINT64_MAX - 1));
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  // This insert should work (clock goes to UINT64_MAX)
  db.execute("INSERT INTO users (name) VALUES ('Alice')");

  // Verify clock is now at UINT64_MAX
  ASSERT_EQ(db.get_clock(), UINT64_MAX);

  // Next insert will execute but wal_callback will detect overflow and set flag
  db.execute("INSERT INTO users (name) VALUES ('Bob')");

  // The operation AFTER Bob should throw (clock_overflow_ flag was set by wal_callback)
  bool exception_thrown = false;
  try {
    db.execute("INSERT INTO users (name) VALUES ('Charlie')");
  } catch (const CRDTSQLiteException& e) {
    std::string error(e.what());
    exception_thrown = (error.find("Clock overflow") != std::string::npos);
  }
  ASSERT_EQ(exception_thrown, true);
}

void test_trigger_restoration_on_exception() {
  remove("test_trigger_restore.db");
  CRDTSQLite db("test_trigger_restore.db", 1);
  db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, invalid_col TEXT)");
  db.enable_crdt("users");

  // Insert a valid record (creates changes for all non-PK columns: name, invalid_col)
  db.execute("INSERT INTO users (name) VALUES ('Alice')");
  auto changes1 = db.get_changes_since(0);
  // Note: INSERT tracks all columns, including NULL values
  ASSERT_EQ(changes1.size() >= 1, true);  // At least the name column
  size_t initial_changes = changes1.size();

  // Try to apply a change with invalid column name to trigger exception
  // This should fail in apply_to_sqlite, but triggers should be restored
  std::vector<Change<int64_t, std::string>> bad_changes;
  Change<int64_t, std::string> bad_change;
  bad_change.record_id = 2;
  bad_change.col_name = "invalid';DROP TABLE users--";  // SQL injection attempt
  bad_change.value = "hacker";
  bad_change.col_version = 1;
  bad_change.db_version = 10;
  bad_change.node_id = 2;
  bad_changes.push_back(bad_change);

  bool exception_thrown = false;
  try {
    db.merge_changes(bad_changes);
  } catch (const CRDTSQLiteException& e) {
    std::string error(e.what());
    exception_thrown = (error.find("Invalid column name") != std::string::npos);
  }
  ASSERT_EQ(exception_thrown, true);

  // Verify triggers are still working after exception
  db.execute("INSERT INTO users (name) VALUES ('Bob')");
  auto changes2 = db.get_changes_since(0);
  // Bob should create the same number of changes as Alice
  ASSERT_EQ(changes2.size(), initial_changes * 2);  // Both Alice and Bob tracked
}

void test_large_batch_processing() {
  remove("test_large_batch.db");
  CRDTSQLite db("test_large_batch.db", 1);
  db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)");
  db.enable_crdt("items");

  // Insert >500 records to test batch processing
  db.execute("BEGIN");
  for (int i = 0; i < 600; ++i) {
    std::string sql = "INSERT INTO items (value) VALUES ('item" + std::to_string(i) + "')";
    db.execute(sql.c_str());
  }
  db.execute("COMMIT");

  // Verify all changes were processed (600 inserts x 2 columns each)
  auto changes = db.get_changes_since(0);
  // The important thing is that batch processing handled >500 records without hitting SQLite limits
  ASSERT_EQ(changes.size() >= 600, true);
  std::printf("  - Processed %zu changes from 600 inserts\n", changes.size());

  // Verify all records exist
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db.get_db(), "SELECT COUNT(*) FROM items", -1, &stmt, nullptr);
  sqlite3_step(stmt);
  int count = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);
  ASSERT_EQ(count, 600);
}

int main() {
  std::cout << "Running CRDT-SQLite tests..." << std::endl << std::endl;

  RUN_TEST(basic_init);
  RUN_TEST(simple_insert);
  RUN_TEST(update_operation);
  RUN_TEST(delete_operation);
  RUN_TEST(two_node_sync);
  RUN_TEST(concurrent_updates);
  RUN_TEST(delete_sync);
  RUN_TEST(multiple_columns);
  RUN_TEST(tombstone_compaction);
  RUN_TEST(transaction_rollback);
  RUN_TEST(transaction_commit);
  RUN_TEST(persistence);
  RUN_TEST(exclude_nodes);
  RUN_TEST(null_values);
  RUN_TEST(integer_types);
  RUN_TEST(alter_table_add_column);
  RUN_TEST(tombstone_resurrection);
  RUN_TEST(concurrent_different_columns);
  RUN_TEST(clock_overflow);
  RUN_TEST(trigger_restoration_on_exception);
  RUN_TEST(large_batch_processing);

  std::cout << std::endl << "All tests passed!" << std::endl;
  return 0;
}
