// test_crdt_sqlite_uint128.cpp
// Tests for CRDT-SQLite using uint128_t record IDs

#ifdef __SIZEOF_INT128__

// Define the record ID type BEFORE including headers
#define CRDT_RECORD_ID_TYPE __uint128_t

#include "crdt_sqlite.hpp"
#include <iostream>
#include <cassert>
#include <cstring>
#include <unistd.h>

// Test helper
void test(const char *name, void (*fn)()) {
  std::cout << "Running test: " << name << "... ";
  fn();
  std::cout << "PASSED" << std::endl;
}

void test_basic_uint128() {
  unlink("test_uint128.db");

  CRDTSQLite db("test_uint128.db", 1);

  // Create a test table with an explicit id column for uint128
  db.execute(R"(
    CREATE TABLE users (
      id BLOB PRIMARY KEY,  -- 16-byte UUID
      name TEXT,
      email TEXT
    )
  )");

  db.enable_crdt("users");

  // Generate a uint128 ID
  uint128_t id1 = RecordIdTraits<uint128_t>::generate_with_node(1);
  std::string id1_str = RecordIdTraits<uint128_t>::to_string(id1);

  std::cout << "\n  Generated ID: " << id1_str << std::endl;

  // Insert record
  std::string insert_sql = "INSERT INTO users (id, name, email) VALUES (?, 'Alice', 'alice@example.com')";
  sqlite3_stmt *stmt = db.prepare(insert_sql.c_str());
  RecordIdTraits<uint128_t>::bind_to_sqlite(stmt, 1, id1);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  // Query back
  std::string query_sql = "SELECT id, name, email FROM users WHERE id = ?";
  stmt = db.prepare(query_sql.c_str());
  RecordIdTraits<uint128_t>::bind_to_sqlite(stmt, 1, id1);

  if (sqlite3_step(stmt) == SQLITE_ROW) {
    uint128_t id_result = RecordIdTraits<uint128_t>::from_sqlite(stmt, 0);
    const char *name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
    const char *email = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));

    assert(id_result == id1);
    assert(std::string(name) == "Alice");
    assert(std::string(email) == "alice@example.com");

    std::cout << "  Retrieved: " << RecordIdTraits<uint128_t>::to_string(id_result)
              << " -> " << name << " <" << email << ">" << std::endl;
  } else {
    assert(false && "Record not found!");
  }

  sqlite3_finalize(stmt);
  unlink("test_uint128.db");
}

void test_two_node_sync_uint128() {
  unlink("test_node1.db");
  unlink("test_node2.db");

  CRDTSQLite db1("test_node1.db", 1);
  CRDTSQLite db2("test_node2.db", 2);

  // Create tables with uuid columns
  const char *create_table = R"(
    CREATE TABLE items (
      id BLOB PRIMARY KEY,
      name TEXT,
      quantity INTEGER
    )
  )";

  db1.execute(create_table);
  db2.execute(create_table);

  db1.enable_crdt("items");
  db2.enable_crdt("items");

  // Node 1: Insert item
  uint128_t id1 = RecordIdTraits<uint128_t>::generate_with_node(1);
  std::string sql1 = "INSERT INTO items (id, name, quantity) VALUES (?, 'Widget', 10)";
  sqlite3_stmt *stmt = db1.prepare(sql1.c_str());
  RecordIdTraits<uint128_t>::bind_to_sqlite(stmt, 1, id1);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  // Node 2: Insert different item
  uint128_t id2 = RecordIdTraits<uint128_t>::generate_with_node(2);
  std::string sql2 = "INSERT INTO items (id, name, quantity) VALUES (?, 'Gadget', 20)";
  stmt = db2.prepare(sql2.c_str());
  RecordIdTraits<uint128_t>::bind_to_sqlite(stmt, 1, id2);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  std::cout << "\n  Node 1 ID: " << RecordIdTraits<uint128_t>::to_string(id1) << std::endl;
  std::cout << "  Node 2 ID: " << RecordIdTraits<uint128_t>::to_string(id2) << std::endl;

  // Verify IDs don't collide (high 64 bits should be different)
  uint64_t high1 = static_cast<uint64_t>(id1 >> 64);
  uint64_t high2 = static_cast<uint64_t>(id2 >> 64);
  std::cout << "  High bits: " << high1 << " vs " << high2 << std::endl;
  assert(high1 == 1 && high2 == 2);

  // Sync: node1 → node2
  auto changes1 = db1.get_changes_since(0);
  db2.merge_changes(changes1);

  // Sync: node2 → node1
  auto changes2 = db2.get_changes_since(0);
  db1.merge_changes(changes2);

  // Both nodes should have 2 items
  stmt = db1.prepare("SELECT COUNT(*) FROM items");
  sqlite3_step(stmt);
  int count1 = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);

  stmt = db2.prepare("SELECT COUNT(*) FROM items");
  sqlite3_step(stmt);
  int count2 = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);

  std::cout << "  Final counts: node1=" << count1 << ", node2=" << count2 << std::endl;
  assert(count1 == 2 && count2 == 2);

  unlink("test_node1.db");
  unlink("test_node2.db");
}

void test_collision_resistance() {
  // Generate 10000 IDs from node 1 and 10000 from node 2
  // Verify no collisions
  std::unordered_set<uint128_t> ids;

  for (int node = 1; node <= 2; node++) {
    for (int i = 0; i < 10000; i++) {
      uint128_t id = RecordIdTraits<uint128_t>::generate_with_node(node);
      assert(ids.find(id) == ids.end() && "Collision detected!");
      ids.insert(id);
    }
  }

  std::cout << "\n  Generated " << ids.size() << " unique IDs with no collisions" << std::endl;
  assert(ids.size() == 20000);
}

void test_deletion_sync_uint128() {
  unlink("test_del_node1.db");
  unlink("test_del_node2.db");

  CRDTSQLite db1("test_del_node1.db", 1);
  CRDTSQLite db2("test_del_node2.db", 2);

  // Create tables with uuid columns
  const char *create_table = R"(
    CREATE TABLE products (
      id BLOB PRIMARY KEY,
      name TEXT,
      price REAL
    )
  )";

  db1.execute(create_table);
  db2.execute(create_table);

  db1.enable_crdt("products");
  db2.enable_crdt("products");

  // Node 1: Insert two products
  uint128_t id1 = RecordIdTraits<uint128_t>::generate_with_node(1);
  uint128_t id2 = RecordIdTraits<uint128_t>::generate_with_node(1);

  std::string sql1 = "INSERT INTO products (id, name, price) VALUES (?, 'Product A', 10.5)";
  sqlite3_stmt *stmt = db1.prepare(sql1.c_str());
  RecordIdTraits<uint128_t>::bind_to_sqlite(stmt, 1, id1);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  std::string sql2 = "INSERT INTO products (id, name, price) VALUES (?, 'Product B', 20.5)";
  stmt = db1.prepare(sql2.c_str());
  RecordIdTraits<uint128_t>::bind_to_sqlite(stmt, 1, id2);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  std::cout << "\n  Node 1 inserted 2 products" << std::endl;

  // Sync to node 2
  auto changes1 = db1.get_changes_since(0);
  db2.merge_changes(changes1);
  uint64_t last_sync_version = db1.get_clock();

  // Verify node 2 has both products
  stmt = db2.prepare("SELECT COUNT(*) FROM products");
  sqlite3_step(stmt);
  int count = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);
  assert(count == 2);
  std::cout << "  Node 2 received both products (count=" << count << ")" << std::endl;

  // Node 1: Delete one product
  std::string delete_sql = "DELETE FROM products WHERE id = ?";
  stmt = db1.prepare(delete_sql.c_str());
  RecordIdTraits<uint128_t>::bind_to_sqlite(stmt, 1, id1);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  std::cout << "  Node 1 deleted Product A" << std::endl;

  // Verify node 1 has only 1 product now
  stmt = db1.prepare("SELECT COUNT(*) FROM products");
  sqlite3_step(stmt);
  count = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);
  assert(count == 1);
  std::cout << "  Node 1 now has 1 product (count=" << count << ")" << std::endl;

  // Sync deletion to node 2 (get changes since last sync)
  auto changes_del = db1.get_changes_since(last_sync_version);
  std::cout << "  Syncing " << changes_del.size() << " changes to node 2" << std::endl;
  db2.merge_changes(changes_del);

  // Verify node 2 also has only 1 product
  stmt = db2.prepare("SELECT COUNT(*) FROM products");
  sqlite3_step(stmt);
  count = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);
  assert(count == 1);
  std::cout << "  Node 2 also has 1 product after sync (count=" << count << ")" << std::endl;

  // Verify tombstone exists on node 1
  size_t tombstones1 = db1.tombstone_count();
  assert(tombstones1 == 1);
  std::cout << "  Node 1 has " << tombstones1 << " tombstone" << std::endl;

  // Verify tombstone synced to node 2
  size_t tombstones2 = db2.tombstone_count();
  assert(tombstones2 == 1);
  std::cout << "  Node 2 has " << tombstones2 << " tombstone" << std::endl;

  unlink("test_del_node1.db");
  unlink("test_del_node2.db");
}

int main() {
  std::cout << "Running CRDT-SQLite uint128_t tests...\n" << std::endl;

  test("basic_uint128", test_basic_uint128);
  test("two_node_sync_uint128", test_two_node_sync_uint128);
  test("collision_resistance", test_collision_resistance);
  test("deletion_sync_uint128", test_deletion_sync_uint128);

  std::cout << "\nAll uint128_t tests passed! ✅" << std::endl;
  return 0;
}

#else
int main() {
  std::cout << "uint128_t not supported on this platform" << std::endl;
  return 0;
}
#endif
