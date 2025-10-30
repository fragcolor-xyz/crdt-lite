// crdt_sqlite_uuid.hpp
// Example of UUID-based CRDT-SQLite wrapper
//
// This shows how to adapt crdt_sqlite.hpp to use UUIDs instead of rowid
// For production use, you'd want to refactor the original to be templated

#ifndef CRDT_SQLITE_UUID_HPP
#define CRDT_SQLITE_UUID_HPP

#include "crdt.hpp"
#include <sqlite3.h>
#include <string>
#include <random>
#include <sstream>
#include <iomanip>

// Simple UUID generator (for demonstration - use a real library in production)
class UUIDGenerator {
public:
  static std::string generate() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    uint64_t high = dis(gen);
    uint64_t low = dis(gen);

    // Format as UUID v4
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    oss << std::setw(8) << (high >> 32);
    oss << '-';
    oss << std::setw(4) << ((high >> 16) & 0xFFFF);
    oss << '-';
    oss << std::setw(4) << (0x4000 | ((high & 0x0FFF)));
    oss << '-';
    oss << std::setw(4) << (0x8000 | ((low >> 48) & 0x3FFF));
    oss << '-';
    oss << std::setw(12) << (low & 0xFFFFFFFFFFFF);

    return oss.str();
  }
};

// UUID-based SQLite CRDT
// Key differences from crdt_sqlite.hpp:
// 1. Record IDs are std::string (UUIDs) instead of int64_t
// 2. Table must have a TEXT PRIMARY KEY for the UUID
// 3. Application generates UUIDs before INSERT
class CRDTSQLiteUUID {
private:
  sqlite3 *db_;
  CRDT<std::string, std::string> crdt_;  // UUID keys
  std::string tracked_table_;
  std::string uuid_column_;

public:
  CRDTSQLiteUUID(const char *path, CrdtNodeId node_id, const std::string &uuid_column = "id")
    : db_(nullptr), crdt_(node_id, nullptr), uuid_column_(uuid_column) {
    // Open database...
  }

  // Application must provide UUID for INSERT
  void execute_with_uuid(const char *sql, const std::string &uuid) {
    // Bind UUID to the statement
    // Track changes with UUID as record_id
  }

  // Generate UUID for convenience
  std::string generate_uuid() {
    return UUIDGenerator::generate();
  }
};

// Example schema for UUID-based tables:
//
// CREATE TABLE users (
//   id TEXT PRIMARY KEY,  -- UUID, not INTEGER
//   name TEXT,
//   email TEXT
// );
//
// Usage:
//   CRDTSQLiteUUID db("app.db", 1);
//   db.enable_crdt("users", "id");
//
//   std::string uuid = db.generate_uuid();
//   db.execute_with_uuid("INSERT INTO users (id, name) VALUES (?, 'Alice')", uuid);

#endif // CRDT_SQLITE_UUID_HPP
