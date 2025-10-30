// crdt_sqlite.hpp
#ifndef CRDT_SQLITE_HPP
#define CRDT_SQLITE_HPP

#include "crdt.hpp"
#include "record_id_types.hpp"
#include <sqlite3.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <optional>
#include <stdexcept>

// Allow users to override the record ID type
// Usage: #define CRDT_RECORD_ID_TYPE uint128_t before including this header
#ifndef CRDT_RECORD_ID_TYPE
using CrdtRecordId = int64_t;
#else
using CrdtRecordId = CRDT_RECORD_ID_TYPE;
#endif

/// Exception thrown for CRDT-SQLite specific errors
class CRDTSQLiteException : public std::runtime_error {
public:
  explicit CRDTSQLiteException(const std::string &msg) : std::runtime_error(msg) {}
};

/// Represents a SQLite value with type information
struct SQLiteValue {
  enum Type { NULL_TYPE, INTEGER, REAL, TEXT, BLOB };

  Type type;
  int64_t int_val;
  double real_val;
  std::string text_val;
  std::vector<uint8_t> blob_val;

  SQLiteValue() : type(NULL_TYPE), int_val(0), real_val(0.0) {}

  static SQLiteValue from_sqlite(sqlite3_value *val);
  std::string to_string() const;
  static SQLiteValue from_string(const std::string &str, Type type);
};

/// Pending change during a transaction
struct PendingChange {
  int operation; // SQLITE_INSERT, SQLITE_UPDATE, SQLITE_DELETE
  CrdtRecordId record_id;
  std::unordered_map<std::string, SQLiteValue> values;
};

/// CRDT-enabled SQLite database wrapper
///
/// This class wraps a SQLite database and enables CRDT synchronization
/// for specified tables. Changes are tracked automatically using SQLite's
/// update hooks, and can be synchronized with other nodes.
///
/// Example usage:
/// ```
/// CRDTSQLite db("myapp.db", 1);
/// db.enable_crdt("users");
///
/// // Use normal SQL
/// db.execute("INSERT INTO users (name, email) VALUES ('Alice', 'a@x.com')");
///
/// // Sync with other nodes
/// auto changes = db.get_changes_since(0);
/// // ... send to other nodes ...
///
/// // Merge remote changes
/// db.merge_changes(remote_changes);
/// ```
class CRDTSQLite {
public:
  /// Creates a CRDT-enabled SQLite database
  ///
  /// @param path Path to the SQLite database file
  /// @param node_id Unique identifier for this node (must be unique across all nodes)
  /// @throws CRDTSQLiteException if database cannot be opened
  CRDTSQLite(const char *path, CrdtNodeId node_id);

  /// Destructor - closes the database
  ~CRDTSQLite();

  // Disable copy (sqlite3* is not copyable)
  CRDTSQLite(const CRDTSQLite &) = delete;
  CRDTSQLite &operator=(const CRDTSQLite &) = delete;

  /// Enables CRDT synchronization for a table
  ///
  /// Creates shadow tables to track column versions and tombstones.
  /// After calling this, all modifications to the table will be tracked.
  ///
  /// @param table_name Name of the table to enable CRDT for
  /// @throws CRDTSQLiteException if table doesn't exist or shadow tables cannot be created
  void enable_crdt(const std::string &table_name);

  /// Executes SQL statement(s)
  ///
  /// Changes to CRDT-enabled tables are tracked automatically.
  ///
  /// @param sql SQL statement(s) to execute
  /// @throws CRDTSQLiteException if execution fails
  void execute(const char *sql);

  /// Prepares a SQL statement
  ///
  /// Use this for parameterized queries. Changes to CRDT-enabled tables
  /// are tracked automatically when the statement is executed.
  ///
  /// @param sql SQL statement to prepare
  /// @return Prepared statement (caller must call sqlite3_finalize)
  /// @throws CRDTSQLiteException if preparation fails
  sqlite3_stmt *prepare(const char *sql);

  /// Gets all changes since a given version
  ///
  /// @param last_db_version Version to get changes since (0 for all changes)
  /// @return Vector of changes that occurred after last_db_version
  std::vector<Change<CrdtRecordId, std::string>> get_changes_since(uint64_t last_db_version);

  /// Gets changes since a version, excluding specific nodes
  ///
  /// @param last_db_version Version to get changes since
  /// @param excluding Set of node IDs to exclude
  /// @return Vector of changes
  std::vector<Change<CrdtRecordId, std::string>> get_changes_since_excluding(
    uint64_t last_db_version,
    const CrdtSet<CrdtNodeId> &excluding);

  /// Merges changes from another node
  ///
  /// Applies changes using the CRDT merge rules, then updates the SQLite
  /// table to reflect accepted changes.
  ///
  /// @param changes Vector of changes to merge
  /// @return Vector of accepted changes (those that won conflict resolution)
  std::vector<Change<CrdtRecordId, std::string>> merge_changes(
    std::vector<Change<CrdtRecordId, std::string>> changes);

  /// Compacts tombstones older than the specified version
  ///
  /// Only call this when ALL nodes have acknowledged the min_acknowledged_version.
  /// Compacting too early may cause deleted records to reappear.
  ///
  /// @param min_acknowledged_version Minimum version acknowledged by all nodes
  /// @return Number of tombstones removed
  size_t compact_tombstones(uint64_t min_acknowledged_version);

  /// Gets the number of tombstones currently stored
  size_t tombstone_count() const;

  /// Gets the current logical clock value
  uint64_t get_clock() const;

  /// Gets the underlying sqlite3* handle
  ///
  /// Use with caution - direct modifications bypass CRDT tracking
  sqlite3 *get_db() { return db_; }

  // NOTE: JSON serialization removed - incomplete implementation
  // Users should implement their own serialization using Change<> structure

private:
  sqlite3 *db_;
  std::string tracked_table_;
  CrdtNodeId node_id_;

  // Pending changes within current transaction
  std::vector<PendingChange> pending_changes_;
  bool in_transaction_;
  bool flushing_changes_;  // Prevent re-entry into flush_changes()

  // Column type cache (column_name -> Type)
  std::unordered_map<std::string, SQLiteValue::Type> column_types_;

  /// Creates shadow tables for CRDT metadata
  void create_shadow_tables(const std::string &table_name);

  /// Caches column types for the tracked table
  void cache_column_types();

  /// Tracks a change from the update hook
  void track_change(int operation, const char *table, CrdtRecordId record_id);

  /// Queries the current values of a row
  std::unordered_map<std::string, SQLiteValue> query_row_values(CrdtRecordId record_id);

  /// Flushes pending changes to CRDT and shadow tables
  void flush_changes();

  /// Applies accepted changes to SQLite table
  void apply_to_sqlite(const std::vector<Change<CrdtRecordId, std::string>> &changes);

  /// SQLite callback for update hook
  static void update_callback(void *ctx, int operation,
                             const char *db_name, const char *table,
                             sqlite3_int64 rowid);

  /// SQLite callback for commit hook
  static int commit_callback(void *ctx);

  /// SQLite callback for rollback hook
  static void rollback_callback(void *ctx);

  /// Helper to execute SQL and check for errors
  void exec_or_throw(const char *sql);

  /// Helper to get error message
  std::string get_error() const;

  /// Validates and sanitizes table name to prevent SQL injection
  static bool is_valid_table_name(const std::string &name);
};

#endif // CRDT_SQLITE_HPP
