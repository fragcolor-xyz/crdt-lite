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

// PendingChange struct removed - we use triggers to populate _pending table instead

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
///
/// Thread Safety:
/// ⚠️  This class is NOT thread-safe. Do not access the same CRDTSQLite instance
/// from multiple threads concurrently. Reasons:
/// - SQLite connections have limited thread safety
/// - This class contains mutable state without synchronization
/// - SQLite hooks (update_hook, commit_hook) are NOT thread-safe callbacks
/// - Hooks may be invoked from different threads if connection is shared
///
/// Safe usage options:
/// - Use one CRDTSQLite instance per thread (each with its own database connection)
/// - Protect ALL access with an external mutex/lock (including hook callbacks)
/// - Use SQLite's WAL mode with proper application-level locking
///
/// ⚠️  NEVER share a CRDTSQLite instance across threads without external synchronization
///
/// Error Handling:
/// This class uses exceptions (CRDTSQLiteException) for all error conditions:
/// - Database connection failures
/// - SQL execution errors
/// - Parameter validation failures (e.g., table name too long, too many excluded nodes)
/// - Schema inconsistencies
///
/// Functions that return numeric values (e.g., get_clock(), tombstone_count()):
/// - Return 0 when no table is tracked (not an error condition)
/// - Return actual count/value otherwise
/// - Never return error codes - use exceptions instead
///
/// Functions that return vectors (e.g., get_changes_since()):
/// - Return empty vector when no changes exist (not an error)
/// - Throw CRDTSQLiteException on actual errors (SQL failure, validation failure)
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
  /// Schema migration support:
  /// - ✅ ALTER TABLE ADD COLUMN - fully automatic
  /// - ❌ DROP TABLE - blocked (would leave orphaned shadow tables)
  /// - ⚠️  RENAME TABLE - not blocked but WILL BREAK shadow tables
  /// - ⚠️  DROP COLUMN - not supported (causes metadata corruption)
  /// - ⚠️  RENAME COLUMN - not supported (causes metadata corruption)
  ///
  /// @param table_name Name of the table to enable CRDT for (max 23 chars)
  /// @throws CRDTSQLiteException if table doesn't exist, shadow tables cannot be created,
  ///         or table_name exceeds 23 characters (to prevent SQLite identifier overflow)
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
  /// @param max_changes Maximum number of changes to return (0 = unlimited)
  /// @return Vector of changes that occurred after last_db_version
  /// @note For unbounded queries (max_changes=0), consider memory implications
  std::vector<Change<CrdtRecordId, std::string>> get_changes_since(
    uint64_t last_db_version, size_t max_changes = 0);

  /// Gets changes since a version, excluding specific nodes
  ///
  /// @param last_db_version Version to get changes since
  /// @param excluding Set of node IDs to exclude (max 100 nodes)
  /// @return Vector of changes
  /// @throws CRDTSQLiteException if excluding.size() > 100
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
  ///
  /// The logical clock uses uint64_t, providing 2^64 unique version numbers.
  ///
  /// **Clock Overflow Limitation:**
  /// After 2^64 operations (~18 quintillion), the clock wraps to 0, breaking
  /// causality and conflict resolution. This is not handled.
  ///
  /// **Impact:** Negligible for all practical use cases. Even at 1 billion
  /// operations per second, overflow would take ~585 years.
  ///
  /// @return Current logical clock value (0 if no table is tracked)
  uint64_t get_clock() const;

  /// Gets the underlying sqlite3* handle
  ///
  /// Use with caution - direct modifications bypass CRDT tracking
  sqlite3 *get_db() { return db_; }

  /// Manually refresh schema metadata after ALTER TABLE
  ///
  /// Normally called automatically after execute(), but use this if you
  /// execute ALTER TABLE via raw sqlite3 API.
  void refresh_schema();

  // NOTE: JSON serialization removed - incomplete implementation
  // Users should implement their own serialization using Change<> structure

private:
  /// RAII wrapper for sqlite3_stmt* to prevent memory leaks
  ///
  /// Automatically calls sqlite3_finalize() on destruction, ensuring statements
  /// are properly cleaned up even if exceptions are thrown.
  ///
  /// Usage:
  ///   Statement stmt(prepare("SELECT ..."));
  ///   sqlite3_bind_int64(stmt.get(), 1, value);
  ///   while (sqlite3_step(stmt.get()) == SQLITE_ROW) { ... }
  ///   // No need to call finalize() - done automatically
  ///
  /// NOTE: Existing code uses raw pointers for historical reasons.
  /// New code should prefer this wrapper for exception safety.
  class Statement {
  public:
    explicit Statement(sqlite3_stmt *stmt) : stmt_(stmt) {}
    ~Statement() { if (stmt_) sqlite3_finalize(stmt_); }
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    Statement(Statement&& other) noexcept : stmt_(other.stmt_) { other.stmt_ = nullptr; }
    Statement& operator=(Statement&& other) noexcept {
      if (this != &other) {
        if (stmt_) sqlite3_finalize(stmt_);
        stmt_ = other.stmt_;
        other.stmt_ = nullptr;
      }
      return *this;
    }
    sqlite3_stmt* get() const { return stmt_; }
    sqlite3_stmt* operator->() const { return stmt_; }
  private:
    sqlite3_stmt *stmt_;
  };

  /// RAII guard for processing_wal_changes_ flag
  ///
  /// Ensures the flag is always reset, even if exceptions occur during
  /// process_pending_changes(). This prevents the wal_callback from being
  /// permanently disabled after an error.
  class ProcessingGuard {
  public:
    explicit ProcessingGuard(bool& flag) : flag_(flag) { flag_ = true; }
    ~ProcessingGuard() { flag_ = false; }
    ProcessingGuard(const ProcessingGuard&) = delete;
    ProcessingGuard& operator=(const ProcessingGuard&) = delete;
  private:
    bool& flag_;
  };

  /// RAII guard for trigger restoration in apply_to_sqlite
  ///
  /// Ensures triggers are always recreated after being dropped, even if
  /// exceptions occur during change application. Without this, the database
  /// would be left without triggers, causing silent data loss (changes won't
  /// be tracked or replicated).
  class TriggerGuard {
  public:
    TriggerGuard(CRDTSQLite* db, const std::string& table, const std::vector<std::string>& columns)
      : db_(db), table_(table), columns_(columns) {}

    ~TriggerGuard() {
      try {
        db_->create_triggers(table_, columns_, false);
      } catch (const std::exception& e) {
        // Log error but don't throw from destructor
        std::fprintf(stderr, "CRITICAL: Failed to restore triggers for %s: %s\n",
                    table_.c_str(), e.what());
      }
    }

    TriggerGuard(const TriggerGuard&) = delete;
    TriggerGuard& operator=(const TriggerGuard&) = delete;
  private:
    CRDTSQLite* db_;
    std::string table_;
    std::vector<std::string> columns_;
  };

  sqlite3 *db_;
  std::string tracked_table_;
  CrdtNodeId node_id_;

  // Column type cache (column_name -> Type)
  std::unordered_map<std::string, SQLiteValue::Type> column_types_;

  // Schema change tracking (for ALTER TABLE auto-handling)
  bool pending_schema_refresh_;

  // Prevent re-entry into wal_callback (process_pending_changes writes trigger wal_hook again)
  bool processing_wal_changes_;

  // Flag set when clock overflow is detected (allows exception to propagate to caller)
  bool clock_overflow_;

  /// Creates shadow tables for CRDT metadata
  void create_shadow_tables(const std::string &table_name);

  /// Caches column types for the specified table
  void cache_column_types(const std::string &table_name);

  /// Queries the current values of a row
  std::unordered_map<std::string, SQLiteValue> query_row_values(CrdtRecordId record_id);

  /// Processes pending changes from _pending table (called by wal_hook)
  void process_pending_changes();

  /// Applies accepted changes to SQLite table
  void apply_to_sqlite(const std::vector<Change<CrdtRecordId, std::string>> &changes);

  /// SQLite callback for authorizer (detects ALTER TABLE)
  static int authorizer_callback(void *ctx, int action_code,
                                 const char *arg1, const char *arg2,
                                 const char *arg3, const char *arg4);

  /// SQLite callback for WAL hook (fires AFTER commit with locks released)
  static int wal_callback(void *ctx, sqlite3 *db, const char *db_name, int num_pages);

  /// SQLite callback for rollback hook (clears pending table)
  static void rollback_callback(void *ctx);

  /// Helper to execute SQL and check for errors
  void exec_or_throw(const char *sql);

  /// Helper to get error message
  std::string get_error() const;

  /// Validates and sanitizes table name to prevent SQL injection
  static bool is_valid_table_name(const std::string &name);

  /// Validates column name to prevent SQL injection
  static bool is_valid_column_name(const std::string &name);

  /// Creates INSERT, UPDATE, DELETE triggers for change tracking
  /// @param table_name Name of the table to create triggers for
  /// @param columns Vector of column names (excluding primary key)
  /// @param use_if_not_exists Whether to use "IF NOT EXISTS" clause
  void create_triggers(const std::string &table_name,
                       const std::vector<std::string> &columns,
                       bool use_if_not_exists);
};

#endif // CRDT_SQLITE_HPP
