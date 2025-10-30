// crdt_sqlite.cpp
#include "crdt_sqlite.hpp"
#include <sstream>
#include <iomanip>
#include <cstring>

// SQLiteValue implementation

SQLiteValue SQLiteValue::from_sqlite(sqlite3_value *val) {
  SQLiteValue result;
  int type = sqlite3_value_type(val);

  switch (type) {
  case SQLITE_NULL:
    result.type = NULL_TYPE;
    break;
  case SQLITE_INTEGER:
    result.type = INTEGER;
    result.int_val = sqlite3_value_int64(val);
    break;
  case SQLITE_FLOAT:
    result.type = REAL;
    result.real_val = sqlite3_value_double(val);
    break;
  case SQLITE_TEXT:
    result.type = TEXT;
    result.text_val = reinterpret_cast<const char *>(sqlite3_value_text(val));
    break;
  case SQLITE_BLOB: {
    result.type = BLOB;
    const uint8_t *blob = reinterpret_cast<const uint8_t *>(sqlite3_value_blob(val));
    int bytes = sqlite3_value_bytes(val);
    result.blob_val.assign(blob, blob + bytes);
    break;
  }
  }

  return result;
}

std::string SQLiteValue::to_string() const {
  std::ostringstream oss;
  switch (type) {
  case NULL_TYPE:
    return "NULL";
  case INTEGER:
    return std::to_string(int_val);
  case REAL:
    oss << std::setprecision(17) << real_val;
    return oss.str();
  case TEXT:
    return text_val;
  case BLOB: {
    // Encode as hex
    oss << "BLOB:";
    for (uint8_t byte : blob_val) {
      oss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(byte);
    }
    return oss.str();
  }
  }
  return "";
}

SQLiteValue SQLiteValue::from_string(const std::string &str, Type type) {
  SQLiteValue result;
  result.type = type;

  switch (type) {
  case NULL_TYPE:
    break;
  case INTEGER:
    result.int_val = std::stoll(str);
    break;
  case REAL:
    result.real_val = std::stod(str);
    break;
  case TEXT:
    result.text_val = str;
    break;
  case BLOB: {
    // Decode from hex (skip "BLOB:" prefix)
    if (str.substr(0, 5) == "BLOB:") {
      std::string hex = str.substr(5);
      for (size_t i = 0; i < hex.length(); i += 2) {
        std::string byte_str = hex.substr(i, 2);
        uint8_t byte = static_cast<uint8_t>(std::stoi(byte_str, nullptr, 16));
        result.blob_val.push_back(byte);
      }
    }
    break;
  }
  }

  return result;
}

// CRDTSQLite implementation

CRDTSQLite::CRDTSQLite(const char *path, CrdtNodeId node_id)
    : db_(nullptr), node_id_(node_id), in_transaction_(false), flushing_changes_(false),
      pending_schema_refresh_(false) {
  int rc = sqlite3_open(path, &db_);
  if (rc != SQLITE_OK) {
    std::string error = "Failed to open database: " + std::string(sqlite3_errmsg(db_));
    sqlite3_close(db_);
    throw CRDTSQLiteException(error);
  }

  // Enable foreign keys
  exec_or_throw("PRAGMA foreign_keys = ON");

  // Install hooks
  sqlite3_set_authorizer(db_, authorizer_callback, this);
  sqlite3_update_hook(db_, update_callback, this);
  sqlite3_commit_hook(db_, commit_callback, this);
  sqlite3_rollback_hook(db_, rollback_callback, this);
}

CRDTSQLite::~CRDTSQLite() {
  if (db_) {
    sqlite3_close(db_);
  }
}

void CRDTSQLite::enable_crdt(const std::string &table_name) {
  if (!tracked_table_.empty()) {
    throw CRDTSQLiteException("CRDT is already enabled for table: " + tracked_table_);
  }

  // SECURITY: Validate table name to prevent SQL injection
  if (!is_valid_table_name(table_name)) {
    throw CRDTSQLiteException("Invalid table name: must contain only alphanumeric characters and underscores");
  }

  // Check if table exists using parameterized query
  std::string check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
  sqlite3_stmt *stmt;
  int rc = sqlite3_prepare_v2(db_, check_sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    throw CRDTSQLiteException("Failed to check table existence: " + get_error());
  }

  sqlite3_bind_text(stmt, 1, table_name.c_str(), -1, SQLITE_TRANSIENT);
  bool exists = (sqlite3_step(stmt) == SQLITE_ROW);
  sqlite3_finalize(stmt);

  if (!exists) {
    throw CRDTSQLiteException("Table does not exist: " + table_name);
  }

  tracked_table_ = table_name;
  cache_column_types();
  create_shadow_tables(table_name);
  // No need to load into memory - we query shadow tables directly
}

void CRDTSQLite::create_shadow_tables(const std::string &table_name) {
  // Determine record_id SQL type
  std::string record_id_type = RecordIdTraits<CrdtRecordId>::sql_type();

  // Create versions table
  std::string versions_table = "_crdt_" + table_name + "_versions";
  std::string create_versions = R"(
    CREATE TABLE IF NOT EXISTS )" + versions_table + R"( (
      record_id )" + record_id_type + R"( NOT NULL,
      col_name TEXT NOT NULL,
      col_version INTEGER NOT NULL,
      db_version INTEGER NOT NULL,
      node_id INTEGER NOT NULL,
      local_db_version INTEGER NOT NULL,
      PRIMARY KEY (record_id, col_name)
    )
  )";
  exec_or_throw(create_versions.c_str());

  // PERFORMANCE: Create index on local_db_version for efficient sync queries
  std::string create_versions_idx = "CREATE INDEX IF NOT EXISTS " + versions_table +
                                    "_local_db_version_idx ON " + versions_table + "(local_db_version)";
  exec_or_throw(create_versions_idx.c_str());

  // Create tombstones table
  std::string tombstones_table = "_crdt_" + table_name + "_tombstones";
  std::string create_tombstones = R"(
    CREATE TABLE IF NOT EXISTS )" + tombstones_table + R"( (
      record_id )" + record_id_type + R"( PRIMARY KEY,
      db_version INTEGER NOT NULL,
      node_id INTEGER NOT NULL,
      local_db_version INTEGER NOT NULL
    )
  )";
  exec_or_throw(create_tombstones.c_str());

  // PERFORMANCE: Create index on local_db_version for efficient sync queries
  std::string create_tombstones_idx = "CREATE INDEX IF NOT EXISTS " + tombstones_table +
                                      "_local_db_version_idx ON " + tombstones_table + "(local_db_version)";
  exec_or_throw(create_tombstones_idx.c_str());

  // Create clock table
  std::string clock_table = "_crdt_" + table_name + "_clock";
  std::string create_clock = R"(
    CREATE TABLE IF NOT EXISTS )" + clock_table + R"( (
      time INTEGER NOT NULL
    )
  )";
  exec_or_throw(create_clock.c_str());

  // Initialize clock if empty
  std::string check_clock = "SELECT COUNT(*) FROM " + clock_table;
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db_, check_clock.c_str(), -1, &stmt, nullptr);
  sqlite3_step(stmt);
  int count = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);

  if (count == 0) {
    std::string init_clock = "INSERT INTO " + clock_table + " VALUES (0)";
    exec_or_throw(init_clock.c_str());
  }

  // Create column types table (stores type info for reconstruction)
  std::string types_table = "_crdt_" + table_name + "_types";
  std::string create_types = R"(
    CREATE TABLE IF NOT EXISTS )" + types_table + R"( (
      col_name TEXT PRIMARY KEY,
      col_type INTEGER NOT NULL
    )
  )";
  exec_or_throw(create_types.c_str());

  // Store column types
  for (const auto &[col_name, col_type] : column_types_) {
    std::string insert_type = "INSERT OR REPLACE INTO " + types_table +
                             " (col_name, col_type) VALUES (?, ?)";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db_, insert_type.c_str(), -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, col_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 2, static_cast<int>(col_type));
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
  }

  // For non-auto-increment types (e.g., uint128_t), create lookaside table
  // This maps SQLite rowid → CRDT ID for deletion handling
  if constexpr (!RecordIdTraits<CrdtRecordId>::is_auto_increment()) {
    std::string lookaside_table = "_crdt_" + table_name + "_lookaside";
    std::string id_type = RecordIdTraits<CrdtRecordId>::sql_type();
    std::string create_lookaside = R"(
      CREATE TABLE IF NOT EXISTS )" + lookaside_table + R"( (
        rowid INTEGER PRIMARY KEY,
        id )" + id_type + R"( NOT NULL UNIQUE
      )
    )";
    exec_or_throw(create_lookaside.c_str());
  }
}

void CRDTSQLite::cache_column_types() {
  std::string pragma_sql = "PRAGMA table_info(" + tracked_table_ + ")";
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db_, pragma_sql.c_str(), -1, &stmt, nullptr);

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    std::string col_name = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1));
    std::string col_type = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2));

    // Map SQLite type affinity to our Type enum
    SQLiteValue::Type type = SQLiteValue::TEXT; // default
    if (col_type.find("INT") != std::string::npos) {
      type = SQLiteValue::INTEGER;
    } else if (col_type.find("REAL") != std::string::npos ||
               col_type.find("FLOAT") != std::string::npos ||
               col_type.find("DOUBLE") != std::string::npos) {
      type = SQLiteValue::REAL;
    } else if (col_type.find("BLOB") != std::string::npos) {
      type = SQLiteValue::BLOB;
    }

    column_types_[col_name] = type;
  }
  sqlite3_finalize(stmt);
}

void CRDTSQLite::refresh_schema() {
  if (tracked_table_.empty()) {
    return;  // No table being tracked
  }

  // Re-scan column types from table
  cache_column_types();

  // Update _crdt_<table>_types table with current columns
  std::string types_table = "_crdt_" + tracked_table_ + "_types";
  for (const auto &[col_name, col_type] : column_types_) {
    std::string insert_type = "INSERT OR REPLACE INTO " + types_table +
                             " (col_name, col_type) VALUES (?, ?)";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db_, insert_type.c_str(), -1, &stmt, nullptr);
    sqlite3_bind_text(stmt, 1, col_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 2, static_cast<int>(col_type));
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
  }
}

void CRDTSQLite::execute(const char *sql) {
  char *err_msg = nullptr;
  int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &err_msg);
  if (rc != SQLITE_OK) {
    std::string error = "SQL execution failed: ";
    if (err_msg) {
      error += err_msg;
      sqlite3_free(err_msg);
    }
    throw CRDTSQLiteException(error);
  }

  // If ALTER TABLE was detected, refresh schema metadata
  if (pending_schema_refresh_) {
    refresh_schema();
    pending_schema_refresh_ = false;
  }
}

sqlite3_stmt *CRDTSQLite::prepare(const char *sql) {
  sqlite3_stmt *stmt;
  int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    throw CRDTSQLiteException("Failed to prepare statement: " + get_error());
  }
  return stmt;
}

std::vector<Change<CrdtRecordId, std::string>>
CRDTSQLite::get_changes_since(uint64_t last_db_version) {
  std::vector<Change<CrdtRecordId, std::string>> changes;

  // Determine id column (rowid or id)
  std::string id_column;
  if constexpr (RecordIdTraits<CrdtRecordId>::is_auto_increment()) {
    id_column = "v.record_id";  // For int64_t, record_id IS rowid
  } else {
    id_column = "t.id";  // For uint128_t, need to join for id column
  }

  // Query regular column changes from shadow tables + main table
  std::string versions_table = "_crdt_" + tracked_table_ + "_versions";
  std::string query = "SELECT v.record_id, v.col_name, v.col_version, v.db_version, "
                     "v.node_id, v.local_db_version, " + id_column + " AS actual_id, t.* "
                     "FROM " + versions_table + " v "
                     "LEFT JOIN " + tracked_table_ + " t ON ";

  if constexpr (RecordIdTraits<CrdtRecordId>::is_auto_increment()) {
    query += "v.record_id = t.rowid ";
  } else {
    query += "v.record_id = t.id ";
  }

  query += "WHERE v.local_db_version > ?";

  sqlite3_stmt *stmt = prepare(query.c_str());
  sqlite3_bind_int64(stmt, 1, last_db_version);

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    CrdtRecordId record_id = RecordIdTraits<CrdtRecordId>::from_sqlite(stmt, 0);
    std::string col_name = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1));
    uint64_t col_version = sqlite3_column_int64(stmt, 2);
    uint64_t db_version = sqlite3_column_int64(stmt, 3);
    CrdtNodeId node_id = sqlite3_column_int64(stmt, 4);
    uint64_t local_db_version = sqlite3_column_int64(stmt, 5);

    // Find column value in result set
    std::optional<std::string> value;
    int num_cols = sqlite3_column_count(stmt);
    for (int i = 7; i < num_cols; i++) {  // Start at 7 (after actual_id)
      const char *name = sqlite3_column_name(stmt, i);
      if (name && col_name == name) {  // FIX: Compare string contents, not pointers
        sqlite3_value *val = sqlite3_column_value(stmt, i);
        SQLiteValue sql_val = SQLiteValue::from_sqlite(val);
        value = sql_val.to_string();
        break;
      }
    }

    changes.emplace_back(record_id, std::move(col_name), std::move(value),
                        col_version, db_version, node_id, local_db_version);
  }
  sqlite3_finalize(stmt);

  // Query tombstones
  std::string tombstones_table = "_crdt_" + tracked_table_ + "_tombstones";
  std::string tomb_query = "SELECT record_id, db_version, node_id, local_db_version "
                          "FROM " + tombstones_table + " "
                          "WHERE local_db_version > ?";

  stmt = prepare(tomb_query.c_str());
  sqlite3_bind_int64(stmt, 1, last_db_version);

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    CrdtRecordId record_id = RecordIdTraits<CrdtRecordId>::from_sqlite(stmt, 0);
    uint64_t db_version = sqlite3_column_int64(stmt, 1);
    CrdtNodeId node_id = sqlite3_column_int64(stmt, 2);
    uint64_t local_db_version = sqlite3_column_int64(stmt, 3);

    // Tombstone: no col_name, no value
    changes.emplace_back(record_id, std::nullopt, std::nullopt,
                        1, db_version, node_id, local_db_version);
  }
  sqlite3_finalize(stmt);

  return changes;
}

std::vector<Change<CrdtRecordId, std::string>>
CRDTSQLite::get_changes_since_excluding(uint64_t last_db_version,
                                       const CrdtSet<CrdtNodeId> &excluding) {
  // Build NOT IN clause for excluded nodes
  std::string excluded_nodes;
  for (auto node : excluding) {
    if (!excluded_nodes.empty()) excluded_nodes += ",";
    excluded_nodes += std::to_string(node);
  }

  std::vector<Change<CrdtRecordId, std::string>> changes;

  // Determine id column
  std::string id_column;
  if constexpr (RecordIdTraits<CrdtRecordId>::is_auto_increment()) {
    id_column = "v.record_id";
  } else {
    id_column = "t.id";
  }

  // Query regular changes
  std::string versions_table = "_crdt_" + tracked_table_ + "_versions";
  std::string query = "SELECT v.record_id, v.col_name, v.col_version, v.db_version, "
                     "v.node_id, v.local_db_version, " + id_column + " AS actual_id, t.* "
                     "FROM " + versions_table + " v "
                     "LEFT JOIN " + tracked_table_ + " t ON ";

  if constexpr (RecordIdTraits<CrdtRecordId>::is_auto_increment()) {
    query += "v.record_id = t.rowid ";
  } else {
    query += "v.record_id = t.id ";
  }

  query += "WHERE v.local_db_version > ?";
  if (!excluded_nodes.empty()) {
    query += " AND v.node_id NOT IN (" + excluded_nodes + ")";
  }

  sqlite3_stmt *stmt = prepare(query.c_str());
  sqlite3_bind_int64(stmt, 1, last_db_version);

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    CrdtRecordId record_id = RecordIdTraits<CrdtRecordId>::from_sqlite(stmt, 0);
    std::string col_name = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1));
    uint64_t col_version = sqlite3_column_int64(stmt, 2);
    uint64_t db_version = sqlite3_column_int64(stmt, 3);
    CrdtNodeId node_id = sqlite3_column_int64(stmt, 4);
    uint64_t local_db_version = sqlite3_column_int64(stmt, 5);

    std::optional<std::string> value;
    int num_cols = sqlite3_column_count(stmt);
    for (int i = 7; i < num_cols; i++) {
      const char *name = sqlite3_column_name(stmt, i);
      if (name && col_name == name) {  // FIX: Compare string contents, not pointers
        sqlite3_value *val = sqlite3_column_value(stmt, i);
        SQLiteValue sql_val = SQLiteValue::from_sqlite(val);
        value = sql_val.to_string();
        break;
      }
    }

    changes.emplace_back(record_id, std::move(col_name), std::move(value),
                        col_version, db_version, node_id, local_db_version);
  }
  sqlite3_finalize(stmt);

  // Query tombstones
  std::string tombstones_table = "_crdt_" + tracked_table_ + "_tombstones";
  std::string tomb_query = "SELECT record_id, db_version, node_id, local_db_version "
                          "FROM " + tombstones_table + " "
                          "WHERE local_db_version > ?";
  if (!excluded_nodes.empty()) {
    tomb_query += " AND node_id NOT IN (" + excluded_nodes + ")";
  }

  stmt = prepare(tomb_query.c_str());
  sqlite3_bind_int64(stmt, 1, last_db_version);

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    CrdtRecordId record_id = RecordIdTraits<CrdtRecordId>::from_sqlite(stmt, 0);
    uint64_t db_version = sqlite3_column_int64(stmt, 1);
    CrdtNodeId node_id = sqlite3_column_int64(stmt, 2);
    uint64_t local_db_version = sqlite3_column_int64(stmt, 3);

    changes.emplace_back(record_id, std::nullopt, std::nullopt,
                        1, db_version, node_id, local_db_version);
  }
  sqlite3_finalize(stmt);

  return changes;
}

std::vector<Change<CrdtRecordId, std::string>>
CRDTSQLite::merge_changes(std::vector<Change<CrdtRecordId, std::string>> changes) {
  std::vector<Change<CrdtRecordId, std::string>> accepted;

  std::string versions_table = "_crdt_" + tracked_table_ + "_versions";
  std::string tombstones_table = "_crdt_" + tracked_table_ + "_tombstones";

  // Get current clock
  uint64_t current_clock = get_clock();

  for (auto& remote : changes) {
    // Update clock (always, even for rejected changes - maintains causality)
    current_clock = std::max(current_clock, remote.db_version);

    // Check if record is tombstoned
    std::string tomb_check = "SELECT db_version, node_id FROM " + tombstones_table + " WHERE record_id = ?";
    sqlite3_stmt *stmt = prepare(tomb_check.c_str());
    RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, remote.record_id);

    bool is_tombstoned = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      uint64_t tomb_db_version = sqlite3_column_int64(stmt, 0);
      CrdtNodeId tomb_node_id = sqlite3_column_int64(stmt, 1);

      // Tombstone always wins (db_version comparison)
      if (tomb_db_version > remote.db_version ||
          (tomb_db_version == remote.db_version && tomb_node_id >= remote.node_id)) {
        is_tombstoned = true;
      }
    }
    sqlite3_finalize(stmt);

    if (!remote.col_name.has_value()) {
      // This is a delete (tombstone)
      if (!is_tombstoned) {
        // Accept tombstone
        std::string insert_tomb = "INSERT OR REPLACE INTO " + tombstones_table +
                                 " (record_id, db_version, node_id, local_db_version) VALUES (?, ?, ?, ?)";
        stmt = prepare(insert_tomb.c_str());
        RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, remote.record_id);
        sqlite3_bind_int64(stmt, 2, remote.db_version);
        sqlite3_bind_int64(stmt, 3, remote.node_id);
        sqlite3_bind_int64(stmt, 4, current_clock);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);

        // Delete from main table
        apply_to_sqlite({remote});

        accepted.push_back(remote);
      }
    } else {
      // Regular field change
      if (is_tombstoned) {
        // Reject - record is tombstoned
        continue;
      }

      // Query current version from shadow table
      std::string version_check = "SELECT col_version, db_version, node_id FROM " +
                                 versions_table + " WHERE record_id = ? AND col_name = ?";
      stmt = prepare(version_check.c_str());
      RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, remote.record_id);
      sqlite3_bind_text(stmt, 2, remote.col_name->c_str(), -1, SQLITE_TRANSIENT);

      bool remote_wins = false;
      if (sqlite3_step(stmt) == SQLITE_ROW) {
        // Existing version - do LWW comparison
        uint64_t local_col_version = sqlite3_column_int64(stmt, 0);
        uint64_t local_db_version = sqlite3_column_int64(stmt, 1);
        CrdtNodeId local_node_id = sqlite3_column_int64(stmt, 2);

        // LWW: compare col_version, then db_version, then node_id
        if (remote.col_version > local_col_version) {
          remote_wins = true;
        } else if (remote.col_version == local_col_version) {
          if (remote.db_version > local_db_version) {
            remote_wins = true;
          } else if (remote.db_version == local_db_version && remote.node_id > local_node_id) {
            remote_wins = true;
          }
        }
      } else {
        // No existing version - accept remote
        remote_wins = true;
      }
      sqlite3_finalize(stmt);

      if (remote_wins) {
        // Update shadow table
        std::string update_version = "INSERT OR REPLACE INTO " + versions_table +
                                    " (record_id, col_name, col_version, db_version, node_id, local_db_version) " +
                                    "VALUES (?, ?, ?, ?, ?, ?)";
        stmt = prepare(update_version.c_str());
        RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, remote.record_id);
        sqlite3_bind_text(stmt, 2, remote.col_name->c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 3, remote.col_version);
        sqlite3_bind_int64(stmt, 4, remote.db_version);
        sqlite3_bind_int64(stmt, 5, remote.node_id);
        sqlite3_bind_int64(stmt, 6, current_clock);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);

        // Update main table
        apply_to_sqlite({remote});

        accepted.push_back(remote);
      }
    }
  }

  // Increment and update clock
  current_clock++;
  std::string clock_table = "_crdt_" + tracked_table_ + "_clock";
  std::string update_clock = "UPDATE " + clock_table + " SET time = ?";
  sqlite3_stmt *stmt = prepare(update_clock.c_str());
  sqlite3_bind_int64(stmt, 1, current_clock);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  return accepted;
}

size_t CRDTSQLite::compact_tombstones(uint64_t min_acknowledged_version) {
  // Remove tombstones from shadow table
  std::string tombstones_table = "_crdt_" + tracked_table_ + "_tombstones";
  std::string delete_sql = "DELETE FROM " + tombstones_table +
                          " WHERE db_version < ?";
  sqlite3_stmt *stmt = prepare(delete_sql.c_str());
  sqlite3_bind_int64(stmt, 1, min_acknowledged_version);
  sqlite3_step(stmt);
  int removed = sqlite3_changes(db_);
  sqlite3_finalize(stmt);

  return removed;
}

size_t CRDTSQLite::tombstone_count() const {
  if (tracked_table_.empty()) {
    return 0;  // No table tracked yet
  }

  std::string tombstones_table = "_crdt_" + tracked_table_ + "_tombstones";
  std::string count_sql = "SELECT COUNT(*) FROM " + tombstones_table;
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db_, count_sql.c_str(), -1, &stmt, nullptr);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    int count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return count;
  }
  sqlite3_finalize(stmt);
  return 0;
}

uint64_t CRDTSQLite::get_clock() const {
  if (tracked_table_.empty()) {
    return 0;  // No table tracked yet
  }

  std::string clock_table = "_crdt_" + tracked_table_ + "_clock";
  std::string query = "SELECT time FROM " + clock_table;
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db_, query.c_str(), -1, &stmt, nullptr);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    uint64_t time = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    return time;
  }
  sqlite3_finalize(stmt);
  return 0;
}

void CRDTSQLite::track_change(int operation, const char *table, CrdtRecordId record_id) {
  if (!table || tracked_table_ != table) {
    return;
  }

  PendingChange change;
  change.operation = operation;

  // For non-auto-increment types (e.g., uint128_t), we need to get the actual ID
  if constexpr (!RecordIdTraits<CrdtRecordId>::is_auto_increment()) {
    if (operation == SQLITE_DELETE) {
      // For DELETE: query lookaside table to get CRDT ID
      // (main table row is already deleted, but lookaside persists the mapping)
      std::string lookaside_table = "_crdt_" + tracked_table_ + "_lookaside";
      std::string query = "SELECT id FROM " + lookaside_table + " WHERE rowid = ?";
      sqlite3_stmt *stmt = prepare(query.c_str());
      sqlite3_bind_int64(stmt, 1, static_cast<int64_t>(record_id));  // rowid

      if (sqlite3_step(stmt) == SQLITE_ROW) {
        change.record_id = RecordIdTraits<CrdtRecordId>::from_sqlite(stmt, 0);
        sqlite3_finalize(stmt);
      } else {
        // No lookaside entry - record was never inserted with proper ID
        sqlite3_finalize(stmt);
        return;
      }
      // NOTE: We DON'T delete from lookaside - keep the mapping for sync
    } else {
      // For INSERT/UPDATE: query the id column using the rowid
      std::string query = "SELECT id FROM " + tracked_table_ + " WHERE rowid = ?";
      sqlite3_stmt *stmt = prepare(query.c_str());
      sqlite3_bind_int64(stmt, 1, static_cast<int64_t>(record_id));  // rowid is always int64

      if (sqlite3_step(stmt) == SQLITE_ROW) {
        change.record_id = RecordIdTraits<CrdtRecordId>::from_sqlite(stmt, 0);
        sqlite3_finalize(stmt);

        // Populate lookaside table: rowid → CRDT ID
        std::string lookaside_table = "_crdt_" + tracked_table_ + "_lookaside";
        std::string insert_lookaside = "INSERT OR REPLACE INTO " + lookaside_table +
                                       " (rowid, id) VALUES (?, ?)";
        stmt = prepare(insert_lookaside.c_str());
        sqlite3_bind_int64(stmt, 1, static_cast<int64_t>(record_id));  // rowid
        RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 2, change.record_id);  // CRDT ID
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
      } else {
        // Record not found or doesn't have id column - skip
        sqlite3_finalize(stmt);
        return;
      }
    }
  } else {
    // For int64_t, rowid IS the record_id
    change.record_id = record_id;
  }

  if (operation == SQLITE_DELETE) {
    // For deletes, we don't need values
  } else {
    // For inserts and updates, query current values
    change.values = query_row_values(change.record_id);
  }

  pending_changes_.push_back(std::move(change));
}

std::unordered_map<std::string, SQLiteValue>
CRDTSQLite::query_row_values(CrdtRecordId record_id) {
  std::unordered_map<std::string, SQLiteValue> values;

  // For uint128_t, query by id column; for int64_t, query by rowid
  std::string query;
  if constexpr (RecordIdTraits<CrdtRecordId>::is_auto_increment()) {
    query = "SELECT * FROM " + tracked_table_ + " WHERE rowid = ?";
  } else {
    query = "SELECT * FROM " + tracked_table_ + " WHERE id = ?";
  }

  sqlite3_stmt *stmt = prepare(query.c_str());
  RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, record_id);

  if (sqlite3_step(stmt) == SQLITE_ROW) {
    int num_cols = sqlite3_column_count(stmt);
    for (int i = 0; i < num_cols; i++) {
      const char *col_name = sqlite3_column_name(stmt, i);
      if (!col_name) continue;

      sqlite3_value *val = sqlite3_column_value(stmt, i);
      values[col_name] = SQLiteValue::from_sqlite(val);
    }
  }

  sqlite3_finalize(stmt);
  return values;
}

void CRDTSQLite::flush_changes() {
  if (pending_changes_.empty() || flushing_changes_) {
    return;  // Prevent re-entry
  }

  flushing_changes_ = true;

  std::string versions_table = "_crdt_" + tracked_table_ + "_versions";
  std::string tombstones_table = "_crdt_" + tracked_table_ + "_tombstones";

  // Get and increment clock
  uint64_t current_clock = get_clock();
  current_clock++;

  // Process each pending change
  for (const auto &change : pending_changes_) {
    if (change.operation == SQLITE_DELETE) {
      // Create tombstone
      std::string insert_tomb = "INSERT OR REPLACE INTO " + tombstones_table +
                               " (record_id, db_version, node_id, local_db_version) VALUES (?, ?, ?, ?)";
      sqlite3_stmt *stmt = prepare(insert_tomb.c_str());
      RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, change.record_id);
      sqlite3_bind_int64(stmt, 2, current_clock);
      sqlite3_bind_int64(stmt, 3, node_id_);
      sqlite3_bind_int64(stmt, 4, current_clock);
      sqlite3_step(stmt);
      sqlite3_finalize(stmt);

      // Remove all column versions for this record
      std::string delete_versions = "DELETE FROM " + versions_table + " WHERE record_id = ?";
      stmt = prepare(delete_versions.c_str());
      RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, change.record_id);
      sqlite3_step(stmt);
      sqlite3_finalize(stmt);

    } else {
      // Handle insert/update - update each column
      for (const auto &[col_name, value] : change.values) {
        // Query current col_version for this column
        std::string query = "SELECT col_version FROM " + versions_table +
                           " WHERE record_id = ? AND col_name = ?";
        sqlite3_stmt *stmt = prepare(query.c_str());
        RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, change.record_id);
        sqlite3_bind_text(stmt, 2, col_name.c_str(), -1, SQLITE_TRANSIENT);

        uint64_t col_version = 0;
        if (sqlite3_step(stmt) == SQLITE_ROW) {
          col_version = sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);

        // Increment col_version
        col_version++;

        // Update shadow table
        std::string insert_version = "INSERT OR REPLACE INTO " + versions_table +
                                    " (record_id, col_name, col_version, db_version, node_id, local_db_version) " +
                                    "VALUES (?, ?, ?, ?, ?, ?)";
        stmt = prepare(insert_version.c_str());
        RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, change.record_id);
        sqlite3_bind_text(stmt, 2, col_name.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 3, col_version);
        sqlite3_bind_int64(stmt, 4, current_clock);
        sqlite3_bind_int64(stmt, 5, node_id_);
        sqlite3_bind_int64(stmt, 6, current_clock);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
      }
    }
  }

  // Update clock in database
  std::string clock_table = "_crdt_" + tracked_table_ + "_clock";
  std::string update_clock = "UPDATE " + clock_table + " SET time = ?";
  sqlite3_stmt *stmt = prepare(update_clock.c_str());
  sqlite3_bind_int64(stmt, 1, current_clock);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  pending_changes_.clear();
  flushing_changes_ = false;
}

void CRDTSQLite::apply_to_sqlite(const std::vector<Change<CrdtRecordId, std::string>> &changes) {
  // Temporarily disable hooks to avoid recursion
  sqlite3_update_hook(db_, nullptr, nullptr);

  // Determine which column to use for WHERE clauses
  std::string id_column;
  if constexpr (RecordIdTraits<CrdtRecordId>::is_auto_increment()) {
    id_column = "rowid";
  } else {
    id_column = "id";
  }

  for (const auto &change : changes) {
    if (!change.col_name.has_value()) {
      // Delete record
      std::string delete_sql = "DELETE FROM " + tracked_table_ + " WHERE " + id_column + " = ?";
      sqlite3_stmt *stmt = prepare(delete_sql.c_str());
      RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, change.record_id);
      sqlite3_step(stmt);
      sqlite3_finalize(stmt);
    } else {
      // Check if record exists
      std::string check_sql = "SELECT COUNT(*) FROM " + tracked_table_ + " WHERE " + id_column + " = ?";
      sqlite3_stmt *stmt = prepare(check_sql.c_str());
      RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, change.record_id);
      sqlite3_step(stmt);
      int count = sqlite3_column_int(stmt, 0);
      sqlite3_finalize(stmt);

      if (!change.value.has_value()) {
        // Field deletion - set to NULL or remove
        if (count > 0) {
          std::string update_sql = "UPDATE " + tracked_table_ + " SET " +
                                  *change.col_name + " = NULL WHERE " + id_column + " = ?";
          stmt = prepare(update_sql.c_str());
          RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, change.record_id);
          sqlite3_step(stmt);
          sqlite3_finalize(stmt);
        }
      } else {
        // Insert or update
        std::string value_str = *change.value;

        // Get column type
        auto type_it = column_types_.find(*change.col_name);
        SQLiteValue::Type col_type = (type_it != column_types_.end()) ?
                                     type_it->second : SQLiteValue::TEXT;

        SQLiteValue value = SQLiteValue::from_string(value_str, col_type);

        if (count == 0) {
          // Insert new record
          std::string insert_sql = "INSERT INTO " + tracked_table_ +
                                  " (" + id_column + ", " + *change.col_name + ") VALUES (?, ?)";
          stmt = prepare(insert_sql.c_str());
          RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 1, change.record_id);

          switch (value.type) {
          case SQLiteValue::NULL_TYPE:
            sqlite3_bind_null(stmt, 2);
            break;
          case SQLiteValue::INTEGER:
            sqlite3_bind_int64(stmt, 2, value.int_val);
            break;
          case SQLiteValue::REAL:
            sqlite3_bind_double(stmt, 2, value.real_val);
            break;
          case SQLiteValue::TEXT:
            sqlite3_bind_text(stmt, 2, value.text_val.c_str(), -1, SQLITE_TRANSIENT);
            break;
          case SQLiteValue::BLOB:
            sqlite3_bind_blob(stmt, 2, value.blob_val.data(), value.blob_val.size(), SQLITE_TRANSIENT);
            break;
          }

          sqlite3_step(stmt);
          sqlite3_finalize(stmt);
        } else {
          // Update existing record
          std::string update_sql = "UPDATE " + tracked_table_ + " SET " +
                                  *change.col_name + " = ? WHERE " + id_column + " = ?";
          stmt = prepare(update_sql.c_str());

          switch (value.type) {
          case SQLiteValue::NULL_TYPE:
            sqlite3_bind_null(stmt, 1);
            break;
          case SQLiteValue::INTEGER:
            sqlite3_bind_int64(stmt, 1, value.int_val);
            break;
          case SQLiteValue::REAL:
            sqlite3_bind_double(stmt, 1, value.real_val);
            break;
          case SQLiteValue::TEXT:
            sqlite3_bind_text(stmt, 1, value.text_val.c_str(), -1, SQLITE_TRANSIENT);
            break;
          case SQLiteValue::BLOB:
            sqlite3_bind_blob(stmt, 1, value.blob_val.data(), value.blob_val.size(), SQLITE_TRANSIENT);
            break;
          }

          RecordIdTraits<CrdtRecordId>::bind_to_sqlite(stmt, 2, change.record_id);
          sqlite3_step(stmt);
          sqlite3_finalize(stmt);
        }
      }
    }
  }

  // Re-enable hooks
  sqlite3_update_hook(db_, update_callback, this);
}

int CRDTSQLite::authorizer_callback(void *ctx, int action_code,
                                    const char *arg1, const char *arg2,
                                    const char *arg3, const char *arg4) {
  auto *self = static_cast<CRDTSQLite *>(ctx);

  // Only monitor operations on CRDT-enabled table
  if (self->tracked_table_.empty()) {
    return SQLITE_OK;  // No table being tracked yet
  }

  // SQLITE_ALTER_TABLE: arg1=database, arg2=table
  if (action_code == SQLITE_ALTER_TABLE) {
    if (arg2 && self->tracked_table_ == arg2) {
      // Flag that schema needs refresh after this statement executes
      // This handles ADD COLUMN automatically
      self->pending_schema_refresh_ = true;
    }
    return SQLITE_OK;  // Allow ALTER TABLE (ADD COLUMN supported)
  }

  // SQLITE_UPDATE on sqlite_master for RENAME TABLE detection
  // When renaming table, SQLite updates sqlite_master.tbl_name
  // ONLY block if it's the tracked table being renamed
  if (action_code == SQLITE_UPDATE && arg1 && std::string(arg1) == "sqlite_master") {
    if (arg2 && std::string(arg2) == "tbl_name") {
      // Check if this is renaming our tracked table
      // arg3 is the trigger/view name (null for table), arg4 is unused
      // We can't easily determine which table is being renamed here
      // So we'll be conservative and only block if we detect issues
      // For now, allow but add a note that RENAME is not supported
      return SQLITE_OK;  // Allow for now (RENAME not well supported anyway)
    }
  }

  return SQLITE_OK;  // Allow by default
}

void CRDTSQLite::update_callback(void *ctx, int operation,
                                 const char *db_name, const char *table,
                                 sqlite3_int64 rowid) {
  auto *self = static_cast<CRDTSQLite *>(ctx);
  self->track_change(operation, table, static_cast<int64_t>(rowid));
}

int CRDTSQLite::commit_callback(void *ctx) {
  auto *self = static_cast<CRDTSQLite *>(ctx);
  try {
    self->flush_changes();
    return 0; // Allow commit
  } catch (const std::exception &e) {
    // If flush fails, abort the transaction to maintain consistency
    return 1; // Abort commit
  }
}

void CRDTSQLite::rollback_callback(void *ctx) {
  auto *self = static_cast<CRDTSQLite *>(ctx);
  self->pending_changes_.clear();
}

void CRDTSQLite::exec_or_throw(const char *sql) {
  char *err_msg = nullptr;
  int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &err_msg);
  if (rc != SQLITE_OK) {
    std::string error = "SQL execution failed: ";
    if (err_msg) {
      error += err_msg;
      sqlite3_free(err_msg);
    }
    throw CRDTSQLiteException(error);
  }
}

std::string CRDTSQLite::get_error() const {
  return sqlite3_errmsg(db_);
}

bool CRDTSQLite::is_valid_table_name(const std::string &name) {
  // Table name must:
  // 1. Not be empty
  // 2. Start with letter or underscore
  // 3. Contain only alphanumeric characters and underscores
  // 4. Not be too long (SQLite limit is 1024 bytes, we use 128 for safety)
  if (name.empty() || name.length() > 128) {
    return false;
  }

  // First character must be letter or underscore
  char first = name[0];
  if (!((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_')) {
    return false;
  }

  // Remaining characters must be alphanumeric or underscore
  for (char c : name) {
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') || c == '_')) {
      return false;
    }
  }

  return true;
}

// NOTE: JSON serialization removed from public API due to incomplete implementation
// Users should implement their own serialization based on the Change<> structure
//
// Example incomplete implementation (DO NOT USE):
//
// std::string CRDTSQLite::changes_to_json(...) {
//   // Incomplete - missing proper escaping, uint128_t handling, etc.
// }
//
// std::vector<Change<...>> CRDTSQLite::changes_from_json(...) {
//   // Incomplete - always returns empty vector
//   return {};
// }
