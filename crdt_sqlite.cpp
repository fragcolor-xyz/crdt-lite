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
    : db_(nullptr), crdt_(node_id, nullptr), in_transaction_(false) {
  int rc = sqlite3_open(path, &db_);
  if (rc != SQLITE_OK) {
    std::string error = "Failed to open database: " + std::string(sqlite3_errmsg(db_));
    sqlite3_close(db_);
    throw CRDTSQLiteException(error);
  }

  // Enable foreign keys
  exec_or_throw("PRAGMA foreign_keys = ON");

  // Install hooks
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

  // Check if table exists
  std::string check_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='" + table_name + "'";
  sqlite3_stmt *stmt;
  int rc = sqlite3_prepare_v2(db_, check_sql.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    throw CRDTSQLiteException("Failed to check table existence: " + get_error());
  }

  bool exists = (sqlite3_step(stmt) == SQLITE_ROW);
  sqlite3_finalize(stmt);

  if (!exists) {
    throw CRDTSQLiteException("Table does not exist: " + table_name);
  }

  tracked_table_ = table_name;
  cache_column_types();
  create_shadow_tables(table_name);
  load_crdt_state(table_name);
}

void CRDTSQLite::create_shadow_tables(const std::string &table_name) {
  // Create versions table
  std::string versions_table = "_crdt_" + table_name + "_versions";
  std::string create_versions = R"(
    CREATE TABLE IF NOT EXISTS )" + versions_table + R"( (
      record_id INTEGER NOT NULL,
      col_name TEXT NOT NULL,
      col_version INTEGER NOT NULL,
      db_version INTEGER NOT NULL,
      node_id INTEGER NOT NULL,
      local_db_version INTEGER NOT NULL,
      PRIMARY KEY (record_id, col_name)
    )
  )";
  exec_or_throw(create_versions.c_str());

  // Create tombstones table
  std::string tombstones_table = "_crdt_" + table_name + "_tombstones";
  std::string create_tombstones = R"(
    CREATE TABLE IF NOT EXISTS )" + tombstones_table + R"( (
      record_id INTEGER PRIMARY KEY,
      db_version INTEGER NOT NULL,
      node_id INTEGER NOT NULL,
      local_db_version INTEGER NOT NULL
    )
  )";
  exec_or_throw(create_tombstones.c_str());

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
}

void CRDTSQLite::load_crdt_state(const std::string &table_name) {
  // Load all changes from shadow tables to reconstruct CRDT state
  std::vector<Change<int64_t, std::string>> changes;

  // Load regular changes
  std::string versions_table = "_crdt_" + table_name + "_versions";
  std::string load_versions = std::string("SELECT v.record_id, v.col_name, v.col_version, v.db_version, ") +
                             "v.node_id, v.local_db_version, t.* " +
                             " FROM " + versions_table + " v" +
                             " LEFT JOIN " + table_name + " t ON v.record_id = t.rowid";

  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db_, load_versions.c_str(), -1, &stmt, nullptr);

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    int64_t record_id = sqlite3_column_int64(stmt, 0);
    std::string col_name = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1));
    uint64_t col_version = sqlite3_column_int64(stmt, 2);
    uint64_t db_version = sqlite3_column_int64(stmt, 3);
    CrdtNodeId node_id = sqlite3_column_int64(stmt, 4);
    uint64_t local_db_version = sqlite3_column_int64(stmt, 5);

    // Find the column value in the main table
    std::optional<std::string> value;
    int num_cols = sqlite3_column_count(stmt);
    for (int i = 6; i < num_cols; i++) {
      const char *name = sqlite3_column_name(stmt, i);
      if (name && name == col_name) {
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

  // Load tombstones
  std::string tombstones_table = "_crdt_" + table_name + "_tombstones";
  std::string load_tombstones = "SELECT record_id, db_version, node_id, local_db_version FROM " +
                                tombstones_table;

  sqlite3_prepare_v2(db_, load_tombstones.c_str(), -1, &stmt, nullptr);

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    int64_t record_id = sqlite3_column_int64(stmt, 0);
    uint64_t db_version = sqlite3_column_int64(stmt, 1);
    CrdtNodeId node_id = sqlite3_column_int64(stmt, 2);
    uint64_t local_db_version = sqlite3_column_int64(stmt, 3);

    changes.emplace_back(record_id, std::nullopt, std::nullopt,
                        1, db_version, node_id, local_db_version);
  }
  sqlite3_finalize(stmt);

  // Reset CRDT with loaded changes
  crdt_.reset(std::move(changes));
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
}

sqlite3_stmt *CRDTSQLite::prepare(const char *sql) {
  sqlite3_stmt *stmt;
  int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    throw CRDTSQLiteException("Failed to prepare statement: " + get_error());
  }
  return stmt;
}

std::vector<Change<int64_t, std::string>>
CRDTSQLite::get_changes_since(uint64_t last_db_version) {
  return crdt_.get_changes_since(last_db_version);
}

std::vector<Change<int64_t, std::string>>
CRDTSQLite::get_changes_since_excluding(uint64_t last_db_version,
                                       const CrdtSet<CrdtNodeId> &excluding) {
  return crdt_.get_changes_since(last_db_version, excluding);
}

std::vector<Change<int64_t, std::string>>
CRDTSQLite::merge_changes(std::vector<Change<int64_t, std::string>> changes) {
  DefaultMergeRule<int64_t, std::string> rule;
  auto accepted = crdt_.merge_changes<true>(std::move(changes), false, rule);

  // Apply accepted changes to SQLite
  apply_to_sqlite(accepted);

  // Update shadow tables
  for (const auto &change : accepted) {
    update_shadow_tables(change);
  }

  // Update clock in database
  std::string clock_table = "_crdt_" + tracked_table_ + "_clock";
  std::string update_clock = "UPDATE " + clock_table + " SET time = ?";
  sqlite3_stmt *stmt = prepare(update_clock.c_str());
  sqlite3_bind_int64(stmt, 1, crdt_.get_clock().current_time());
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  return accepted;
}

size_t CRDTSQLite::compact_tombstones(uint64_t min_acknowledged_version) {
  size_t removed = crdt_.compact_tombstones(min_acknowledged_version);

  // Also remove from shadow table
  std::string tombstones_table = "_crdt_" + tracked_table_ + "_tombstones";
  std::string delete_sql = "DELETE FROM " + tombstones_table +
                          " WHERE db_version < ?";
  sqlite3_stmt *stmt = prepare(delete_sql.c_str());
  sqlite3_bind_int64(stmt, 1, min_acknowledged_version);
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  return removed;
}

size_t CRDTSQLite::tombstone_count() const {
  return crdt_.tombstone_count();
}

uint64_t CRDTSQLite::get_clock() const {
  return crdt_.get_clock().current_time();
}

void CRDTSQLite::track_change(int operation, const char *table, int64_t rowid) {
  if (table != tracked_table_) {
    return;
  }

  PendingChange change;
  change.operation = operation;
  change.rowid = rowid;

  if (operation == SQLITE_DELETE) {
    // For deletes, we don't need values
  } else {
    // For inserts and updates, query current values
    change.values = query_row_values(rowid);
  }

  pending_changes_.push_back(std::move(change));
}

std::unordered_map<std::string, SQLiteValue>
CRDTSQLite::query_row_values(int64_t rowid) {
  std::unordered_map<std::string, SQLiteValue> values;

  std::string query = "SELECT * FROM " + tracked_table_ + " WHERE rowid = ?";
  sqlite3_stmt *stmt = prepare(query.c_str());
  sqlite3_bind_int64(stmt, 1, rowid);

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
  if (pending_changes_.empty()) {
    return;
  }

  // Process each pending change
  for (const auto &change : pending_changes_) {
    if (change.operation == SQLITE_DELETE) {
      // Handle delete
      std::vector<Change<int64_t, std::string>> delete_changes;
      crdt_.delete_record(change.rowid, delete_changes);
      for (const auto &crdt_change : delete_changes) {
        update_shadow_tables(crdt_change);
      }
    } else {
      // Handle insert/update
      std::vector<std::pair<std::string, std::string>> fields;
      for (const auto &[col_name, value] : change.values) {
        fields.emplace_back(col_name, value.to_string());
      }

      std::vector<Change<int64_t, std::string>> changes;
      crdt_.insert_or_update_from_container(change.rowid, std::move(fields), changes);

      for (const auto &crdt_change : changes) {
        update_shadow_tables(crdt_change);
      }
    }
  }

  // Update clock in database
  std::string clock_table = "_crdt_" + tracked_table_ + "_clock";
  std::string update_clock = "UPDATE " + clock_table + " SET time = ?";
  sqlite3_stmt *stmt = prepare(update_clock.c_str());
  sqlite3_bind_int64(stmt, 1, crdt_.get_clock().current_time());
  sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  pending_changes_.clear();
}

void CRDTSQLite::apply_to_sqlite(const std::vector<Change<int64_t, std::string>> &changes) {
  // Temporarily disable hooks to avoid recursion
  sqlite3_update_hook(db_, nullptr, nullptr);

  for (const auto &change : changes) {
    if (!change.col_name.has_value()) {
      // Delete record
      std::string delete_sql = "DELETE FROM " + tracked_table_ + " WHERE rowid = ?";
      sqlite3_stmt *stmt = prepare(delete_sql.c_str());
      sqlite3_bind_int64(stmt, 1, change.record_id);
      sqlite3_step(stmt);
      sqlite3_finalize(stmt);
    } else {
      // Check if record exists
      std::string check_sql = "SELECT COUNT(*) FROM " + tracked_table_ + " WHERE rowid = ?";
      sqlite3_stmt *stmt = prepare(check_sql.c_str());
      sqlite3_bind_int64(stmt, 1, change.record_id);
      sqlite3_step(stmt);
      int count = sqlite3_column_int(stmt, 0);
      sqlite3_finalize(stmt);

      if (!change.value.has_value()) {
        // Field deletion - set to NULL or remove
        if (count > 0) {
          std::string update_sql = "UPDATE " + tracked_table_ + " SET " +
                                  *change.col_name + " = NULL WHERE rowid = ?";
          stmt = prepare(update_sql.c_str());
          sqlite3_bind_int64(stmt, 1, change.record_id);
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
                                  " (rowid, " + *change.col_name + ") VALUES (?, ?)";
          stmt = prepare(insert_sql.c_str());
          sqlite3_bind_int64(stmt, 1, change.record_id);

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
                                  *change.col_name + " = ? WHERE rowid = ?";
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

          sqlite3_bind_int64(stmt, 2, change.record_id);
          sqlite3_step(stmt);
          sqlite3_finalize(stmt);
        }
      }
    }
  }

  // Re-enable hooks
  sqlite3_update_hook(db_, update_callback, this);
}

void CRDTSQLite::update_shadow_tables(const Change<int64_t, std::string> &change) {
  if (!change.col_name.has_value()) {
    // Tombstone - update tombstones table
    std::string tombstones_table = "_crdt_" + tracked_table_ + "_tombstones";
    std::string insert_sql = "INSERT OR REPLACE INTO " + tombstones_table +
                            " (record_id, db_version, node_id, local_db_version) "
                            "VALUES (?, ?, ?, ?)";
    sqlite3_stmt *stmt = prepare(insert_sql.c_str());
    sqlite3_bind_int64(stmt, 1, change.record_id);
    sqlite3_bind_int64(stmt, 2, change.db_version);
    sqlite3_bind_int64(stmt, 3, change.node_id);
    sqlite3_bind_int64(stmt, 4, change.local_db_version);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    // Remove from versions table
    std::string versions_table = "_crdt_" + tracked_table_ + "_versions";
    std::string delete_sql = "DELETE FROM " + versions_table + " WHERE record_id = ?";
    stmt = prepare(delete_sql.c_str());
    sqlite3_bind_int64(stmt, 1, change.record_id);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
  } else {
    // Regular change - update versions table
    std::string versions_table = "_crdt_" + tracked_table_ + "_versions";
    std::string insert_sql = "INSERT OR REPLACE INTO " + versions_table +
                            " (record_id, col_name, col_version, db_version, node_id, local_db_version) "
                            "VALUES (?, ?, ?, ?, ?, ?)";
    sqlite3_stmt *stmt = prepare(insert_sql.c_str());
    sqlite3_bind_int64(stmt, 1, change.record_id);
    sqlite3_bind_text(stmt, 2, change.col_name->c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 3, change.col_version);
    sqlite3_bind_int64(stmt, 4, change.db_version);
    sqlite3_bind_int64(stmt, 5, change.node_id);
    sqlite3_bind_int64(stmt, 6, change.local_db_version);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
  }
}

void CRDTSQLite::update_callback(void *ctx, int operation,
                                 const char *db_name, const char *table,
                                 sqlite3_int64 rowid) {
  auto *self = static_cast<CRDTSQLite *>(ctx);
  self->track_change(operation, table, static_cast<int64_t>(rowid));
}

int CRDTSQLite::commit_callback(void *ctx) {
  auto *self = static_cast<CRDTSQLite *>(ctx);
  self->flush_changes();
  return 0; // Allow commit
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

// Simple JSON serialization (for demo purposes - production should use a proper JSON library)
std::string CRDTSQLite::changes_to_json(const std::vector<Change<int64_t, std::string>> &changes) {
  std::ostringstream oss;
  oss << "[";
  bool first = true;
  for (const auto &change : changes) {
    if (!first) oss << ",";
    first = false;

    oss << "{\"record_id\":" << change.record_id << ",";

    if (change.col_name.has_value()) {
      oss << "\"col_name\":\"" << *change.col_name << "\",";
    } else {
      oss << "\"col_name\":null,";
    }

    if (change.value.has_value()) {
      // Escape quotes in value
      std::string escaped = *change.value;
      size_t pos = 0;
      while ((pos = escaped.find('"', pos)) != std::string::npos) {
        escaped.insert(pos, "\\");
        pos += 2;
      }
      oss << "\"value\":\"" << escaped << "\",";
    } else {
      oss << "\"value\":null,";
    }

    oss << "\"col_version\":" << change.col_version << ",";
    oss << "\"db_version\":" << change.db_version << ",";
    oss << "\"node_id\":" << change.node_id << ",";
    oss << "\"local_db_version\":" << change.local_db_version << "}";
  }
  oss << "]";
  return oss.str();
}

std::vector<Change<int64_t, std::string>>
CRDTSQLite::changes_from_json(const std::string &json) {
  // Simple JSON parser for demo purposes
  // Production code should use a proper JSON library
  std::vector<Change<int64_t, std::string>> changes;

  // This is a very basic parser - just for demonstration
  // In production, use nlohmann/json or similar

  return changes;
}
