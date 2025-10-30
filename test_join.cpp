#include <sqlite3.h>
#include <iostream>
#include <string>

int main() {
  sqlite3 *db;
  sqlite3_open("test_debug.db", &db);
  
  std::string table_name = "users";
  std::string versions_table = "_crdt_" + table_name + "_versions";
  std::string load_versions = "SELECT v.record_id, v.col_name, v.col_version, v.db_version, "
                             "v.node_id, v.local_db_version, t." + table_name + ".*" +
                             " FROM " + versions_table + " v" +
                             " LEFT JOIN " + table_name + " t ON v.record_id = t.rowid";
  
  std::cout << "Query: " << load_versions << std::endl << std::endl;
  
  sqlite3_stmt *stmt;
  int rc = sqlite3_prepare_v2(db, load_versions.c_str(), -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    std::cout << "Error: " << sqlite3_errmsg(db) << std::endl;
    return 1;
  }
  
  int cols = sqlite3_column_count(stmt);
  std::cout << "Columns: " << cols << std::endl;
  for (int i = 0; i < cols; i++) {
    std::cout << i << ": " << sqlite3_column_name(stmt, i) << std::endl;
  }
  
  std::cout << "\nRows:" << std::endl;
  int row_count = 0;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    row_count++;
    for (int i = 0; i < 6; i++) {  // Just the version columns
      std::cout << sqlite3_column_int64(stmt, i) << "\t";
    }
    std::cout << std::endl;
  }
  
  std::cout << "\nTotal rows: " << row_count << std::endl;
  
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  return 0;
}
