#include <sqlite3.h>
#include <iostream>

void print_query(sqlite3 *db, const char *query) {
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, query, -1, &stmt, nullptr);
  
  int cols = sqlite3_column_count(stmt);
  
  // Print headers
  for (int i = 0; i < cols; i++) {
    std::cout << sqlite3_column_name(stmt, i) << "\t";
  }
  std::cout << "\n";
  
  // Print rows
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    for (int i = 0; i < cols; i++) {
      const char *val = (const char*)sqlite3_column_text(stmt, i);
      std::cout << (val ? val : "NULL") << "\t";
    }
    std::cout << "\n";
  }
  
  sqlite3_finalize(stmt);
}

int main() {
  sqlite3 *db;
  sqlite3_open("test_debug.db", &db);
  
  std::cout << "=== users table ===" << std::endl;
  print_query(db, "SELECT * FROM users");
  
  std::cout << "\n=== _crdt_users_versions ===" << std::endl;
  print_query(db, "SELECT * FROM _crdt_users_versions");
  
  std::cout << "\n=== _crdt_users_clock ===" << std::endl;
  print_query(db, "SELECT * FROM _crdt_users_clock");
  
  sqlite3_close(db);
  return 0;
}
