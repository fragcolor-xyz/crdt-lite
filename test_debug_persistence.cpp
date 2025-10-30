#include "crdt_sqlite.hpp"
#include <iostream>
#include <filesystem>

int main() {
  const char *db_path = "test_debug.db";
  
  // Remove if exists
  if (std::filesystem::exists(db_path)) {
    std::filesystem::remove(db_path);
  }

  uint64_t final_clock;

  // Scope 1: Create and populate
  {
    CRDTSQLite db(db_path, 1);
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    db.enable_crdt("users");

    std::cout << "After enable, clock: " << db.get_clock() << std::endl;

    db.execute("INSERT INTO users (name) VALUES ('Alice')");
    std::cout << "After Alice insert, clock: " << db.get_clock() << std::endl;

    db.execute("INSERT INTO users (name) VALUES ('Bob')");
    std::cout << "After Bob insert, clock: " << db.get_clock() << std::endl;

    final_clock = db.get_clock();
    std::cout << "Final clock in scope 1: " << final_clock << std::endl;
  }

  // Scope 2: Reopen
  {
    CRDTSQLite db(db_path, 1);
    std::cout << "After reopen, clock: " << db.get_clock() << std::endl;
    
    db.enable_crdt("users");
    std::cout << "After enable_crdt, clock: " << db.get_clock() << std::endl;

    auto changes = db.get_changes_since(0);
    std::cout << "Number of changes: " << changes.size() << std::endl;
    for (const auto &c : changes) {
      std::cout << "  Change: col_version=" << c.col_version 
                << " db_version=" << c.db_version
                << " local_db_version=" << c.local_db_version << std::endl;
    }

    std::cout << "Expected clock: " << final_clock << ", Got: " << db.get_clock() << std::endl;
  }

  return 0;
}
