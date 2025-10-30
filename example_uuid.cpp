// Example: Using UUIDs with CRDT-SQLite
#include "crdt.hpp"
#include <uuid/uuid.h>  // libuuid
#include <string>
#include <iostream>

// Helper to generate UUIDs
std::string generate_uuid() {
  uuid_t uuid;
  uuid_generate(uuid);

  char uuid_str[37];
  uuid_unparse(uuid, uuid_str);

  return std::string(uuid_str);
}

int main() {
  // CRDT with STRING keys (for UUIDs) instead of int64
  CRDT<std::string, std::string> crdt(1, nullptr);

  // Generate UUID for record ID
  std::string user_id = generate_uuid();  // e.g., "550e8400-e29b-41d4-a716-446655440000"

  std::cout << "Generated user ID: " << user_id << std::endl;

  // Insert with UUID
  std::vector<Change<std::string, std::string>> changes;
  crdt.insert_or_update(user_id, changes,
    std::make_pair("name", std::string("Alice")),
    std::make_pair("email", std::string("alice@example.com"))
  );

  // Sync with another node - UUID ensures no collisions!
  // Node 1 generates: "550e8400-..."
  // Node 2 generates: "7c9e6679-..."  <- Different, no collision ✅

  std::cout << "Changes created: " << changes.size() << std::endl;
  std::cout << "Record ID in change: " << changes[0].record_id << std::endl;

  return 0;
}
