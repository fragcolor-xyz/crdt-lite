// test_uint128.cpp
// Demonstrates using 128-bit record IDs with the existing CRDT template

#include "crdt.hpp"
#include "record_id_types.hpp"
#include <iostream>
#include <cassert>

#ifdef __SIZEOF_INT128__

int main() {
  std::cout << "Testing CRDT with uint128_t record IDs..." << std::endl;

  // Create two nodes with uint128_t keys
  CRDT<uint128_t, std::string> node1(1, nullptr);
  CRDT<uint128_t, std::string> node2(2, nullptr);

  // Generate random 128-bit IDs
  uint128_t record1 = RecordIdTraits<uint128_t>::generate_with_node(1);
  uint128_t record2 = RecordIdTraits<uint128_t>::generate_with_node(2);

  std::cout << "Record 1 ID: " << RecordIdTraits<uint128_t>::to_string(record1) << std::endl;
  std::cout << "Record 2 ID: " << RecordIdTraits<uint128_t>::to_string(record2) << std::endl;

  // Insert on node 1
  std::vector<Change<uint128_t, std::string>> changes1;
  node1.insert_or_update(record1, changes1,
    std::make_pair(std::string("name"), std::string("Alice")),
    std::make_pair(std::string("email"), std::string("alice@example.com"))
  );

  std::cout << "Node 1 inserted " << changes1.size() << " changes" << std::endl;

  // Insert on node 2
  std::vector<Change<uint128_t, std::string>> changes2;
  node2.insert_or_update(record2, changes2,
    std::make_pair(std::string("name"), std::string("Bob")),
    std::make_pair(std::string("email"), std::string("bob@example.com"))
  );

  std::cout << "Node 2 inserted " << changes2.size() << " changes" << std::endl;

  // Sync: Node 1 -> Node 2
  DefaultMergeRule<uint128_t, std::string> rule;
  node2.merge_changes(std::move(changes1), false, rule);

  // Sync: Node 2 -> Node 1
  node1.merge_changes(std::move(changes2), false, rule);

  // Verify both nodes have both records
  assert(node1.get_record(record1, false) != nullptr);
  assert(node1.get_record(record2, false) != nullptr);
  assert(node2.get_record(record1, false) != nullptr);
  assert(node2.get_record(record2, false) != nullptr);

  std::cout << "✅ Sync successful - both nodes have both records" << std::endl;

  // Test collision resistance
  std::cout << "\nTesting collision resistance..." << std::endl;

  // Generate 10000 IDs from two different nodes
  std::unordered_set<uint128_t> ids;
  for (int i = 0; i < 5000; i++) {
    ids.insert(RecordIdTraits<uint128_t>::generate_with_node(1));
    ids.insert(RecordIdTraits<uint128_t>::generate_with_node(2));
  }

  // Should have exactly 10000 unique IDs (no collisions)
  assert(ids.size() == 10000);
  std::cout << "✅ Generated 10000 unique IDs with no collisions" << std::endl;

  // Test concurrent updates with same record ID (should converge)
  uint128_t shared_record = RecordIdTraits<uint128_t>::generate();

  CRDT<uint128_t, std::string> node3(3, nullptr);
  CRDT<uint128_t, std::string> node4(4, nullptr);

  std::vector<Change<uint128_t, std::string>> changes3;
  node3.insert_or_update(shared_record, changes3,
    std::make_pair(std::string("value"), std::string("From Node 3"))
  );

  std::vector<Change<uint128_t, std::string>> changes4;
  node4.insert_or_update(shared_record, changes4,
    std::make_pair(std::string("value"), std::string("From Node 4"))
  );

  // Bidirectional sync
  node3.merge_changes(std::move(changes4), false, rule);
  node4.merge_changes(std::move(changes3), false, rule);

  // Should converge to same value (node 4 wins due to higher node_id)
  auto *rec3 = node3.get_record(shared_record, false);
  auto *rec4 = node4.get_record(shared_record, false);

  assert(rec3 != nullptr && rec4 != nullptr);
  assert(rec3->fields.at("value") == rec4->fields.at("value"));
  assert(rec3->fields.at("value") == "From Node 4");

  std::cout << "✅ Concurrent updates converged correctly" << std::endl;

  std::cout << "\nAll uint128_t tests passed! ✅" << std::endl;

  return 0;
}

#else

int main() {
  std::cout << "128-bit integers not supported on this platform" << std::endl;
  return 0;
}

#endif
