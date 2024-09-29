// tests.cpp
#include "crdt.hpp"

#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>

// Helper function to generate unique IDs (simulating UUIDs)
CrdtString generate_uuid() {
  static uint64_t counter = 0;
  return "uuid-" + std::to_string(++counter);
}

/// Simple assertion helper
void assert_true(bool condition, const CrdtString &message) {
  if (!condition) {
    std::cerr << "Assertion failed: " << message << std::endl;
    exit(1);
  }
}

int main() {
  // Test Case: Basic Insert and Merge using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Node1 inserts a record
    CrdtString record_id = generate_uuid();
    CrdtString form_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields1 = {{"id", record_id},
                                               {"form_id", form_id},
                                               {"tag", "Node1Tag"},
                                               {"created_at", "2023-10-01T12:00:00Z"},
                                               {"created_by", "User1"}};
    auto changes1 = node1.insert_or_update(record_id, std::move(fields1));

    // Node2 inserts the same record with different data
    CrdtMap<CrdtString, CrdtString> fields2 = {{"id", record_id},
                                               {"form_id", form_id},
                                               {"tag", "Node2Tag"},
                                               {"created_at", "2023-10-01T12:05:00Z"},
                                               {"created_by", "User2"}};
    auto changes2 = node2.insert_or_update(record_id, std::move(fields2));

    // Merge node2's changes into node1
    node1.merge_changes(std::move(changes2));

    // Merge node1's changes into node2
    node2.merge_changes(std::move(changes1));

    // Both nodes should resolve the conflict and have the same data
    assert_true(node1.get_data() == node2.get_data(), "Basic Insert and Merge: Data mismatch");
    assert_true(node1.get_data().at(record_id).fields.at("tag") == "Node2Tag",
                "Basic Insert and Merge: Tag should be 'Node2Tag'");
    assert_true(node1.get_data().at(record_id).fields.at("created_by") == "User2",
                "Basic Insert and Merge: created_by should be 'User2'");
    std::cout << "Test 'Basic Insert and Merge' passed." << std::endl;
  }

  // Test Case: Updates with Conflicts using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Insert a shared record
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"id", record_id}, {"tag", "InitialTag"}};
    auto changes_init1 = node1.insert_or_update(record_id, std::move(fields));
    auto changes_init2 = node2.insert_or_update(record_id, std::move(fields));

    // Merge initial inserts
    node1.merge_changes(std::move(changes_init2));
    node2.merge_changes(std::move(changes_init1));

    // Node1 updates 'tag'
    CrdtMap<CrdtString, CrdtString> updates1 = {{"tag", "Node1UpdatedTag"}};
    auto change_update1 = node1.insert_or_update(record_id, std::move(updates1));

    // Node2 updates 'tag'
    CrdtMap<CrdtString, CrdtString> updates2 = {{"tag", "Node2UpdatedTag"}};
    auto change_update2 = node2.insert_or_update(record_id, std::move(updates2));

    // Merge changes
    node1.merge_changes(std::move(change_update2));
    node2.merge_changes(std::move(change_update1));

    // Conflict resolved based on site_id (Node2 has higher site_id)
    assert_true(node1.get_data().at(record_id).fields.at("tag") == "Node2UpdatedTag",
                "Updates with Conflicts: Tag resolution mismatch");
    assert_true(node1.get_data() == node2.get_data(), "Updates with Conflicts: Data mismatch");
    std::cout << "Test 'Updates with Conflicts' passed." << std::endl;
  }

  // Test Case: Delete and Merge using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Insert and sync a record
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"id", record_id}, {"tag", "ToBeDeleted"}};
    auto changes_init = node1.insert_or_update(record_id, std::move(fields));

    // Merge to node2
    node2.merge_changes(std::move(changes_init));

    // Node1 deletes the record
    auto changes_delete = node1.delete_record(record_id);

    // Merge the deletion to node2
    node2.merge_changes(std::move(changes_delete));

    // Both nodes should reflect the deletion
    assert_true(node1.get_data().at(record_id).fields.empty(), "Delete and Merge: Node1 should have empty fields");
    assert_true(node2.get_data().at(record_id).fields.empty(), "Delete and Merge: Node2 should have empty fields");
    assert_true(node1.get_data().at(record_id).column_versions.find("__deleted__") !=
                    node1.get_data().at(record_id).column_versions.end(),
                "Delete and Merge: Node1 should have '__deleted__' column version");
    assert_true(node2.get_data().at(record_id).column_versions.find("__deleted__") !=
                    node2.get_data().at(record_id).column_versions.end(),
                "Delete and Merge: Node2 should have '__deleted__' column version");
    std::cout << "Test 'Delete and Merge' passed." << std::endl;
  }

  // Test Case: Tombstone Handling using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Insert a record and delete it on node1
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"id", record_id}, {"tag", "Temporary"}};
    auto changes_insert = node1.insert_or_update(record_id, std::move(fields));
    auto changes_delete = node1.delete_record(record_id);

    // Merge changes to node2
    node2.merge_changes(std::move(changes_insert));
    node2.merge_changes(std::move(changes_delete));

    // Node2 tries to insert the same record
    auto changes_attempt_insert = node2.insert_or_update(record_id, std::move(fields));

    // Merge changes back to node1
    node1.merge_changes(std::move(changes_attempt_insert));

    // Node2 should respect the tombstone
    assert_true(node2.get_data().at(record_id).fields.empty(), "Tombstone Handling: Node2 should have empty fields");
    assert_true(node2.get_data().at(record_id).column_versions.find("__deleted__") !=
                    node2.get_data().at(record_id).column_versions.end(),
                "Tombstone Handling: Node2 should have '__deleted__' column version");
    std::cout << "Test 'Tombstone Handling' passed." << std::endl;
  }

  // Test Case: Conflict Resolution with site_id using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Both nodes insert a record with the same id
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields1 = {{"id", record_id}, {"tag", "Node1Tag"}};
    CrdtMap<CrdtString, CrdtString> fields2 = {{"id", record_id}, {"tag", "Node2Tag"}};
    auto changes1 = node1.insert_or_update(record_id, std::move(fields1));
    auto changes2 = node2.insert_or_update(record_id, std::move(fields2));

    // Merge changes
    node1.merge_changes(std::move(changes2));
    node2.merge_changes(std::move(changes1));

    // Both nodes update the 'tag' field multiple times
    CrdtMap<CrdtString, CrdtString> updates1 = {{"tag", "Node1Tag1"}};
    auto changes_update1 = node1.insert_or_update(record_id, std::move(updates1));

    updates1 = {{"tag", "Node1Tag2"}};
    auto changes_update2 = node1.insert_or_update(record_id, std::move(updates1));

    CrdtMap<CrdtString, CrdtString> updates2 = {{"tag", "Node2Tag1"}};
    auto changes_update3 = node2.insert_or_update(record_id, std::move(updates2));

    updates2 = {{"tag", "Node2Tag2"}};
    auto changes_update4 = node2.insert_or_update(record_id, std::move(updates2));

    // Merge changes
    node1.merge_changes(std::move(changes_update4));
    node2.merge_changes(std::move(changes_update2));
    node2.merge_changes(std::move(changes_update1));
    node1.merge_changes(std::move(changes_update3));

    // Since node2 has a higher site_id, its latest update should prevail
    CrdtString expected_tag = "Node2Tag2";

    assert_true(node1.get_data().at(record_id).fields.at("tag") == expected_tag, "Conflict Resolution: Tag resolution mismatch");
    assert_true(node1.get_data() == node2.get_data(), "Conflict Resolution: Data mismatch");
    std::cout << "Test 'Conflict Resolution with site_id' passed." << std::endl;
  }

  // Test Case: Logical Clock Update using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Node1 inserts a record
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"id", record_id}, {"tag", "Node1Tag"}};
    auto changes_insert = node1.insert_or_update(record_id, std::move(fields));

    // Node2 receives the change
    node2.merge_changes(std::move(changes_insert));

    // Node2's clock should be updated
    assert_true(node2.get_clock().current_time() > 0, "Logical Clock Update: Node2 clock should be greater than 0");
    assert_true(node2.get_clock().current_time() >= node1.get_clock().current_time(),
                "Logical Clock Update: Node2 clock should be >= Node1 clock");
    std::cout << "Test 'Logical Clock Update' passed." << std::endl;
  }

  // Test Case: Merge without Conflicts using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Node1 inserts a record
    CrdtString record_id1 = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields1 = {{"id", record_id1}, {"tag", "Node1Record"}};
    auto changes1 = node1.insert_or_update(record_id1, std::move(fields1));

    // Node2 inserts a different record
    CrdtString record_id2 = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields2 = {{"id", record_id2}, {"tag", "Node2Record"}};
    auto changes2 = node2.insert_or_update(record_id2, std::move(fields2));

    // Merge changes
    node1.merge_changes(std::move(changes2));
    node2.merge_changes(std::move(changes1));

    // Both nodes should have both records
    assert_true(node1.get_data().find(record_id1) != node1.get_data().end(),
                "Merge without Conflicts: Node1 should contain record_id1");
    assert_true(node1.get_data().find(record_id2) != node1.get_data().end(),
                "Merge without Conflicts: Node1 should contain record_id2");
    assert_true(node2.get_data().find(record_id1) != node2.get_data().end(),
                "Merge without Conflicts: Node2 should contain record_id1");
    assert_true(node2.get_data().find(record_id2) != node2.get_data().end(),
                "Merge without Conflicts: Node2 should contain record_id2");
    assert_true(node1.get_data() == node2.get_data(), "Merge without Conflicts: Data mismatch between Node1 and Node2");
    std::cout << "Test 'Merge without Conflicts' passed." << std::endl;
  }

  // Test Case: Multiple Merges using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Node1 inserts a record
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"id", record_id}, {"tag", "InitialTag"}};
    auto changes_init = node1.insert_or_update(record_id, std::move(fields));

    // Merge to node2
    node2.merge_changes(std::move(changes_init));

    // Node2 updates the record
    CrdtMap<CrdtString, CrdtString> updates2 = {{"tag", "UpdatedByNode2"}};
    auto changes_update2 = node2.insert_or_update(record_id, std::move(updates2));

    // Node1 updates the record
    CrdtMap<CrdtString, CrdtString> updates1 = {{"tag", "UpdatedByNode1"}};
    auto changes_update1 = node1.insert_or_update(record_id, std::move(updates1));

    // Merge changes
    node1.merge_changes(std::move(changes_update2));
    node2.merge_changes(std::move(changes_update1));

    // Since node2 has a higher site_id, its latest update should prevail
    CrdtString expected_tag = "UpdatedByNode2";

    assert_true(node1.get_data().at(record_id).fields.at("tag") == expected_tag, "Multiple Merges: Tag resolution mismatch");
    assert_true(node1.get_data() == node2.get_data(), "Multiple Merges: Data mismatch between Node1 and Node2");
    std::cout << "Test 'Multiple Merges' passed." << std::endl;
  }

  // Test Case: Inserting After Deletion using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Node1 inserts and deletes a record
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"id", record_id}, {"tag", "Temporary"}};
    auto changes_insert = node1.insert_or_update(record_id, std::move(fields));
    auto changes_delete = node1.delete_record(record_id);

    // Merge deletion to node2
    node2.merge_changes(std::move(changes_insert));
    node2.merge_changes(std::move(changes_delete));

    // Node2 tries to insert the same record
    auto changes_attempt_insert = node2.insert_or_update(record_id, std::move(fields));

    // Merge changes back to node1
    node1.merge_changes(std::move(changes_attempt_insert));

    // The deletion should prevail
    assert_true(node1.get_data().at(record_id).fields.empty(), "Inserting After Deletion: Node1 should have empty fields");
    assert_true(node2.get_data().at(record_id).fields.empty(), "Inserting After Deletion: Node2 should have empty fields");
    assert_true(node1.get_data().at(record_id).column_versions.find("__deleted__") !=
                    node1.get_data().at(record_id).column_versions.end(),
                "Inserting After Deletion: Node1 should have '__deleted__' column version");
    assert_true(node2.get_data().at(record_id).column_versions.find("__deleted__") !=
                    node2.get_data().at(record_id).column_versions.end(),
                "Inserting After Deletion: Node2 should have '__deleted__' column version");
    std::cout << "Test 'Inserting After Deletion' passed." << std::endl;
  }

  // Test Case: Offline Changes Then Merge using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Both nodes start with an empty state

    // Node1 inserts a record
    CrdtString record_id1 = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields1 = {{"id", record_id1}, {"tag", "Node1Tag"}};
    auto changes1 = node1.insert_or_update(record_id1, std::move(fields1));

    // Node2 is offline and inserts a different record
    CrdtString record_id2 = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields2 = {{"id", record_id2}, {"tag", "Node2Tag"}};
    auto changes2 = node2.insert_or_update(record_id2, std::move(fields2));

    // Now, node2 comes online and merges changes from node1
    uint64_t last_db_version_node2 = 0;
    sync_nodes(node1, node2, last_db_version_node2);

    // Similarly, node1 merges changes from node2
    uint64_t last_db_version_node1 = 0;
    sync_nodes(node2, node1, last_db_version_node1);

    // Both nodes should now have both records
    assert_true(node1.get_data().find(record_id1) != node1.get_data().end(),
                "Offline Changes Then Merge: Node1 should contain record_id1");
    assert_true(node1.get_data().find(record_id2) != node1.get_data().end(),
                "Offline Changes Then Merge: Node1 should contain record_id2");
    assert_true(node2.get_data().find(record_id1) != node2.get_data().end(),
                "Offline Changes Then Merge: Node2 should contain record_id1");
    assert_true(node2.get_data().find(record_id2) != node2.get_data().end(),
                "Offline Changes Then Merge: Node2 should contain record_id2");
    assert_true(node1.get_data() == node2.get_data(), "Offline Changes Then Merge: Data mismatch between Node1 and Node2");
    std::cout << "Test 'Offline Changes Then Merge' passed." << std::endl;
  }

  // Test Case: Conflicting Updates with Different Last DB Versions using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Both nodes insert the same record
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields1 = {{"id", record_id}, {"tag", "InitialTag"}};
    CrdtMap<CrdtString, CrdtString> fields2 = {{"id", record_id}, {"tag", "InitialTag"}};
    auto changes_init1 = node1.insert_or_update(record_id, std::move(fields1));
    auto changes_init2 = node2.insert_or_update(record_id, std::move(fields2));

    // Merge initial inserts
    node1.merge_changes(std::move(changes_init2));
    node2.merge_changes(std::move(changes_init1));

    // Node1 updates 'tag' twice
    CrdtMap<CrdtString, CrdtString> updates_node1 = {{"tag", "Node1Tag1"}};
    auto changes_node1_update1 = node1.insert_or_update(record_id, std::move(updates_node1));

    updates_node1 = {{"tag", "Node1Tag2"}};
    auto changes_node1_update2 = node1.insert_or_update(record_id, std::move(updates_node1));

    // Node2 updates 'tag' once
    CrdtMap<CrdtString, CrdtString> updates_node2 = {{"tag", "Node2Tag1"}};
    auto changes_node2_update1 = node2.insert_or_update(record_id, std::move(updates_node2));

    // Merge node1's changes into node2
    node2.merge_changes(std::move(changes_node1_update1));
    node2.merge_changes(std::move(changes_node1_update2));

    // Merge node2's changes into node1
    node1.merge_changes(std::move(changes_node2_update1));

    // The 'tag' should reflect the latest update based on db_version and site_id Assuming node1 has a higher db_version due to
    // two updates
    CrdtString final_tag = "Node1Tag2";

    assert_true(node1.get_data().at(record_id).fields.at("tag") == final_tag,
                "Conflicting Updates: Final tag should be 'Node1Tag2'");
    assert_true(node2.get_data().at(record_id).fields.at("tag") == final_tag,
                "Conflicting Updates: Final tag should be 'Node1Tag2'");
    assert_true(node1.get_data() == node2.get_data(), "Conflicting Updates: Data mismatch between Node1 and Node2");
    std::cout << "Test 'Conflicting Updates with Different Last DB Versions' passed." << std::endl;
  }

  // // Test Case: Clock Synchronization After Merges using insert_or_update
  // {
  //   CRDT<CrdtString, CrdtString> node1(1);
  //   CRDT<CrdtString, CrdtString> node2(2);
  //   CRDT<CrdtString, CrdtString> node3(3);

  //   // Merge trackers
  //   uint64_t last_db_version_node1 = 0;
  //   uint64_t last_db_version_node2 = 0;
  //   uint64_t last_db_version_node3 = 0;

  //   // Node1 inserts a record
  //   CrdtString record_id1 = generate_uuid();
  //   CrdtMap<CrdtString, CrdtString> fields1 = {{"id", record_id1}, {"tag", "Node1Tag"}};
  //   auto changes1 = node1.insert_or_update(record_id1, std::move(fields1));

  //   // Node2 inserts another record
  //   CrdtString record_id2 = generate_uuid();
  //   CrdtMap<CrdtString, CrdtString> fields2 = {{"id", record_id2}, {"tag", "Node2Tag"}};
  //   auto changes2 = node2.insert_or_update(record_id2, std::move(fields2));

  //   // Node3 inserts a third record
  //   CrdtString record_id3 = generate_uuid();
  //   CrdtMap<CrdtString, CrdtString> fields3 = {{"id", record_id3}, {"tag", "Node3Tag"}};
  //   auto changes3 = node3.insert_or_update(record_id3, std::move(fields3));

  //   // First round of merges
  //   // Merge node1's changes into node2 and node3
  //   sync_nodes(node1, node2, last_db_version_node2);
  //   sync_nodes(node1, node3, last_db_version_node3);

  //   // Merge node2's changes into node1 and node3
  //   sync_nodes(node2, node1, last_db_version_node1);
  //   sync_nodes(node2, node3, last_db_version_node3);

  //   // Merge node3's changes into node1 and node2
  //   sync_nodes(node3, node1, last_db_version_node1);
  //   sync_nodes(node3, node2, last_db_version_node2);

  //   // All nodes should have all three records
  //   assert_true(node1.get_data() == node2.get_data(), "Clock Synchronization: Node1 and Node2 data mismatch");
  //   assert_true(node2.get_data() == node3.get_data(), "Clock Synchronization: Node2 and Node3 data mismatch");
  //   assert_true(node1.get_data() == node3.get_data(), "Clock Synchronization: Node1 and Node3 data mismatch");

  //   // Check that logical clocks are properly updated
  //   uint64_t min_expected_clock_value = 3; // At least 3 inserts happened
  //   assert_true(node1.get_clock().current_time() >= min_expected_clock_value, "Clock Synchronization: Node1 clock too low");
  //   assert_true(node2.get_clock().current_time() >= min_expected_clock_value, "Clock Synchronization: Node2 clock too low");
  //   assert_true(node3.get_clock().current_time() >= min_expected_clock_value, "Clock Synchronization: Node3 clock too low");

  //   // Capture max clock before another round of merges
  //   uint64_t max_clock_before_merge =
  //       std::max({node1.get_clock().current_time(), node2.get_clock().current_time(), node3.get_clock().current_time()});

  //   // Perform another round of merges
  //   sync_nodes(node1, node2, last_db_version_node2);
  //   sync_nodes(node2, node3, last_db_version_node3);
  //   sync_nodes(node3, node1, last_db_version_node1);

  //   // Check that clocks have been updated after merges
  //   assert_true(node1.get_clock().current_time() > max_clock_before_merge, "Clock Synchronization: Node1 clock did not update");
  //   assert_true(node2.get_clock().current_time() > max_clock_before_merge, "Clock Synchronization: Node2 clock did not update");
  //   assert_true(node3.get_clock().current_time() > max_clock_before_merge, "Clock Synchronization: Node3 clock did not update");

  //   // Since clocks don't need to be identical, we don't assert equality
  //   std::cout << "Test 'Clock Synchronization After Merges' passed." << std::endl;
  // }

  // Test Case: Atomic Sync Per Transaction using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Node1 inserts a record
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"id", record_id}, {"tag", "InitialTag"}};
    auto changes_node1 = node1.insert_or_update(record_id, std::move(fields));

    // Sync immediately after the transaction
    node2.merge_changes(std::move(changes_node1));

    // Verify synchronization
    assert_true(node2.get_data().find(record_id) != node2.get_data().end(),
                "Atomic Sync: Node2 should contain the inserted record");
    assert_true(node2.get_data().at(record_id).fields.at("tag") == "InitialTag", "Atomic Sync: Tag should be 'InitialTag'");
    std::cout << "Test 'Atomic Sync Per Transaction' passed." << std::endl;
  }

  // Test Case: Concurrent Updates using insert_or_update
  {
    CRDT<CrdtString, CrdtString> node1(1);
    CRDT<CrdtString, CrdtString> node2(2);

    // Insert a record on node1
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"id", record_id}, {"tag", "InitialTag"}};
    auto changes_insert = node1.insert_or_update(record_id, std::move(fields));

    // Merge to node2
    node2.merge_changes(std::move(changes_insert));

    // Concurrently update 'tag' on both nodes
    CrdtMap<CrdtString, CrdtString> updates_node1 = {{"tag", "Node1TagUpdate"}};
    auto changes_update1 = node1.insert_or_update(record_id, std::move(updates_node1));

    CrdtMap<CrdtString, CrdtString> updates_node2 = {{"tag", "Node2TagUpdate"}};
    auto changes_update2 = node2.insert_or_update(record_id, std::move(updates_node2));

    // Merge changes
    node1.merge_changes(std::move(changes_update2));
    node2.merge_changes(std::move(changes_update1));

    // Conflict resolution based on site_id (Node2 has higher site_id)
    CrdtString expected_tag = "Node2TagUpdate";

    assert_true(node1.get_data().at(record_id).fields.at("tag") == expected_tag,
                "Concurrent Updates: Tag should be 'Node2TagUpdate'");
    assert_true(node2.get_data().at(record_id).fields.at("tag") == expected_tag,
                "Concurrent Updates: Tag should be 'Node2TagUpdate'");
    std::cout << "Test 'Concurrent Updates' passed." << std::endl;
  }

  // Test Case: Get Changes Since After Loading with Merge Versions
  {
    // Initialize CRDT with pre-loaded changes
    CrdtVector<Change<CrdtString, CrdtString>> changes;
    CrdtNodeId node_id = 1;

    CrdtString record_id = generate_uuid();
    changes.emplace_back(Change<CrdtString, CrdtString>(record_id, "field1", "value1", 1, 1, node_id));
    CRDT<CrdtString, CrdtString> crdt_loaded(node_id, std::move(changes));

    // Make additional changes after loading
    CrdtMap<CrdtString, CrdtString> new_fields = {{"field2", "value2"}};
    auto changes_new = crdt_loaded.insert_or_update(record_id, std::move(new_fields));

    // Retrieve changes since db_version 1
    CrdtVector<Change<CrdtString, CrdtString>> retrieved_changes = crdt_loaded.get_changes_since(1);

    // Should include only the new change
    assert_true(retrieved_changes.size() == 1, "Get Changes Since: Should retrieve one new change");
    assert_true(retrieved_changes[0].col_name.has_value() && retrieved_changes[0].col_name.value() == "field2",
                "Get Changes Since: Retrieved change should be for 'field2'");
    assert_true(retrieved_changes[0].value.has_value() && retrieved_changes[0].value.value() == "value2",
                "Get Changes Since: Retrieved change 'field2' value mismatch");
    std::cout << "Test 'Get Changes Since After Loading with Merge Versions' passed." << std::endl;
  }

  // Test Case: Prevent Reapplication of Changes Loaded via Constructor
  {
    // Initialize CRDT with pre-loaded changes
    CrdtVector<Change<CrdtString, CrdtString>> changes;
    CrdtNodeId node_id = 1;

    CrdtString record_id = generate_uuid();
    changes.emplace_back(Change<CrdtString, CrdtString>(record_id, "field1", "value1", 1, 1, node_id));
    CRDT<CrdtString, CrdtString> crdt_loaded(node_id, std::move(changes));

    // Attempt to merge the same changes again
    crdt_loaded.merge_changes({Change<CrdtString, CrdtString>(record_id, "field1", "value1", 1, 1, node_id)});

    // Verify that no duplicate changes are applied
    const auto &data = crdt_loaded.get_data();
    assert_true(data.at(record_id).fields.at("field1") == "value1",
                "Prevent Reapplication: 'field1' value should remain 'value1'");
    std::cout << "Test 'Prevent Reapplication of Changes Loaded via Constructor' passed." << std::endl;
  }

  // Test Case: Complex Merge Scenario with Merge DB Versions
  {
    // Initialize two CRDTs with pre-loaded changes
    CrdtVector<Change<CrdtString, CrdtString>> changes_node1;
    CrdtNodeId node1_id = 1;

    CrdtString record_id = generate_uuid();
    changes_node1.emplace_back(Change<CrdtString, CrdtString>(record_id, "field1", "node1_value1", 1, 1, node1_id));
    CRDT<CrdtString, CrdtString> node1_crdt(node1_id, std::move(changes_node1));

    CrdtVector<Change<CrdtString, CrdtString>> changes_node2;
    CrdtNodeId node2_id = 2;
    changes_node2.emplace_back(Change<CrdtString, CrdtString>(record_id, "field1", "node2_value1", 2, 2, node2_id));
    CRDT<CrdtString, CrdtString> node2_crdt(node2_id, std::move(changes_node2));

    // Merge node2 into node1
    node1_crdt.merge_changes({Change<CrdtString, CrdtString>(record_id, "field1", "node2_value1", 2, 2, node2_id)});

    // Merge node1 into node2
    node2_crdt.merge_changes({Change<CrdtString, CrdtString>(record_id, "field1", "node1_value1", 1, 1, node1_id)});

    // Verify conflict resolution based on db_version and node_id
    // node2's change should prevail since it has a higher db_version
    assert_true(node1_crdt.get_data().at(record_id).fields.at("field1") == "node2_value1",
                "Complex Merge: node2's change should prevail in node1");
    assert_true(node2_crdt.get_data().at(record_id).fields.at("field1") == "node2_value1",
                "Complex Merge: node2's change should prevail in node2");
    std::cout << "Test 'Complex Merge Scenario with Merge DB Versions' passed." << std::endl;
  }

  // Test Case: get_changes_since Considers merge_db_version Correctly
  {
    // Initialize CRDT and perform initial changes
    CRDT<CrdtString, CrdtString> crdt(1);
    CrdtString record_id = generate_uuid();
    CrdtMap<CrdtString, CrdtString> fields = {{"field1", "value1"}};
    auto changes_init = crdt.insert_or_update(record_id, std::move(fields));

    // Apply changes and set merge_db_version via constructor
    CRDT<CrdtString, CrdtString> crdt_loaded(2, std::move(changes_init));

    // Make new changes after loading
    CrdtMap<CrdtString, CrdtString> new_fields = {{"field2", "value2"}};
    auto changes_new = crdt_loaded.insert_or_update(record_id, std::move(new_fields));

    // Get changes since db_version 1
    CrdtVector<Change<CrdtString, CrdtString>> retrieved_changes = crdt_loaded.get_changes_since(1);

    // Should include only the new change
    assert_true(retrieved_changes.size() == 1, "get_changes_since: Should retrieve one new change");
    assert_true(retrieved_changes[0].col_name.has_value() && retrieved_changes[0].col_name.value() == "field2",
                "get_changes_since: Retrieved change should be for 'field2'");
    assert_true(retrieved_changes[0].value.has_value() && retrieved_changes[0].value.value() == "value2",
                "get_changes_since: Retrieved change 'field2' value mismatch");
    std::cout << "Test 'get_changes_since Considers merge_db_version Correctly' passed." << std::endl;
  }

  // Test Case: Multiple Loads and Merges with Merge DB Versions
  {
    // Simulate loading from disk multiple times
    CrdtVector<Change<CrdtString, CrdtString>> changes_load1;
    CrdtNodeId node_id = 1;

    CrdtString record_id1 = generate_uuid();
    changes_load1.emplace_back(Change<CrdtString, CrdtString>(record_id1, "field1", "value1", 1, 1, node_id));
    CRDT<CrdtString, CrdtString> crdt1(node_id, std::move(changes_load1));

    CrdtVector<Change<CrdtString, CrdtString>> changes_load2;
    CrdtString record_id2 = generate_uuid();
    changes_load2.emplace_back(Change<CrdtString, CrdtString>(record_id2, "field2", "value2", 2, 2, node_id));
    CRDT<CrdtString, CrdtString> crdt2(node_id, std::move(changes_load2));

    // Merge crdt2 into crdt1
    crdt1.merge_changes({Change<CrdtString, CrdtString>(record_id2, "field2", "value2", 2, 2, node_id)});

    // Make additional changes
    CrdtMap<CrdtString, CrdtString> new_fields = {{"field3", "value3"}};
    auto changes_new = crdt1.insert_or_update(record_id1, std::move(new_fields));

    // Get changes since db_version 3
    CrdtVector<Change<CrdtString, CrdtString>> retrieved_changes = crdt1.get_changes_since(3);

    // Should include only the new change
    assert_true(retrieved_changes.size() == 1, "Multiple Loads and Merges: Should retrieve one new change");
    assert_true(retrieved_changes[0].col_name.has_value() && retrieved_changes[0].col_name.value() == "field3",
                "Multiple Loads and Merges: Retrieved change should be for 'field3'");
    assert_true(retrieved_changes[0].value.has_value() && retrieved_changes[0].value.value() == "value3",
                "Multiple Loads and Merges: Retrieved change 'field3' value mismatch");
    std::cout << "Test 'Multiple Loads and Merges with Merge DB Versions' passed." << std::endl;
  }

  std::cout << "All tests passed successfully!" << std::endl;
  return 0;
}