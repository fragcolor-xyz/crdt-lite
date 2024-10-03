// list_crdt_tests.cpp

#include "list_crdt.hpp"
#include <cassert>
#include <iostream>

// Test 1: Simple insertion
void test_simple_insertion() {
  ListCRDT<std::string> list(1);
  uint64_t eid = list.generate_eid();
  list.insert_or_move(eid, std::nullopt, std::nullopt, "A");
  auto elements = list.get_elements();
  assert(elements.size() == 1 && elements[0] == "A");
  std::cout << "Test 1 passed: Simple insertion.\n";
}

// Test 2: Multiple insertions maintaining order
void test_multiple_insertions() {
  ListCRDT<std::string> list(1);
  uint64_t eidA = list.generate_eid();
  list.insert_or_move(eidA, std::nullopt, std::nullopt, "A"); // Insert at beginning
  auto posA = list.get_elements_by_eid().at(eidA).id;
  uint64_t eidB = list.generate_eid();
  list.insert_or_move(eidB, posA, std::nullopt, "B"); // Insert after A
  auto elements = list.get_elements();
  assert(elements.size() == 2 && elements[0] == "A" && elements[1] == "B");
  std::cout << "Test 2 passed: Multiple insertions maintaining order.\n";
}

// Test 3: Deletion of elements
void test_deletion() {
  ListCRDT<std::string> list(1);
  uint64_t eid = list.generate_eid();
  list.insert_or_move(eid, std::nullopt, std::nullopt, "A");
  list.erase(eid);
  auto elements = list.get_elements();
  assert(elements.empty());
  std::cout << "Test 3 passed: Deletion of elements.\n";
}

// Test 4: Insertion between elements
void test_insertion_between() {
  ListCRDT<std::string> list(1);
  uint64_t eidA = list.generate_eid();
  list.insert_or_move(eidA, std::nullopt, std::nullopt, "A");
  auto posA = list.get_elements_by_eid().at(eidA).id;
  uint64_t eidC = list.generate_eid();
  list.insert_or_move(eidC, posA, std::nullopt, "C");
  auto posC = list.get_elements_by_eid().at(eidC).id;
  uint64_t eidB = list.generate_eid();
  list.insert_or_move(eidB, posA, posC, "B");
  auto elements = list.get_elements();
  assert(elements.size() == 3 && elements[0] == "A" && elements[1] == "B" && elements[2] == "C");
  std::cout << "Test 4 passed: Insertion between elements.\n";
}

// Test 5: Concurrent insertions and merging
void test_concurrent_insertions() {
  // Replica 1
  ListCRDT<std::string> list1(1);
  uint64_t eidA = list1.generate_eid();
  list1.insert_or_move(eidA, std::nullopt, std::nullopt, "A");
  auto posA1 = list1.get_elements_by_eid().at(eidA).id;

  // Replica 2
  ListCRDT<std::string> list2(2);
  list2.merge(list1.get_all_elements());
  auto posA2 = list2.get_elements_by_eid().at(eidA).id;

  // Concurrent insertions
  uint64_t eidB = list1.generate_eid();
  list1.insert_or_move(eidB, posA1, std::nullopt, "B"); // List1 inserts B after A
  uint64_t eidC = list2.generate_eid();
  list2.insert_or_move(eidC, posA2, std::nullopt, "C"); // List2 inserts C after A

  // Merge changes
  list1.merge(list2.get_all_elements());
  list2.merge(list1.get_all_elements());

  // Verify both lists have the same elements
  auto elements1 = list1.get_elements();
  auto elements2 = list2.get_elements();
  assert(elements1 == elements2 && elements1.size() == 3 && elements1[0] == "A");
  std::cout << "Test 5 passed: Concurrent insertions and merging.\n";
}

// Test 6: Concurrent deletions
void test_concurrent_deletions() {
  // Replica 1
  ListCRDT<std::string> list1(1);
  uint64_t eidA = list1.generate_eid();
  list1.insert_or_move(eidA, std::nullopt, std::nullopt, "A");

  // Replica 2
  ListCRDT<std::string> list2(2);
  list2.merge(list1.get_all_elements());

  // Concurrent deletions
  list1.erase(eidA);
  list2.erase(eidA);

  // Merge changes
  list1.merge(list2.get_all_elements());
  list2.merge(list1.get_all_elements());

  // Verify both lists are empty
  assert(list1.get_elements().empty() && list2.get_elements().empty());
  std::cout << "Test 6 passed: Concurrent deletions.\n";
}

// Test 7: Insertions at the beginning and end
void test_insertion_at_ends() {
  ListCRDT<std::string> list(1);
  uint64_t eidB = list.generate_eid();
  list.insert_or_move(eidB, std::nullopt, std::nullopt, "B"); // Insert at beginning
  auto posB = list.get_elements_by_eid().at(eidB).id;
  uint64_t eidA = list.generate_eid();
  list.insert_or_move(eidA, std::nullopt, posB, "A"); // Insert before B
  uint64_t eidC = list.generate_eid();
  list.insert_or_move(eidC, posB, std::nullopt, "C"); // Insert after B
  auto elements = list.get_elements();
  assert(elements.size() == 3 && elements[0] == "A" && elements[1] == "B" && elements[2] == "C");
  std::cout << "Test 7 passed: Insertions at the beginning and end.\n";
}

// Test 8: Merging lists with overlapping operations
void test_merging_overlapping_operations() {
  // Replica 1
  ListCRDT<std::string> list1(1);
  uint64_t eidA = list1.generate_eid();
  list1.insert_or_move(eidA, std::nullopt, std::nullopt, "A");
  auto posA1 = list1.get_elements_by_eid().at(eidA).id;

  // Replica 2
  ListCRDT<std::string> list2(2);
  list2.insert_or_move(eidA, std::nullopt, std::nullopt, "A");
  auto posA2 = list2.get_elements_by_eid().at(eidA).id;

  // Concurrent operations
  uint64_t eidB = list1.generate_eid();
  list1.insert_or_move(eidB, posA1, std::nullopt, "B");

  uint64_t eidC = list2.generate_eid();
  list2.insert_or_move(eidC, posA2, std::nullopt, "C");

  // Merge changes
  list1.merge(list2.get_all_elements());
  list2.merge(list1.get_all_elements());

  // Verify both lists have the same elements
  auto elements1 = list1.get_elements();
  auto elements2 = list2.get_elements();
  assert(elements1 == elements2 && elements1[0] == "A");
  std::cout << "Test 8 passed: Merging lists with overlapping operations.\n";
}

// void test_duplicate_position_ids() {
//   ListCRDT<std::string> list(1);
//   // Manually create elements with the same PositionID
//   PositionID pid({1}, 1);
//   uint64_t eid1 = list.generate_eid();
//   ListElement<std::string> elem1(pid, "A", eid1, 0);
//   uint64_t eid2 = list.generate_eid();
//   ListElement<std::string> elem2(pid, "B", eid2, 0);

//   list.merge({elem1, elem2});

//   auto elements = list.get_elements();
//   // Since IDs are the same, conflict resolution will determine which one remains
//   assert(elements.size() == 1);
//   std::cout << "Test 9 passed: Handling duplicate PositionIDs.\n";
// }

// Corrected Line 175 in Test 10
void test_erase_non_existent() {
  ListCRDT<std::string> list(1);
  uint64_t eid = (uint64_t(2) << 32) | 1; // EID from a different node
  list.erase(eid);                        // Should not cause an error
  std::cout << "Test 10 passed: Erasing non-existent elements does not cause errors.\n";
}

// Test 11: Moving elements by remove and re-insert
void test_move_element() {
  ListCRDT<std::string> list(1);
  // Initial insertion
  uint64_t eidA = list.generate_eid();
  list.insert_or_move(eidA, std::nullopt, std::nullopt, "A");
  auto posA = list.get_elements_by_eid().at(eidA).id;
  uint64_t eidB = list.generate_eid();
  list.insert_or_move(eidB, posA, std::nullopt, "B");
  auto posB = list.get_elements_by_eid().at(eidB).id;
  uint64_t eidC = list.generate_eid();
  list.insert_or_move(eidC, posB, std::nullopt, "C");
  auto posC = list.get_elements_by_eid().at(eidC).id;

  // Move "B" to after "C"
  list.insert_or_move(eidB, posC, std::nullopt, "B");

  auto elements = list.get_elements();
  assert(elements.size() == 3 && elements[0] == "A" && elements[1] == "C" && elements[2] == "B");
  std::cout << "Test 11 passed: Moving elements by remove and re-insert.\n";
}

void test_concurrent_moves() {
  // Replica 1
  ListCRDT<std::string> list1(1);
  uint64_t eidA = list1.generate_eid();
  uint64_t eidB = list1.generate_eid();
  uint64_t eidC = list1.generate_eid();

  list1.insert_or_move(eidA, std::nullopt, std::nullopt, "A");
  auto posA1 = list1.get_elements_by_eid().at(eidA).id;
  list1.insert_or_move(eidB, posA1, std::nullopt, "B");
  auto posB1 = list1.get_elements_by_eid().at(eidB).id;
  list1.insert_or_move(eidC, posB1, std::nullopt, "C");
  auto posC1 = list1.get_elements_by_eid().at(eidC).id;

  // Replica 2
  ListCRDT<std::string> list2(2);
  list2.merge(list1.get_all_elements());

  // Replica 1 moves "B" after "C"
  list1.insert_or_move(eidB, posC1, std::nullopt, "B");

  // Replica 2 moves "A" after "B"
  auto posB2 = list2.get_elements_by_eid().at(eidB).id;
  list2.insert_or_move(eidA, posB2, std::nullopt, "A");

  // Merge changes
  list1.merge(list2.get_all_elements());
  list2.merge(list1.get_all_elements());

  // Verify that both replicas converge to the same state
  auto elements1 = list1.get_elements();
  auto elements2 = list2.get_elements();

  // Expected sequence after merging
  std::vector<std::string> expected_sequence{"C", "A", "B"};

  assert(elements1 == elements2);
  assert(elements1 == expected_sequence);

  std::cout << "Test 12 passed: Concurrent moves on different replicas.\n";
}

// Test 13: Moving element to the beginning
void test_move_to_beginning() {
  ListCRDT<std::string> list(1);
  uint64_t eidB = list.generate_eid();
  list.insert_or_move(eidB, std::nullopt, std::nullopt, "B");
  auto posB = list.get_elements_by_eid().at(eidB).id;
  uint64_t eidC = list.generate_eid();
  list.insert_or_move(eidC, posB, std::nullopt, "C");
  auto posC = list.get_elements_by_eid().at(eidC).id;
  uint64_t eidA = list.generate_eid();
  list.insert_or_move(eidA, posB, posC, "A");
  auto posA = list.get_elements_by_eid().at(eidA).id;

  // Move "C" to the beginning
  list.insert_or_move(eidC, std::nullopt, posB, "C");

  auto elements = list.get_elements();
  assert(elements.size() == 3 && elements[0] == "C" && elements[1] == "B" && elements[2] == "A");
  std::cout << "Test 13 passed: Moving element to the beginning.\n";
}

// Test 14: Moving element to the end
void test_move_to_end() {
  ListCRDT<std::string> list(1);
  uint64_t eidA = list.generate_eid();
  list.insert_or_move(eidA, std::nullopt, std::nullopt, "A");
  auto posA = list.get_elements_by_eid().at(eidA).id;
  uint64_t eidB = list.generate_eid();
  list.insert_or_move(eidB, posA, std::nullopt, "B");
  auto posB = list.get_elements_by_eid().at(eidB).id;
  uint64_t eidC = list.generate_eid();
  list.insert_or_move(eidC, posB, std::nullopt, "C");
  auto posC = list.get_elements_by_eid().at(eidC).id;

  // Move "A" to the end
  list.insert_or_move(eidA, posC, std::nullopt, "A");

  auto elements = list.get_elements();
  assert(elements.size() == 3 && elements[0] == "B" && elements[1] == "C" && elements[2] == "A");
  std::cout << "Test 14 passed: Moving element to the end.\n";
}

// Test 15: Moving element and handling deletions
void test_move_with_deletion() {
  // Replica 1
  ListCRDT<std::string> list1(1);
  uint64_t eidA = list1.generate_eid();
  uint64_t eidB = list1.generate_eid();
  list1.insert_or_move(eidA, std::nullopt, std::nullopt, "A");
  auto posA1 = list1.get_elements_by_eid().at(eidA).id;
  list1.insert_or_move(eidB, posA1, std::nullopt, "B");
  auto posB1 = list1.get_elements_by_eid().at(eidB).id;

  // Replica 2
  ListCRDT<std::string> list2(2);
  list2.merge(list1.get_all_elements());

  // Replica 1 moves "B" before "A"
  list1.insert_or_move(eidB, std::nullopt, posA1, "B");

  // Replica 2 deletes "A"
  list2.erase(eidA);

  // Merge changes
  list1.merge(list2.get_all_elements());
  list2.merge(list1.get_all_elements());

  // Verify both replicas converge
  auto elements1 = list1.get_elements();
  auto elements2 = list2.get_elements();

  // Expected sequence: ["B"] since "A" is deleted
  assert(elements1 == elements2 && elements1.size() == 1 && elements1[0] == "B");
  std::cout << "Test 15 passed: Moving element and handling deletions.\n";
}

void test_text_crdt() {
  // Create a CRDT for a text file
  ListCRDT<std::string> text_crdt(1);

  // Initialize from a file
  text_crdt.initialize_from_file("example.txt");

  // Insert a new line at line number 2
  uint64_t eid_new_line = text_crdt.generate_eid();
  text_crdt.insert_line(eid_new_line, 2, "This is a new line.");

  // Delete line number 4
  text_crdt.delete_line(4);

  // Save the updated text back to the file
  text_crdt.save_to_file("example_updated.txt");
}

int main() {
  test_simple_insertion();
  test_multiple_insertions();
  test_deletion();
  test_insertion_between();
  test_concurrent_insertions();
  test_concurrent_deletions();
  test_insertion_at_ends();
  test_merging_overlapping_operations();
  // test_duplicate_position_ids();
  test_erase_non_existent();

  // New tests for moving elements
  test_move_element();
  test_concurrent_moves();
  test_move_to_beginning();
  test_move_to_end();
  test_move_with_deletion();

  test_text_crdt();

  std::cout << "All tests passed successfully.\n";
  return 0;
}
