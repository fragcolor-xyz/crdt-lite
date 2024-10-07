#include "list_crdt.hpp"
#include <string>

int main() {
  // Create two replicas
  ListCRDT<std::string> replicaA(1); // Use CrdtNodeId 1 for replica A
  ListCRDT<std::string> replicaB(2); // Use CrdtNodeId 2 for replica B

  // Replica A inserts "Hello" at index 0
  replicaA.insert(0, "Hello");
  // Replica A inserts "World" at index 1
  replicaA.insert(1, "World");

  // Replica B inserts "Goodbye" at index 0
  replicaB.insert(0, "Goodbye");
  // Replica B inserts "Cruel" at index 1
  replicaB.insert(1, "Cruel");

  // Print initial states
  std::cout << "Replica A (Visible): ";
  replicaA.print_visible(); // Expected: Hello World

  std::cout << "Replica B (Visible): ";
  replicaB.print_visible(); // Expected: Goodbye Cruel

  // Merge replicas
  replicaA.merge(replicaB);
  replicaB.merge(replicaA);

  // Print merged states
  std::cout << "\nAfter merging:" << std::endl;

  std::cout << "Replica A (Visible): ";
  replicaA.print_visible(); // Expected order based on origins and IDs

  std::cout << "Replica B (Visible): ";
  replicaB.print_visible(); // Should match Replica A

  // Perform concurrent insertions
  replicaA.insert(2, "!");
  replicaB.insert(2, "?");

  // Merge again
  replicaA.merge(replicaB);
  replicaB.merge(replicaA);

  // Print after concurrent insertions and merging
  std::cout << "\nAfter concurrent insertions and merging:" << std::endl;

  std::cout << "Replica A (Visible): ";
  replicaA.print_visible(); // Expected: Hello Goodbye World ! ?

  std::cout << "Replica B (Visible): ";
  replicaB.print_visible(); // Should match Replica A

  // Demonstrate deletions
  replicaA.delete_element(1); // Delete "Goodbye"
  replicaB.delete_element(0); // Delete "Hello"

  // Merge deletions
  replicaA.merge(replicaB);
  replicaB.merge(replicaA);

  // Print states after deletions and merging
  std::cout << "\nAfter deletions and merging:" << std::endl;

  std::cout << "Replica A (Visible): ";
  replicaA.print_visible(); // Expected: World ! ?

  std::cout << "Replica B (Visible): ";
  replicaB.print_visible(); // Should match Replica A

  // Perform garbage collection
  replicaA.garbage_collect();
  replicaB.garbage_collect();

  // Print all elements after garbage collection
  std::cout << "\nAfter garbage collection:" << std::endl;

  std::cout << "Replica A (All Elements):" << std::endl;
  replicaA.print_all_elements();

  std::cout << "Replica B (All Elements):" << std::endl;
  replicaB.print_all_elements();

  // Demonstrate delta generation and application
  // Replica A inserts "New Line" at index 2
  replicaA.insert(2, "New Line");

  // Generate delta from Replica A to Replica B
  auto delta = replicaA.generate_delta(replicaB);

  // Apply delta to Replica B
  replicaB.apply_delta(delta.first, delta.second);

  // Print final states after delta synchronization
  std::cout << "\nAfter delta synchronization:" << std::endl;

  std::cout << "Replica A (Visible): ";
  replicaA.print_visible(); // Expected: World ! New Line ?

  std::cout << "Replica B (Visible): ";
  replicaB.print_visible(); // Should match Replica A

  return 0;
}
