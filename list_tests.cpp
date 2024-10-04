#include "list_crdt.hpp"

// Usage Example
int main() {
  // Create two replicas
  ListCRDT replicaA("A");
  ListCRDT replicaB("B");

  // Replica A inserts "Hello" at index 0
  replicaA.insert(0, "Hello");
  // Replica A inserts "World" at index 1
  replicaA.insert(1, "World");

  // Replica B inserts "Goodbye" at index 0
  replicaB.insert(0, "Goodbye");
  // Replica B inserts "Cruel" at index 1
  replicaB.insert(1, "Cruel");

  // Print initial states
  std::cout << "Replica A: ";
  replicaA.print(); // Expected: Hello World

  std::cout << "Replica B: ";
  replicaB.print(); // Expected: Goodbye Cruel

  // Merge replicas
  replicaA.merge(replicaB);
  replicaB.merge(replicaA);

  // Print merged states
  std::cout << "After merging:" << std::endl;

  std::cout << "Replica A: ";
  replicaA.print(); // Expected order based on origins and IDs

  std::cout << "Replica B: ";
  replicaB.print(); // Should match Replica A

  // Perform concurrent insertions
  replicaA.insert(2, "!");
  replicaB.insert(2, "?");

  // Merge again
  replicaA.merge(replicaB);
  replicaB.merge(replicaA);

  // Print final states
  std::cout << "After concurrent insertions and merging:" << std::endl;

  std::cout << "Replica A: ";
  replicaA.print(); // Expected: Goodbye Cruel Hello World ! ?

  std::cout << "Replica B: ";
  replicaB.print(); // Should match Replica A

  // Demonstrate deletions
  replicaA.delete_element(1); // Delete "Cruel"
  replicaB.delete_element(0); // Delete "Goodbye"

  // Merge deletions
  replicaA.merge(replicaB);
  replicaB.merge(replicaA);

  // Print states after deletions
  std::cout << "After deletions and merging:" << std::endl;

  std::cout << "Replica A: ";
  replicaA.print(); // Expected: Hello World ! ?

  std::cout << "Replica B: ";
  replicaB.print(); // Should match Replica A

  // Demonstrate delta generation and application
  // Replica A inserts "New Line" at index 2
  replicaA.insert(2, "New Line");

  // Generate delta from Replica A to Replica B
  auto delta = replicaA.generate_delta(replicaB);

  // Apply delta to Replica B
  replicaB.apply_delta(delta.first, delta.second);

  // Print final states after delta synchronization
  std::cout << "After delta synchronization:" << std::endl;

  std::cout << "Replica A: ";
  replicaA.print(); // Expected: Hello World New Line ! ?

  std::cout << "Replica B: ";
  replicaB.print(); // Should match Replica A

  return 0;
}