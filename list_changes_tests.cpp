#include "list_crdt_changes.hpp"
#include <string>
#include <cassert>

void print_list(const std::vector<std::string>& list, const std::string& label) {
    std::cout << label << ": [";
    for (size_t i = 0; i < list.size(); i++) {
        if (i > 0) std::cout << ", ";
        std::cout << list[i];
    }
    std::cout << "]\n";
}

void assert_lists_equal(const std::vector<std::string>& a, const std::vector<std::string>& b) {
    print_list(a, "List A");
    print_list(b, "List B");
    
    assert(a.size() == b.size());
    for (size_t i = 0; i < a.size(); i++) {
        assert(a[i] == b[i]);
    }
}

void test_changes_tracking() {
    // Create two replicas with change tracking
    ListCRDTWithChanges<std::string> replicaA(1);
    ListCRDTWithChanges<std::string> replicaB(2);

    // Track changes for replica A
    CrdtVector<ListChange<std::string>> changesA;
    
    // Make some changes to replica A
    replicaA.insert(0, "Hello", &changesA);
    replicaA.insert(1, "World", &changesA);
    
    // Verify changes were tracked
    assert(changesA.size() == 2);
    assert(changesA[0].value.value() == "Hello");
    assert(changesA[1].value.value() == "World");

    // Apply changes to replica B
    replicaB.apply_changes(std::move(changesA));

    // Verify both replicas have the same state
    assert_lists_equal(replicaA.get_values(), replicaB.get_values());

    std::cout << "Changes tracking test passed!\n";
}

void test_sync_with_version() {
    std::cout << "\nStarting sync test...\n";
    
    ListCRDTWithChanges<std::string> replicaA(1);
    ListCRDTWithChanges<std::string> replicaB(2);
    uint64_t last_sync_version = 0;

    std::cout << "Initial state:\n";
    print_list(replicaA.get_values(), "Replica A");
    print_list(replicaB.get_values(), "Replica B");

    // Make changes to both replicas
    std::cout << "\nMaking changes to replicas...\n";
    replicaA.insert(0, "First");
    replicaA.insert(1, "Second");
    
    replicaB.insert(0, "Third");
    replicaB.insert(1, "Fourth");

    std::cout << "After changes:\n";
    print_list(replicaA.get_values(), "Replica A");
    print_list(replicaB.get_values(), "Replica B");

    // Sync from A to B
    std::cout << "\nSyncing A to B...\n";
    sync_list_nodes(replicaA, replicaB, last_sync_version);
    print_list(replicaB.get_values(), "Replica B after A->B sync");
    
    // Sync from B to A
    std::cout << "Syncing B to A...\n";
    sync_list_nodes(replicaB, replicaA, last_sync_version);
    print_list(replicaA.get_values(), "Replica A after B->A sync");

    // Verify both replicas converged to the same state
    std::cout << "\nVerifying convergence...\n";
    assert_lists_equal(replicaA.get_values(), replicaB.get_values());

    // Make new changes and sync again
    std::cout << "\nMaking new change to A...\n";
    replicaA.insert(2, "Fifth");
    sync_list_nodes(replicaA, replicaB, last_sync_version);
    
    // Verify the new change was synced
    std::cout << "\nFinal verification...\n";
    assert_lists_equal(replicaA.get_values(), replicaB.get_values());

    std::cout << "Sync with version test passed!\n";
}

void test_concurrent_changes() {
    ListCRDTWithChanges<std::string> replicaA(1);
    ListCRDTWithChanges<std::string> replicaB(2);
    
    CrdtVector<ListChange<std::string>> changesA, changesB;

    // Make concurrent changes
    replicaA.insert(0, "A1", &changesA);
    replicaA.insert(1, "A2", &changesA);
    
    replicaB.insert(0, "B1", &changesB);
    replicaB.insert(1, "B2", &changesB);

    // Apply changes both ways
    replicaA.apply_changes(std::move(changesB));
    replicaB.apply_changes(std::move(changesA));

    // Verify convergence
    assert_lists_equal(replicaA.get_values(), replicaB.get_values());

    // Test concurrent deletion
    changesA.clear();
    changesB.clear();

    replicaA.delete_element(0, &changesA);
    replicaB.delete_element(1, &changesB);

    replicaA.apply_changes(std::move(changesB));
    replicaB.apply_changes(std::move(changesA));

    // Verify convergence after deletions
    assert_lists_equal(replicaA.get_values(), replicaB.get_values());

    std::cout << "Concurrent changes test passed!\n";
}

int main() {
    std::cout << "Running ListCRDTWithChanges tests...\n\n";
    
    test_changes_tracking();
    test_sync_with_version();
    test_concurrent_changes();
    
    std::cout << "\nAll tests passed!\n";
    return 0;
} 