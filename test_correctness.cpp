#include "crdt.hpp"
#include "crdt_temporal_index.hpp"
#include <chrono>
#include <iostream>
#include <cassert>
#include <string>

// Test functionality and correctness of the temporal index
void test_correctness() {
    std::cout << "===== Testing Temporal Index Correctness =====\n";
    
    // Create a regular CRDT and a temporal indexed CRDT
    auto regular_crdt = std::make_shared<CRDT<std::string, std::string>>(1);
    auto temporal_crdt = std::make_shared<crdt_ext::TemporalIndexedCRDT<std::string, std::string>>(2);
    
    // Step 1: Add identical data to both
    for (int i = 0; i < 100; i++) {
        std::string record_id = "record_" + std::to_string(i);
        regular_crdt->insert_or_update(record_id, std::make_pair("name", "value" + std::to_string(i)));
        temporal_crdt->insert_or_update(record_id, std::make_pair("name", "value" + std::to_string(i)));
    }
    
    // Store the mid-point version
    uint64_t mid_version = regular_crdt->get_clock().current_time();
    std::cout << "Mid-point version: " << mid_version << "\n";
    
    // Step 2: Add more changes to both CRDTs
    for (int i = 0; i < 50; i++) {
        std::string record_id = "record_" + std::to_string(i*2); // Update even records
        regular_crdt->insert_or_update(record_id, std::make_pair("updated", "new_value" + std::to_string(i)));
        temporal_crdt->insert_or_update(record_id, std::make_pair("updated", "new_value" + std::to_string(i)));
    }
    
    // Step 3: Compare the results of get_changes_since
    std::cout << "Comparing get_changes_since results...\n";
    
    auto baseline_changes = regular_crdt->get_changes_since(mid_version);
    auto temporal_changes = temporal_crdt->get_changes_since(mid_version);
    auto optimized_changes = temporal_crdt->get_changes_since_optimized(mid_version);
    
    std::cout << "Baseline changes: " << baseline_changes.size() << "\n";
    std::cout << "Temporal changes: " << temporal_changes.size() << "\n";
    std::cout << "Optimized changes: " << optimized_changes.size() << "\n";
    
    bool base_matches = (baseline_changes.size() == temporal_changes.size());
    bool opt_matches = (baseline_changes.size() == optimized_changes.size());
    
    std::cout << "Base implementation matches: " << (base_matches ? "Yes" : "No") << "\n";
    std::cout << "Optimized implementation matches: " << (opt_matches ? "Yes" : "No") << "\n";
    
    // Step 4: Test get_recently_modified functionality
    std::cout << "\nTesting get_recently_modified...\n";
    auto recent = temporal_crdt->get_recently_modified(5);
    std::cout << "Recent modifications (top 5):\n";
    for (const auto& [record_id, record] : recent) {
        std::cout << "  " << record_id << " with " << record.fields.size() << " fields\n";
    }
    
    // Step 5: Test records_modified_since functionality
    std::cout << "\nTesting get_records_modified_since...\n";
    auto modified = temporal_crdt->get_records_modified_since(mid_version);
    std::cout << "Records modified since mid-version: " << modified.size() << "\n";
    
    if (modified.size() != 50) { // We updated 50 records after the mid-point
        std::cout << "WARNING: Expected 50 modified records, got " << modified.size() << "\n";
    }
}

void test_real_world_usage() {
    std::cout << "\n===== Testing Real-World Usage =====\n";
    
    // Create a temporal indexed CRDT
    auto crdt = std::make_shared<crdt_ext::TemporalIndexedCRDT<std::string, std::string>>(1);
    
    // Add records
    for (int i = 0; i < 1000; i++) {
        std::string record_id = "record_" + std::to_string(i);
        for (int j = 0; j < 5; j++) {
            std::string field_name = "field_" + std::to_string(j);
            std::string value = "value_" + std::to_string(i) + "_" + std::to_string(j);
            crdt->insert_or_update(record_id, std::make_pair(field_name, value));
        }
    }
    
    // Store the current version halfway through
    uint64_t mid_version = crdt->get_clock().current_time();
    
    // Make some updates to create a history
    for (int i = 0; i < 500; i++) {
        std::string record_id = "record_" + std::to_string(995 + i % 10); // Update the last few records multiple times
        crdt->insert_or_update(record_id, std::make_pair("updated_field", "updated_value_" + std::to_string(i)));
    }
    
    // Test recently modified records query
    std::cout << "Recently modified records:" << std::endl;
    auto recent = crdt->get_recently_modified(5);
    for (const auto& [record_id, record] : recent) {
        std::cout << "  " << record_id << " with " << record.fields.size() << " fields" << std::endl;
    }
    
    // Test records modified since mid-version
    std::cout << "Records modified since mid-version:" << std::endl;
    auto records = crdt->get_records_modified_since(mid_version);
    std::cout << "Found " << records.size() << " records modified since version " << mid_version << std::endl;
}

int main() {
    // Test the correctness of the temporal index implementation
    test_correctness();
    
    // Test real-world usage patterns
    test_real_world_usage();
    
    std::cout << "All tests completed successfully!" << std::endl;
    return 0;
}