#include "crdt.hpp"
#include "crdt_temporal_index.hpp"
#include <chrono>
#include <iostream>
#include <random>
#include <string>

// Utility to measure execution time
template <typename Func>
double measure_time_ms(Func&& func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// Create test data directly in the CRDT's data structure to avoid constructor issues
void setup_test_data_direct(auto& crdt, int record_count, int fields_per_record) {
    uint64_t version_counter = 1;
    
    for (int i = 0; i < record_count; i++) {
        std::string record_id = "record_" + std::to_string(i);
        
        // Create a Record manually
        Record<std::string> record;
        
        // Add fields
        for (int j = 0; j < fields_per_record; j++) {
            std::string field_name = "field_" + std::to_string(j);
            std::string value = "value_" + std::to_string(i) + "_" + std::to_string(j);
            
            // Insert field value
            record.fields.insert_or_assign(field_name, value);
            
            // Setup version information properly
            // col_version: 1 (first version)
            // db_version: incremental global version
            // node_id: from the CRDT
            // local_db_version: CRITICAL - must match db_version for get_changes_since to work
            uint64_t db_ver = version_counter++;
            record.column_versions.insert_or_assign(
                field_name, 
                ColumnVersion(1, db_ver, 1, db_ver) // local_db_version = db_version
            );
        }
        
        // Add to the CRDT data
        crdt.get_data().insert_or_assign(record_id, record);
    }
}

int main() {
    std::cout << "=== CRDT Temporal Index Performance Test ===\n" << std::endl;
    
    // Parameters for the test
    const int RECORD_COUNT = 5000;  // 5,000 records for faster testing
    const int FIELDS = 5;           // 5 fields per record
    
    // Create CRDTs
    std::cout << "Creating CRDTs..." << std::endl;
    auto regular_crdt = std::make_shared<CRDT<std::string, std::string>>(1);
    auto temporal_crdt = std::make_shared<crdt_ext::TemporalIndexedCRDT<std::string, std::string>>(2);
    
    // Setup identical data
    std::cout << "Setting up test data..." << std::endl;
    uint64_t total_versions = RECORD_COUNT * FIELDS;
    setup_test_data_direct(*regular_crdt, RECORD_COUNT, FIELDS);
    setup_test_data_direct(*temporal_crdt, RECORD_COUNT, FIELDS);
    
    // Manually update the clock time to reflect our versions
    // This is a bit of a hack, but necessary for testing
    uint64_t base_version = total_versions;
    
    // Verify clock state
    std::cout << "Regular CRDT clock: " << regular_crdt->get_clock().current_time() << std::endl;
    std::cout << "Temporal CRDT clock: " << temporal_crdt->get_clock().current_time() << std::endl;
    
    // Rebuild the temporal index
    std::cout << "Building temporal index..." << std::endl;
    temporal_crdt->rebuild_temporal_index();
    
    // Test 1: Small time window (recent 10% of changes)
    std::cout << "\n=== Test 1: Small Time Window (recent 10% of changes) ===" << std::endl;
    uint64_t recent_version = base_version * 0.9;
    
    // Regular implementation
    double regular_small = measure_time_ms([&]() {
        auto changes = regular_crdt->get_changes_since(recent_version);
        std::cout << "Regular CRDT: Retrieved " << changes.size() << " changes" << std::endl;
    });
    
    // Base method in temporal CRDT
    double temporal_base_small = measure_time_ms([&]() {
        auto changes = temporal_crdt->get_changes_since(recent_version);
        std::cout << "Temporal CRDT (base): Retrieved " << changes.size() << " changes" << std::endl;
    });
    
    // Optimized method in temporal CRDT
    double temporal_opt_small = measure_time_ms([&]() {
        auto changes = temporal_crdt->get_changes_since_optimized(recent_version);
        std::cout << "Temporal CRDT (optimized): Retrieved " << changes.size() << " changes" << std::endl;
    });
    
    // Check if results are consistent
    bool small_consistent = (temporal_crdt->get_changes_since(recent_version).size() == 
                           temporal_crdt->get_changes_since_optimized(recent_version).size());
    
    std::cout << "\nPerformance (small window):" << std::endl;
    std::cout << "  Regular:            " << regular_small << " ms" << std::endl;
    std::cout << "  Temporal (base):    " << temporal_base_small << " ms" << std::endl;
    std::cout << "  Temporal (optimized): " << temporal_opt_small << " ms" << std::endl;
    std::cout << "  Optimized vs Regular: " << (regular_small / temporal_opt_small) << "x" << std::endl;
    std::cout << "  Results consistent: " << (small_consistent ? "Yes" : "No") << std::endl;
    
    // Test 2: Large time window (75% of all changes)
    std::cout << "\n=== Test 2: Large Time Window (75% of all changes) ===" << std::endl;
    uint64_t early_version = base_version * 0.25;
    
    // Regular implementation
    double regular_large = measure_time_ms([&]() {
        auto changes = regular_crdt->get_changes_since(early_version);
        std::cout << "Regular CRDT: Retrieved " << changes.size() << " changes" << std::endl;
    });
    
    // Base method in temporal CRDT
    double temporal_base_large = measure_time_ms([&]() {
        auto changes = temporal_crdt->get_changes_since(early_version);
        std::cout << "Temporal CRDT (base): Retrieved " << changes.size() << " changes" << std::endl;
    });
    
    // Optimized method in temporal CRDT
    double temporal_opt_large = measure_time_ms([&]() {
        auto changes = temporal_crdt->get_changes_since_optimized(early_version);
        std::cout << "Temporal CRDT (optimized): Retrieved " << changes.size() << " changes" << std::endl;
    });
    
    // Check if results are consistent
    bool large_consistent = (temporal_crdt->get_changes_since(early_version).size() == 
                           temporal_crdt->get_changes_since_optimized(early_version).size());
    
    std::cout << "\nPerformance (large window):" << std::endl;
    std::cout << "  Regular:            " << regular_large << " ms" << std::endl;
    std::cout << "  Temporal (base):    " << temporal_base_large << " ms" << std::endl;
    std::cout << "  Temporal (optimized): " << temporal_opt_large << " ms" << std::endl;
    std::cout << "  Optimized vs Regular: " << (regular_large / temporal_opt_large) << "x" << std::endl;
    std::cout << "  Results consistent: " << (large_consistent ? "Yes" : "No") << std::endl;
    
    // Test 3: Temporal query performance
    std::cout << "\n=== Test 3: Temporal Query Performance ===" << std::endl;
    
    // Get recently modified records
    double recent_time = measure_time_ms([&]() {
        auto recent = temporal_crdt->get_recently_modified(10);
        std::cout << "Retrieved " << recent.size() << " recently modified records" << std::endl;
    });
    std::cout << "get_recently_modified time: " << recent_time << " ms" << std::endl;
    
    // Get records modified since mid version
    uint64_t mid_version = base_version * 0.5;
    double modified_time = measure_time_ms([&]() {
        auto modified = temporal_crdt->get_records_modified_since(mid_version);
        std::cout << "Retrieved " << modified.size() << " records modified since mid-version" << std::endl;
    });
    std::cout << "get_records_modified_since time: " << modified_time << " ms" << std::endl;
    
    std::cout << "\nAll performance tests completed!" << std::endl;
    return 0;
}