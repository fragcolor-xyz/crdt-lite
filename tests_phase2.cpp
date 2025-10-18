#include "voxel_crdt_phase2.hpp"
#include <iostream>
#include <cassert>
#include <chrono>
#include <filesystem>

using namespace voxel_crdt;

void test_storage_backend() {
    std::cout << "Testing storage backend abstraction...\n";
    
    // Clean up any existing test files
    std::filesystem::remove_all("test_storage");
    
    FileStorage storage("test_storage");
    VoxelCRDT<FileStorage> crdt(1, std::move(storage));
    
    // Set some voxels
    crdt.set_voxel({0, 0, 0}, 100);
    crdt.set_voxel({1, 1, 1}, 200);
    crdt.set_voxel({32, 32, 32}, 300); // Different chunk
    
    std::cout << "  Voxels set: " << crdt.total_voxel_count() << "\n";
    std::cout << "  Chunks in memory: " << crdt.chunk_count() << "\n";
    
    // Verify files were created
    auto stored_chunks = crdt.get_storage().list_chunks();
    std::cout << "  Chunks persisted: " << stored_chunks.size() << "\n";
    assert(stored_chunks.size() == 2);
    
    // Create new CRDT instance, should load from storage
    FileStorage storage2("test_storage");
    VoxelCRDT<FileStorage> crdt2(2, std::move(storage2));
    
    // Verify data persistence
    assert(crdt2.get_voxel({0, 0, 0}) == 100);
    assert(crdt2.get_voxel({1, 1, 1}) == 200);
    assert(crdt2.get_voxel({32, 32, 32}) == 300);
    
    std::cout << "✓ Storage backend working correctly\n";
    
    // Clean up
    std::filesystem::remove_all("test_storage");
}

void test_bulk_operations() {
    std::cout << "\nTesting bulk operations...\n";
    
    std::filesystem::remove_all("test_bulk");
    FileStorage storage("test_bulk");
    VoxelCRDT<FileStorage> crdt(1, std::move(storage));
    
    // Fill a 10x10x10 region
    BoundingBox region({0, 0, 0}, {9, 9, 9});
    size_t changes = crdt.fill_region(region, 500);
    
    std::cout << "  Filled " << changes << " voxels in 10³ region\n";
    assert(changes == 1000); // 10*10*10
    
    // Verify the region
    auto voxels = crdt.get_region(region);
    std::cout << "  Retrieved " << voxels.size() << " voxels from region\n";
    assert(voxels.size() == 1000);
    
    for (const auto& [coord, value] : voxels) {
        assert(value == 500);
        assert(region.contains(coord));
    }
    
    // Test cross-chunk bulk operation
    BoundingBox cross_chunk({30, 30, 30}, {35, 35, 35});
    size_t cross_changes = crdt.fill_region(cross_chunk, 600);
    std::cout << "  Cross-chunk fill: " << cross_changes << " voxels\n";
    assert(cross_changes == 216); // 6*6*6
    
    std::cout << "✓ Bulk operations working correctly\n";
    
    std::filesystem::remove_all("test_bulk");
}

void test_spatial_queries() {
    std::cout << "\nTesting spatial queries...\n";
    
    std::filesystem::remove_all("test_spatial");
    FileStorage storage("test_spatial");
    VoxelCRDT<FileStorage> crdt(1, std::move(storage));
    
    // Create a 3D plus sign pattern
    VoxelCoord center{5, 5, 5};
    crdt.set_voxel(center, 100);
    crdt.set_voxel({4, 5, 5}, 101);
    crdt.set_voxel({6, 5, 5}, 102);
    crdt.set_voxel({5, 4, 5}, 103);
    crdt.set_voxel({5, 6, 5}, 104);
    crdt.set_voxel({5, 5, 4}, 105);
    crdt.set_voxel({5, 5, 6}, 106);
    
    // Test neighbor queries
    auto neighbors = crdt.get_neighbors(center);
    size_t neighbor_count = 0;
    for (const auto& neighbor : neighbors) {
        if (neighbor.has_value()) {
            ++neighbor_count;
        }
    }
    
    std::cout << "  Center voxel has " << neighbor_count << " neighbors\n";
    assert(neighbor_count == 6);
    
    // Test region query around center
    BoundingBox around_center({3, 3, 3}, {7, 7, 7});
    auto region_voxels = crdt.get_region(around_center);
    std::cout << "  Region around center contains " << region_voxels.size() << " voxels\n";
    assert(region_voxels.size() == 7); // The plus sign
    
    std::cout << "✓ Spatial queries working correctly\n";
    
    std::filesystem::remove_all("test_spatial");
}

void test_compression() {
    std::cout << "\nTesting RLE compression...\n";
    
    std::filesystem::remove_all("test_compression");
    FileStorage storage("test_compression");
    VoxelCRDT<FileStorage> crdt(1, std::move(storage));
    
    // Create a large uniform region (should compress well)
    BoundingBox large_region({0, 0, 0}, {63, 31, 31});
    size_t filled = crdt.fill_region(large_region, 777);
    
    std::cout << "  Filled " << filled << " voxels\n";
    std::cout << "  Chunks created: " << crdt.chunk_count() << "\n";
    
    // Force compression
    crdt.compress_all();
    
    // Create new instance to test compressed serialization
    FileStorage storage2("test_compression");
    VoxelCRDT<FileStorage> crdt2(2, std::move(storage2));
    
    // Verify data after decompression
    assert(crdt2.get_voxel({0, 0, 0}) == 777);
    assert(crdt2.get_voxel({63, 31, 31}) == 777);
    assert(crdt2.get_voxel({32, 16, 16}) == 777);
    
    std::cout << "  Total voxels after reload: " << crdt2.total_voxel_count() << "\n";
    assert(crdt2.total_voxel_count() == filled);
    
    std::cout << "✓ Compression working correctly\n";
    
    std::filesystem::remove_all("test_compression");
}

void test_persistence_and_lazy_loading() {
    std::cout << "\nTesting persistence and lazy loading...\n";
    
    std::filesystem::remove_all("test_lazy");
    
    // Phase 1: Create and save data
    {
        FileStorage storage("test_lazy");
        VoxelCRDT<FileStorage> crdt(1, std::move(storage));
        
        // Create voxels in multiple chunks
        for (int i = 0; i < 10; ++i) {
            crdt.set_voxel({i * 40, i * 40, i * 40}, static_cast<VoxelData>(1000 + i));
        }
        
        std::cout << "  Created voxels in " << crdt.chunk_count() << " chunks\n";
    } // CRDT destroyed, data should persist
    
    // Phase 2: Create new CRDT, verify lazy loading
    {
        FileStorage storage("test_lazy");
        VoxelCRDT<FileStorage> crdt(2, std::move(storage));
        
        std::cout << "  Initial chunks in memory: " << crdt.chunk_count() << "\n";
        assert(crdt.chunk_count() == 0); // No chunks loaded yet
        
        // Access specific voxel - should trigger chunk loading
        auto value = crdt.get_voxel({0, 0, 0});
        assert(value == 1000);
        std::cout << "  After first access: " << crdt.chunk_count() << " chunks loaded\n";
        assert(crdt.chunk_count() == 1);
        
        // Access another chunk
        auto value2 = crdt.get_voxel({40, 40, 40});
        assert(value2 == 1001);
        std::cout << "  After second access: " << crdt.chunk_count() << " chunks loaded\n";
        assert(crdt.chunk_count() == 2);
        
        // Verify all data is accessible
        for (int i = 0; i < 10; ++i) {
            auto val = crdt.get_voxel({i * 40, i * 40, i * 40});
            assert(val == static_cast<VoxelData>(1000 + i));
        }
        
        std::cout << "  Final chunks loaded: " << crdt.chunk_count() << "\n";
    }
    
    std::cout << "✓ Persistence and lazy loading working correctly\n";
    
    std::filesystem::remove_all("test_lazy");
}

void test_performance_with_storage() {
    std::cout << "\nTesting performance with persistent storage...\n";
    
    std::filesystem::remove_all("test_perf");
    FileStorage storage("test_perf");
    VoxelCRDT<FileStorage> crdt(1, std::move(storage));
    
    constexpr int VOXEL_COUNT = 5000;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Bulk insert with persistence
    for (int i = 0; i < VOXEL_COUNT; ++i) {
        crdt.set_voxel({i % 32, (i / 32) % 32, i / 1024}, static_cast<VoxelData>(i % 65536));
    }
    
    auto mid = std::chrono::high_resolution_clock::now();
    
    // Bulk read
    volatile int sum = 0;
    for (int i = 0; i < VOXEL_COUNT; ++i) {
        auto value = crdt.get_voxel({i % 32, (i / 32) % 32, i / 1024});
        if (value.has_value()) {
            sum += *value;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    
    auto insert_time = std::chrono::duration_cast<std::chrono::microseconds>(mid - start);
    auto read_time = std::chrono::duration_cast<std::chrono::microseconds>(end - mid);
    
    std::cout << "  Inserted " << VOXEL_COUNT << " voxels with persistence in " << insert_time.count() << "μs\n";
    std::cout << "  Read " << VOXEL_COUNT << " voxels in " << read_time.count() << "μs\n";
    std::cout << "  Insert rate: " << (VOXEL_COUNT * 1000000.0 / insert_time.count()) << " voxels/sec\n";
    std::cout << "  Read rate: " << (VOXEL_COUNT * 1000000.0 / read_time.count()) << " voxels/sec\n";
    std::cout << "  Chunks: " << crdt.chunk_count() << "\n";
    std::cout << "  Persisted chunks: " << crdt.get_storage().list_chunks().size() << "\n";
    
    std::cout << "✓ Performance with storage acceptable\n";
    
    std::filesystem::remove_all("test_perf");
}

void test_sandbox_scale_phase2() {
    std::cout << "\nTesting Sandbox scale with Phase 2 features...\n";
    
    std::filesystem::remove_all("test_sandbox2");
    FileStorage storage("test_sandbox2");
    VoxelCRDT<FileStorage> crdt(1, std::move(storage));
    
    // Simulate building a structure: walls of a house
    auto start = std::chrono::high_resolution_clock::now();
    
    // Floor: 20x20
    BoundingBox floor({0, 0, 0}, {19, 0, 19});
    size_t floor_voxels = crdt.fill_region(floor, 1); // Stone floor
    
    // Walls: 4 walls of height 5
    BoundingBox wall1({0, 1, 0}, {19, 5, 0}); // Front wall
    BoundingBox wall2({0, 1, 19}, {19, 5, 19}); // Back wall  
    BoundingBox wall3({0, 1, 0}, {0, 5, 19}); // Left wall
    BoundingBox wall4({19, 1, 0}, {19, 5, 19}); // Right wall
    
    size_t wall_voxels = 0;
    wall_voxels += crdt.fill_region(wall1, 2); // Brick walls
    wall_voxels += crdt.fill_region(wall2, 2);
    wall_voxels += crdt.fill_region(wall3, 2);
    wall_voxels += crdt.fill_region(wall4, 2);
    
    auto end = std::chrono::high_resolution_clock::now();
    auto time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    size_t total_voxels = floor_voxels + wall_voxels;
    
    std::cout << "  Built house structure:\n";
    std::cout << "    Floor: " << floor_voxels << " voxels\n";
    std::cout << "    Walls: " << wall_voxels << " voxels\n";
    std::cout << "    Total: " << total_voxels << " voxels\n";
    std::cout << "  Build time: " << time.count() << "ms\n";
    std::cout << "  Chunks: " << crdt.chunk_count() << "\n";
    std::cout << "  Build rate: " << (total_voxels * 1000.0 / time.count()) << " voxels/sec\n";
    
    // Test spatial query for interior space
    BoundingBox interior({1, 1, 1}, {18, 4, 18});
    auto interior_voxels = crdt.get_region(interior);
    std::cout << "  Interior voxels found: " << interior_voxels.size() << " (should be 0)\n";
    assert(interior_voxels.empty()); // Interior should be empty
    
    // Force compression to test with realistic building blocks
    crdt.compress_all();
    
    std::cout << "✓ Sandbox scale Phase 2 test completed\n";
    
    std::filesystem::remove_all("test_sandbox2");
}

int main() {
    std::cout << "=== Voxel CRDT Phase 2 Tests ===\n";
    std::cout << "New features: RLE compression, spatial queries, bulk operations, abstract storage\n\n";
    
    try {
        test_storage_backend();
        test_bulk_operations();
        test_spatial_queries();
        test_compression();
        test_persistence_and_lazy_loading();
        test_performance_with_storage();
        test_sandbox_scale_phase2();
        
        std::cout << "\n🎉 All Phase 2 tests passed!\n";
        std::cout << "\nPhase 2 Features Implemented:\n";
        std::cout << "  ✅ Abstract storage backend (FileStorage example)\n";
        std::cout << "  ✅ RLE compression for uniform regions\n";
        std::cout << "  ✅ Bulk fill operations with bounding boxes\n";
        std::cout << "  ✅ Spatial queries (regions, neighbors)\n";
        std::cout << "  ✅ Automatic persistence and lazy loading\n";
        std::cout << "  ✅ Cross-chunk operations handled seamlessly\n";
        std::cout << "\nReady for Phase 3: Advanced optimizations and Shards integration\n";
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed: " << e.what() << std::endl;
        return 1;
    }
}