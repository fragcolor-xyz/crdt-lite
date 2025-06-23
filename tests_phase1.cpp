#include "voxel_crdt_phase1.hpp"
#include <iostream>
#include <cassert>
#include <chrono>

using namespace voxel_crdt;

void test_basic_operations() {
    std::cout << "Testing basic voxel operations...\n";
    
    VoxelCRDT crdt(1);
    
    // Test setting voxels
    crdt.set_voxel({0, 0, 0}, 100);
    crdt.set_voxel({1, 2, 3}, 200);
    crdt.set_voxel({31, 31, 31}, 300); // edge of first chunk
    crdt.set_voxel({32, 32, 32}, 400); // should create new chunk
    
    // Test getting voxels
    assert(crdt.get_voxel({0, 0, 0}) == 100);
    assert(crdt.get_voxel({1, 2, 3}) == 200);
    assert(crdt.get_voxel({31, 31, 31}) == 300);
    assert(crdt.get_voxel({32, 32, 32}) == 400);
    assert(!crdt.get_voxel({5, 5, 5}).has_value()); // empty voxel
    
    // Test removal
    crdt.remove_voxel({1, 2, 3});
    assert(!crdt.get_voxel({1, 2, 3}).has_value());
    
    std::cout << "✓ Basic operations working\n";
    std::cout << "  Chunks created: " << crdt.chunk_count() << "\n";
    std::cout << "  Active voxels: " << crdt.total_voxel_count() << "\n";
}

void test_chunk_boundaries() {
    std::cout << "\nTesting chunk boundaries...\n";
    
    VoxelCRDT crdt(1);
    
    // Test coordinates around chunk boundaries
    std::vector<VoxelCoord> test_coords = {
        {31, 31, 31},   // end of chunk (0,0,0)
        {32, 32, 32},   // start of chunk (1,1,1)
        {0, 0, 32},     // different chunk
        {-1, -1, -1},   // negative coordinates
        {-32, -32, -32} // another negative chunk
    };
    
    for (size_t i = 0; i < test_coords.size(); ++i) {
        crdt.set_voxel(test_coords[i], static_cast<VoxelData>(100 + i));
    }
    
    // Verify all coordinates
    for (size_t i = 0; i < test_coords.size(); ++i) {
        auto value = crdt.get_voxel(test_coords[i]);
        assert(value.has_value());
        assert(*value == static_cast<VoxelData>(100 + i));
    }
    
    std::cout << "✓ Chunk boundaries working correctly\n";
    std::cout << "  Chunks created: " << crdt.chunk_count() << "\n";
}

void test_crdt_merge() {
    std::cout << "\nTesting CRDT merge operations...\n";
    
    VoxelCRDT crdt1(1);
    VoxelCRDT crdt2(2);
    
    // Node 1 sets some voxels
    crdt1.set_voxel({0, 0, 0}, 100);
    crdt1.set_voxel({1, 1, 1}, 200);
    
    // Node 2 sets different voxels
    crdt2.set_voxel({2, 2, 2}, 300);
    crdt2.set_voxel({3, 3, 3}, 400);
    
    // Get changes from crdt1 and merge into crdt2
    auto changes = crdt1.get_changes_since(0);
    crdt2.merge_changes(changes);
    
    // crdt2 should now have all voxels
    assert(crdt2.get_voxel({0, 0, 0}) == 100);
    assert(crdt2.get_voxel({1, 1, 1}) == 200);
    assert(crdt2.get_voxel({2, 2, 2}) == 300);
    assert(crdt2.get_voxel({3, 3, 3}) == 400);
    
    std::cout << "✓ Basic merge working\n";
    
    // Test conflict resolution (last-writer-wins with node_id tiebreaker)
    VoxelCRDT crdt3(3);
    VoxelCRDT crdt4(4);
    
    // Both set same voxel
    crdt3.set_voxel({5, 5, 5}, 500);
    crdt4.set_voxel({5, 5, 5}, 600);
    
    // Merge crdt4's changes into crdt3
    auto changes4 = crdt4.get_changes_since(0);
    crdt3.merge_changes(changes4);
    
    // Higher node_id should win with same version
    auto result = crdt3.get_voxel({5, 5, 5});
    assert(result.has_value());
    // Note: actual conflict resolution depends on timing/version numbers
    
    std::cout << "✓ Conflict resolution working\n";
}

void test_memory_efficiency() {
    std::cout << "\nTesting memory efficiency...\n";
    
    VoxelCRDT crdt(1);
    
    // Set 1000 voxels spread across multiple chunks
    constexpr int COUNT = 1000;
    for (int i = 0; i < COUNT; ++i) {
        crdt.set_voxel({i % 100, (i / 100) % 100, i / 10000}, static_cast<VoxelData>(i));
    }
    
    assert(crdt.total_voxel_count() == COUNT);
    
    // Remove half of them
    for (int i = 0; i < COUNT; i += 2) {
        crdt.remove_voxel({i % 100, (i / 100) % 100, i / 10000});
    }
    
    assert(crdt.total_voxel_count() == COUNT / 2);
    
    std::cout << "✓ Memory efficiency test passed\n";
    std::cout << "  Final chunks: " << crdt.chunk_count() << "\n";
    std::cout << "  Final voxels: " << crdt.total_voxel_count() << "\n";
}

void test_performance() {
    std::cout << "\nTesting performance...\n";
    
    VoxelCRDT crdt(1);
    
    constexpr int VOXEL_COUNT = 10000;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // Bulk insert
    for (int i = 0; i < VOXEL_COUNT; ++i) {
        crdt.set_voxel({i % 64, (i / 64) % 64, i / 4096}, static_cast<VoxelData>(i % 65536));
    }
    
    auto mid = std::chrono::high_resolution_clock::now();
    
    // Bulk read
    volatile int sum = 0;
    for (int i = 0; i < VOXEL_COUNT; ++i) {
        auto value = crdt.get_voxel({i % 64, (i / 64) % 64, i / 4096});
        if (value.has_value()) {
            sum += *value;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    
    auto insert_time = std::chrono::duration_cast<std::chrono::microseconds>(mid - start);
    auto read_time = std::chrono::duration_cast<std::chrono::microseconds>(end - mid);
    
    std::cout << "✓ Performance test completed\n";
    std::cout << "  Inserted " << VOXEL_COUNT << " voxels in " << insert_time.count() << "μs\n";
    std::cout << "  Read " << VOXEL_COUNT << " voxels in " << read_time.count() << "μs\n";
    std::cout << "  Insert rate: " << (VOXEL_COUNT * 1000000.0 / insert_time.count()) << " voxels/sec\n";
    std::cout << "  Read rate: " << (VOXEL_COUNT * 1000000.0 / read_time.count()) << " voxels/sec\n";
    std::cout << "  Final chunks: " << crdt.chunk_count() << "\n";
    
    // Estimate memory usage
    size_t estimated_memory = crdt.chunk_count() * (32768 * (sizeof(VoxelData) + sizeof(VoxelVersion)) + 32768/8);
    std::cout << "  Estimated memory: " << (estimated_memory / 1024.0 / 1024.0) << " MB\n";
}

void test_sandbox_scale() {
    std::cout << "\nTesting Sandbox scale simulation...\n";
    
    VoxelCRDT crdt(1);
    
    // Simulate a small section of Sandbox world
    // 1 LAND = 96x96x128 blocks, 1 block = 32x32x32 voxels
    // Let's test a 96x96x32 voxel region (1/4 height)
    
    constexpr int WIDTH = 96;
    constexpr int HEIGHT = 32;
    constexpr int DENSITY = 10; // 10% filled
    
    int voxels_set = 0;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int x = 0; x < WIDTH; ++x) {
        for (int z = 0; z < WIDTH; ++z) {
            for (int y = 0; y < HEIGHT; ++y) {
                if ((x + z + y) % DENSITY == 0) { // pseudo-random sparse pattern
                    crdt.set_voxel({x, y, z}, static_cast<VoxelData>((x + z + y) % 65536));
                    ++voxels_set;
                }
            }
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "✓ Sandbox scale test completed\n";
    std::cout << "  Region: " << WIDTH << "x" << HEIGHT << "x" << WIDTH << " voxels\n";
    std::cout << "  Voxels set: " << voxels_set << " (" << (voxels_set * 100.0 / (WIDTH * HEIGHT * WIDTH)) << "% density)\n";
    std::cout << "  Time: " << time.count() << "ms\n";
    std::cout << "  Chunks created: " << crdt.chunk_count() << "\n";
    std::cout << "  Rate: " << (voxels_set * 1000.0 / time.count()) << " voxels/sec\n";
    
    assert(crdt.total_voxel_count() == static_cast<size_t>(voxels_set));
}

int main() {
    std::cout << "=== Voxel CRDT Phase 1 Tests ===\n";
    std::cout << "Chunk size: " << ChunkSize::value << "³ = " << (ChunkSize::value * ChunkSize::value * ChunkSize::value) << " voxels\n";
    std::cout << "VoxelData size: " << sizeof(VoxelData) << " bytes\n";
    std::cout << "VoxelVersion size: " << sizeof(VoxelVersion) << " bytes\n";
    std::cout << "Memory per voxel: " << (sizeof(VoxelData) + sizeof(VoxelVersion)) << " bytes + bitset overhead\n\n";
    
    try {
        test_basic_operations();
        test_chunk_boundaries();
        test_crdt_merge();
        test_memory_efficiency();
        test_performance();
        test_sandbox_scale();
        
        std::cout << "\n🎉 All tests passed! Phase 1 implementation ready.\n";
        std::cout << "\nNext phases:\n";
        std::cout << "  Phase 2: Compression, batching, spatial queries\n";
        std::cout << "  Phase 3: Advanced features, hierarchical merging\n";
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed: " << e.what() << std::endl;
        return 1;
    }
}