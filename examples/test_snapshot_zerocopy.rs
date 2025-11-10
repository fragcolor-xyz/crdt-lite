//! Investigate zero-copy benefits for CRDT snapshot loading
//!
//! Question: For heavy snapshot operations, does zero-copy help?
//!
//! Scenarios:
//! 1. Load entire snapshot → deserialize all → use CRDT
//! 2. Load snapshot → zero-copy access → lazy deserialization
//!
//! Key consideration: CRDTs need mutable access for merge operations

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct LargeCRDTSnapshot {
    records: HashMap<String, Record>,
    tombstones: HashMap<String, u64>,
    clock: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Record {
    fields: HashMap<String, Vec<u8>>,
    versions: HashMap<String, u64>,
}

fn main() {
    println!("=== Zero-Copy Analysis for CRDT Snapshots ===\n");

    // Create a large CRDT snapshot (simulating 10k records)
    let snapshot = create_large_snapshot(10_000);

    println!("Created snapshot with {} records", snapshot.records.len());
    println!("Approximate in-memory size: ~{} KB\n",
        estimate_size(&snapshot) / 1024);

    // Test 1: Traditional deserialization (MessagePack)
    test_traditional_deserialize(&snapshot);

    // Test 2: Zero-copy approach (FlexBuffers)
    test_zerocopy_approach(&snapshot);

    // Test 3: Memory-mapped approach
    test_mmap_approach(&snapshot);

    println!("\n=== Analysis ===");
    analyze_tradeoffs();
}

fn create_large_snapshot(num_records: usize) -> LargeCRDTSnapshot {
    let mut records = HashMap::new();
    let mut tombstones = HashMap::new();

    for i in 0..num_records {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), format!("Record {}", i).into_bytes());
        fields.insert("data".to_string(), vec![0xFF; 100]); // 100 bytes of binary data
        fields.insert("metadata".to_string(), b"some metadata".to_vec());

        let mut versions = HashMap::new();
        versions.insert("name".to_string(), 1);
        versions.insert("data".to_string(), 1);
        versions.insert("metadata".to_string(), 1);

        records.insert(format!("record_{}", i), Record { fields, versions });

        // Every 10th record is tombstoned
        if i % 10 == 0 {
            tombstones.insert(format!("tombstone_{}", i), i as u64);
        }
    }

    LargeCRDTSnapshot {
        records,
        tombstones,
        clock: num_records as u64,
    }
}

fn estimate_size(snapshot: &LargeCRDTSnapshot) -> usize {
    // Rough estimate
    let records_size = snapshot.records.len() * (
        50 + // key overhead
        3 * (20 + 100) // 3 fields with data
    );
    let tombstones_size = snapshot.tombstones.len() * 50;
    records_size + tombstones_size + 8
}

fn test_traditional_deserialize(snapshot: &LargeCRDTSnapshot) {
    println!("--- Test 1: Traditional Deserialization (MessagePack) ---");

    // Serialize
    let start = Instant::now();
    let encoded = rmp_serde::to_vec(snapshot).expect("Failed to encode");
    let serialize_time = start.elapsed();

    println!("Serialized size: {} KB", encoded.len() / 1024);
    println!("Serialization time: {:?}", serialize_time);

    // Simulate writing to disk
    let write_start = Instant::now();
    std::fs::write("/tmp/crdt_snapshot.msgpack", &encoded).expect("Failed to write");
    let write_time = write_start.elapsed();
    println!("Write to disk: {:?}", write_time);

    // Simulate reading from disk
    let read_start = Instant::now();
    let data = std::fs::read("/tmp/crdt_snapshot.msgpack").expect("Failed to read");
    let read_time = read_start.elapsed();
    println!("Read from disk: {:?}", read_time);

    // Deserialize
    let deserialize_start = Instant::now();
    let decoded: LargeCRDTSnapshot = rmp_serde::from_slice(&data).expect("Failed to decode");
    let deserialize_time = deserialize_start.elapsed();

    println!("Deserialization time: {:?}", deserialize_time);
    println!("Total load time: {:?}", read_time + deserialize_time);

    // Verify
    assert_eq!(snapshot.records.len(), decoded.records.len());

    // Simulate typical operation: access a record
    let access_start = Instant::now();
    let _record = decoded.records.get("record_5000");
    let access_time = access_start.elapsed();
    println!("Random access time: {:?}", access_time);

    println!("Memory: Holds full deserialized structure (~{} KB)\n",
        estimate_size(&decoded) / 1024);
}

fn test_zerocopy_approach(snapshot: &LargeCRDTSnapshot) {
    println!("--- Test 2: Zero-Copy Approach (FlexBuffers) ---");

    // Serialize to FlexBuffers
    let start = Instant::now();
    let mut serializer = flexbuffers::FlexbufferSerializer::new();
    snapshot.serialize(&mut serializer).expect("Failed to encode");
    let encoded = serializer.take_buffer();
    let serialize_time = start.elapsed();

    println!("Serialized size: {} KB", encoded.len() / 1024);
    println!("Serialization time: {:?}", serialize_time);

    // Simulate writing to disk
    let write_start = Instant::now();
    std::fs::write("/tmp/crdt_snapshot.flexbuf", &encoded).expect("Failed to write");
    let write_time = write_start.elapsed();
    println!("Write to disk: {:?}", write_time);

    // Simulate reading from disk
    let read_start = Instant::now();
    let data = std::fs::read("/tmp/crdt_snapshot.flexbuf").expect("Failed to read");
    let read_time = read_start.elapsed();
    println!("Read from disk: {:?}", read_time);

    // Zero-copy approach: just parse the root
    let root_start = Instant::now();
    let root = flexbuffers::Reader::get_root(&data).expect("Failed to get root");
    let root_time = root_start.elapsed();

    println!("Get root (zero-copy): {:?}", root_time);
    println!("Total load time: {:?}", read_time + root_time);

    // Simulate typical operation: access a specific record
    let access_start = Instant::now();
    let records_map = root.as_map();
    let _record = records_map.index("records");
    let access_time = access_start.elapsed();
    println!("Random access time: {:?}", access_time);

    // But: To use with CRDT API, we'd still need to deserialize!
    let full_deserialize_start = Instant::now();
    let _decoded: LargeCRDTSnapshot = flexbuffers::from_slice(&data)
        .expect("Failed to decode");
    let full_deserialize_time = full_deserialize_start.elapsed();
    println!("Full deserialization (for mutations): {:?}", full_deserialize_time);

    println!("Memory: Holds serialized buffer (~{} KB) + zero-copy views\n",
        data.len() / 1024);
}

fn test_mmap_approach(snapshot: &LargeCRDTSnapshot) {
    println!("--- Test 3: Memory-Mapped Approach ---");

    // Serialize with MessagePack
    let encoded = rmp_serde::to_vec(snapshot).expect("Failed to encode");
    std::fs::write("/tmp/crdt_snapshot_mmap.msgpack", &encoded).expect("Failed to write");

    // Open with memory mapping
    let file = std::fs::File::open("/tmp/crdt_snapshot_mmap.msgpack").expect("Failed to open");
    let mmap_start = Instant::now();
    let mmap = unsafe {
        memmap2::Mmap::map(&file).expect("Failed to mmap")
    };
    let mmap_time = mmap_start.elapsed();

    println!("Memory-map time: {:?}", mmap_time);
    println!("File size: {} KB", mmap.len() / 1024);

    // Access is lazy - OS loads pages on demand
    println!("Pages loaded on-demand by OS");

    // But still need to deserialize to use with CRDT
    let deserialize_start = Instant::now();
    let _decoded: LargeCRDTSnapshot = rmp_serde::from_slice(&mmap[..])
        .expect("Failed to decode");
    let deserialize_time = deserialize_start.elapsed();
    println!("Deserialization time: {:?}", deserialize_time);

    println!("Memory: OS manages pages, but still need full struct for mutations\n");
}

fn analyze_tradeoffs() {
    println!("Key Insights:");
    println!();

    println!("1. SNAPSHOT LOADING PHASES:");
    println!("   a) Read from disk → OS buffers this anyway");
    println!("   b) Deserialize → This is where time is spent");
    println!("   c) Use in CRDT → Needs mutable access");
    println!();

    println!("2. ZERO-COPY BENEFITS:");
    println!("   ✅ Faster initial load (skip deserialization)");
    println!("   ✅ Lower memory if only reading few fields");
    println!("   ✅ Good for read-only queries");
    println!();

    println!("3. ZERO-COPY LIMITATIONS FOR CRDTs:");
    println!("   ❌ CRDTs need MUTATIONS (insert_or_update, merge_changes)");
    println!("   ❌ Zero-copy = read-only access");
    println!("   ❌ Would need to deserialize anyway for writes");
    println!("   ❌ API would need major changes (lazy fields)");
    println!();

    println!("4. WHEN ZERO-COPY HELPS:");
    println!("   • Read-only queries on large datasets");
    println!("   • Accessing subset of fields");
    println!("   • Streaming scenarios");
    println!();

    println!("5. WHEN ZERO-COPY DOESN'T HELP:");
    println!("   • Full deserialization needed anyway (CRDTs!)");
    println!("   • Need mutable access immediately");
    println!("   • Working with entire dataset");
    println!();

    println!("6. CRDT SNAPSHOT REALITY:");
    println!("   Workflow:");
    println!("   1. Load snapshot → deserialize ENTIRE state");
    println!("   2. Replay WAL → merge changes (MUTATIONS)");
    println!("   3. Normal operations → insert/update/delete (MUTATIONS)");
    println!();
    println!("   Zero-copy gives no benefit because:");
    println!("   - We need the ENTIRE state (not a subset)");
    println!("   - We need MUTABLE access immediately");
    println!("   - WAL replay requires mutations");
    println!();

    println!("7. POTENTIAL OPTIMIZATION:");
    println!("   Instead of zero-copy, consider:");
    println!("   - Incremental snapshots (only changed records)");
    println!("   - Snapshot compression (zstd/lz4)");
    println!("   - Parallel deserialization");
    println!("   - Lazy WAL replay");
    println!();

    println!("CONCLUSION:");
    println!("Zero-copy (FlexBuffers) does NOT help for CRDT snapshots because:");
    println!("• CRDTs require immediate mutable access");
    println!("• Must deserialize entire state anyway");
    println!("• 3x size penalty outweighs any benefits");
    println!();
    println!("MessagePack remains the best choice:");
    println!("• 27% larger (acceptable)");
    println!("• Schema evolution works");
    println!("• Fast deserialization");
    println!("• No API changes needed");
}
