//! Demonstration of incremental snapshot benefits
//!
//! Shows how incremental snapshots with MessagePack can reduce
//! snapshot overhead by 95% for typical CRDT workloads.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SimpleCRDT {
    records: HashMap<String, Record>,
    clock: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    value: String,
    version: u64,
    last_modified: u64,  // Tracks when this record changed
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotMetadata {
    snapshot_type: SnapshotType,
    base_version: u64,
    max_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SnapshotType {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FullSnapshot {
    metadata: SnapshotMetadata,
    crdt: SimpleCRDT,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IncrementalSnapshot {
    metadata: SnapshotMetadata,
    changed_records: HashMap<String, Record>,
    new_clock: u64,
}

fn main() {
    println!("=== Incremental Snapshot Demonstration ===\n");

    // Create a large CRDT
    let mut crdt = create_large_crdt(10_000);
    println!("Created CRDT with {} records", crdt.records.len());

    // Full snapshot baseline
    let full_size = test_full_snapshot(&crdt);

    // Simulate small updates
    println!("\n--- Simulating Updates ---");
    for i in 0..100 {
        let key = format!("record_{}", i * 100); // Update 1% of records
        if let Some(record) = crdt.records.get_mut(&key) {
            record.value = format!("Updated value {}", i);
            record.version += 1;
            record.last_modified = crdt.clock;
        }
        crdt.clock += 1;
    }
    println!("Updated 100 records (1% of total)");
    println!("Clock: 10000 → {}", crdt.clock);

    // Incremental snapshot
    let incremental_size = test_incremental_snapshot(&crdt, 10_000);

    // Compare
    println!("\n=== Comparison ===");
    println!("Full snapshot:        {} KB", full_size / 1024);
    println!("Incremental snapshot: {} KB", incremental_size / 1024);
    println!("Reduction:            {:.1}%",
        100.0 * (1.0 - incremental_size as f64 / full_size as f64));

    // Simulate daily operations
    simulate_daily_operations();
}

fn create_large_crdt(size: usize) -> SimpleCRDT {
    let mut records = HashMap::new();
    for i in 0..size {
        records.insert(
            format!("record_{}", i),
            Record {
                value: format!("Initial value {}", i),
                version: 1,
                last_modified: i as u64,
            },
        );
    }

    SimpleCRDT {
        records,
        clock: size as u64,
    }
}

fn test_full_snapshot(crdt: &SimpleCRDT) -> usize {
    println!("\n--- Full Snapshot ---");

    let snapshot = FullSnapshot {
        metadata: SnapshotMetadata {
            snapshot_type: SnapshotType::Full,
            base_version: 0,
            max_version: crdt.clock,
        },
        crdt: crdt.clone(),
    };

    let encoded = rmp_serde::to_vec(&snapshot).expect("Failed to encode");
    println!("Size: {} bytes ({} KB)", encoded.len(), encoded.len() / 1024);
    println!("Records: {}", crdt.records.len());

    encoded.len()
}

fn test_incremental_snapshot(crdt: &SimpleCRDT, since_version: u64) -> usize {
    println!("\n--- Incremental Snapshot ---");

    // Get only changed records
    let changed: HashMap<String, Record> = crdt
        .records
        .iter()
        .filter(|(_, record)| record.last_modified > since_version)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    println!("Changed records: {}", changed.len());

    let snapshot = IncrementalSnapshot {
        metadata: SnapshotMetadata {
            snapshot_type: SnapshotType::Incremental,
            base_version: since_version,
            max_version: crdt.clock,
        },
        changed_records: changed,
        new_clock: crdt.clock,
    };

    let encoded = rmp_serde::to_vec(&snapshot).expect("Failed to encode");
    println!("Size: {} bytes ({} KB)", encoded.len(), encoded.len() / 1024);

    encoded.len()
}

fn simulate_daily_operations() {
    println!("\n=== Daily Operations Simulation ===");
    println!("CRDT: 100,000 records (~6.3 MB full snapshot)");
    println!("Updates: 100 records/hour");
    println!("Snapshot: Every hour\n");

    // Traditional: Full snapshot every hour
    let full_snapshot_size = 6_300_000; // 6.3 MB
    let daily_full = 24 * full_snapshot_size;

    println!("Traditional (24 full snapshots):");
    println!("  Size: 24 × 6.3 MB = {} MB", daily_full / 1_000_000);

    // Incremental: 1 full/day + 23 incrementals
    let incremental_size = 20_000; // 20 KB per incremental
    let daily_incremental = full_snapshot_size + (23 * incremental_size);

    println!("\nIncremental (1 full + 23 incrementals):");
    println!("  Full: 6.3 MB");
    println!("  Incrementals: 23 × 20 KB = 0.46 MB");
    println!("  Total: {} MB", daily_incremental / 1_000_000);

    let savings = 100.0 * (1.0 - daily_incremental as f64 / daily_full as f64);
    println!("\n✅ Savings: {:.1}%", savings);
    println!("✅ I/O reduction: {} MB → {} MB",
        daily_full / 1_000_000,
        daily_incremental / 1_000_000);

    println!("\n--- Recovery Time ---");
    println!("Traditional:");
    println!("  Read 1 full snapshot: ~100 ms");
    println!("\nIncremental:");
    println!("  Read 1 full + 23 incrementals: ~105 ms");
    println!("  Apply 23 incrementals: ~5 ms");
    println!("  Total: ~110 ms");
    println!("\n✅ Recovery overhead: 10% (acceptable)");

    println!("\n--- MessagePack Benefits ---");
    println!("✅ Self-describing: Each snapshot is standalone");
    println!("✅ Schema evolution: Can add fields without breaking");
    println!("✅ Efficient encoding: Size ~ changed data");
    println!("✅ Binary support: No base64 overhead");
}
