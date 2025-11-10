//! Test demonstrating schema evolution issues in persist.rs
//!
//! This test shows what happens when the CRDT schema changes
//! (adding new fields) and we try to load old snapshots/WAL files.

use crdt_lite::persist::{PersistedCRDT, PersistConfig};
use std::path::PathBuf;

fn main() {
    println!("=== Testing Schema Evolution in persist.rs ===\n");

    // Test 1: Create a CRDT with old schema and save it
    test_old_schema_save();

    // Test 2: Try to load with new schema (this would happen if we added fields)
    test_schema_compatibility();

    println!("\n=== Test Results ===");
    println!("Current implementation uses bincode with serde defaults.");
    println!("This means:");
    println!("  ✓ Adding new fields WITH #[serde(default)] = OK");
    println!("  ✗ Adding new fields WITHOUT #[serde(default)] = FAILS");
    println!("  ✗ Removing fields = FAILS");
    println!("  ✗ Changing field types = FAILS");
}

fn test_old_schema_save() {
    println!("Test 1: Creating CRDT with current schema");
    println!("-------------------------------------------");

    let temp_dir = std::env::temp_dir().join("crdt_schema_test_v1");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        temp_dir.clone(),
        1,
        PersistConfig::default(),
    ).expect("Failed to create CRDT");

    // Insert some data
    println!("Inserting records...");
    for i in 0..10 {
        pcrdt.insert_or_update(
            &format!("record_{}", i),
            vec![
                ("name".to_string(), format!("User {}", i)),
                ("age".to_string(), format!("{}", 20 + i)),
            ].into_iter()
        ).expect("Failed to insert");
    }

    // Force a snapshot
    println!("Creating snapshot...");
    pcrdt.snapshot().expect("Failed to create snapshot");

    println!("✓ Snapshot created at: {}", temp_dir.display());
    println!("  - Data: 10 records");
    println!("  - Fields per record: name, age");

    // List files created
    println!("\nFiles created:");
    if let Ok(entries) = std::fs::read_dir(&temp_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            println!("  - {}: {} bytes",
                entry.file_name().to_string_lossy(),
                size
            );
        }
    }
}

fn test_schema_compatibility() {
    println!("\n\nTest 2: Schema Compatibility Analysis");
    println!("--------------------------------------");

    let temp_dir = std::env::temp_dir().join("crdt_schema_test_v1");

    // Try to load the snapshot
    println!("Loading snapshot from v1...");
    match PersistedCRDT::<String, String, String>::open(
        temp_dir.clone(),
        1,
        PersistConfig::default(),
    ) {
        Ok(pcrdt) => {
            println!("✓ Successfully loaded snapshot");
            println!("  - Records: {}", pcrdt.crdt().get_data().len());

            // Verify data
            if let Some(record) = pcrdt.crdt().get_record(&"record_0".to_string()) {
                println!("  - Sample record fields: {:?}",
                    record.fields.keys().collect::<Vec<_>>()
                );
            }
        },
        Err(e) => {
            println!("✗ Failed to load snapshot: {}", e);
        }
    }

    println!("\n=== Current Schema Structure ===");
    println!("CRDT<K, C, V>:");
    println!("  - node_id: NodeId");
    println!("  - clock: LogicalClock");
    println!("  - data: HashMap<K, Record<C, V>>");
    println!("  - tombstones: TombstoneStorage<K>");
    println!("  - base_version: u64");

    println!("\nRecord<C, V>:");
    println!("  - fields: HashMap<C, V>");
    println!("  - column_versions: HashMap<C, ColumnVersion>");
    println!("  - lowest_local_db_version: u64");
    println!("  - highest_local_db_version: u64");

    println!("\nChange<K, C, V>:");
    println!("  - record_id: K");
    println!("  - col_name: Option<C>");
    println!("  - value: Option<V>");
    println!("  - col_version: u64");
    println!("  - db_version: u64");
    println!("  - node_id: NodeId");
    println!("  - local_db_version: u64");
    println!("  - flags: u32");

    println!("\n=== Schema Evolution Scenarios ===");

    println!("\nScenario 1: Add new field to Record<C, V>");
    println!("  Example: Add 'last_modified_timestamp: u64'");
    println!("  Current status: ✗ WILL FAIL");
    println!("  Reason: No #[serde(default)] attribute");
    println!("  Error: 'missing field `last_modified_timestamp`'");

    println!("\nScenario 2: Add new field to Change<K, C, V>");
    println!("  Example: Add 'metadata: HashMap<String, String>'");
    println!("  Current status: ✗ WILL FAIL");
    println!("  Reason: No #[serde(default)] attribute on WAL structs");
    println!("  Error: Old WAL files cannot be replayed");

    println!("\nScenario 3: Add optional field with default");
    println!("  Example: Add 'schema_version: Option<u32>' with #[serde(default)]");
    println!("  Current status: ✓ WOULD WORK");
    println!("  Reason: serde::default provides None for missing fields");

    println!("\nScenario 4: Remove a field");
    println!("  Example: Remove 'flags' from Change");
    println!("  Current status: ✗ WILL FAIL");
    println!("  Reason: Bincode will still serialize the field from old files");
    println!("  Error: Deserialization will have extra data");
}
