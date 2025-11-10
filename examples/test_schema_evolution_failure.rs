//! Concrete test demonstrating schema evolution failure
//!
//! This test creates a snapshot with the current schema,
//! then tries to deserialize it as if we had added a new field.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Original Record structure (what's currently in lib.rs)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordV1 {
    fields: HashMap<String, String>,
    column_versions: HashMap<String, u64>,
    lowest_local_db_version: u64,
    highest_local_db_version: u64,
}

// New Record structure with added field (simulating schema change)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordV2NoDefault {
    fields: HashMap<String, String>,
    column_versions: HashMap<String, u64>,
    lowest_local_db_version: u64,
    highest_local_db_version: u64,
    // NEW FIELD: This would break deserialization of old data
    last_modified_timestamp: u64,
}

// New Record structure with added field AND default attribute
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecordV2WithDefault {
    fields: HashMap<String, String>,
    column_versions: HashMap<String, u64>,
    lowest_local_db_version: u64,
    highest_local_db_version: u64,
    // NEW FIELD with default: This would work!
    #[serde(default)]
    last_modified_timestamp: u64,
}

fn main() {
    println!("=== Demonstrating Schema Evolution Behavior ===\n");

    // Create a V1 record and serialize it
    println!("Step 1: Create and serialize RecordV1");
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), "Alice".to_string());
    fields.insert("age".to_string(), "30".to_string());

    let mut versions = HashMap::new();
    versions.insert("name".to_string(), 1u64);
    versions.insert("age".to_string(), 1u64);

    let record_v1 = RecordV1 {
        fields,
        column_versions: versions,
        lowest_local_db_version: 1,
        highest_local_db_version: 1,
    };

    let serialized = bincode::serde::encode_to_vec(&record_v1, bincode::config::standard())
        .expect("Failed to serialize");
    println!("  ✓ Serialized RecordV1: {} bytes", serialized.len());
    println!("  Fields: {:?}", record_v1.fields.keys().collect::<Vec<_>>());

    // Try to deserialize as V1 (should work)
    println!("\nStep 2: Deserialize as RecordV1 (same schema)");
    let (deserialized_v1, _): (RecordV1, _) =
        bincode::serde::decode_from_slice(&serialized, bincode::config::standard())
            .expect("Failed to deserialize as V1");
    println!("  ✓ Success! Fields: {:?}", deserialized_v1.fields.keys().collect::<Vec<_>>());

    // Try to deserialize as V2 WITHOUT default (should fail)
    println!("\nStep 3: Deserialize as RecordV2NoDefault (new field, no default)");
    match bincode::serde::decode_from_slice::<RecordV2NoDefault, _>(
        &serialized,
        bincode::config::standard()
    ) {
        Ok(_) => println!("  ✓ Unexpectedly succeeded!"),
        Err(e) => {
            println!("  ✗ FAILED as expected!");
            println!("  Error: {}", e);
            println!("\n  This is what would happen when:");
            println!("    1. You add a field to Record<C, V> without #[serde(default)]");
            println!("    2. Try to load an old snapshot");
            println!("    3. PersistedCRDT::open() would fail");
        }
    }

    // Try to deserialize as V2 WITH default (should work)
    println!("\nStep 4: Deserialize as RecordV2WithDefault (new field WITH default)");
    match bincode::serde::decode_from_slice::<RecordV2WithDefault, _>(
        &serialized,
        bincode::config::standard()
    ) {
        Ok((record, _)) => {
            println!("  ✓ SUCCESS!");
            println!("  Fields: {:?}", record.fields.keys().collect::<Vec<_>>());
            println!("  New field value (defaulted): {}", record.last_modified_timestamp);
            println!("\n  This demonstrates that:");
            println!("    1. Adding fields WITH #[serde(default)] is safe");
            println!("    2. Old snapshots can still be loaded");
            println!("    3. New field gets default value (0 for u64)");
        }
        Err(e) => println!("  ✗ Failed: {}", e),
    }

    println!("\n=== Summary ===");
    println!("Current persist.rs behavior:");
    println!("  • Uses bincode with serde for serialization");
    println!("  • No explicit #[serde(default)] on struct fields");
    println!("  • Adding new fields WILL BREAK loading old snapshots/WAL");
    println!("  • No schema versioning mechanism");
    println!("\nRecommendations:");
    println!("  1. Add #[serde(default)] to all fields that might be added");
    println!("  2. Add schema_version field to track format changes");
    println!("  3. Implement migration logic for incompatible changes");
    println!("  4. Document schema evolution policy for users");
}
