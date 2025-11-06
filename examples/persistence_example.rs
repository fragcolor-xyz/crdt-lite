//! Example demonstrating the persistence layer with WAL and hooks.
//!
//! Run with: cargo run --example persistence_example --features persist

#[cfg(feature = "persist")]
use crdt_lite::persist::{PersistedCRDT, PersistConfig};
#[cfg(feature = "persist")]
use std::path::PathBuf;
#[cfg(feature = "persist")]
use std::sync::mpsc;

#[cfg(feature = "persist")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CRDT Persistence Example ===\n");

    // Create a channel to simulate network broadcasting
    let (tx, rx) = mpsc::channel();

    // Open or create a persisted CRDT
    let data_dir = PathBuf::from("./example_data");
    let config = PersistConfig {
        snapshot_threshold: 5, // Snapshot every 5 changes
        enable_compression: false,
    };

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        data_dir.clone(),
        1, // node_id
        config,
    )?;

    // Add a post-operation hook to broadcast changes
    let tx_clone = tx.clone();
    pcrdt.add_post_hook(Box::new(move |changes: &[crdt_lite::Change<String, String, String>]| {
        println!("📡 Broadcasting {} changes to network", changes.len());
        let _ = tx_clone.send(changes.to_vec());
    }));

    println!("✓ Opened PersistedCRDT at {:?}\n", data_dir);

    // Insert some user records
    println!("Inserting user records...");
    pcrdt.insert_or_update(
        &"user1".to_string(),
        [
            ("name".to_string(), "Alice".to_string()),
            ("email".to_string(), "alice@example.com".to_string()),
            ("role".to_string(), "admin".to_string()),
        ]
        .into_iter(),
    )?;

    pcrdt.insert_or_update(
        &"user2".to_string(),
        [
            ("name".to_string(), "Bob".to_string()),
            ("email".to_string(), "bob@example.com".to_string()),
            ("role".to_string(), "user".to_string()),
        ]
        .into_iter(),
    )?;

    println!("✓ Inserted 2 users (6 field changes total)\n");

    // Update a user
    println!("Updating user1's role...");
    pcrdt.insert_or_update(
        &"user1".to_string(),
        [("role".to_string(), "superadmin".to_string())].into_iter(),
    )?;

    println!("✓ Updated user1\n");

    // Check if snapshot was created
    println!(
        "Changes since last snapshot: {}",
        pcrdt.changes_since_snapshot()
    );
    println!("Note: Automatic snapshot triggers at 5 changes\n");

    // Get all changes for sync
    let all_changes = pcrdt.get_changes_since(0);
    println!("Total changes in CRDT: {}\n", all_changes.len());

    // Simulate receiving changes from network
    println!("Checking for network broadcasts...");
    let mut total_broadcasts = 0;
    while let Ok(_changes) = rx.try_recv() {
        total_broadcasts += 1;
    }
    println!("✓ Received {} broadcast notifications\n", total_broadcasts);

    // Query the data
    println!("Current data:");
    if let Some(user1) = pcrdt.crdt().get_record(&"user1".to_string()) {
        println!(
            "  user1: name={}, email={}, role={}",
            user1.fields.get("name").unwrap(),
            user1.fields.get("email").unwrap(),
            user1.fields.get("role").unwrap()
        );
    }

    if let Some(user2) = pcrdt.crdt().get_record(&"user2".to_string()) {
        println!(
            "  user2: name={}, email={}, role={}",
            user2.fields.get("name").unwrap(),
            user2.fields.get("email").unwrap(),
            user2.fields.get("role").unwrap()
        );
    }

    println!("\n✓ All operations persisted to disk!");
    println!("  Snapshot: {:?}", data_dir.join("snapshot.bin"));
    println!("  WAL files: {:?}/wal_*.bin", data_dir);

    // Demonstrate recovery
    println!("\n=== Testing Recovery ===");
    drop(pcrdt); // Close the CRDT

    println!("Reopening from disk...");
    let recovered =
        PersistedCRDT::<String, String, String>::open(data_dir.clone(), 1, PersistConfig::default())?;

    println!("✓ Successfully recovered from persistence!");
    println!(
        "  Clock time: {}",
        recovered.crdt().get_clock().current_time()
    );

    // Verify data is intact
    if let Some(user1) = recovered.crdt().get_record(&"user1".to_string()) {
        println!(
            "  user1 recovered: role={}",
            user1.fields.get("role").unwrap()
        );
        assert_eq!(user1.fields.get("role").unwrap(), "superadmin");
    }

    println!("\n✨ Persistence example complete!");
    println!("Data directory: {:?}", data_dir);
    println!("Run again to see recovery in action.");

    Ok(())
}

#[cfg(not(feature = "persist"))]
fn main() {
    eprintln!("This example requires the 'persist' feature.");
    eprintln!("Run with: cargo run --example persistence_example --features persist");
    std::process::exit(1);
}
