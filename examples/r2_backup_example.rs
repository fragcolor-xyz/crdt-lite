//! Example demonstrating snapshot and WAL hooks for R2 backup.
//!
//! Run with: cargo run --example r2_backup_example --features persist

#[cfg(feature = "persist")]
use crdt_lite::persist::{PersistedCRDT, PersistConfig};
#[cfg(feature = "persist")]
use std::path::PathBuf;
#[cfg(feature = "persist")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "persist")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CRDT with R2 Backup Hooks ===\n");

    // Simulate R2 backup tracking
    let uploaded_snapshots = Arc::new(Mutex::new(Vec::new()));
    let uploaded_wal_segments = Arc::new(Mutex::new(Vec::new()));

    // Open persisted CRDT
    let data_dir = PathBuf::from("./r2_backup_example_data");
    let config = PersistConfig {
        snapshot_threshold: 3, // Snapshot every 3 changes for demo
        enable_compression: false,
    };

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        data_dir.clone(),
        1, // node_id
        config,
    )?;

    // Add snapshot hook - simulates uploading to R2
    let snapshots_clone = uploaded_snapshots.clone();
    pcrdt.add_snapshot_hook(Box::new(move |snapshot_path: &PathBuf| {
        println!("📸 Snapshot created: {:?}", snapshot_path.file_name().unwrap());
        println!("   └─ Uploading to R2...");

        // In real app: tokio::spawn(async move { r2.put(...) })
        // For demo: just track it
        snapshots_clone.lock().unwrap().push(snapshot_path.clone());

        println!("   └─ ✓ Uploaded to R2");
    }));

    // Add WAL segment hook - simulates uploading sealed segments to R2
    let wal_clone = uploaded_wal_segments.clone();
    pcrdt.add_wal_segment_hook(Box::new(move |segment_path: &PathBuf| {
        println!("📝 WAL segment sealed: {:?}", segment_path.file_name().unwrap());
        println!("   └─ Uploading to R2...");

        // In real app: tokio::spawn(async move { r2.put(...) })
        // For demo: just track it
        wal_clone.lock().unwrap().push(segment_path.clone());

        println!("   └─ ✓ Uploaded to R2");
    }));

    println!("✓ CRDT opened with backup hooks\n");

    // Insert data - will trigger snapshots and hooks
    println!("Inserting records...\n");

    for i in 1..=10 {
        pcrdt.insert_or_update(
            &format!("user{}", i),
            [
                ("name".to_string(), format!("User {}", i)),
                ("email".to_string(), format!("user{}@example.com", i)),
            ]
            .into_iter(),
        )?;

        println!("  → Inserted user{} ({} changes since snapshot)",
                 i, pcrdt.changes_since_snapshot());

        // Small delay to show progression
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    println!("\n✅ All operations complete!\n");

    // Show backup status
    let snapshots = uploaded_snapshots.lock().unwrap();
    let wal_segments = uploaded_wal_segments.lock().unwrap();

    println!("📦 Backup Summary:");
    println!("  Snapshots uploaded: {}", snapshots.len());
    for (i, path) in snapshots.iter().enumerate() {
        println!("    {}. {:?}", i + 1, path.file_name().unwrap());
    }

    println!("\n  WAL segments uploaded: {}", wal_segments.len());
    for (i, path) in wal_segments.iter().enumerate() {
        println!("    {}. {:?}", i + 1, path.file_name().unwrap());
    }

    // Demonstrate cleanup
    println!("\n🧹 Cleanup demo:");
    println!("  Before cleanup:");
    let snapshot_count_before = std::fs::read_dir(&data_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .count();
    println!("    Local snapshots: {}", snapshot_count_before);

    // Keep only last 2 snapshots locally (rest are in R2)
    pcrdt.cleanup_old_snapshots(2)?;

    let snapshot_count_after = std::fs::read_dir(&data_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .count();
    println!("  After cleanup (keep 2):");
    println!("    Local snapshots: {}", snapshot_count_after);
    println!("    (Old snapshots safely backed up in R2)");

    println!("\n💡 Recovery flow:");
    println!("  1. Container starts → Download latest snapshot from R2");
    println!("  2. Download WAL segments since snapshot");
    println!("  3. PersistedCRDT::open() recovers from local files");
    println!("  4. Continue operations with hooks uploading new changes");

    println!("\n✨ Example complete! Data in: {:?}", data_dir);

    Ok(())
}

#[cfg(not(feature = "persist"))]
fn main() {
    eprintln!("This example requires the 'persist' feature.");
    eprintln!("Run with: cargo run --example r2_backup_example --features persist");
    std::process::exit(1);
}
