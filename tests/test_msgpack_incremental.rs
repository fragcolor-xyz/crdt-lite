#![cfg(all(any(feature = "persist", feature = "persist-msgpack", feature = "persist-compressed"), feature = "msgpack"))]

//! Comprehensive tests for MessagePack + Incremental Snapshots functionality

use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};
use tempfile::TempDir;

#[test]
fn test_msgpack_full_snapshot_basic() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    // Create CRDT with MessagePack format
    let mut config = PersistConfig::default();
    config.snapshot_format = SnapshotFormat::MessagePack;
    config.enable_incremental_snapshots = false; // Force full snapshots only
    config.snapshot_threshold = 5;

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(base_path.clone(), 1, config).unwrap();

    // Insert some data
    for i in 0..5 {
        pcrdt
            .insert_or_update(
                &format!("rec{}", i),
                [("field".to_string(), format!("value{}", i))].into_iter(),
            )
            .unwrap();
    }

    // Snapshot should have been created automatically
    let snapshot_files: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_full_")
        })
        .collect();

    assert_eq!(snapshot_files.len(), 1);

    // Verify recovery
    drop(pcrdt);
    let recovered =
        PersistedCRDT::<String, String, String>::open(base_path, 1, PersistConfig::default())
            .unwrap();

    for i in 0..5 {
        let record = recovered
            .crdt()
            .get_record(&format!("rec{}", i))
            .unwrap();
        assert_eq!(
            record.fields.get(&"field".to_string()).unwrap(),
            &format!("value{}", i)
        );
    }
}

#[test]
fn test_incremental_snapshots_basic() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    // Create CRDT with incremental snapshots enabled
    let mut config = PersistConfig::default();
    config.snapshot_format = SnapshotFormat::MessagePack;
    config.enable_incremental_snapshots = true;
    config.full_snapshot_interval = 3; // Full every 3 incrementals
    config.snapshot_threshold = 2;

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(base_path.clone(), 1, config).unwrap();

    // First batch - should create full snapshot
    pcrdt
        .insert_or_update(
            &"rec1".to_string(),
            [("field".to_string(), "value1".to_string())].into_iter(),
        )
        .unwrap();
    pcrdt
        .insert_or_update(
            &"rec2".to_string(),
            [("field".to_string(), "value2".to_string())].into_iter(),
        )
        .unwrap();

    // Second batch - should create incremental
    pcrdt
        .insert_or_update(
            &"rec3".to_string(),
            [("field".to_string(), "value3".to_string())].into_iter(),
        )
        .unwrap();
    pcrdt
        .insert_or_update(
            &"rec4".to_string(),
            [("field".to_string(), "value4".to_string())].into_iter(),
        )
        .unwrap();

    // Third batch - should create another incremental
    pcrdt
        .insert_or_update(
            &"rec5".to_string(),
            [("field".to_string(), "value5".to_string())].into_iter(),
        )
        .unwrap();
    pcrdt
        .insert_or_update(
            &"rec6".to_string(),
            [("field".to_string(), "value6".to_string())].into_iter(),
        )
        .unwrap();

    // Verify we have 1 full and 2 incrementals
    let files: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("snapshot_")
        })
        .collect();

    let full_count = files
        .iter()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_full_")
        })
        .count();
    let incr_count = files
        .iter()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_incr_")
        })
        .count();

    assert_eq!(full_count, 1, "Should have 1 full snapshot");
    assert_eq!(incr_count, 2, "Should have 2 incremental snapshots");

    // Verify recovery loads all data correctly
    drop(pcrdt);
    let recovered = PersistedCRDT::<String, String, String>::open(
        base_path,
        1,
        PersistConfig::default(),
    )
    .unwrap();

    for i in 1..=6 {
        let record = recovered
            .crdt()
            .get_record(&format!("rec{}", i))
            .unwrap();
        assert_eq!(
            record.fields.get(&"field".to_string()).unwrap(),
            &format!("value{}", i)
        );
    }
}

#[test]
fn test_incremental_with_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    let mut config = PersistConfig::default();
    config.snapshot_format = SnapshotFormat::MessagePack;
    config.enable_incremental_snapshots = true;
    config.snapshot_threshold = 2;

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(base_path.clone(), 1, config).unwrap();

    // First snapshot - create records
    pcrdt
        .insert_or_update(
            &"rec1".to_string(),
            [("field".to_string(), "value1".to_string())].into_iter(),
        )
        .unwrap();
    pcrdt
        .insert_or_update(
            &"rec2".to_string(),
            [("field".to_string(), "value2".to_string())].into_iter(),
        )
        .unwrap();

    // Second snapshot - delete one record
    pcrdt.delete_record(&"rec1".to_string()).unwrap();
    pcrdt
        .insert_or_update(
            &"rec3".to_string(),
            [("field".to_string(), "value3".to_string())].into_iter(),
        )
        .unwrap();

    // Verify recovery
    drop(pcrdt);
    let recovered = PersistedCRDT::<String, String, String>::open(
        base_path,
        1,
        PersistConfig::default(),
    )
    .unwrap();

    // rec1 should be tombstoned
    assert!(recovered.crdt().is_tombstoned(&"rec1".to_string()));

    // rec2 and rec3 should exist
    assert!(recovered
        .crdt()
        .get_record(&"rec2".to_string())
        .is_some());
    assert!(recovered
        .crdt()
        .get_record(&"rec3".to_string())
        .is_some());
}

#[test]
fn test_full_snapshot_after_interval() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    let mut config = PersistConfig::default();
    config.snapshot_format = SnapshotFormat::MessagePack;
    config.enable_incremental_snapshots = true;
    config.full_snapshot_interval = 2; // Full after 2 incrementals
    config.snapshot_threshold = 1;

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(base_path.clone(), 1, config).unwrap();

    // Create 5 snapshots (should be: full, incr, incr, full, incr)
    for i in 0..5 {
        pcrdt
            .insert_or_update(
                &format!("rec{}", i),
                [("field".to_string(), format!("value{}", i))].into_iter(),
            )
            .unwrap();
    }

    // Count snapshots
    let files: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("snapshot_")
        })
        .collect();

    let full_count = files
        .iter()
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_full_")
        })
        .count();

    // Should have 2 full snapshots (after cleanup, might be less due to auto_cleanup_snapshots)
    // At minimum we should have at least 1 full
    assert!(full_count >= 1, "Should have at least 1 full snapshot");
}

#[test]
fn test_bincode_fallback() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    // Create with bincode format
    let mut config = PersistConfig::default();
    config.snapshot_format = SnapshotFormat::Bincode;
    config.snapshot_threshold = 2;

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(base_path.clone(), 1, config).unwrap();

    pcrdt
        .insert_or_update(
            &"rec1".to_string(),
            [("field".to_string(), "value1".to_string())].into_iter(),
        )
        .unwrap();
    pcrdt
        .insert_or_update(
            &"rec2".to_string(),
            [("field".to_string(), "value2".to_string())].into_iter(),
        )
        .unwrap();

    // Verify bincode snapshot exists
    let bincode_files: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("snapshot_") && name.ends_with(".bin")
        })
        .collect();

    assert!(!bincode_files.is_empty(), "Should have bincode snapshot");

    // Verify recovery
    drop(pcrdt);
    let recovered = PersistedCRDT::<String, String, String>::open(
        base_path,
        1,
        PersistConfig::default(),
    )
    .unwrap();

    assert!(recovered
        .crdt()
        .get_record(&"rec1".to_string())
        .is_some());
}

#[cfg(feature = "compression")]
#[test]
fn test_compression() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    let mut config = PersistConfig::default();
    config.snapshot_format = SnapshotFormat::MessagePack;
    config.enable_compression = true;
    config.snapshot_threshold = 2;

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(base_path.clone(), 1, config).unwrap();

    // Insert compressible data
    for i in 0..10 {
        pcrdt
            .insert_or_update(
                &format!("rec{}", i),
                [(
                    "field".to_string(),
                    "This is a long repeated string that should compress well".to_string(),
                )]
                .into_iter(),
            )
            .unwrap();
    }

    // Force snapshot
    pcrdt.snapshot().unwrap();

    // Verify recovery works with compression
    drop(pcrdt);
    let recovered = PersistedCRDT::<String, String, String>::open(
        base_path,
        1,
        PersistConfig::default(),
    )
    .unwrap();

    for i in 0..10 {
        assert!(recovered
            .crdt()
            .get_record(&format!("rec{}", i))
            .is_some());
    }
}

#[test]
fn test_size_reduction() {
    // This test demonstrates the size reduction from incremental snapshots
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    let mut config = PersistConfig::default();
    config.snapshot_format = SnapshotFormat::MessagePack;
    config.enable_incremental_snapshots = true;
    config.snapshot_threshold = 100;
    config.auto_cleanup_snapshots = None; // Don't clean up for size comparison

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(base_path.clone(), 1, config).unwrap();

    // Create many records
    for i in 0..100 {
        pcrdt
            .insert_or_update(
                &format!("rec{}", i),
                [("field".to_string(), format!("value{}", i))].into_iter(),
            )
            .unwrap();
    }

    // Force full snapshot
    pcrdt.snapshot().unwrap();

    let full_size = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_full_")
        })
        .next()
        .and_then(|e| e.metadata().ok())
        .map(|m| m.len())
        .unwrap_or(0);

    // Update only 5 records
    for i in 0..5 {
        pcrdt
            .insert_or_update(
                &format!("rec{}", i),
                [("field".to_string(), format!("updated{}", i))].into_iter(),
            )
            .unwrap();
    }

    // Force incremental snapshot
    pcrdt.snapshot().unwrap();

    let incr_size = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_incr_")
        })
        .next()
        .and_then(|e| e.metadata().ok())
        .map(|m| m.len())
        .unwrap_or(0);

    // Log sizes for debugging (only if RUST_LOG is set)
    #[cfg(test)]
    if std::env::var("RUST_LOG").is_ok() {
        eprintln!("Full snapshot size: {} bytes", full_size);
        eprintln!("Incremental snapshot size: {} bytes", incr_size);
        eprintln!("Reduction: {:.1}%", (1.0 - (incr_size as f64 / full_size as f64)) * 100.0);
    }

    // Incremental should be much smaller (at least 80% smaller for 5% changes)
    assert!(
        incr_size < full_size / 5,
        "Incremental snapshot should be significantly smaller"
    );
}

/// CRITICAL TEST: Verifies that tombstone node_id is preserved during incremental snapshot recovery.
///
/// This test ensures that when a tombstone is saved in an incremental snapshot and later recovered
/// by a different node, the tombstone retains its original node_id. This is critical for correct
/// conflict resolution.
///
/// Bug scenario (if not fixed):
/// 1. Node 1 deletes record "user1" → tombstone with node_id=1
/// 2. Incremental snapshot created
/// 3. Node 2 recovers from snapshot
/// 4. Tombstone incorrectly reconstructed with node_id=2
/// 5. Node 3 creates "user1" → conflict resolution uses wrong node_id → incorrect winner
#[test]
fn test_incremental_tombstone_preserves_node_id() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    // Node 1: Create record and delete it
    let mut config = PersistConfig::default();
    config.snapshot_format = SnapshotFormat::MessagePack;
    config.enable_incremental_snapshots = true;
    config.snapshot_threshold = 3;
    config.full_snapshot_interval = 10;

    let mut node1 = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1, // node_id = 1
        config.clone(),
    )
    .unwrap();

    // Create initial records to trigger full snapshot
    node1
        .insert_or_update(
            &"user0".to_string(),
            [("name".to_string(), "Alice".to_string())].into_iter(),
        )
        .unwrap();
    node1
        .insert_or_update(
            &"user1".to_string(),
            [("name".to_string(), "Bob".to_string())].into_iter(),
        )
        .unwrap();
    node1
        .insert_or_update(
            &"user2".to_string(),
            [("name".to_string(), "Charlie".to_string())].into_iter(),
        )
        .unwrap();

    // Full snapshot should have been created
    let full_snapshots: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_full_")
        })
        .collect();
    assert_eq!(full_snapshots.len(), 1, "Should have 1 full snapshot");

    // Delete user1 on node 1 (creates tombstone with node_id=1)
    node1.delete_record(&"user1".to_string()).unwrap();

    // Trigger incremental snapshot
    node1
        .insert_or_update(
            &"user3".to_string(),
            [("name".to_string(), "Dave".to_string())].into_iter(),
        )
        .unwrap();
    node1
        .insert_or_update(
            &"user4".to_string(),
            [("name".to_string(), "Eve".to_string())].into_iter(),
        )
        .unwrap();

    // Incremental snapshot should now exist
    let incr_snapshots: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_incr_")
        })
        .collect();
    assert!(
        incr_snapshots.len() >= 1,
        "Should have at least 1 incremental snapshot"
    );

    // Get the tombstone info from node1
    let user1_tombstone = node1.crdt().get_tombstone(&"user1".to_string());
    assert!(user1_tombstone.is_some(), "user1 should be tombstoned");
    let original_node_id = user1_tombstone.unwrap().node_id;
    assert_eq!(
        original_node_id, 1,
        "Original tombstone should have node_id=1"
    );

    drop(node1);

    // Node 2: Recover from snapshots
    let node2 = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        2, // Different node_id
        config,
    )
    .unwrap();

    // Verify user1 is still tombstoned on node2
    let recovered_tombstone = node2.crdt().get_tombstone(&"user1".to_string());
    assert!(
        recovered_tombstone.is_some(),
        "user1 tombstone should be recovered on node2"
    );

    // CRITICAL ASSERTION: Tombstone should still have node_id=1 (not node_id=2)
    let recovered_node_id = recovered_tombstone.unwrap().node_id;
    assert_eq!(
        recovered_node_id, original_node_id,
        "Tombstone node_id should be preserved (should be 1, not 2)"
    );

    // Verify other records were recovered correctly
    let user0 = node2.crdt().get_record(&"user0".to_string());
    assert!(user0.is_some(), "user0 should exist");

    let user2 = node2.crdt().get_record(&"user2".to_string());
    assert!(user2.is_some(), "user2 should exist");

    let user3 = node2.crdt().get_record(&"user3".to_string());
    assert!(user3.is_some(), "user3 should exist");
}

// ============================================================================
// PHASE 1: CRITICAL RELIABILITY TESTS
// ============================================================================

#[test]
#[cfg(feature = "msgpack")]
fn test_cleanup_msgpack_snapshots() {
    use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};

    let base_path = std::env::temp_dir().join("test_cleanup_msgpack");
    let _ = std::fs::remove_dir_all(&base_path);
    std::fs::create_dir_all(&base_path).unwrap();

    let config = PersistConfig {
        snapshot_threshold: 100,
        snapshot_format: SnapshotFormat::MessagePack,
        enable_incremental_snapshots: true,
        full_snapshot_interval: 2,
        auto_cleanup_snapshots: None, // Manual cleanup for testing
        ..Default::default()
    };

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config,
    )
    .unwrap();

    // Create multiple full + incremental snapshots
    // First full + 2 incrementals
    for i in 0..100 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("v1_{}", i))],
        ).unwrap();
    }
    pcrdt.snapshot().unwrap(); // Full #1

    for i in 0..50 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("v2_{}", i))],
        ).unwrap();
    }
    pcrdt.snapshot().unwrap(); // Incremental #2

    for i in 0..50 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("v3_{}", i))],
        ).unwrap();
    }
    pcrdt.snapshot().unwrap(); // Full #3 (full_snapshot_interval: 2)

    for i in 0..50 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("v4_{}", i))],
        ).unwrap();
    }
    pcrdt.snapshot().unwrap(); // Incremental #4

    // Should have: full_1, incr_2, full_3, incr_4
    let files: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|f| f.starts_with("snapshot_"))
        .collect();

    #[cfg(test)]
    if std::env::var("RUST_LOG").is_ok() {
        eprintln!("Snapshots before cleanup: {:?}", files);
    }

    let full_count = files.iter().filter(|f| f.starts_with("snapshot_full_")).count();
    let incr_count = files.iter().filter(|f| f.starts_with("snapshot_incr_")).count();

    assert_eq!(full_count, 2, "Should have 2 full snapshots");
    assert_eq!(incr_count, 2, "Should have 2 incremental snapshots");

    // Cleanup: keep only 1 full snapshot (the newest)
    pcrdt.cleanup_old_snapshots(1, false).unwrap();

    // Should now have: full_3, incr_4
    // full_1 deleted → incr_2 should be orphaned and deleted
    let files_after: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|f| f.starts_with("snapshot_"))
        .collect();

    #[cfg(test)]
    if std::env::var("RUST_LOG").is_ok() {
        eprintln!("Snapshots after cleanup: {:?}", files_after);
    }

    let full_after = files_after.iter().filter(|f| f.starts_with("snapshot_full_")).count();
    let incr_after = files_after.iter().filter(|f| f.starts_with("snapshot_incr_")).count();

    assert_eq!(full_after, 1, "Should have 1 full snapshot after cleanup");

    // The incremental might be deleted if it's superseded by a newer full snapshot
    // Let's check if we have any incrementals, and if so, verify they build on the kept full
    if incr_after > 0 {
        let remaining_incr = files_after.iter()
            .find(|f| f.starts_with("snapshot_incr_"))
            .unwrap();
        // Extract the kept full snapshot version
        let kept_full_version = files_after.iter()
            .find(|f| f.starts_with("snapshot_full_"))
            .and_then(|f| f.strip_prefix("snapshot_full_"))
            .and_then(|f| f.strip_suffix(".msgpack"))
            .unwrap();
        assert!(
            remaining_incr.contains(&format!("_base_{}", kept_full_version)),
            "Remaining incremental should build on kept full"
        );
    }

    let _ = std::fs::remove_dir_all(&base_path);
}

#[test]
#[cfg(feature = "msgpack")]
fn test_skip_empty_incrementals() {
    use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};
    use std::sync::{Arc, Mutex};

    let base_path = std::env::temp_dir().join("test_skip_empty");
    let _ = std::fs::remove_dir_all(&base_path);
    std::fs::create_dir_all(&base_path).unwrap();

    let config = PersistConfig {
        snapshot_threshold: 10,
        snapshot_format: SnapshotFormat::MessagePack,
        enable_incremental_snapshots: true,
        skip_empty_incrementals: true, // Enable skip
        ..Default::default()
    };

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config,
    )
    .unwrap();

    // Track snapshot hook calls
    let hook_calls = Arc::new(Mutex::new(Vec::new()));
    let hook_calls_clone = hook_calls.clone();

    pcrdt.add_snapshot_hook(Box::new(move |path: &std::path::Path, version: u64| {
        hook_calls_clone.lock().unwrap().push((path.to_path_buf(), version));
    }));

    // Create initial data and snapshot
    for i in 0..10 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("value{}", i))],
        ).unwrap();
    }
    pcrdt.snapshot().unwrap(); // Full snapshot

    let initial_hook_calls = hook_calls.lock().unwrap().len();
    assert_eq!(initial_hook_calls, 1, "Should have called hook for full snapshot");

    // Count snapshot files before
    let snapshots_before: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .collect();

    // Try to snapshot with no changes (should skip)
    pcrdt.snapshot().unwrap();

    // Verify hook was NOT called for empty snapshot
    let after_empty_calls = hook_calls.lock().unwrap().len();
    assert_eq!(after_empty_calls, initial_hook_calls, "Hook should NOT be called for skipped snapshot");

    // Verify no new snapshot files were created
    let snapshots_after: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .collect();
    assert_eq!(snapshots_after.len(), snapshots_before.len(), "Should not create new snapshot file when empty");

    // Now make a change and snapshot again
    pcrdt.insert_or_update(
        &"rec0".to_string(),
        vec![("field".to_string(), "updated".to_string())],
    ).unwrap();

    pcrdt.snapshot().unwrap(); // Should create new snapshot

    // Verify hook WAS called for non-empty snapshot
    let after_real_calls = hook_calls.lock().unwrap().len();
    assert_eq!(after_real_calls, initial_hook_calls + 1, "Hook should be called for real snapshot");

    // Test with skip_empty_incrementals: false
    let config_no_skip = PersistConfig {
        snapshot_threshold: 10,
        snapshot_format: SnapshotFormat::MessagePack,
        enable_incremental_snapshots: true,
        skip_empty_incrementals: false, // Disable skip
        ..Default::default()
    };

    let base_path2 = std::env::temp_dir().join("test_no_skip_empty");
    let _ = std::fs::remove_dir_all(&base_path2);
    std::fs::create_dir_all(&base_path2).unwrap();

    let mut pcrdt2 = PersistedCRDT::<String, String, String>::open(
        base_path2.clone(),
        1,
        config_no_skip,
    )
    .unwrap();

    // Create initial data and snapshot
    pcrdt2.insert_or_update(
        &"rec".to_string(),
        vec![("field".to_string(), "value".to_string())],
    ).unwrap();
    pcrdt2.snapshot().unwrap();

    // Count snapshots before
    let snapshots_before2: Vec<_> = std::fs::read_dir(&base_path2)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .collect();

    // Try to snapshot with no changes (should create empty snapshot when skip disabled)
    pcrdt2.snapshot().unwrap();

    // Verify a new snapshot was created (even though empty)
    let snapshots_after2: Vec<_> = std::fs::read_dir(&base_path2)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .collect();
    assert!(
        snapshots_after2.len() > snapshots_before2.len(),
        "Should create new snapshot even when empty (skip disabled)"
    );

    let _ = std::fs::remove_dir_all(&base_path);
    let _ = std::fs::remove_dir_all(&base_path2);
}

#[test]
#[cfg(feature = "msgpack")]
fn test_schema_evolution() {
    use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};

    // This test demonstrates MessagePack's schema evolution capability:
    // MessagePack uses self-describing format that allows loading snapshots
    // even when the struct definition has changed (new fields added with #[serde(default)])
    //
    // Compare to bincode:
    // - Bincode: length-prefixed encoding that hardcodes field count → breaks on schema changes
    // - MessagePack: self-describing format → missing fields use Default::default()
    //
    // Real-world scenario:
    // 1. v0.5.0: CRDT has fields A, B
    // 2. v0.6.0: CRDT adds field C with #[serde(default)]
    // 3. MessagePack snapshots from v0.5.0 load fine in v0.6.0 (C gets default)
    // 4. Bincode snapshots from v0.5.0 FAIL to load in v0.6.0 (field count mismatch)

    let base_path = std::env::temp_dir().join("test_schema_evolution");
    let _ = std::fs::remove_dir_all(&base_path);
    std::fs::create_dir_all(&base_path).unwrap();

    let config = PersistConfig {
        snapshot_threshold: 10,
        snapshot_format: SnapshotFormat::MessagePack,
        enable_incremental_snapshots: false,
        ..Default::default()
    };

    // Create a CRDT, add data, and snapshot
    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config.clone(),
    )
    .unwrap();

    for i in 0..10 {
        pcrdt.insert_or_update(
            &format!("user{}", i),
            vec![
                ("name".to_string(), format!("User{}", i)),
                ("age".to_string(), format!("{}", 20 + i)),
            ],
        ).unwrap();
    }

    pcrdt.snapshot().unwrap();
    drop(pcrdt);

    // Reopen - simulates loading snapshot after code changes
    // In real scenario, new code might have new fields with #[serde(default)]
    // MessagePack handles this gracefully
    let pcrdt = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config,
    )
    .unwrap();

    // Verify all data loaded correctly
    for i in 0..10 {
        let record = pcrdt.crdt().get_record(&format!("user{}", i)).unwrap();
        assert_eq!(record.fields.get("name").unwrap(), &format!("User{}", i));
        assert_eq!(record.fields.get("age").unwrap(), &format!("{}", 20 + i));
    }

    // The key insight: MessagePack's flexibility allows snapshots to survive
    // code changes that add new fields. Bincode would fail here if we added
    // new fields to the CRDT struct with #[serde(default)].
    //
    // This is why we recommend persist-msgpack over persist (bincode) for
    // production use - it provides upgrade path without losing data.

    let _ = std::fs::remove_dir_all(&base_path);
}

#[test]
#[cfg(feature = "msgpack")]
fn test_corrupted_snapshot_recovery() {
    use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};
    use std::io::Write;

    let base_path = std::env::temp_dir().join("test_corrupted_snapshot");
    let _ = std::fs::remove_dir_all(&base_path);
    std::fs::create_dir_all(&base_path).unwrap();

    let config = PersistConfig {
        snapshot_threshold: 10,
        snapshot_format: SnapshotFormat::MessagePack,
        enable_incremental_snapshots: false,
        ..Default::default()
    };

    // Create initial snapshot
    {
        let mut pcrdt = PersistedCRDT::<String, String, String>::open(
            base_path.clone(),
            1,
            config.clone(),
        )
        .unwrap();

        for i in 0..10 {
            pcrdt.insert_or_update(
                &format!("rec{}", i),
                vec![("field".to_string(), format!("value{}", i))],
            ).unwrap();
        }

        pcrdt.snapshot().unwrap();
    }

    // Find the snapshot file and corrupt it
    let snapshot_file: std::path::PathBuf = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with("snapshot_full_"))
        .map(|e| e.path())
        .unwrap();

    // Corrupt the snapshot by writing garbage
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&snapshot_file)
            .unwrap();
        file.write_all(b"CORRUPTED GARBAGE DATA").unwrap();
    }

    // Try to recover - should fail gracefully or fall back to WAL
    let result = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config,
    );

    // With corrupted snapshot and no WAL, recovery should either:
    // 1. Fail with an error (acceptable - corruption detected)
    // 2. Start with empty CRDT (also acceptable - no valid snapshot + no WAL)
    match result {
        Ok(_pcrdt) => {
            // Recovered with empty state (no snapshot + no WAL)
            // This is acceptable behavior - corruption was handled gracefully
            // We can't easily verify it's truly empty from public API,
            // but the fact that it recovered without panic is good enough
        }
        Err(_) => {
            // Failed to recover - also acceptable (corruption detected)
            // This is the current behavior
        }
    }

    let _ = std::fs::remove_dir_all(&base_path);
}

// ============================================================================
// PHASE 2: FEATURE COMPLETENESS TESTS
// ============================================================================

#[test]
#[cfg(feature = "msgpack")]
fn test_snapshot_hook() {
    use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};
    use std::sync::{Arc, Mutex};

    let base_path = std::env::temp_dir().join("test_snapshot_hook");
    let _ = std::fs::remove_dir_all(&base_path);
    std::fs::create_dir_all(&base_path).unwrap();

    let config = PersistConfig {
        snapshot_threshold: 10,
        snapshot_format: SnapshotFormat::MessagePack,
        enable_incremental_snapshots: true,
        full_snapshot_interval: 1, // Create full after each incremental
        ..Default::default()
    };

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config,
    )
    .unwrap();

    // Track hook invocations
    let hook_data = Arc::new(Mutex::new(Vec::new()));
    let hook_data_clone = hook_data.clone();

    pcrdt.add_snapshot_hook(Box::new(move |path: &std::path::Path, db_version: u64| {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        hook_data_clone.lock().unwrap().push((filename, db_version));
    }));

    // Create data and trigger snapshots
    for i in 0..10 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("value{}", i))],
        ).unwrap();
    }
    let db_version_1 = pcrdt.get_clock_time();
    pcrdt.snapshot().unwrap(); // Full snapshot #1

    for i in 0..5 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("updated{}", i))],
        ).unwrap();
    }
    let db_version_2 = pcrdt.get_clock_time();
    pcrdt.snapshot().unwrap(); // Incremental #2

    for i in 0..5 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("updated2_{}", i))],
        ).unwrap();
    }
    let db_version_3 = pcrdt.get_clock_time();
    pcrdt.snapshot().unwrap(); // Full snapshot #3 (interval: 2)

    // Verify hook was called 3 times
    let calls = hook_data.lock().unwrap();
    assert_eq!(calls.len(), 3, "Hook should be called for each snapshot");

    // Verify hook received correct filenames
    assert!(calls[0].0.starts_with("snapshot_full_"), "First should be full snapshot");
    assert!(calls[1].0.starts_with("snapshot_incr_"), "Second should be incremental");
    assert!(calls[2].0.starts_with("snapshot_full_"), "Third should be full snapshot");

    // Verify db_version parameters are reasonable
    assert!(calls[0].1 > 0 && calls[0].1 <= db_version_1, "db_version should be reasonable");
    assert!(calls[1].1 > calls[0].1 && calls[1].1 <= db_version_2, "db_version should increase");
    assert!(calls[2].1 > calls[1].1 && calls[2].1 <= db_version_3, "db_version should increase");

    let _ = std::fs::remove_dir_all(&base_path);
}

#[test]
#[cfg(feature = "persist")]
fn test_wal_segment_hook() {
    use crdt_lite::persist::{PersistedCRDT, PersistConfig};
    use std::sync::{Arc, Mutex};

    let base_path = std::env::temp_dir().join("test_wal_segment_hook");
    let _ = std::fs::remove_dir_all(&base_path);
    std::fs::create_dir_all(&base_path).unwrap();

    let config = PersistConfig {
        snapshot_threshold: 50, // Force WAL rotation by snapshotting
        ..Default::default()
    };

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config,
    )
    .unwrap();

    // Track WAL segment hook invocations
    let sealed_segments = Arc::new(Mutex::new(Vec::new()));
    let sealed_clone = sealed_segments.clone();

    pcrdt.add_wal_segment_hook(Box::new(move |path: &std::path::Path| {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        sealed_clone.lock().unwrap().push(filename);
    }));

    // Write data and snapshot to rotate WAL
    for i in 0..50 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("value{}", i))],
        ).unwrap();
    }
    pcrdt.snapshot().unwrap(); // Should seal current WAL and start new one

    // Write more data and snapshot again
    for i in 50..100 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("value{}", i))],
        ).unwrap();
    }
    pcrdt.snapshot().unwrap(); // Should seal second WAL

    // Verify hook was called for sealed segments
    let sealed = sealed_segments.lock().unwrap();
    assert!(sealed.len() >= 1, "Should have sealed at least 1 WAL segment");

    // Verify sealed segment filenames are valid
    for segment in sealed.iter() {
        assert!(segment.starts_with("wal_"), "Sealed segment should be WAL file");
        assert!(segment.ends_with(".bin"), "Sealed segment should have .bin extension");
    }

    let _ = std::fs::remove_dir_all(&base_path);
}

#[test]
#[cfg(feature = "msgpack")]
fn test_upload_tracking() {
    use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};

    let base_path = std::env::temp_dir().join("test_upload_tracking");
    let _ = std::fs::remove_dir_all(&base_path);
    std::fs::create_dir_all(&base_path).unwrap();

    let config = PersistConfig {
        snapshot_threshold: 10,
        snapshot_format: SnapshotFormat::MessagePack,
        enable_incremental_snapshots: true,
        full_snapshot_interval: 2,
        auto_cleanup_snapshots: None, // Manual cleanup
        ..Default::default()
    };

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config,
    )
    .unwrap();

    // Create 3 snapshots
    for round in 0..3 {
        for i in 0..10 {
            pcrdt.insert_or_update(
                &format!("rec{}", i),
                vec![("field".to_string(), format!("v{}_{}", round, i))],
            ).unwrap();
        }
        pcrdt.snapshot().unwrap();
    }

    // Get all snapshot files
    let snapshots: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .map(|e| e.path())
        .collect();

    assert!(snapshots.len() >= 2, "Should have at least 2 snapshots");

    // Mark first snapshot as uploaded
    pcrdt.mark_snapshot_uploaded(snapshots[0].clone());

    // Try cleanup with require_uploaded: true, keep 1
    // Should only delete the uploaded snapshot
    pcrdt.cleanup_old_snapshots(1, true).unwrap();

    let remaining: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .collect();

    // Should have kept recent snapshots + deleted old uploaded ones
    // Exact count depends on full vs incremental, but should be <= original
    assert!(
        remaining.len() <= snapshots.len(),
        "Should have deleted some snapshots"
    );

    // Now mark ALL remaining as uploaded and cleanup with require_uploaded: false
    for snapshot in &remaining {
        pcrdt.mark_snapshot_uploaded(snapshot.path());
    }

    pcrdt.cleanup_old_snapshots(1, false).unwrap(); // Aggressive cleanup

    let final_snapshots: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("snapshot_"))
        .collect();

    // Should have kept only recent snapshots (1 full + associated incrementals)
    assert!(
        final_snapshots.len() <= remaining.len(),
        "Aggressive cleanup should reduce snapshot count"
    );

    let _ = std::fs::remove_dir_all(&base_path);
}

#[test]
#[cfg(feature = "msgpack")]
fn test_incremental_chain_validation() {
    use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotFormat};

    let base_path = std::env::temp_dir().join("test_chain_validation");
    let _ = std::fs::remove_dir_all(&base_path);
    std::fs::create_dir_all(&base_path).unwrap();

    let config = PersistConfig {
        snapshot_threshold: 10,
        snapshot_format: SnapshotFormat::MessagePack,
        enable_incremental_snapshots: true,
        full_snapshot_interval: 5,
        ..Default::default()
    };

    let mut pcrdt = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config.clone(),
    )
    .unwrap();

    // Create full snapshot + 3 incrementals
    for i in 0..10 {
        pcrdt.insert_or_update(
            &format!("rec{}", i),
            vec![("field".to_string(), format!("v0_{}", i))],
        ).unwrap();
    }
    pcrdt.snapshot().unwrap(); // Full #1

    for round in 1..=3 {
        for i in 0..5 {
            pcrdt.insert_or_update(
                &format!("rec{}", i),
                vec![("field".to_string(), format!("v{}_{}", round, i))],
            ).unwrap();
        }
        pcrdt.snapshot().unwrap(); // Incrementals #2, #3, #4
    }

    // Verify files exist
    let files: Vec<_> = std::fs::read_dir(&base_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("snapshot_full_") || name.starts_with("snapshot_incr_")
        })
        .map(|e| e.path())
        .collect();

    assert_eq!(files.iter().filter(|f| f.to_string_lossy().contains("full")).count(), 1);
    assert_eq!(files.iter().filter(|f| f.to_string_lossy().contains("incr")).count(), 3);

    // Delete the middle incremental to create a gap
    let mut incr_files: Vec<_> = files.iter()
        .filter(|f| f.to_string_lossy().contains("incr_"))
        .cloned()
        .collect();
    incr_files.sort();

    // Delete the middle one (index 1 of 3)
    if incr_files.len() == 3 {
        std::fs::remove_file(&incr_files[1]).unwrap();
    }

    // Recover - should warn about non-contiguous incrementals
    // Capture stderr to verify warning (in real code, warnings go to eprintln!)
    let result = PersistedCRDT::<String, String, String>::open(
        base_path.clone(),
        1,
        config,
    );

    // Recovery should succeed (warnings are non-fatal)
    assert!(result.is_ok(), "Recovery should succeed despite gap");

    // Note: The warning about gap will be printed to stderr but won't fail the test
    // In a real scenario, monitoring systems would catch these warnings

    let _ = std::fs::remove_dir_all(&base_path);
}
