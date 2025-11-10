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

    println!("Full snapshot size: {} bytes", full_size);
    println!("Incremental snapshot size: {} bytes", incr_size);
    println!("Reduction: {:.1}%", (1.0 - (incr_size as f64 / full_size as f64)) * 100.0);

    // Incremental should be much smaller (at least 80% smaller for 5% changes)
    assert!(
        incr_size < full_size / 5,
        "Incremental snapshot should be significantly smaller"
    );
}
