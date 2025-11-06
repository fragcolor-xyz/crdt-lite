#![cfg(feature = "persist")]

use crdt_lite::persist::{PersistedCRDT, PersistConfig};
use crdt_lite::{Change, CRDT};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("crdt_persist_test_{}", name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn test_basic_persistence() {
    let dir = temp_dir("basic");

    // Create and write some data
    {
        let mut pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        let changes = pcrdt
            .insert_or_update(
                &"user1".to_string(),
                [
                    ("name".to_string(), "Alice".to_string()),
                    ("age".to_string(), "30".to_string()),
                ]
                .into_iter(),
            )
            .unwrap();

        assert_eq!(changes.len(), 2);
    }

    // Reopen and verify data persisted
    {
        let pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        let record = pcrdt.crdt().get_record(&"user1".to_string()).unwrap();
        assert_eq!(record.fields.get("name").unwrap(), "Alice");
        assert_eq!(record.fields.get("age").unwrap(), "30");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_wal_replay() {
    let dir = temp_dir("wal_replay");

    // Create and write multiple operations
    {
        let mut pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        // Insert 3 records
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

        pcrdt
            .insert_or_update(
                &"rec3".to_string(),
                [("field".to_string(), "value3".to_string())].into_iter(),
            )
            .unwrap();

        // Update one record
        pcrdt
            .insert_or_update(
                &"rec1".to_string(),
                [("field".to_string(), "updated".to_string())].into_iter(),
            )
            .unwrap();

        // Delete one record
        pcrdt.delete_record(&"rec2".to_string()).unwrap();
    }

    // Reopen and verify all operations replayed correctly
    {
        let pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        // rec1 should have updated value
        let rec1 = pcrdt.crdt().get_record(&"rec1".to_string()).unwrap();
        assert_eq!(rec1.fields.get("field").unwrap(), "updated");

        // rec2 should be tombstoned
        assert!(pcrdt.crdt().is_tombstoned(&"rec2".to_string()));

        // rec3 should exist
        let rec3 = pcrdt.crdt().get_record(&"rec3".to_string()).unwrap();
        assert_eq!(rec3.fields.get("field").unwrap(), "value3");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_snapshot_rotation() {
    let dir = temp_dir("snapshot_rotation");

    let config = PersistConfig {
        snapshot_threshold: 5,
        enable_compression: false,
        auto_cleanup_snapshots: None, // Manual cleanup for testing
    };

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, config).unwrap();

    // Insert 10 records (should trigger 2 snapshots)
    for i in 0..10 {
        pcrdt
            .insert_or_update(
                &format!("rec{}", i),
                [("field".to_string(), format!("value{}", i))].into_iter(),
            )
            .unwrap();
    }

    // Should have versioned snapshot files
    let snapshot_files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("snapshot_")
        })
        .collect();
    assert!(snapshot_files.len() > 0, "Should have at least one snapshot");

    // Should have a current WAL segment but old ones deleted after snapshot
    let wal_files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("wal_")
        })
        .collect();

    // Should have at least the current WAL segment
    assert!(wal_files.len() > 0, "Should have current WAL segment");

    // Reopen and verify all data present
    let pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    for i in 0..10 {
        let rec = pcrdt.crdt().get_record(&format!("rec{}", i)).unwrap();
        assert_eq!(rec.fields.get("field").unwrap(), &format!("value{}", i));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_merge_remote_changes() {
    let dir = temp_dir("merge_remote");

    let mut pcrdt1 =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    // Create a second CRDT (simulating remote node)
    let mut crdt2: CRDT<String, String, String> = CRDT::new(2, None);

    // Both insert to same record with different fields
    pcrdt1
        .insert_or_update(
            &"user".to_string(),
            [("name".to_string(), "Alice".to_string())].into_iter(),
        )
        .unwrap();

    let changes2 = crdt2.insert_or_update(
        &"user".to_string(),
        [("age".to_string(), "30".to_string())].into_iter(),
    );

    // Merge remote changes into persisted CRDT
    let accepted = pcrdt1.merge_changes(changes2).unwrap();
    assert_eq!(accepted.len(), 1);

    // Verify both fields present
    let record = pcrdt1.crdt().get_record(&"user".to_string()).unwrap();
    assert_eq!(record.fields.get("name").unwrap(), "Alice");
    assert_eq!(record.fields.get("age").unwrap(), "30");

    // Reopen and verify merged state persisted
    let pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    let record = pcrdt.crdt().get_record(&"user".to_string()).unwrap();
    assert_eq!(record.fields.get("name").unwrap(), "Alice");
    assert_eq!(record.fields.get("age").unwrap(), "30");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_post_operation_hook() {
    let dir = temp_dir("post_hook");

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    // Track changes via hook
    let collected = Arc::new(Mutex::new(Vec::new()));
    let collected_clone = collected.clone();

    pcrdt.add_post_hook(Box::new(move |changes: &[Change<String, String, String>]| {
        collected_clone.lock().unwrap().extend_from_slice(changes);
    }));

    // Perform operations
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

    // Verify hook was called
    let changes = collected.lock().unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].record_id, "rec1");
    assert_eq!(changes[1].record_id, "rec2");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_batch_collector() {
    let dir = temp_dir("batch_collector");

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    // Perform multiple operations
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

    // Check batch has accumulated changes
    assert_eq!(pcrdt.peek_batch().len(), 2);

    // Take batch (clears it)
    let batch = pcrdt.take_batch();
    assert_eq!(batch.len(), 2);
    assert_eq!(pcrdt.peek_batch().len(), 0);

    // Do more operations
    pcrdt
        .insert_or_update(
            &"rec3".to_string(),
            [("field".to_string(), "value3".to_string())].into_iter(),
        )
        .unwrap();

    assert_eq!(pcrdt.peek_batch().len(), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_manual_snapshot() {
    let dir = temp_dir("manual_snapshot");

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    // Insert some data
    pcrdt
        .insert_or_update(
            &"rec1".to_string(),
            [("field".to_string(), "value1".to_string())].into_iter(),
        )
        .unwrap();

    assert_eq!(pcrdt.changes_since_snapshot(), 1);

    // Manually trigger snapshot
    pcrdt.snapshot().unwrap();

    // Counter should reset
    assert_eq!(pcrdt.changes_since_snapshot(), 0);

    // Snapshot file should exist (versioned)
    let has_snapshot = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("snapshot_"));
    assert!(has_snapshot, "Should have a versioned snapshot file");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_get_changes_since() {
    let dir = temp_dir("get_changes_since");

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    // Get initial clock time
    let initial_time = pcrdt.get_clock_time();

    // Insert some data
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

    // Get changes since initial time
    let changes = pcrdt.get_changes_since(initial_time);
    assert_eq!(changes.len(), 2);

    // Get changes from current time (should be empty)
    let current_time = pcrdt.get_clock_time();
    let no_changes = pcrdt.get_changes_since(current_time);
    assert_eq!(no_changes.len(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_compact_tombstones_with_persistence() {
    let dir = temp_dir("compact_tombstones");

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    // Create and delete some records
    pcrdt
        .insert_or_update(
            &"rec1".to_string(),
            [("field".to_string(), "value1".to_string())].into_iter(),
        )
        .unwrap();

    pcrdt.delete_record(&"rec1".to_string()).unwrap();

    // Verify tombstone exists
    assert!(pcrdt.crdt().is_tombstoned(&"rec1".to_string()));
    assert_eq!(pcrdt.crdt().tombstone_count(), 1);

    // Compact tombstones using the safe atomic method
    // Use a version higher than current to ensure tombstone is compacted
    let clock_time = pcrdt.get_clock_time() + 1;
    pcrdt.compact_tombstones_with_cleanup(clock_time).unwrap();

    // Tombstone should be gone
    assert_eq!(pcrdt.crdt().tombstone_count(), 0);

    // Reopen and verify compaction persisted
    let pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    assert_eq!(pcrdt.crdt().tombstone_count(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_zombie_record_prevention() {
    // This test demonstrates that compact_tombstones_with_cleanup() prevents zombie records
    // whereas compact_tombstones() without cleanup can cause them to reappear on recovery
    let dir = temp_dir("zombie_prevention");

    // Create a record, delete it, and compact WITHOUT cleanup
    {
        let mut pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        // Create and delete a record
        pcrdt
            .insert_or_update(
                &"zombie".to_string(),
                [("data".to_string(), "will_resurrect".to_string())].into_iter(),
            )
            .unwrap();

        pcrdt.delete_record(&"zombie".to_string()).unwrap();

        // Verify tombstone exists
        assert!(pcrdt.crdt().is_tombstoned(&"zombie".to_string()));
        assert_eq!(pcrdt.crdt().tombstone_count(), 1);

        // Compact WITHOUT cleanup (unsafe!)
        let clock_time = pcrdt.get_clock_time() + 1;
        pcrdt.compact_tombstones(clock_time).unwrap();

        // Tombstone is gone from memory
        assert_eq!(pcrdt.crdt().tombstone_count(), 0);

        // But old WAL segments still contain the deletion!
        // Simulating crash by dropping without cleanup
    }

    // Reopen and verify zombie record DOES reappear (because old WAL is replayed)
    {
        let pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        // Without cleanup, the tombstone reappears from WAL replay
        // This is the zombie record problem!
        assert_eq!(pcrdt.crdt().tombstone_count(), 1,
            "Zombie record appeared! This demonstrates why cleanup_old_wal_segments() is required");
        assert!(pcrdt.crdt().is_tombstoned(&"zombie".to_string()));
    }

    // Now test the safe version
    {
        let mut pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        // Use the safe atomic method
        let clock_time = pcrdt.get_clock_time() + 1;
        pcrdt.compact_tombstones_with_cleanup(clock_time).unwrap();

        // Tombstone should be gone
        assert_eq!(pcrdt.crdt().tombstone_count(), 0);
    }

    // Reopen again - tombstone should STAY gone (no zombie)
    {
        let pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        // With cleanup, tombstone stays gone - no zombie!
        assert_eq!(pcrdt.crdt().tombstone_count(), 0);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_crash_recovery() {
    let dir = temp_dir("crash_recovery");

    // Simulate crash by not properly closing
    {
        let mut pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        for i in 0..5 {
            pcrdt
                .insert_or_update(
                    &format!("rec{}", i),
                    [("field".to_string(), format!("value{}", i))].into_iter(),
                )
                .unwrap();
        }
        // Drop without explicit close - simulates crash
    }

    // Reopen - should recover from WAL
    {
        let pcrdt =
            PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
                .unwrap();

        // All records should be present
        for i in 0..5 {
            let rec = pcrdt.crdt().get_record(&format!("rec{}", i)).unwrap();
            assert_eq!(rec.fields.get("field").unwrap(), &format!("value{}", i));
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_multi_node_sync_with_persistence() {
    let dir1 = temp_dir("sync_node1");
    let dir2 = temp_dir("sync_node2");

    let mut pcrdt1 =
        PersistedCRDT::<String, String, String>::open(dir1.clone(), 1, PersistConfig::default())
            .unwrap();

    let mut pcrdt2 =
        PersistedCRDT::<String, String, String>::open(dir2.clone(), 2, PersistConfig::default())
            .unwrap();

    // Node 1 creates records
    pcrdt1
        .insert_or_update(
            &"shared".to_string(),
            [("from_node1".to_string(), "data1".to_string())].into_iter(),
        )
        .unwrap();

    // Node 2 creates records
    pcrdt2
        .insert_or_update(
            &"shared".to_string(),
            [("from_node2".to_string(), "data2".to_string())].into_iter(),
        )
        .unwrap();

    // Sync node1 → node2
    let changes_from_1 = pcrdt1.get_changes_since(0);
    pcrdt2.merge_changes(changes_from_1).unwrap();

    // Sync node2 → node1
    let changes_from_2 = pcrdt2.get_changes_since(0);
    pcrdt1.merge_changes(changes_from_2).unwrap();

    // Both nodes should have both fields
    let rec1 = pcrdt1.crdt().get_record(&"shared".to_string()).unwrap();
    assert_eq!(rec1.fields.get("from_node1").unwrap(), "data1");
    assert_eq!(rec1.fields.get("from_node2").unwrap(), "data2");

    let rec2 = pcrdt2.crdt().get_record(&"shared".to_string()).unwrap();
    assert_eq!(rec2.fields.get("from_node1").unwrap(), "data1");
    assert_eq!(rec2.fields.get("from_node2").unwrap(), "data2");

    // Reopen both and verify persistence
    let pcrdt1 =
        PersistedCRDT::<String, String, String>::open(dir1.clone(), 1, PersistConfig::default())
            .unwrap();

    let pcrdt2 =
        PersistedCRDT::<String, String, String>::open(dir2.clone(), 2, PersistConfig::default())
            .unwrap();

    let rec1 = pcrdt1.crdt().get_record(&"shared".to_string()).unwrap();
    assert_eq!(rec1.fields.get("from_node1").unwrap(), "data1");
    assert_eq!(rec1.fields.get("from_node2").unwrap(), "data2");

    let rec2 = pcrdt2.crdt().get_record(&"shared".to_string()).unwrap();
    assert_eq!(rec2.fields.get("from_node1").unwrap(), "data1");
    assert_eq!(rec2.fields.get("from_node2").unwrap(), "data2");

    let _ = std::fs::remove_dir_all(&dir1);
    let _ = std::fs::remove_dir_all(&dir2);
}

#[test]
fn test_delete_field_persistence() {
    let dir = temp_dir("delete_field");

    let mut pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    // Insert record with multiple fields
    pcrdt
        .insert_or_update(
            &"rec".to_string(),
            [
                ("field1".to_string(), "value1".to_string()),
                ("field2".to_string(), "value2".to_string()),
            ]
            .into_iter(),
        )
        .unwrap();

    // Delete one field
    pcrdt.delete_field(&"rec".to_string(), &"field1".to_string()).unwrap();

    // Verify field deleted
    let rec = pcrdt.crdt().get_record(&"rec".to_string()).unwrap();
    assert!(rec.fields.get("field1").is_none());
    assert_eq!(rec.fields.get("field2").unwrap(), "value2");

    // Reopen and verify
    let pcrdt =
        PersistedCRDT::<String, String, String>::open(dir.clone(), 1, PersistConfig::default())
            .unwrap();

    let rec = pcrdt.crdt().get_record(&"rec".to_string()).unwrap();
    assert!(rec.fields.get("field1").is_none());
    assert_eq!(rec.fields.get("field2").unwrap(), "value2");

    let _ = std::fs::remove_dir_all(&dir);
}
