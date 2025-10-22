use crdt_lite::*;

/// Helper function to generate unique IDs (simulating UUIDs)
fn generate_uuid() -> String {
  use std::sync::atomic::{AtomicU64, Ordering};
  static COUNTER: AtomicU64 = AtomicU64::new(0);
  format!("uuid-{}", COUNTER.fetch_add(1, Ordering::SeqCst))
}

#[test]
fn test_basic_insert_and_merge() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Node1 inserts a record
  let record_id = generate_uuid();
  let form_id = generate_uuid();
  let changes1 = node1.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("form_id".to_string(), form_id.clone()),
      ("tag".to_string(), "Node1Tag".to_string()),
      ("created_at".to_string(), "2023-10-01T12:00:00Z".to_string()),
      ("created_by".to_string(), "User1".to_string()),
    ],
  );

  // Node2 inserts the same record with different data
  let changes2 = node2.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("form_id".to_string(), form_id),
      ("tag".to_string(), "Node2Tag".to_string()),
      ("created_at".to_string(), "2023-10-01T12:05:00Z".to_string()),
      ("created_by".to_string(), "User2".to_string()),
    ],
  );

  // Merge node2's changes into node1
  let merge_rule = DefaultMergeRule;
  node1.merge_changes(changes2, &merge_rule);

  // Merge node1's changes into node2
  node2.merge_changes(changes1, &merge_rule);

  // Both nodes should resolve the conflict and have the same data
  assert_eq!(node1.get_data(), node2.get_data());
  assert_eq!(
    node1
      .get_record(&record_id)
      .unwrap()
      .fields
      .get("tag")
      .unwrap(),
    "Node2Tag"
  );
  assert_eq!(
    node1
      .get_record(&record_id)
      .unwrap()
      .fields
      .get("created_by")
      .unwrap(),
    "User2"
  );
}

#[test]
fn test_updates_with_conflicts() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Insert a shared record
  let record_id = generate_uuid();
  let changes_init1 = node1.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "InitialTag".to_string()),
    ],
  );
  let changes_init2 = node2.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "InitialTag".to_string()),
    ],
  );

  // Merge initial inserts
  let merge_rule = DefaultMergeRule;
  node1.merge_changes(changes_init2, &merge_rule);
  node2.merge_changes(changes_init1, &merge_rule);

  // Node1 updates 'tag'
  let change_update1 = node1.insert_or_update(
    &record_id,
    vec![("tag".to_string(), "Node1UpdatedTag".to_string())],
  );

  // Node2 updates 'tag'
  let change_update2 = node2.insert_or_update(
    &record_id,
    vec![("tag".to_string(), "Node2UpdatedTag".to_string())],
  );

  // Merge changes
  node1.merge_changes(change_update2, &merge_rule);
  node2.merge_changes(change_update1, &merge_rule);

  // Conflict resolved based on node_id (Node2 has higher node_id)
  assert_eq!(
    node1
      .get_record(&record_id)
      .unwrap()
      .fields
      .get("tag")
      .unwrap(),
    "Node2UpdatedTag"
  );
  assert_eq!(node1.get_data(), node2.get_data());
}

#[test]
fn test_delete_and_merge() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Insert and sync a record
  let record_id = generate_uuid();
  let changes_init = node1.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "ToBeDeleted".to_string()),
    ],
  );

  // Merge to node2
  let merge_rule = DefaultMergeRule;
  node2.merge_changes(changes_init, &merge_rule);

  // Node1 deletes the record
  let changes_delete = vec![node1.delete_record(&record_id).unwrap()];

  // Merge the deletion to node2
  node2.merge_changes(changes_delete, &merge_rule);

  // Both nodes should reflect the deletion
  assert!(node1.get_record(&record_id).is_none());
  assert!(node2.get_record(&record_id).is_none());
  assert!(node1.get_tombstone(&record_id).is_some());
  assert!(node2.get_tombstone(&record_id).is_some());
}

#[test]
fn test_tombstone_handling() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Insert a record and delete it on node1
  let record_id = generate_uuid();
  let changes_insert = node1.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "Temporary".to_string()),
    ],
  );
  let changes_delete = vec![node1.delete_record(&record_id).unwrap()];

  // Merge changes to node2
  let merge_rule = DefaultMergeRule;
  node2.merge_changes(changes_insert, &merge_rule);
  node2.merge_changes(changes_delete, &merge_rule);

  // Node2 tries to insert the same record
  let changes_attempt_insert = node2.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "Temporary".to_string()),
    ],
  );

  // Merge changes back to node1
  node1.merge_changes(changes_attempt_insert, &merge_rule);

  // Node2 should respect the tombstone
  assert!(node2.get_record(&record_id).is_none());
  assert!(node2.get_tombstone(&record_id).is_some());
}

#[test]
fn test_conflict_resolution_with_node_id() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Both nodes insert a record with the same id
  let record_id = generate_uuid();
  let changes1 = node1.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "Node1Tag".to_string()),
    ],
  );
  let changes2 = node2.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "Node2Tag".to_string()),
    ],
  );

  // Merge changes
  let merge_rule = DefaultMergeRule;
  node1.merge_changes(changes2.clone(), &merge_rule);
  node2.merge_changes(changes1.clone(), &merge_rule);

  // Both nodes update the 'tag' field multiple times
  let changes_update1 = node1.insert_or_update(
    &record_id,
    vec![("tag".to_string(), "Node1Tag1".to_string())],
  );
  let changes_update2 = node1.insert_or_update(
    &record_id,
    vec![("tag".to_string(), "Node1Tag2".to_string())],
  );
  let changes_update3 = node2.insert_or_update(
    &record_id,
    vec![("tag".to_string(), "Node2Tag1".to_string())],
  );
  let changes_update4 = node2.insert_or_update(
    &record_id,
    vec![("tag".to_string(), "Node2Tag2".to_string())],
  );

  // Merge changes
  node1.merge_changes(changes_update4, &merge_rule);
  node2.merge_changes(changes_update2, &merge_rule);
  node2.merge_changes(changes_update1, &merge_rule);
  node1.merge_changes(changes_update3, &merge_rule);

  // Since node2 has a higher node_id, its latest update should prevail
  let expected_tag = "Node2Tag2";

  assert_eq!(
    node1
      .get_record(&record_id)
      .unwrap()
      .fields
      .get("tag")
      .unwrap(),
    expected_tag
  );
  assert_eq!(node1.get_data(), node2.get_data());
}

#[test]
fn test_logical_clock_update() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Node1 inserts a record
  let record_id = generate_uuid();
  let changes_insert = node1.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "Node1Tag".to_string()),
    ],
  );

  // Node2 receives the change
  let merge_rule = DefaultMergeRule;
  node2.merge_changes(changes_insert, &merge_rule);

  // Node2's clock should be updated
  assert!(node2.get_clock().current_time() > 0);
  assert!(node2.get_clock().current_time() >= node1.get_clock().current_time());
}

#[test]
fn test_merge_without_conflicts() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Node1 inserts a record
  let record_id1 = generate_uuid();
  let changes1 = node1.insert_or_update(
    &record_id1,
    vec![
      ("id".to_string(), record_id1.clone()),
      ("tag".to_string(), "Node1Record".to_string()),
    ],
  );

  // Node2 inserts a different record
  let record_id2 = generate_uuid();
  let changes2 = node2.insert_or_update(
    &record_id2,
    vec![
      ("id".to_string(), record_id2.clone()),
      ("tag".to_string(), "Node2Record".to_string()),
    ],
  );

  // Merge changes
  let merge_rule = DefaultMergeRule;
  node1.merge_changes(changes2, &merge_rule);
  node2.merge_changes(changes1, &merge_rule);

  // Both nodes should have both records
  assert!(node1.get_record(&record_id1).is_some());
  assert!(node1.get_record(&record_id2).is_some());
  assert!(node2.get_record(&record_id1).is_some());
  assert!(node2.get_record(&record_id2).is_some());
  assert_eq!(node1.get_data(), node2.get_data());
}

#[test]
fn test_multiple_merges() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Node1 inserts a record
  let record_id = generate_uuid();
  let changes_init = node1.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "InitialTag".to_string()),
    ],
  );

  // Merge to node2
  let merge_rule = DefaultMergeRule;
  node2.merge_changes(changes_init, &merge_rule);

  // Node2 updates the record
  let changes_update2 = node2.insert_or_update(
    &record_id,
    vec![("tag".to_string(), "UpdatedByNode2".to_string())],
  );

  // Node1 updates the record
  let changes_update1 = node1.insert_or_update(
    &record_id,
    vec![("tag".to_string(), "UpdatedByNode1".to_string())],
  );

  // Merge changes
  node1.merge_changes(changes_update2, &merge_rule);
  node2.merge_changes(changes_update1, &merge_rule);

  // Since node2 has a higher node_id, its latest update should prevail
  let expected_tag = "UpdatedByNode2";

  assert_eq!(
    node1
      .get_record(&record_id)
      .unwrap()
      .fields
      .get("tag")
      .unwrap(),
    expected_tag
  );
  assert_eq!(node1.get_data(), node2.get_data());
}

#[test]
fn test_inserting_after_deletion() {
  let mut node1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String, String> = CRDT::new(2, None);

  // Node1 inserts and deletes a record
  let record_id = generate_uuid();
  let changes_insert = node1.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "Temporary".to_string()),
    ],
  );
  let changes_delete = vec![node1.delete_record(&record_id).unwrap()];

  // Merge deletion to node2
  let merge_rule = DefaultMergeRule;
  node2.merge_changes(changes_insert, &merge_rule);
  node2.merge_changes(changes_delete, &merge_rule);

  // Node2 tries to insert the same record
  let changes_attempt_insert = node2.insert_or_update(
    &record_id,
    vec![
      ("id".to_string(), record_id.clone()),
      ("tag".to_string(), "Temporary".to_string()),
    ],
  );

  // Merge changes back to node1
  node1.merge_changes(changes_attempt_insert, &merge_rule);

  // The deletion should prevail
  assert!(node1.get_record(&record_id).is_none());
  assert!(node2.get_record(&record_id).is_none());
  assert!(node1.get_tombstone(&record_id).is_some());
  assert!(node2.get_tombstone(&record_id).is_some());
}

#[test]
fn test_get_changes_since() {
  let mut crdt: CRDT<String, String, String> = CRDT::new(1, None);

  let record_id = generate_uuid();
  let _ = crdt.insert_or_update(
    &record_id,
    vec![("field1".to_string(), "value1".to_string())],
  );

  // Make additional changes
  let _ = crdt.insert_or_update(
    &record_id,
    vec![("field2".to_string(), "value2".to_string())],
  );

  // Retrieve changes since db_version 1
  let retrieved_changes = crdt.get_changes_since(1);

  // Should include only the new change
  assert_eq!(retrieved_changes.len(), 1);
  assert_eq!(retrieved_changes[0].col_name, Some("field2".to_string()));
  assert_eq!(retrieved_changes[0].value, Some("value2".to_string()));
}

#[test]
fn test_change_compression() {
  let record_id = "record1".to_string();

  // Create multiple changes for the same record and field
  let mut changes = vec![
    Change::new(
      record_id.clone(),
      Some("field1".to_string()),
      Some("value1".to_string()),
      1,
      1,
      1,
      1,
      0,
    ),
    Change::new(
      record_id.clone(),
      Some("field1".to_string()),
      Some("value2".to_string()),
      2,
      2,
      1,
      2,
      0,
    ),
    Change::new(
      record_id.clone(),
      Some("field1".to_string()),
      Some("value3".to_string()),
      3,
      3,
      1,
      3,
      0,
    ),
  ];

  CRDT::<String, String, String>::compress_changes(&mut changes);

  // Should only keep the most recent change
  assert_eq!(changes.len(), 1);
  assert_eq!(changes[0].value, Some("value3".to_string()));
  assert_eq!(changes[0].col_version, 3);
}

#[test]
fn test_tombstone_compaction() {
  let mut crdt: CRDT<String, String, String> = CRDT::new(1, None);

  let record_id = generate_uuid();
  let _ = crdt.insert_or_update(&record_id, vec![("tag".to_string(), "test".to_string())]);

  let _ = crdt.delete_record(&record_id);

  assert_eq!(crdt.tombstone_count(), 1);

  // Compact tombstones older than version 10
  let removed = crdt.compact_tombstones(10);
  assert_eq!(removed, 1);
  assert_eq!(crdt.tombstone_count(), 0);
}

#[test]
fn test_empty_changes_merge() {
  let mut crdt: CRDT<String, String, String> = CRDT::new(1, None);
  let merge_rule = DefaultMergeRule;

  // Merging empty changes should not panic
  let accepted = crdt.merge_changes(vec![], &merge_rule);
  assert!(accepted.is_empty());
}

#[test]
fn test_inserting_after_tombstone() {
  let mut crdt: CRDT<String, String, String> = CRDT::new(1, None);

  let record_id = generate_uuid();
  let _ = crdt.insert_or_update(&record_id, vec![("field".to_string(), "value".to_string())]);
  let _ = crdt.delete_record(&record_id);

  // Attempting to insert into a tombstoned record should fail silently
  let changes = crdt.insert_or_update(&record_id, vec![("new".to_string(), "data".to_string())]);
  assert!(changes.is_empty());
  assert!(crdt.is_tombstoned(&record_id));
}

#[test]
fn test_multiple_deletes_same_record() {
  let mut crdt: CRDT<String, String, String> = CRDT::new(1, None);

  let record_id = generate_uuid();
  let _ = crdt.insert_or_update(&record_id, vec![("field".to_string(), "value".to_string())]);

  // First delete
  let delete1 = crdt.delete_record(&record_id);
  assert!(delete1.is_some());

  // Second delete should return None
  let delete2 = crdt.delete_record(&record_id);
  assert!(delete2.is_none());
}

#[test]
fn test_large_version_numbers() {
  // Create a CRDT from changes with very large version numbers
  let record_id = generate_uuid();
  let changes = vec![Change::new(
    record_id.clone(),
    Some("field".to_string()),
    Some("initial".to_string()),
    1,
    u64::MAX - 100, // Very large db_version
    1,
    u64::MAX - 100,
    0,
  )];

  let mut crdt: CRDT<String, String, String> = CRDT::from_changes(1, changes);

  // This should not panic even with very large version numbers
  let new_changes = crdt.insert_or_update(&record_id, vec![("field2".to_string(), "value".to_string())]);

  assert_eq!(new_changes.len(), 1);
  assert!(new_changes[0].db_version > u64::MAX - 100);
}

#[test]
fn test_compress_changes_with_deletion() {
  let record_id = "record1".to_string();

  // Create changes: update field1, update field2, then delete record
  let mut changes = vec![
    Change::new(
      record_id.clone(),
      Some("field1".to_string()),
      Some("value1".to_string()),
      1,
      1,
      1,
      1,
      0,
    ),
    Change::new(
      record_id.clone(),
      Some("field2".to_string()),
      Some("value2".to_string()),
      1,
      2,
      1,
      2,
      0,
    ),
    Change::new(record_id.clone(), None, None, u64::MAX, 3, 1, 3, 0),
  ];

  CRDT::<String, String, String>::compress_changes(&mut changes);

  // After compression, only the deletion should remain (it supersedes all field updates)
  assert_eq!(changes.len(), 1);
  assert!(changes[0].col_name.is_none());
}

#[test]
fn test_get_changes_since_with_no_changes() {
  let crdt: CRDT<String, String, String> = CRDT::new(1, None);

  // Getting changes when nothing has changed should return empty vec
  let changes = crdt.get_changes_since(0);
  assert!(changes.is_empty());
}

#[test]
fn test_partial_field_updates() {
  let mut crdt: CRDT<String, String, String> = CRDT::new(1, None);
  let record_id = generate_uuid();

  // Insert a record with multiple fields
  let _ = crdt.insert_or_update(
    &record_id,
    vec![
      ("name".to_string(), "Alice".to_string()),
      ("email".to_string(), "alice@example.com".to_string()),
      ("role".to_string(), "user".to_string()),
      ("status".to_string(), "active".to_string()),
    ],
  );

  // Verify all fields are present
  let record = crdt.get_record(&record_id).unwrap();
  assert_eq!(record.fields.get("name").unwrap(), "Alice");
  assert_eq!(record.fields.get("email").unwrap(), "alice@example.com");
  assert_eq!(record.fields.get("role").unwrap(), "user");
  assert_eq!(record.fields.get("status").unwrap(), "active");

  // Update ONLY the role field
  let _ = crdt.insert_or_update(&record_id, vec![("role".to_string(), "admin".to_string())]);

  // Verify that ONLY the role field changed, other fields remain unchanged
  let record_after = crdt.get_record(&record_id).unwrap();
  assert_eq!(record_after.fields.get("name").unwrap(), "Alice"); // Unchanged
  assert_eq!(record_after.fields.get("email").unwrap(), "alice@example.com"); // Unchanged
  assert_eq!(record_after.fields.get("role").unwrap(), "admin"); // Changed
  assert_eq!(record_after.fields.get("status").unwrap(), "active"); // Unchanged

  // Verify that the column version increased only for the updated field
  let role_version = record_after.column_versions.get("role").unwrap();
  let name_version = record_after.column_versions.get("name").unwrap();
  assert_eq!(role_version.col_version, 2); // Incremented from 1 to 2
  assert_eq!(name_version.col_version, 1); // Still at 1
}

#[test]
fn test_delete_field() {
  let mut crdt1: CRDT<String, String, String> = CRDT::new(1, None);
  let mut crdt2: CRDT<String, String, String> = CRDT::new(2, None);
  let record_id = generate_uuid();

  // Insert a record with multiple fields
  let _ = crdt1.insert_or_update(
    &record_id,
    vec![
      ("name".to_string(), "Alice".to_string()),
      ("email".to_string(), "alice@example.com".to_string()),
      ("role".to_string(), "admin".to_string()),
      ("status".to_string(), "active".to_string()),
    ],
  );

  // Verify all fields exist
  let record = crdt1.get_record(&record_id).unwrap();
  assert_eq!(record.fields.len(), 4);
  assert!(record.fields.contains_key("email"));

  // Delete the email field
  let delete_change = crdt1.delete_field(&record_id, &"email".to_string());
  assert!(delete_change.is_some());

  let change = delete_change.unwrap();
  assert_eq!(change.col_name, Some("email".to_string()));
  assert_eq!(change.value, None); // None indicates field deletion
  assert_eq!(change.col_version, 2); // Incremented from 1

  // Verify field is deleted but ColumnVersion remains (field tombstone)
  let record_after = crdt1.get_record(&record_id).unwrap();
  assert_eq!(record_after.fields.len(), 3); // One less field
  assert!(!record_after.fields.contains_key("email")); // Field is gone
  assert!(record_after.column_versions.contains_key("email")); // But version info remains

  // Sync full state from crdt1 to crdt2 (including the deletion)
  let merge_rule = DefaultMergeRule;
  let all_changes = crdt1.get_changes_since(0);
  crdt2.merge_changes(all_changes, &merge_rule);

  let record2 = crdt2.get_record(&record_id).unwrap();
  assert_eq!(record2.fields.len(), 3);
  assert!(!record2.fields.contains_key("email"));
  assert!(record2.column_versions.contains_key("email"));

  // Test edge cases
  // 1. Deleting non-existent field
  let result = crdt1.delete_field(&record_id, &"nonexistent".to_string());
  assert!(result.is_none());

  // 2. Deleting field from non-existent record
  let result = crdt1.delete_field(&"nonexistent_record".to_string(), &"name".to_string());
  assert!(result.is_none());

  // 3. Deleting field from tombstoned record
  let _ = crdt1.delete_record(&record_id);
  let result = crdt1.delete_field(&record_id, &"name".to_string());
  assert!(result.is_none());
}
