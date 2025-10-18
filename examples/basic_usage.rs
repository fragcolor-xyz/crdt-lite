use crdt_lite::{DefaultMergeRule, CRDT};

fn main() {
  println!("=== CRDT Rust Port - Basic Usage Example ===\n");

  // Create two distributed nodes
  let mut node1: CRDT<String, String> = CRDT::new(1, None);
  let mut node2: CRDT<String, String> = CRDT::new(2, None);

  println!("Created two CRDT nodes with IDs 1 and 2\n");

  // Node 1: Insert a user record
  println!("Node 1: Inserting user 'alice'...");
  let changes1 = node1.insert_or_update(
    &"alice".to_string(),
    vec![
      ("name".to_string(), "Alice".to_string()),
      ("email".to_string(), "alice@example.com".to_string()),
      ("role".to_string(), "admin".to_string()),
    ],
  );
  println!("  Created {} changes", changes1.len());

  // Node 2: Insert the same user with different data (concurrent update)
  println!("\nNode 2: Inserting user 'alice' with different data...");
  let changes2 = node2.insert_or_update(
    &"alice".to_string(),
    vec![
      ("name".to_string(), "Alice Smith".to_string()),
      ("email".to_string(), "alice.smith@example.com".to_string()),
      ("role".to_string(), "user".to_string()),
    ],
  );
  println!("  Created {} changes", changes2.len());

  // Merge changes (simulating distributed synchronization)
  println!("\n--- Merging changes ---");
  let merge_rule = DefaultMergeRule;

  println!("Node 1: Receiving changes from Node 2...");
  node1.merge_changes(changes2, &merge_rule);

  println!("Node 2: Receiving changes from Node 1...");
  node2.merge_changes(changes1, &merge_rule);

  // Check convergence
  println!("\n--- Results ---");
  if node1.get_data() == node2.get_data() {
    println!("✓ Both nodes have converged to the same state!");
  } else {
    println!("✗ Nodes have different states (unexpected)");
  }

  // Display the final state
  if let Some(record) = node1.get_record(&"alice".to_string()) {
    println!("\nFinal record for 'alice':");
    for (field, value) in &record.fields {
      println!("  {}: {}", field, value);
    }
    println!("\nNote: Node 2 won the conflict due to higher node_id (last-write-wins)");
  }

  // Demonstrate updates
  println!("\n--- Updating a field ---");
  println!("Node 1: Updating alice's role to 'superadmin'...");
  let update_changes = node1.insert_or_update(
    &"alice".to_string(),
    vec![("role".to_string(), "superadmin".to_string())],
  );

  println!("Node 2: Merging update...");
  node2.merge_changes(update_changes, &merge_rule);

  if let Some(record) = node2.get_record(&"alice".to_string()) {
    println!("Updated role: {}", record.fields.get("role").unwrap());
  }

  // Demonstrate deletion
  println!("\n--- Deleting a record ---");
  println!("Node 1: Deleting user 'alice'...");
  if let Some(delete_change) = node1.delete_record(&"alice".to_string()) {
    println!("  Created tombstone");

    println!("Node 2: Merging deletion...");
    node2.merge_changes(vec![delete_change], &merge_rule);
  }

  println!(
    "\nIs 'alice' tombstoned on Node 1? {}",
    node1.is_tombstoned(&"alice".to_string())
  );
  println!(
    "Is 'alice' tombstoned on Node 2? {}",
    node2.is_tombstoned(&"alice".to_string())
  );

  // Demonstrate that tombstoned records cannot be re-inserted
  println!("\n--- Attempting to re-insert tombstoned record ---");
  let reinsert_attempt = node1.insert_or_update(
    &"alice".to_string(),
    vec![("name".to_string(), "Alice Reborn".to_string())],
  );
  println!(
    "Changes created: {} (tombstone prevents insertion)",
    reinsert_attempt.len()
  );

  // Show change tracking
  println!("\n--- Change Tracking ---");
  let mut node3: CRDT<String, String> = CRDT::new(3, None);

  let _ = node3.insert_or_update(
    &"bob".to_string(),
    vec![("name".to_string(), "Bob".to_string())],
  );

  let version_1 = node3.get_clock().current_time();

  let _ = node3.insert_or_update(
    &"bob".to_string(),
    vec![("email".to_string(), "bob@example.com".to_string())],
  );

  let _ = node3.insert_or_update(
    &"charlie".to_string(),
    vec![("name".to_string(), "Charlie".to_string())],
  );

  let changes_since_v1 = node3.get_changes_since(version_1);
  println!(
    "Changes since version {}: {}",
    version_1,
    changes_since_v1.len()
  );
  for change in &changes_since_v1 {
    println!(
      "  - {:?}: {:?} = {:?}",
      change.record_id, change.col_name, change.value
    );
  }

  println!("\n=== Example Complete ===");
}
