//! # crdt-lite
//!
//! A lightweight, column-based CRDT (Conflict-free Replicated Data Type) implementation in Rust.
//!
//! This library provides a generic CRDT with last-write-wins semantics, supporting:
//! - Generic key and value types
//! - Logical clock for causality tracking
//! - Tombstone-based deletion
//! - Parent-child CRDT hierarchies
//! - Custom merge rules and comparators
//! - Change compression
//! - Optional serialization support via Serde
//!
//! ## Features
//!
//! This crate provides optional serialization support through feature flags:
//!
//! - `serde` - Enables Serde support for all CRDT types
//! - `json` - Enables JSON serialization (includes `serde` + `serde_json`)
//! - `binary` - Enables binary serialization (includes `serde` + `bincode`)
//!
//! ### Usage Example with Serialization
//!
//! ```toml
//! [dependencies]
//! crdt-lite = { version = "0.1", features = ["json", "binary"] }
//! ```
//!
//! ```rust,ignore
//! use crdt_lite::CRDT;
//!
//! // Create and populate a CRDT
//! let mut crdt: CRDT<String, String> = CRDT::new(1, None);
//! let fields = vec![("name".to_string(), "Alice".to_string())];
//! crdt.insert_or_update(&"user1".to_string(), fields);
//!
//! // Serialize to JSON (requires "json" feature)
//! let json = crdt.to_json().unwrap();
//!
//! // Deserialize from JSON
//! let restored: CRDT<String, String> = CRDT::from_json(&json).unwrap();
//!
//! // Serialize to binary (requires "binary" feature)
//! let bytes = crdt.to_bytes().unwrap();
//! let restored: CRDT<String, String> = CRDT::from_bytes(&bytes).unwrap();
//!
//! // Or use generic serde with any format (requires "serde" feature)
//! let json = serde_json::to_string(&crdt).unwrap();
//! let restored: CRDT<String, String> = serde_json::from_str(&json).unwrap();
//! ```
//!
//! **Note on Parent Relationships**: Parent-child CRDT hierarchies are not serialized.
//! After deserialization, the `parent` field will always be `None`. Applications must
//! rebuild parent-child relationships if needed.
//!
//! ## Security and Resource Management
//!
//! ### Logical Clock Overflow
//! The logical clock uses `u64` for version numbers. While overflow is theoretically possible
//! after 2^64 operations (extremely unlikely in practice), applications with extreme longevity
//! should be aware of this limitation.
//!
//! ### DoS Protection and Tombstone Management
//! Tombstones accumulate indefinitely unless manually compacted. To prevent memory exhaustion:
//! - Call `compact_tombstones()` periodically after all nodes have acknowledged a version
//! - Implement application-level rate limiting for operations
//! - Consider setting resource limits on the number of records and tombstones
//!
//! **Important**: Only compact tombstones when ALL participating nodes have acknowledged
//! the minimum version. Compacting too early may cause deleted records to reappear on
//! nodes that haven't received the deletion yet.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(feature = "std")]
use std::{
  cmp::Ordering,
  collections::{HashMap, HashSet},
  hash::Hash,
  sync::Arc,
};

#[cfg(not(feature = "std"))]
use alloc::{
  string::String,
  sync::Arc,
  vec::Vec,
};
#[cfg(not(feature = "std"))]
use core::{cmp::Ordering, hash::Hash};
#[cfg(not(feature = "std"))]
use hashbrown::{HashMap, HashSet};

/// Type alias for node IDs
pub type NodeId = u64;

/// Type alias for column keys (field names)
pub type ColumnKey = String;

/// Column version used for tombstone changes
/// Using u64::MAX ensures tombstones are treated as having the highest possible version
const TOMBSTONE_COL_VERSION: u64 = u64::MAX;

/// Represents a single change in the CRDT.
///
/// A change can represent:
/// - An insertion or update of a column value (when `col_name` is `Some`)
/// - A deletion of a specific column (when `col_name` is `Some` and `value` is `None`)
/// - A deletion of an entire record (when `col_name` is `None`)
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Change<K, V> {
  pub record_id: K,
  /// `None` represents tombstone of the record
  pub col_name: Option<ColumnKey>,
  /// `None` represents deletion of the column (not the record)
  pub value: Option<V>,
  pub col_version: u64,
  pub db_version: u64,
  pub node_id: NodeId,
  /// Local db_version when the change was created (useful for `get_changes_since`)
  pub local_db_version: u64,
  /// Optional flags to indicate the type of change (ephemeral, not stored)
  pub flags: u32,
}

impl<K: Eq, V: Eq> Eq for Change<K, V> {}

impl<K, V> Change<K, V> {
  /// Creates a new Change with all parameters
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    record_id: K,
    col_name: Option<ColumnKey>,
    value: Option<V>,
    col_version: u64,
    db_version: u64,
    node_id: NodeId,
    local_db_version: u64,
    flags: u32,
  ) -> Self {
    Self {
      record_id,
      col_name,
      value,
      col_version,
      db_version,
      node_id,
      local_db_version,
      flags,
    }
  }
}

/// Represents version information for a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ColumnVersion {
  pub col_version: u64,
  pub db_version: u64,
  pub node_id: NodeId,
  /// Local db_version when the change was created
  pub local_db_version: u64,
}

impl ColumnVersion {
  pub fn new(col_version: u64, db_version: u64, node_id: NodeId, local_db_version: u64) -> Self {
    Self {
      col_version,
      db_version,
      node_id,
      local_db_version,
    }
  }
}

/// Minimal version information for tombstones.
///
/// Stores essential data: db_version for conflict resolution, node_id for sync exclusion,
/// and local_db_version for sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TombstoneInfo {
  pub db_version: u64,
  pub node_id: NodeId,
  pub local_db_version: u64,
}

impl TombstoneInfo {
  pub fn new(db_version: u64, node_id: NodeId, local_db_version: u64) -> Self {
    Self {
      db_version,
      node_id,
      local_db_version,
    }
  }

  /// Helper to create a ColumnVersion for comparison with regular columns
  pub fn as_column_version(&self) -> ColumnVersion {
    ColumnVersion::new(
      TOMBSTONE_COL_VERSION,
      self.db_version,
      self.node_id,
      self.local_db_version,
    )
  }
}

/// Represents a logical clock for maintaining causality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LogicalClock {
  time: u64,
}

impl LogicalClock {
  /// Creates a new logical clock starting at 0
  pub fn new() -> Self {
    Self { time: 0 }
  }

  /// Increments the clock for a local event and returns the new time
  pub fn tick(&mut self) -> u64 {
    self.time += 1;
    self.time
  }

  /// Updates the clock based on a received time and returns the new time
  pub fn update(&mut self, received_time: u64) -> u64 {
    self.time = self.time.max(received_time);
    self.time += 1;
    self.time
  }

  /// Sets the logical clock to a specific time
  pub fn set_time(&mut self, time: u64) {
    self.time = time;
  }

  /// Retrieves the current time
  pub fn current_time(&self) -> u64 {
    self.time
  }
}

impl Default for LogicalClock {
  fn default() -> Self {
    Self::new()
  }
}

/// Storage for tombstones (deleted records).
///
/// Uses a HashMap for efficient lookups and supports compaction.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TombstoneStorage<K: Hash + Eq> {
  entries: HashMap<K, TombstoneInfo>,
}

impl<K: Hash + Eq> TombstoneStorage<K> {
  pub fn new() -> Self {
    Self {
      entries: HashMap::new(),
    }
  }

  pub fn insert_or_assign(&mut self, key: K, info: TombstoneInfo) {
    self.entries.insert(key, info);
  }

  pub fn find(&self, key: &K) -> Option<TombstoneInfo> {
    self.entries.get(key).copied()
  }

  pub fn erase(&mut self, key: &K) -> bool {
    self.entries.remove(key).is_some()
  }

  pub fn clear(&mut self) {
    self.entries.clear();
  }

  pub fn iter(&self) -> impl Iterator<Item = (&K, &TombstoneInfo)> {
    self.entries.iter()
  }

  pub fn len(&self) -> usize {
    self.entries.len()
  }

  pub fn is_empty(&self) -> bool {
    self.entries.is_empty()
  }

  /// Compact tombstones older than the specified version.
  ///
  /// Returns the number of tombstones removed.
  pub fn compact(&mut self, min_acknowledged_version: u64) -> usize {
    let initial_len = self.entries.len();
    self
      .entries
      .retain(|_, info| info.db_version >= min_acknowledged_version);
    initial_len - self.entries.len()
  }
}

impl<K: Hash + Eq> Default for TombstoneStorage<K> {
  fn default() -> Self {
    Self::new()
  }
}

/// Represents a record in the CRDT.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Record<V> {
  pub fields: HashMap<ColumnKey, V>,
  pub column_versions: HashMap<ColumnKey, ColumnVersion>,
  /// Track version boundaries for efficient filtering
  pub lowest_local_db_version: u64,
  pub highest_local_db_version: u64,
}

impl<V> Record<V> {
  pub fn new() -> Self {
    Self {
      fields: HashMap::new(),
      column_versions: HashMap::new(),
      lowest_local_db_version: u64::MAX,
      highest_local_db_version: 0,
    }
  }

  /// Creates a record from existing fields and column versions
  pub fn from_parts(
    fields: HashMap<ColumnKey, V>,
    column_versions: HashMap<ColumnKey, ColumnVersion>,
  ) -> Self {
    let mut lowest = u64::MAX;
    let mut highest = 0;

    for ver in column_versions.values() {
      if ver.local_db_version < lowest {
        lowest = ver.local_db_version;
      }
      if ver.local_db_version > highest {
        highest = ver.local_db_version;
      }
    }

    Self {
      fields,
      column_versions,
      lowest_local_db_version: lowest,
      highest_local_db_version: highest,
    }
  }
}

impl<V: PartialEq> PartialEq for Record<V> {
  fn eq(&self, other: &Self) -> bool {
    // Compare only fields, not column_versions (those will differ per node)
    self.fields == other.fields
  }
}

impl<V> Default for Record<V> {
  fn default() -> Self {
    Self::new()
  }
}

/// Trait for merge rules that determine conflict resolution.
///
/// Implementations should return `true` if the remote change should be accepted,
/// `false` otherwise.
pub trait MergeRule<K, V> {
  /// Determines whether to accept a remote change over a local one
  fn should_accept(
    &self,
    local_col: u64,
    local_db: u64,
    local_node: NodeId,
    remote_col: u64,
    remote_db: u64,
    remote_node: NodeId,
  ) -> bool;

  /// Convenience method for Change objects
  fn should_accept_change(&self, local: &Change<K, V>, remote: &Change<K, V>) -> bool {
    self.should_accept(
      local.col_version,
      local.db_version,
      local.node_id,
      remote.col_version,
      remote.db_version,
      remote.node_id,
    )
  }
}

/// Default merge rule implementing last-write-wins semantics.
///
/// Comparison priority:
/// 1. Column version (higher wins)
/// 2. DB version (higher wins)
/// 3. Node ID (higher wins as tiebreaker)
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultMergeRule;

impl<K, V> MergeRule<K, V> for DefaultMergeRule {
  fn should_accept(
    &self,
    local_col: u64,
    local_db: u64,
    local_node: NodeId,
    remote_col: u64,
    remote_db: u64,
    remote_node: NodeId,
  ) -> bool {
    match remote_col.cmp(&local_col) {
      Ordering::Greater => true,
      Ordering::Less => false,
      Ordering::Equal => match remote_db.cmp(&local_db) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => remote_node > local_node,
      },
    }
  }
}

/// Trait for change comparators used in sorting and compression.
pub trait ChangeComparator<K, V> {
  fn compare(&self, a: &Change<K, V>, b: &Change<K, V>) -> Ordering;
}

/// Default change comparator.
///
/// Sorts by:
/// 1. Record ID (ascending)
/// 2. Column name presence (deletions/tombstones last)
/// 3. Column name (ascending)
/// 4. Column version (descending - most recent first)
/// 5. DB version (descending)
/// 6. Node ID (descending)
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultChangeComparator;

impl<K: Ord, V> ChangeComparator<K, V> for DefaultChangeComparator {
  fn compare(&self, a: &Change<K, V>, b: &Change<K, V>) -> Ordering {
    // Compare record IDs
    match a.record_id.cmp(&b.record_id) {
      Ordering::Equal => {}
      ord => return ord,
    }

    // Deletions (None) come last for each record
    match (a.col_name.as_ref(), b.col_name.as_ref()) {
      (None, None) => {}
      (None, Some(_)) => return Ordering::Greater,
      (Some(_), None) => return Ordering::Less,
      (Some(a_col), Some(b_col)) => match a_col.cmp(b_col) {
        Ordering::Equal => {}
        ord => return ord,
      },
    }

    // Compare versions (descending - most recent first)
    match b.col_version.cmp(&a.col_version) {
      Ordering::Equal => {}
      ord => return ord,
    }

    match b.db_version.cmp(&a.db_version) {
      Ordering::Equal => {}
      ord => return ord,
    }

    b.node_id.cmp(&a.node_id)
  }
}

/// Main CRDT structure, generic over key (K) and value (V) types.
///
/// This implements a column-based CRDT with last-write-wins semantics.
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound(serialize = "K: serde::Serialize, V: serde::Serialize")))]
#[cfg_attr(feature = "serde", serde(bound(deserialize = "K: serde::de::DeserializeOwned + Hash + Eq + Clone, V: serde::de::DeserializeOwned + Clone")))]
pub struct CRDT<K: Hash + Eq + Clone, V: Clone> {
  node_id: NodeId,
  clock: LogicalClock,
  data: HashMap<K, Record<V>>,
  tombstones: TombstoneStorage<K>,
  #[cfg_attr(feature = "serde", serde(skip, default))]
  parent: Option<Arc<CRDT<K, V>>>,
  #[allow(dead_code)]
  base_version: u64,
}

impl<K: Hash + Eq + Clone, V: Clone> CRDT<K, V> {
  /// Creates a new empty CRDT.
  ///
  /// # Arguments
  ///
  /// * `node_id` - Unique identifier for this CRDT node
  /// * `parent` - Optional parent CRDT for hierarchical structures
  pub fn new(node_id: NodeId, parent: Option<Arc<CRDT<K, V>>>) -> Self {
    let (clock, base_version) = if let Some(ref p) = parent {
      let parent_clock = p.clock;
      let base = parent_clock.current_time();
      (parent_clock, base)
    } else {
      (LogicalClock::new(), 0)
    };

    Self {
      node_id,
      clock,
      data: HashMap::new(),
      tombstones: TombstoneStorage::new(),
      parent,
      base_version,
    }
  }

  /// Creates a CRDT from a list of changes (e.g., loaded from disk).
  ///
  /// # Arguments
  ///
  /// * `node_id` - The unique identifier for this CRDT node
  /// * `changes` - A list of changes to apply to reconstruct the CRDT state
  pub fn from_changes(node_id: NodeId, changes: Vec<Change<K, V>>) -> Self {
    let mut crdt = Self::new(node_id, None);
    crdt.apply_changes(changes);
    crdt
  }

  /// Resets the CRDT to a state as if it was constructed with the given changes.
  ///
  /// # Arguments
  ///
  /// * `changes` - A list of changes to apply to reconstruct the CRDT state
  pub fn reset(&mut self, changes: Vec<Change<K, V>>) {
    self.data.clear();
    self.tombstones.clear();
    self.clock = LogicalClock::new();
    self.apply_changes(changes);
  }

  /// Applies a list of changes to reconstruct the CRDT state.
  fn apply_changes(&mut self, changes: Vec<Change<K, V>>) {
    // Determine the maximum db_version from the changes
    let max_db_version = changes
      .iter()
      .map(|c| c.db_version.max(c.local_db_version))
      .max()
      .unwrap_or(0);

    // Set the logical clock to the maximum db_version
    self.clock.set_time(max_db_version);

    // Apply each change to reconstruct the CRDT state
    for change in changes {
      let record_id = change.record_id.clone();
      let col_name = change.col_name.clone();
      let remote_col_version = change.col_version;
      let remote_db_version = change.db_version;
      let remote_node_id = change.node_id;
      let remote_local_db_version = change.local_db_version;
      let remote_value = change.value;

      if col_name.is_none() {
        // Handle deletion
        self.data.remove(&record_id);

        // Store deletion information in tombstones
        self.tombstones.insert_or_assign(
          record_id,
          TombstoneInfo::new(remote_db_version, remote_node_id, remote_local_db_version),
        );
      } else if let Some(col_key) = col_name {
        // Handle insertion or update
        if !self.is_record_tombstoned(&record_id, false) {
          let record = self.get_or_create_record_unchecked(&record_id, false);

          // Insert or update the field value
          if let Some(value) = remote_value {
            record.fields.insert(col_key.clone(), value);
          }

          // Update the column version info
          let col_ver = ColumnVersion::new(
            remote_col_version,
            remote_db_version,
            remote_node_id,
            remote_local_db_version,
          );
          record.column_versions.insert(col_key, col_ver);

          // Update version boundaries
          if remote_local_db_version < record.lowest_local_db_version {
            record.lowest_local_db_version = remote_local_db_version;
          }
          if remote_local_db_version > record.highest_local_db_version {
            record.highest_local_db_version = remote_local_db_version;
          }
        }
      }
    }
  }

  /// Inserts a new record or updates an existing record in the CRDT.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record
  /// * `fields` - An iterator of (column_name, value) pairs
  ///
  /// # Returns
  ///
  /// A vector of changes created by this operation
  #[must_use = "changes should be propagated to other nodes"]
  pub fn insert_or_update<I>(&mut self, record_id: &K, fields: I) -> Vec<Change<K, V>>
  where
    I: IntoIterator<Item = (ColumnKey, V)>,
  {
    self.insert_or_update_with_flags(record_id, 0, fields)
  }

  /// Inserts a new record or updates an existing record with flags.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record
  /// * `flags` - Flags to indicate the type of change
  /// * `fields` - An iterator of (column_name, value) pairs
  ///
  /// # Returns
  ///
  /// A vector of changes created by this operation
  #[must_use = "changes should be propagated to other nodes"]
  pub fn insert_or_update_with_flags<I>(
    &mut self,
    record_id: &K,
    flags: u32,
    fields: I,
  ) -> Vec<Change<K, V>>
  where
    I: IntoIterator<Item = (ColumnKey, V)>,
  {
    let db_version = self.clock.tick();

    // Check if the record is tombstoned
    if self.is_record_tombstoned(record_id, false) {
      return Vec::new();
    }

    let mut changes = Vec::new();
    let node_id = self.node_id; // Store node_id before mutable borrow
    let record = self.get_or_create_record_unchecked(record_id, false);

    for (col_name, value) in fields {
      let col_version = if let Some(col_info) = record.column_versions.get_mut(&col_name) {
        col_info.col_version += 1;
        col_info.db_version = db_version;
        col_info.node_id = node_id;
        col_info.local_db_version = db_version;
        col_info.col_version
      } else {
        record.column_versions.insert(
          col_name.clone(),
          ColumnVersion::new(1, db_version, node_id, db_version),
        );
        1
      };

      // Update record version boundaries
      if db_version < record.lowest_local_db_version {
        record.lowest_local_db_version = db_version;
      }
      if db_version > record.highest_local_db_version {
        record.highest_local_db_version = db_version;
      }

      record.fields.insert(col_name.clone(), value.clone());
      changes.push(Change::new(
        record_id.clone(),
        Some(col_name),
        Some(value),
        col_version,
        db_version,
        node_id,
        db_version,
        flags,
      ));
    }

    changes
  }

  /// Deletes a record by marking it as tombstoned.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record
  ///
  /// # Returns
  ///
  /// An optional Change representing the deletion
  #[must_use = "changes should be propagated to other nodes"]
  pub fn delete_record(&mut self, record_id: &K) -> Option<Change<K, V>> {
    self.delete_record_with_flags(record_id, 0)
  }

  /// Deletes a record with flags.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record
  /// * `flags` - Flags to indicate the type of change
  ///
  /// # Returns
  ///
  /// An optional Change representing the deletion
  #[must_use = "changes should be propagated to other nodes"]
  pub fn delete_record_with_flags(&mut self, record_id: &K, flags: u32) -> Option<Change<K, V>> {
    if self.is_record_tombstoned(record_id, false) {
      return None;
    }

    let db_version = self.clock.tick();

    // Mark as tombstone and remove data
    self.data.remove(record_id);

    // Store deletion information in tombstones
    self.tombstones.insert_or_assign(
      record_id.clone(),
      TombstoneInfo::new(db_version, self.node_id, db_version),
    );

    Some(Change::new(
      record_id.clone(),
      None,
      None,
      TOMBSTONE_COL_VERSION,
      db_version,
      self.node_id,
      db_version,
      flags,
    ))
  }

  /// Merges incoming changes into the CRDT.
  ///
  /// # Arguments
  ///
  /// * `changes` - Vector of changes to merge
  /// * `merge_rule` - The merge rule to use for conflict resolution
  ///
  /// # Returns
  ///
  /// Vector of accepted changes (if requested)
  pub fn merge_changes<R: MergeRule<K, V>>(
    &mut self,
    changes: Vec<Change<K, V>>,
    merge_rule: &R,
  ) -> Vec<Change<K, V>> {
    self.merge_changes_impl(changes, false, merge_rule)
  }

  fn merge_changes_impl<R: MergeRule<K, V>>(
    &mut self,
    changes: Vec<Change<K, V>>,
    ignore_parent: bool,
    merge_rule: &R,
  ) -> Vec<Change<K, V>> {
    let mut accepted_changes = Vec::new();

    if changes.is_empty() {
      return accepted_changes;
    }

    for change in changes {
      // Move values from change to avoid unnecessary clones
      let Change {
        record_id,
        col_name,
        value: remote_value,
        col_version: remote_col_version,
        db_version: remote_db_version,
        node_id: remote_node_id,
        flags,
        ..
      } = change;

      // Always update the logical clock to maintain causal consistency
      let new_local_db_version = self.clock.update(remote_db_version);

      // Skip all changes for tombstoned records
      if self.is_record_tombstoned(&record_id, ignore_parent) {
        continue;
      }

      // Retrieve local column version information
      let local_col_info = if col_name.is_none() {
        // For deletions, check tombstones
        self
          .tombstones
          .find(&record_id)
          .map(|info| info.as_column_version())
      } else if let Some(ref col) = col_name {
        // For column updates, check the record
        self
          .get_record_ptr(&record_id, ignore_parent)
          .and_then(|record| record.column_versions.get(col).copied())
      } else {
        None
      };

      // Determine whether to accept the remote change
      let should_accept = if let Some(local_info) = local_col_info {
        merge_rule.should_accept(
          local_info.col_version,
          local_info.db_version,
          local_info.node_id,
          remote_col_version,
          remote_db_version,
          remote_node_id,
        )
      } else {
        true
      };

      if should_accept {
        if let Some(col_key) = col_name {
          // Handle insertion or update
          let record = self.get_or_create_record_unchecked(&record_id, ignore_parent);

          // Update field value
          if let Some(value) = remote_value.clone() {
            record.fields.insert(col_key.clone(), value);
          } else {
            // If remote_value is None, remove the field
            record.fields.remove(&col_key);
          }

          // Update the column version info and record version boundaries
          record.column_versions.insert(
            col_key.clone(),
            ColumnVersion::new(
              remote_col_version,
              remote_db_version,
              remote_node_id,
              new_local_db_version,
            ),
          );

          // Update version boundaries
          if new_local_db_version < record.lowest_local_db_version {
            record.lowest_local_db_version = new_local_db_version;
          }
          if new_local_db_version > record.highest_local_db_version {
            record.highest_local_db_version = new_local_db_version;
          }

          accepted_changes.push(Change::new(
            record_id,
            Some(col_key),
            remote_value,
            remote_col_version,
            remote_db_version,
            remote_node_id,
            new_local_db_version,
            flags,
          ));
        } else {
          // Handle deletion
          self.data.remove(&record_id);

          // Store deletion information in tombstones
          self.tombstones.insert_or_assign(
            record_id.clone(),
            TombstoneInfo::new(remote_db_version, remote_node_id, new_local_db_version),
          );

          accepted_changes.push(Change::new(
            record_id,
            None,
            None,
            remote_col_version,
            remote_db_version,
            remote_node_id,
            new_local_db_version,
            flags,
          ));
        }
      }
    }

    accepted_changes
  }

  /// Retrieves all changes since a given `last_db_version`.
  ///
  /// # Arguments
  ///
  /// * `last_db_version` - The database version to retrieve changes since
  ///
  /// # Returns
  ///
  /// A vector of changes
  #[must_use]
  pub fn get_changes_since(&self, last_db_version: u64) -> Vec<Change<K, V>>
  where
    K: Ord,
  {
    self.get_changes_since_excluding(last_db_version, &HashSet::new())
  }

  /// Retrieves all changes since a given `last_db_version`, excluding specific nodes.
  pub fn get_changes_since_excluding(
    &self,
    last_db_version: u64,
    excluding: &HashSet<NodeId>,
  ) -> Vec<Change<K, V>>
  where
    K: Ord,
  {
    let mut changes = Vec::new();

    // Get changes from parent
    if let Some(ref parent) = self.parent {
      let parent_changes = parent.get_changes_since_excluding(last_db_version, excluding);
      changes.extend(parent_changes);
    }

    // Get changes from regular records
    for (record_id, record) in &self.data {
      // Skip records that haven't changed since last_db_version
      if record.highest_local_db_version <= last_db_version {
        continue;
      }

      for (col_name, clock_info) in &record.column_versions {
        if clock_info.local_db_version > last_db_version && !excluding.contains(&clock_info.node_id)
        {
          let value = record.fields.get(col_name).cloned();

          changes.push(Change::new(
            record_id.clone(),
            Some(col_name.clone()),
            value,
            clock_info.col_version,
            clock_info.db_version,
            clock_info.node_id,
            clock_info.local_db_version,
            0,
          ));
        }
      }
    }

    // Get deletion changes from tombstones
    for (record_id, tombstone_info) in self.tombstones.iter() {
      if tombstone_info.local_db_version > last_db_version
        && !excluding.contains(&tombstone_info.node_id)
      {
        changes.push(Change::new(
          record_id.clone(),
          None,
          None,
          TOMBSTONE_COL_VERSION,
          tombstone_info.db_version,
          tombstone_info.node_id,
          tombstone_info.local_db_version,
          0,
        ));
      }
    }

    if self.parent.is_some() {
      // Compress changes to remove redundant operations
      Self::compress_changes(&mut changes);
    }

    changes
  }

  /// Compresses a vector of changes in-place by removing redundant changes.
  ///
  /// Changes are sorted and then compressed using a two-pointer technique.
  ///
  /// # Performance
  ///
  /// This method uses `sort_unstable_by` which provides O(n log n) average time complexity
  /// but does not preserve the relative order of equal elements. Since the comparator
  /// provides a total ordering, stability is not required.
  pub fn compress_changes(changes: &mut Vec<Change<K, V>>)
  where
    K: Ord,
  {
    if changes.is_empty() {
      return;
    }

    // Sort changes using the DefaultChangeComparator
    // Use sort_unstable for better performance since we don't need stable sorting
    let comparator = DefaultChangeComparator;
    changes.sort_unstable_by(|a, b| comparator.compare(a, b));

    // Use two-pointer technique to compress in-place
    let mut write = 0;
    for read in 1..changes.len() {
      if changes[read].record_id != changes[write].record_id {
        // New record, always keep it
        write += 1;
        if write != read {
          changes[write] = changes[read].clone();
        }
      } else if changes[read].col_name.is_none() && changes[write].col_name.is_some() {
        // Current read is a deletion, backtrack to first change for this record
        // and replace it with the deletion, effectively discarding all field updates
        let mut first_pos = write;
        while first_pos > 0 && changes[first_pos - 1].record_id == changes[read].record_id {
          first_pos -= 1;
        }
        changes[first_pos] = changes[read].clone();
        write = first_pos;
      } else if changes[read].col_name != changes[write].col_name
        && changes[write].col_name.is_some()
      {
        // New column for the same record
        write += 1;
        if write != read {
          changes[write] = changes[read].clone();
        }
      }
      // Else: same record and column, keep the existing one (most recent due to sorting)
    }

    changes.truncate(write + 1);
  }

  /// Retrieves a reference to a record if it exists.
  pub fn get_record(&self, record_id: &K) -> Option<&Record<V>> {
    self.get_record_ptr(record_id, false)
  }

  /// Checks if a record is tombstoned.
  pub fn is_tombstoned(&self, record_id: &K) -> bool {
    self.is_record_tombstoned(record_id, false)
  }

  /// Gets tombstone information for a record.
  pub fn get_tombstone(&self, record_id: &K) -> Option<TombstoneInfo> {
    if let Some(info) = self.tombstones.find(record_id) {
      return Some(info);
    }

    if let Some(ref parent) = self.parent {
      return parent.get_tombstone(record_id);
    }

    None
  }

  /// Removes tombstones older than the specified version.
  ///
  /// # Safety and DoS Mitigation
  ///
  /// **IMPORTANT**: Only call this method when ALL participating nodes have acknowledged
  /// the `min_acknowledged_version`. Compacting too early may cause deleted records to
  /// reappear on nodes that haven't received the deletion yet.
  ///
  /// To prevent DoS via tombstone accumulation:
  /// - Call this method periodically as part of your sync protocol
  /// - Track which versions have been acknowledged by all nodes
  /// - Consider implementing a tombstone limit and rejecting operations when exceeded
  ///
  /// # Arguments
  ///
  /// * `min_acknowledged_version` - Tombstones with db_version < this value will be removed
  ///
  /// # Returns
  ///
  /// The number of tombstones removed
  pub fn compact_tombstones(&mut self, min_acknowledged_version: u64) -> usize {
    self.tombstones.compact(min_acknowledged_version)
  }

  /// Gets the number of tombstones currently stored.
  pub fn tombstone_count(&self) -> usize {
    self.tombstones.len()
  }

  /// Gets the current logical clock.
  pub fn get_clock(&self) -> &LogicalClock {
    &self.clock
  }

  /// Gets a reference to the internal data map.
  pub fn get_data(&self) -> &HashMap<K, Record<V>> {
    &self.data
  }

  /// Serializes the CRDT to a JSON string.
  ///
  /// Note: The parent relationship is not serialized and must be rebuilt after deserialization.
  ///
  /// # Errors
  ///
  /// Returns an error if serialization fails.
  #[cfg(feature = "json")]
  pub fn to_json(&self) -> Result<String, serde_json::Error>
  where
    K: serde::Serialize,
    V: serde::Serialize,
  {
    serde_json::to_string(self)
  }

  /// Deserializes a CRDT from a JSON string.
  ///
  /// Note: The parent relationship is not deserialized and will be `None`.
  /// Applications must rebuild parent-child relationships if needed.
  ///
  /// # Errors
  ///
  /// Returns an error if deserialization fails.
  #[cfg(feature = "json")]
  pub fn from_json(json: &str) -> Result<Self, serde_json::Error>
  where
    K: serde::de::DeserializeOwned + Hash + Eq + Clone,
    V: serde::de::DeserializeOwned + Clone,
  {
    serde_json::from_str(json)
  }

  /// Serializes the CRDT to bytes using bincode.
  ///
  /// Note: The parent relationship is not serialized and must be rebuilt after deserialization.
  ///
  /// # Errors
  ///
  /// Returns an error if serialization fails.
  #[cfg(feature = "binary")]
  pub fn to_bytes(&self) -> Result<Vec<u8>, bincode::error::EncodeError>
  where
    K: serde::Serialize,
    V: serde::Serialize,
  {
    bincode::serde::encode_to_vec(self, bincode::config::standard())
  }

  /// Deserializes a CRDT from bytes using bincode.
  ///
  /// Note: The parent relationship is not deserialized and will be `None`.
  /// Applications must rebuild parent-child relationships if needed.
  ///
  /// # Errors
  ///
  /// Returns an error if deserialization fails.
  #[cfg(feature = "binary")]
  pub fn from_bytes(bytes: &[u8]) -> Result<Self, bincode::error::DecodeError>
  where
    K: serde::de::DeserializeOwned + Hash + Eq + Clone,
    V: serde::de::DeserializeOwned + Clone,
  {
    let (result, _len) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
    Ok(result)
  }

  // Helper methods

  fn is_record_tombstoned(&self, record_id: &K, ignore_parent: bool) -> bool {
    if self.tombstones.find(record_id).is_some() {
      return true;
    }

    if !ignore_parent {
      if let Some(ref parent) = self.parent {
        return parent.is_record_tombstoned(record_id, false);
      }
    }

    false
  }

  fn get_or_create_record_unchecked(
    &mut self,
    record_id: &K,
    ignore_parent: bool,
  ) -> &mut Record<V> {
    #[cfg(feature = "std")]
    use std::collections::hash_map::Entry;
    #[cfg(not(feature = "std"))]
    use hashbrown::hash_map::Entry;

    match self.data.entry(record_id.clone()) {
      Entry::Occupied(e) => e.into_mut(),
      Entry::Vacant(e) => {
        let record = if !ignore_parent {
          self
            .parent
            .as_ref()
            .and_then(|p| p.get_record_ptr(record_id, false))
            .cloned()
            .unwrap_or_else(Record::new)
        } else {
          Record::new()
        };
        e.insert(record)
      }
    }
  }

  fn get_record_ptr(&self, record_id: &K, ignore_parent: bool) -> Option<&Record<V>> {
    if let Some(record) = self.data.get(record_id) {
      return Some(record);
    }

    if !ignore_parent {
      if let Some(ref parent) = self.parent {
        return parent.get_record_ptr(record_id, false);
      }
    }

    None
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_logical_clock() {
    let mut clock = LogicalClock::new();
    assert_eq!(clock.current_time(), 0);

    let t1 = clock.tick();
    assert_eq!(t1, 1);
    assert_eq!(clock.current_time(), 1);

    let t2 = clock.update(5);
    assert_eq!(t2, 6);
    assert_eq!(clock.current_time(), 6);
  }

  #[test]
  fn test_tombstone_storage() {
    let mut storage = TombstoneStorage::new();
    let info = TombstoneInfo::new(10, 1, 10);

    storage.insert_or_assign("key1".to_string(), info);
    assert_eq!(storage.len(), 1);

    assert_eq!(storage.find(&"key1".to_string()), Some(info));
    assert_eq!(storage.find(&"key2".to_string()), None);

    let removed = storage.compact(15);
    assert_eq!(removed, 1);
    assert_eq!(storage.len(), 0);
  }

  #[test]
  fn test_basic_insert() {
    let mut crdt: CRDT<String, String> = CRDT::new(1, None);

    let fields = vec![
      ("name".to_string(), "Alice".to_string()),
      ("age".to_string(), "30".to_string()),
    ];

    let changes = crdt.insert_or_update(&"user1".to_string(), fields);

    assert_eq!(changes.len(), 2);
    assert_eq!(crdt.get_data().len(), 1);

    let record = crdt.get_record(&"user1".to_string()).unwrap();
    assert_eq!(record.fields.get("name").unwrap(), "Alice");
    assert_eq!(record.fields.get("age").unwrap(), "30");
  }

  #[test]
  fn test_delete_record() {
    let mut crdt: CRDT<String, String> = CRDT::new(1, None);

    let fields = vec![("name".to_string(), "Bob".to_string())];
    let _ = crdt.insert_or_update(&"user2".to_string(), fields);

    let delete_change = crdt.delete_record(&"user2".to_string());
    assert!(delete_change.is_some());
    assert!(crdt.is_tombstoned(&"user2".to_string()));
    assert_eq!(crdt.get_data().len(), 0);
  }

  #[test]
  fn test_merge_changes() {
    let mut crdt1: CRDT<String, String> = CRDT::new(1, None);
    let mut crdt2: CRDT<String, String> = CRDT::new(2, None);

    let fields1 = vec![("tag".to_string(), "Node1".to_string())];
    let changes1 = crdt1.insert_or_update(&"record1".to_string(), fields1);

    let fields2 = vec![("tag".to_string(), "Node2".to_string())];
    let changes2 = crdt2.insert_or_update(&"record1".to_string(), fields2);

    let merge_rule = DefaultMergeRule;
    crdt1.merge_changes(changes2, &merge_rule);
    crdt2.merge_changes(changes1, &merge_rule);

    // Node2 has higher node_id, so its value should win
    assert_eq!(
      crdt1
        .get_record(&"record1".to_string())
        .unwrap()
        .fields
        .get("tag")
        .unwrap(),
      "Node2"
    );
    assert_eq!(crdt1.get_data(), crdt2.get_data());
  }

  #[test]
  #[cfg(feature = "serde")]
  fn test_change_serialization() {
    #[allow(unused_variables)]
    let change = Change::new(
      "record1".to_string(),
      Some("name".to_string()),
      Some("Alice".to_string()),
      1,
      10,
      1,
      10,
      0,
    );

    // Test JSON serialization
    #[cfg(feature = "json")]
    {
      let json = serde_json::to_string(&change).unwrap();
      let deserialized: Change<String, String> = serde_json::from_str(&json).unwrap();
      assert_eq!(change, deserialized);
    }

    // Test binary serialization
    #[cfg(feature = "binary")]
    {
      let bytes = bincode::serde::encode_to_vec(&change, bincode::config::standard()).unwrap();
      let (deserialized, _): (Change<String, String>, _) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
      assert_eq!(change, deserialized);
    }
  }

  #[test]
  #[cfg(feature = "serde")]
  fn test_record_serialization() {
    let mut fields = HashMap::new();
    fields.insert("name".to_string(), "Bob".to_string());
    fields.insert("age".to_string(), "25".to_string());

    let mut column_versions = HashMap::new();
    column_versions.insert("name".to_string(), ColumnVersion::new(1, 10, 1, 10));
    column_versions.insert("age".to_string(), ColumnVersion::new(1, 11, 1, 11));

    #[allow(unused_variables)]
    let record = Record::from_parts(fields, column_versions);

    // Test JSON serialization
    #[cfg(feature = "json")]
    {
      let json = serde_json::to_string(&record).unwrap();
      let deserialized: Record<String> = serde_json::from_str(&json).unwrap();
      assert_eq!(record, deserialized);
    }

    // Test binary serialization
    #[cfg(feature = "binary")]
    {
      let bytes = bincode::serde::encode_to_vec(&record, bincode::config::standard()).unwrap();
      let (deserialized, _): (Record<String>, _) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
      assert_eq!(record, deserialized);
    }
  }

  #[test]
  #[cfg(feature = "json")]
  fn test_crdt_json_serialization() {
    let mut crdt: CRDT<String, String> = CRDT::new(1, None);

    // Add some data
    let fields = vec![
      ("name".to_string(), "Alice".to_string()),
      ("age".to_string(), "30".to_string()),
    ];
    let _ = crdt.insert_or_update(&"user1".to_string(), fields);

    let fields2 = vec![("name".to_string(), "Bob".to_string())];
    let _ = crdt.insert_or_update(&"user2".to_string(), fields2);

    // Delete one record
    let _ = crdt.delete_record(&"user2".to_string());

    // Serialize to JSON
    let json = crdt.to_json().unwrap();

    // Deserialize
    let deserialized: CRDT<String, String> = CRDT::from_json(&json).unwrap();

    // Verify data is preserved
    assert_eq!(crdt.get_data().len(), deserialized.get_data().len());
    assert_eq!(
      crdt.get_record(&"user1".to_string()).unwrap().fields,
      deserialized.get_record(&"user1".to_string()).unwrap().fields
    );

    // Verify tombstones are preserved
    assert_eq!(crdt.tombstone_count(), deserialized.tombstone_count());
    assert!(deserialized.is_tombstoned(&"user2".to_string()));

    // Verify clock is preserved
    assert_eq!(
      crdt.get_clock().current_time(),
      deserialized.get_clock().current_time()
    );

    // Verify parent is None after deserialization
    let has_parent = deserialized.parent.is_some();
    assert!(!has_parent);
  }

  #[test]
  #[cfg(feature = "binary")]
  fn test_crdt_binary_serialization() {
    let mut crdt: CRDT<String, String> = CRDT::new(1, None);

    // Add some data
    let fields = vec![
      ("name".to_string(), "Alice".to_string()),
      ("age".to_string(), "30".to_string()),
    ];
    let _ = crdt.insert_or_update(&"user1".to_string(), fields);

    // Serialize to bytes
    let bytes = crdt.to_bytes().unwrap();

    // Deserialize
    let deserialized: CRDT<String, String> = CRDT::from_bytes(&bytes).unwrap();

    // Verify data is preserved
    assert_eq!(crdt.get_data().len(), deserialized.get_data().len());
    assert_eq!(
      crdt.get_record(&"user1".to_string()).unwrap().fields,
      deserialized.get_record(&"user1".to_string()).unwrap().fields
    );

    // Verify clock is preserved
    assert_eq!(
      crdt.get_clock().current_time(),
      deserialized.get_clock().current_time()
    );
  }

  #[test]
  #[cfg(feature = "serde")]
  fn test_parent_not_serialized() {
    // Create a parent CRDT
    let mut parent: CRDT<String, String> = CRDT::new(1, None);
    let fields = vec![("parent_field".to_string(), "parent_value".to_string())];
    let _ = parent.insert_or_update(&"parent_record".to_string(), fields);

    // Create a child CRDT with parent
    let parent_arc = Arc::new(parent);
    let mut child = CRDT::new(2, Some(parent_arc.clone()));
    let child_fields = vec![("child_field".to_string(), "child_value".to_string())];
    let _ = child.insert_or_update(&"child_record".to_string(), child_fields);

    // Serialize and deserialize the child
    #[cfg(feature = "json")]
    {
      let json = serde_json::to_string(&child).unwrap();
      let deserialized: CRDT<String, String> = serde_json::from_str(&json).unwrap();

      // Verify parent is None
      assert!(deserialized.parent.is_none());

      // Verify child's own data is preserved
      assert!(deserialized.get_record(&"child_record".to_string()).is_some());

      // Verify parent's data is NOT in deserialized child
      assert!(deserialized.get_record(&"parent_record".to_string()).is_none());
    }
  }
}
