//! Persistence layer for CRDTs with WAL-based storage and hooks.
//!
//! This module provides a `PersistedCRDT` wrapper around the core `CRDT` type that:
//! - Maintains an append-only Write-Ahead Log (WAL) for durability
//! - Automatically creates snapshots to prevent unbounded WAL growth
//! - Provides hooks for pre-operation validation and post-operation broadcasting
//! - Returns changes from all operations for async network propagation
//!
//! # Example
//!
//! ```no_run
//! use crdt_lite::persist::{PersistedCRDT, PersistConfig};
//! use std::path::PathBuf;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open or create a persisted CRDT
//! let mut pcrdt = PersistedCRDT::<String, String, String>::open(
//!     PathBuf::from("./data"),
//!     1, // node_id
//!     PersistConfig::default(),
//! )?;
//!
//! // Operations automatically persist and return changes for network broadcast
//! let changes = pcrdt.insert_or_update(
//!     &"user123".to_string(),
//!     [("name".to_string(), "Alice".to_string())].into_iter()
//! )?;
//!
//! // Send changes to network peers
//! // network_broadcast(changes);
//! # Ok(())
//! # }
//! ```

mod hooks;
mod wal;

pub use hooks::{HookError, PostOpHook, PreOpHook, SnapshotHook, WalSegmentHook};

use crate::{Change, DefaultMergeRule, MergeRule, NodeId, CRDT};
use std::collections::HashSet;
use std::hash::Hash;
use std::io;
use std::path::PathBuf;
use wal::WalWriter;

/// Configuration for the persistence layer.
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Number of changes before automatic snapshot creation (default: 1000)
    pub snapshot_threshold: usize,
    /// Whether to enable compression for snapshots and WAL files (default: false)
    pub enable_compression: bool,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            snapshot_threshold: 1000,
            enable_compression: false,
        }
    }
}

/// Error types for persistence operations.
#[derive(Debug)]
pub enum PersistError {
    /// I/O error during file operations
    Io(io::Error),
    /// Serialization/deserialization error
    Codec(bincode::error::EncodeError),
    /// Deserialization error
    Decode(bincode::error::DecodeError),
    /// Hook rejected the operation
    HookRejection(HookError),
    /// Invalid persistence directory structure
    InvalidDirectory(String),
}

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistError::Io(e) => write!(f, "I/O error: {}", e),
            PersistError::Codec(e) => write!(f, "Encoding error: {}", e),
            PersistError::Decode(e) => write!(f, "Decoding error: {}", e),
            PersistError::HookRejection(e) => write!(f, "Hook rejected operation: {}", e),
            PersistError::InvalidDirectory(msg) => write!(f, "Invalid directory: {}", msg),
        }
    }
}

impl std::error::Error for PersistError {}

impl From<io::Error> for PersistError {
    fn from(e: io::Error) -> Self {
        PersistError::Io(e)
    }
}

impl From<bincode::error::EncodeError> for PersistError {
    fn from(e: bincode::error::EncodeError) -> Self {
        PersistError::Codec(e)
    }
}

impl From<bincode::error::DecodeError> for PersistError {
    fn from(e: bincode::error::DecodeError) -> Self {
        PersistError::Decode(e)
    }
}

impl From<HookError> for PersistError {
    fn from(e: HookError) -> Self {
        PersistError::HookRejection(e)
    }
}

/// A CRDT with persistence, WAL, and hook support.
///
/// This wrapper provides:
/// - Automatic persistence of all operations to a Write-Ahead Log
/// - Periodic snapshots to prevent unbounded WAL growth
/// - Pre-operation and post-operation hooks
/// - Batch change collection for efficient network broadcasting
///
/// # Type Parameters
///
/// - `K`: Record key type (must be `Hash + Eq + Clone`)
/// - `C`: Column/field key type (must be `Hash + Eq + Clone`)
/// - `V`: Value type (must be `Clone`)
pub struct PersistedCRDT<K, C, V>
where
    K: Hash + Eq + Clone,
    C: Hash + Eq + Clone,
    V: Clone,
{
    /// The underlying CRDT instance
    crdt: CRDT<K, C, V>,
    /// WAL writer for append-only change log
    wal: WalWriter<K, C, V>,
    /// Persistence configuration
    config: PersistConfig,
    /// Pre-operation hooks (validation)
    pre_hooks: Vec<Box<dyn PreOpHook<K, C, V>>>,
    /// Post-operation hooks (broadcasting)
    post_hooks: Vec<Box<dyn PostOpHook<K, C, V>>>,
    /// Snapshot hooks (backup/replication)
    snapshot_hooks: Vec<Box<dyn SnapshotHook>>,
    /// WAL segment hooks (archival)
    wal_segment_hooks: Vec<Box<dyn WalSegmentHook>>,
    /// Changes accumulated since last snapshot
    changes_since_snapshot: usize,
    /// Current snapshot version number
    snapshot_version: u64,
    /// Base directory for persistence files
    base_path: PathBuf,
    /// Changes collected for batch operations
    batch_collector: Vec<Change<K, C, V>>,
}

impl<K, C, V> PersistedCRDT<K, C, V>
where
    K: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    C: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Opens or creates a persisted CRDT at the specified path.
    ///
    /// If the directory contains a snapshot and WAL files, they are loaded and replayed.
    /// Otherwise, a new empty CRDT is created.
    ///
    /// # Arguments
    ///
    /// * `base_path` - Directory for persistence files (created if doesn't exist)
    /// * `node_id` - Unique identifier for this node
    /// * `config` - Persistence configuration
    ///
    /// # Errors
    ///
    /// Returns `PersistError` if:
    /// - Directory cannot be created
    /// - Existing files are corrupted
    /// - I/O errors occur
    pub fn open(
        base_path: PathBuf,
        node_id: NodeId,
        config: PersistConfig,
    ) -> Result<Self, PersistError> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&base_path)?;

        // Try to load existing state
        let (crdt, wal, changes_since_snapshot, snapshot_version) = Self::recover(&base_path, node_id)?;

        Ok(Self {
            crdt,
            wal,
            config,
            pre_hooks: Vec::new(),
            post_hooks: Vec::new(),
            snapshot_hooks: Vec::new(),
            wal_segment_hooks: Vec::new(),
            changes_since_snapshot,
            snapshot_version,
            base_path,
            batch_collector: Vec::new(),
        })
    }

    /// Returns a reference to the underlying CRDT for read-only operations.
    ///
    /// Use this for queries that don't modify the CRDT state.
    pub fn crdt(&self) -> &CRDT<K, C, V> {
        &self.crdt
    }

    /// Adds a pre-operation hook for validation.
    ///
    /// Pre-hooks are called before any operation is applied to the CRDT.
    /// If any hook returns an error, the operation is rejected.
    pub fn add_pre_hook(&mut self, hook: Box<dyn PreOpHook<K, C, V>>) {
        self.pre_hooks.push(hook);
    }

    /// Adds a post-operation hook for broadcasting.
    ///
    /// Post-hooks are called after an operation has been successfully applied
    /// and persisted. They cannot reject the operation.
    pub fn add_post_hook(&mut self, hook: Box<dyn PostOpHook<K, C, V>>) {
        self.post_hooks.push(hook);
    }

    /// Adds a snapshot hook for backup/replication.
    ///
    /// Snapshot hooks are called after a snapshot has been created and fsynced.
    /// The snapshot file is immutable (sealed) and safe to upload/copy.
    pub fn add_snapshot_hook(&mut self, hook: Box<dyn SnapshotHook>) {
        self.snapshot_hooks.push(hook);
    }

    /// Adds a WAL segment hook for archival.
    ///
    /// WAL segment hooks are called after a segment has been sealed (rotated out).
    /// The segment file is immutable and safe to upload/archive.
    pub fn add_wal_segment_hook(&mut self, hook: Box<dyn WalSegmentHook>) {
        self.wal_segment_hooks.push(hook);
    }

    /// Returns the accumulated batch of changes and clears the collector.
    ///
    /// Useful for batching multiple operations before network broadcast.
    pub fn take_batch(&mut self) -> Vec<Change<K, C, V>> {
        std::mem::take(&mut self.batch_collector)
    }

    /// Returns a reference to the current batch without clearing it.
    pub fn peek_batch(&self) -> &[Change<K, C, V>] {
        &self.batch_collector
    }

    /// Manually trigger a snapshot.
    ///
    /// This writes the current CRDT state to disk, fsyncs it, and rotates the WAL.
    /// Snapshots are also created automatically based on `snapshot_threshold`.
    pub fn snapshot(&mut self) -> Result<(), PersistError> {
        self.create_snapshot()
    }

    /// Get the current number of changes since the last snapshot.
    pub fn changes_since_snapshot(&self) -> usize {
        self.changes_since_snapshot
    }

    /// Deletes old snapshot files, keeping only the N most recent.
    ///
    /// Call this after successfully uploading snapshots to remote storage.
    ///
    /// # Arguments
    ///
    /// * `keep_count` - Number of most recent snapshots to keep
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crdt_lite::persist::{PersistedCRDT, PersistConfig};
    /// # use std::path::PathBuf;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    /// #     PathBuf::from("./data"), 1, PersistConfig::default())?;
    /// // After uploading to R2, keep only last 2 snapshots locally
    /// pcrdt.cleanup_old_snapshots(2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cleanup_old_snapshots(&self, keep_count: usize) -> Result<(), PersistError> {
        let mut snapshots = Vec::new();

        // Find all snapshot files
        if let Ok(entries) = std::fs::read_dir(&self.base_path) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with("snapshot_") && filename_str.ends_with(".bin") {
                    if let Some(num_str) = filename_str
                        .strip_prefix("snapshot_")
                        .and_then(|s| s.strip_suffix(".bin"))
                    {
                        if let Ok(version) = num_str.parse::<u64>() {
                            snapshots.push((version, entry.path()));
                        }
                    }
                }
            }
        }

        // Sort by version (newest first)
        snapshots.sort_by(|a, b| b.0.cmp(&a.0));

        // Delete all except the most recent N
        for (_, path) in snapshots.iter().skip(keep_count) {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }

    /// Deletes old WAL segment files, keeping only the N most recent.
    ///
    /// Call this after successfully uploading WAL segments to remote storage.
    ///
    /// # Arguments
    ///
    /// * `keep_count` - Number of most recent segments to keep
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crdt_lite::persist::{PersistedCRDT, PersistConfig};
    /// # use std::path::PathBuf;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    /// #     PathBuf::from("./data"), 1, PersistConfig::default())?;
    /// // After uploading to R2, keep only last 3 WAL segments locally
    /// pcrdt.cleanup_old_wal_segments(3)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cleanup_old_wal_segments(&self, keep_count: usize) -> Result<(), PersistError> {
        let mut segments = Vec::new();

        // Find all WAL segment files
        if let Ok(entries) = std::fs::read_dir(&self.base_path) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with("wal_") && filename_str.ends_with(".bin") {
                    if let Some(num_str) = filename_str
                        .strip_prefix("wal_")
                        .and_then(|s| s.strip_suffix(".bin"))
                    {
                        if let Ok(segment_num) = num_str.parse::<u64>() {
                            segments.push((segment_num, entry.path()));
                        }
                    }
                }
            }
        }

        // Sort by segment number (newest first)
        segments.sort_by(|a, b| b.0.cmp(&a.0));

        // Delete all except the most recent N
        for (_, path) in segments.iter().skip(keep_count) {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }
}

// Private implementation methods
impl<K, C, V> PersistedCRDT<K, C, V>
where
    K: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    C: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Recovers CRDT state from disk: loads snapshot + replays WAL files.
    fn recover(
        base_path: &PathBuf,
        node_id: NodeId,
    ) -> Result<(CRDT<K, C, V>, WalWriter<K, C, V>, usize, u64), PersistError> {
        // Find the latest snapshot file (snapshot_000001.bin, snapshot_000002.bin, etc.)
        let mut max_snapshot_version = 0u64;
        let mut latest_snapshot_path = None;

        if let Ok(entries) = std::fs::read_dir(base_path) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with("snapshot_") && filename_str.ends_with(".bin") {
                    // Parse version number from filename (snapshot_000001.bin)
                    if let Some(num_str) = filename_str
                        .strip_prefix("snapshot_")
                        .and_then(|s| s.strip_suffix(".bin"))
                    {
                        if let Ok(num) = num_str.parse::<u64>() {
                            if num > max_snapshot_version {
                                max_snapshot_version = num;
                                latest_snapshot_path = Some(entry.path());
                            }
                        }
                    }
                }
            }
        }

        // Load the latest snapshot or create new CRDT
        let crdt = if let Some(path) = latest_snapshot_path {
            let bytes = std::fs::read(&path)?;
            CRDT::from_bytes(&bytes).map_err(PersistError::Decode)?
        } else {
            CRDT::new(node_id, None)
        };

        // Create WAL writer
        let wal = WalWriter::new(base_path.clone())?;

        // Replay all WAL files
        let (recovered_crdt, change_count) = Self::replay_wal(crdt, base_path)?;

        Ok((recovered_crdt, wal, change_count, max_snapshot_version))
    }

    /// Replays all WAL files on top of the base CRDT state.
    fn replay_wal(
        mut crdt: CRDT<K, C, V>,
        base_path: &PathBuf,
    ) -> Result<(CRDT<K, C, V>, usize), PersistError> {
        let mut total_changes = 0;

        // Find all WAL files (wal_*.bin)
        let mut wal_files: Vec<_> = std::fs::read_dir(base_path)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("wal_")
                    && entry.file_name().to_string_lossy().ends_with(".bin")
            })
            .collect();

        // Sort by filename to replay in order
        wal_files.sort_by_key(|entry| entry.file_name());

        // Replay each WAL file
        for entry in wal_files {
            let changes = wal::read_wal_file(&entry.path())?;
            total_changes += changes.len();
            crdt.merge_changes(changes, &DefaultMergeRule);
        }

        Ok((crdt, total_changes))
    }

    /// Creates a snapshot of the current CRDT state and rotates the WAL.
    fn create_snapshot(&mut self) -> Result<(), PersistError> {
        // Serialize CRDT to bytes
        let bytes = self
            .crdt
            .to_bytes()
            .map_err(|e| PersistError::Codec(e))?;

        // Increment snapshot version
        self.snapshot_version += 1;

        // Write to versioned snapshot file (snapshot_000001.bin, snapshot_000002.bin, etc.)
        let snapshot_path = self
            .base_path
            .join(format!("snapshot_{:06}.bin", self.snapshot_version));
        std::fs::write(&snapshot_path, &bytes)?;

        // fsync the snapshot (scope to ensure file is closed on Windows)
        {
            let file = std::fs::File::open(&snapshot_path)?;
            file.sync_all()?;
        } // file is dropped here, releasing any locks

        // Call snapshot hooks (file is now sealed and immutable)
        for hook in &self.snapshot_hooks {
            hook.on_snapshot(&snapshot_path);
        }

        // Rotate WAL (seals old segments, calls hooks, deletes them, starts new one)
        let _ = self.wal.rotate(&self.base_path, &self.wal_segment_hooks)?;

        // Reset counter
        self.changes_since_snapshot = 0;

        Ok(())
    }

    /// Checks if automatic snapshot threshold is reached and creates snapshot if needed.
    fn check_auto_snapshot(&mut self) -> Result<(), PersistError> {
        if self.changes_since_snapshot >= self.config.snapshot_threshold {
            self.create_snapshot()?;
        }
        Ok(())
    }

    /// Appends changes to the WAL and calls post-operation hooks.
    fn persist_and_notify(&mut self, changes: &[Change<K, C, V>]) -> Result<(), PersistError> {
        // Append to WAL (no fsync, OS buffers)
        self.wal.append(changes)?;

        // Update counter
        self.changes_since_snapshot += changes.len();

        // Add to batch collector
        self.batch_collector.extend_from_slice(changes);

        // Call post-hooks
        for hook in &self.post_hooks {
            hook.after_op(changes);
        }

        // Check if we need to snapshot
        self.check_auto_snapshot()?;

        Ok(())
    }
}

// Public CRDT operation methods that integrate persistence
impl<K, C, V> PersistedCRDT<K, C, V>
where
    K: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    C: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Inserts or updates fields in a record.
    ///
    /// This operation:
    /// 1. Applies changes to the in-memory CRDT
    /// 2. Appends changes to the WAL
    /// 3. Calls post-operation hooks
    /// 4. Returns changes for network broadcast
    /// 5. Auto-snapshots if threshold is reached
    ///
    /// # Arguments
    ///
    /// * `record_id` - The record to modify
    /// * `fields` - Iterator of (column, value) pairs to insert/update
    ///
    /// # Returns
    ///
    /// Vector of changes that should be broadcast to network peers.
    pub fn insert_or_update<I>(&mut self, record_id: &K, fields: I) -> Result<Vec<Change<K, C, V>>, PersistError>
    where
        I: IntoIterator<Item = (C, V)>,
    {
        // Apply to CRDT
        let changes = self.crdt.insert_or_update(record_id, fields);

        // Persist and notify
        self.persist_and_notify(&changes)?;

        Ok(changes)
    }

    /// Deletes an entire record (creates tombstone).
    ///
    /// See `insert_or_update` for operation flow.
    pub fn delete_record(&mut self, record_id: &K) -> Result<Option<Change<K, C, V>>, PersistError> {
        // Apply to CRDT
        let change = self.crdt.delete_record(record_id);

        if let Some(ref c) = change {
            // Persist and notify
            self.persist_and_notify(&[c.clone()])?;
        }

        Ok(change)
    }

    /// Deletes a specific field from a record.
    ///
    /// See `insert_or_update` for operation flow.
    pub fn delete_field(&mut self, record_id: &K, field_name: &C) -> Result<Option<Change<K, C, V>>, PersistError> {
        // Apply to CRDT
        let change = self.crdt.delete_field(record_id, field_name);

        if let Some(ref c) = change {
            // Persist and notify
            self.persist_and_notify(&[c.clone()])?;
        }

        Ok(change)
    }

    /// Merges incoming changes from remote nodes.
    ///
    /// This operation:
    /// 1. Applies changes to the in-memory CRDT
    /// 2. Appends accepted changes to the WAL
    /// 3. Calls post-operation hooks
    /// 4. Returns accepted changes
    /// 5. Auto-snapshots if threshold is reached
    ///
    /// # Arguments
    ///
    /// * `changes` - Changes received from remote nodes
    ///
    /// # Returns
    ///
    /// Vector of changes that were accepted (won conflict resolution).
    pub fn merge_changes(&mut self, changes: Vec<Change<K, C, V>>) -> Result<Vec<Change<K, C, V>>, PersistError> {
        // Apply to CRDT with default merge rule
        let accepted = self.crdt.merge_changes(changes, &DefaultMergeRule);

        // Persist and notify (only accepted changes)
        self.persist_and_notify(&accepted)?;

        Ok(accepted)
    }

    /// Merges incoming changes with a custom merge rule.
    pub fn merge_changes_with_rule<R: MergeRule<K, C, V>>(
        &mut self,
        changes: Vec<Change<K, C, V>>,
        merge_rule: &R,
    ) -> Result<Vec<Change<K, C, V>>, PersistError> {
        // Apply to CRDT with custom merge rule
        let accepted = self.crdt.merge_changes(changes, merge_rule);

        // Persist and notify
        self.persist_and_notify(&accepted)?;

        Ok(accepted)
    }

    /// Gets changes since a specific version, excluding certain nodes.
    ///
    /// Useful for syncing with remote peers.
    pub fn get_changes_since_excluding(
        &self,
        last_db_version: u64,
        excluding: &HashSet<NodeId>,
    ) -> Vec<Change<K, C, V>>
    where
        K: Ord,
        C: Ord,
    {
        self.crdt.get_changes_since_excluding(last_db_version, excluding)
    }

    /// Gets all changes since a specific version.
    pub fn get_changes_since(&self, last_db_version: u64) -> Vec<Change<K, C, V>>
    where
        K: Ord,
        C: Ord,
    {
        self.crdt.get_changes_since(last_db_version)
    }

    /// Compacts tombstones that have been acknowledged by all nodes.
    ///
    /// **IMPORTANT**: Only call this after verifying all nodes have acknowledged
    /// the `min_acknowledged_version`. Otherwise, deleted records may reappear.
    pub fn compact_tombstones(&mut self, min_acknowledged_version: u64) -> Result<(), PersistError> {
        self.crdt.compact_tombstones(min_acknowledged_version);
        // Force snapshot after compaction to persist the cleaned state
        self.create_snapshot()?;
        Ok(())
    }

    /// Returns the current logical clock value.
    pub fn get_clock_time(&self) -> u64 {
        self.crdt.get_clock().current_time()
    }

    /// Returns the node ID of this CRDT instance.
    pub fn node_id(&self) -> NodeId {
        self.crdt.node_id
    }
}
