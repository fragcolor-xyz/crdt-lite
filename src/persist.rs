//! Persistence layer for CRDTs with WAL-based storage and hooks.
//!
//! This module provides a `PersistedCRDT` wrapper around the core `CRDT` type that:
//! - Maintains an append-only Write-Ahead Log (WAL) for durability
//! - Automatically creates snapshots based on change count OR time elapsed
//! - Provides hooks for post-operation broadcasting and file sealing notifications
//! - Returns changes from all operations for async network propagation
//!
//! # Durability Guarantees (Important for CRDT Design)
//!
//! **By design, WAL writes are NOT fsynced immediately.** This is an intentional design choice
//! optimized for distributed CRDTs, where eventual consistency is more important than single-node
//! durability.
//!
//! ## Failure Modes and Data Safety
//!
//! | Failure Type | Data Loss | Why |
//! |--------------|-----------|-----|
//! | Process crash | **None** | OS page cache survives process termination |
//! | Kernel panic | **~0-30s** | Depends on kernel writeback (typically 30s) |
//! | Power failure | **Up to 1000 ops** | Unflushed WAL + page cache lost |
//!
//! **Key insight:** Most failures (process crashes) are fully recoverable even without fsync,
//! because the OS kernel maintains a page cache that survives process termination. The kernel
//! typically flushes dirty pages to disk every 30 seconds (configurable via `vm.dirty_writeback_centisecs`).
//!
//! ## Why No Fsync Per Write?
//!
//! 1. **Performance**: Fsync is expensive (10-100x slower than buffered writes)
//! 2. **Most crashes are safe anyway**: Process crashes (most common) don't lose page cache
//! 3. **CRDT Semantics**: Eventual consistency means data loss on one node is recoverable
//!    - If this node has power failure before fsync, peers have the data
//!    - On recovery, this node syncs from peers and gets the changes back
//!    - System-wide convergence is maintained
//! 4. **Broadcast First**: Hooks fire before fsync to minimize network propagation delay
//!    - Faster propagation = smaller conflict windows
//!    - Local durability deferred to batch fsync during snapshot
//!
//! ## What Snapshots Provide
//!
//! - **Guaranteed persistence**: Fsynced to disk (survives power failure)
//! - **Bounded recovery time**: No need to replay thousands of WAL operations
//! - **WAL compaction**: Can safely delete old segments after snapshot
//! - **Hook triggering**: Ensures backup hooks fire even during low activity
//!
//! Snapshots are triggered by either:
//! - **Change count** (default: 1000 operations) - for high-activity nodes
//! - **Time elapsed** (default: 5 minutes) - for low-activity nodes
//!
//! This dual-trigger ensures backup hooks fire regularly even if a node sees only a few changes
//! per day, preventing backup gaps in distributed storage scenarios.
//!
//! ## When This Design Is Appropriate
//!
//! ✅ **Multi-node CRDT deployments** (recommended use case)
//! - Changes replicate to peers before local fsync
//! - Crash recovery syncs from network
//! - No data loss from distributed system perspective
//!
//! ✅ **Single-node deployments with process isolation**
//! - Process crashes are fully recoverable (page cache intact)
//! - Kernel writeback provides reasonable power failure protection (~30s window)
//! - Time-based snapshots (default: 5 min) ensure regular backup hook triggering
//!
//! ⚠️ **Single-node deployments without power protection**
//! - Power failure can lose up to 5 minutes of data (default `snapshot_interval_secs`)
//! - No peers to recover from
//! - Mitigation: Use UPS, reduce `snapshot_interval_secs` to 60s, or accept risk
//!
//! ## Platform Notes
//!
//! **Windows**: File locking is stricter than Unix. The implementation includes 10ms sleeps
//! after file handle releases to allow the OS to propagate lock releases. Under heavy load
//! or slow systems, this may still fail. If you encounter `PermissionDenied` errors on Windows,
//! consider adding retry logic in your application.
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

pub use hooks::{PostOpHook, SnapshotHook, WalSegmentHook};

use crate::{Change, DefaultMergeRule, MergeRule, NodeId, Record, TombstoneInfo, CRDT};
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::io;
use std::path::PathBuf;
use std::time::Instant;
use wal::WalWriter;

/// Snapshot format type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SnapshotFormat {
    /// Legacy bincode format (no schema evolution)
    Bincode,
    /// MessagePack format (supports schema evolution)
    MessagePack,
}

/// Snapshot type - either full or incremental.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SnapshotType {
    /// Full snapshot containing entire CRDT state
    Full,
    /// Incremental snapshot containing only changes since a base version
    Incremental {
        /// The base version this incremental builds on
        base_version: u64,
    },
}

/// Metadata for snapshots (both full and incremental).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotMetadata {
    /// Type of snapshot (full or incremental)
    pub snapshot_type: SnapshotType,
    /// The logical clock version this snapshot represents
    pub version: u64,
    /// Format used for serialization
    pub format: SnapshotFormat,
    /// Timestamp when snapshot was created
    pub created_at: u64,
}

/// Full snapshot structure for MessagePack serialization.
#[cfg(feature = "msgpack")]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound(serialize = "K: serde::Serialize, C: serde::Serialize, V: serde::Serialize")))]
#[cfg_attr(feature = "serde", serde(bound(deserialize = "K: serde::de::DeserializeOwned + Hash + Eq + Clone, C: serde::de::DeserializeOwned + Hash + Eq + Clone, V: serde::de::DeserializeOwned + Clone")))]
pub struct FullSnapshot<K, C, V> {
    pub metadata: SnapshotMetadata,
    pub crdt: CRDT<K, C, V>,
}

/// Incremental snapshot structure for MessagePack serialization.
#[cfg(feature = "msgpack")]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(bound(serialize = "K: serde::Serialize, C: serde::Serialize, V: serde::Serialize")))]
#[cfg_attr(feature = "serde", serde(bound(deserialize = "K: serde::de::DeserializeOwned + Hash + Eq + Clone, C: serde::de::DeserializeOwned + Hash + Eq + Clone, V: serde::de::DeserializeOwned + Clone")))]
pub struct IncrementalSnapshot<K, C, V> {
    pub metadata: SnapshotMetadata,
    /// Only records that changed since base version
    pub changed_records: HashMap<K, Record<C, V>>,
    /// Only tombstones added since base version
    pub new_tombstones: HashMap<K, TombstoneInfo>,
    /// Updated logical clock
    pub clock_version: u64,
}

/// Configuration for the persistence layer.
#[derive(Debug, Clone)]
pub struct PersistConfig {
    /// Number of changes before automatic snapshot creation (default: 1000)
    pub snapshot_threshold: usize,
    /// Time interval in seconds before automatic snapshot creation (default: Some(300) = 5 minutes)
    /// Set to None to disable time-based snapshots
    /// Used to ensure snapshot hooks fire even during low activity
    pub snapshot_interval_secs: Option<u64>,
    /// Auto-cleanup old snapshots after rotation (None = manual cleanup only, Some(N) = keep N most recent)
    /// Default: Some(3) to prevent unbounded disk growth
    pub auto_cleanup_snapshots: Option<usize>,
    /// Maximum number of changes to accumulate in batch_collector before auto-flush
    /// Default: Some(10000) to prevent unbounded memory growth
    /// Set to None to disable auto-flush (you MUST call take_batch() periodically)
    pub max_batch_size: Option<usize>,
    /// Snapshot format (Bincode or MessagePack)
    /// Default: MessagePack (supports schema evolution)
    pub snapshot_format: SnapshotFormat,
    /// Enable incremental snapshots (only available with MessagePack)
    /// Default: true
    pub enable_incremental_snapshots: bool,
    /// Number of incremental snapshots before creating a full snapshot
    /// Default: 10 (balance between recovery time and I/O savings)
    pub full_snapshot_interval: usize,
    /// Enable compression (zstd) for snapshots
    /// Default: false (can be enabled for further size reduction)
    pub enable_compression: bool,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            snapshot_threshold: 1000,
            snapshot_interval_secs: Some(300), // 5 minutes
            auto_cleanup_snapshots: Some(3),
            max_batch_size: Some(10000),
            snapshot_format: SnapshotFormat::MessagePack,
            enable_incremental_snapshots: true,
            full_snapshot_interval: 10,
            enable_compression: false,
        }
    }
}

/// Error types for persistence operations.
#[derive(Debug)]
pub enum PersistError {
    /// I/O error during file operations
    Io(io::Error),
    /// Bincode serialization error
    BincodeEncode(bincode::error::EncodeError),
    /// Bincode deserialization error
    BincodeDecode(bincode::error::DecodeError),
    /// MessagePack encoding error
    #[cfg(feature = "msgpack")]
    MsgpackEncode(rmp_serde::encode::Error),
    /// MessagePack decoding error
    #[cfg(feature = "msgpack")]
    MsgpackDecode(rmp_serde::decode::Error),
    /// Compression error
    #[cfg(feature = "compression")]
    Compression(std::io::Error),
    /// Invalid persistence directory structure
    InvalidDirectory(String),
    /// Unsupported feature (e.g., incremental snapshots with bincode)
    UnsupportedFeature(String),
}

impl std::fmt::Display for PersistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PersistError::Io(e) => write!(f, "I/O error: {}", e),
            PersistError::BincodeEncode(e) => write!(f, "Bincode encoding error: {}", e),
            PersistError::BincodeDecode(e) => write!(f, "Bincode decoding error: {}", e),
            #[cfg(feature = "msgpack")]
            PersistError::MsgpackEncode(e) => write!(f, "MessagePack encoding error: {}", e),
            #[cfg(feature = "msgpack")]
            PersistError::MsgpackDecode(e) => write!(f, "MessagePack decoding error: {}", e),
            #[cfg(feature = "compression")]
            PersistError::Compression(e) => write!(f, "Compression error: {}", e),
            PersistError::InvalidDirectory(msg) => write!(f, "Invalid directory: {}", msg),
            PersistError::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),
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
        PersistError::BincodeEncode(e)
    }
}

impl From<bincode::error::DecodeError> for PersistError {
    fn from(e: bincode::error::DecodeError) -> Self {
        PersistError::BincodeDecode(e)
    }
}

#[cfg(feature = "msgpack")]
impl From<rmp_serde::encode::Error> for PersistError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        PersistError::MsgpackEncode(e)
    }
}

#[cfg(feature = "msgpack")]
impl From<rmp_serde::decode::Error> for PersistError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        PersistError::MsgpackDecode(e)
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
    /// Snapshots that have been successfully uploaded (for safe auto-cleanup)
    uploaded_snapshots: HashSet<PathBuf>,
    /// WAL segments that have been successfully uploaded (for safe auto-cleanup)
    uploaded_wal_segments: HashSet<PathBuf>,
    /// Time of last snapshot (for time-based snapshots)
    last_snapshot_time: Instant,
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
            post_hooks: Vec::new(),
            snapshot_hooks: Vec::new(),
            wal_segment_hooks: Vec::new(),
            changes_since_snapshot,
            snapshot_version,
            base_path,
            batch_collector: Vec::new(),
            uploaded_snapshots: HashSet::new(),
            uploaded_wal_segments: HashSet::new(),
            last_snapshot_time: Instant::now(),
        })
    }

    /// Returns a reference to the underlying CRDT for read-only operations.
    ///
    /// Use this for queries that don't modify the CRDT state.
    pub fn crdt(&self) -> &CRDT<K, C, V> {
        &self.crdt
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
    /// # Auto-Flush Protection
    ///
    /// By default (`max_batch_size: Some(10000)`), the batch is automatically cleared when it reaches
    /// 10,000 changes to prevent OOM. **Changes are discarded silently** when this happens!
    ///
    /// To prevent losing changes:
    /// - Call `take_batch()` regularly (every 100-1000 ops or 1-10 seconds)
    /// - Monitor batch size with `peek_batch().len()`
    /// - Set `max_batch_size: None` if you have a reliable polling loop
    ///
    /// # Memory Usage Without Auto-Flush
    ///
    /// At 1000 ops/sec with typical changes (~200 bytes each):
    /// - After 1 second: ~200 KB
    /// - After 1 minute: ~12 MB
    /// - After 1 hour: ~720 MB (exceeds default limit after 10 seconds)
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crdt_lite::persist::{PersistedCRDT, PersistConfig};
    /// # use std::path::PathBuf;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    /// #     PathBuf::from("./data"), 1, PersistConfig::default())?;
    /// // Perform operations...
    /// for i in 0..100 {
    ///     pcrdt.insert_or_update(&format!("rec{}", i),
    ///         [("field".to_string(), "value".to_string())].into_iter())?;
    /// }
    ///
    /// // Take batch and broadcast to network
    /// let batch = pcrdt.take_batch();
    /// // network.broadcast(batch);
    /// # Ok(())
    /// # }
    /// ```
    pub fn take_batch(&mut self) -> Vec<Change<K, C, V>> {
        std::mem::take(&mut self.batch_collector)
    }

    /// Returns a reference to the current batch without clearing it.
    ///
    /// **WARNING**: This does NOT clear the batch. Memory will continue to grow until
    /// `take_batch()` is called. See `take_batch()` documentation for memory usage warnings.
    pub fn peek_batch(&self) -> &[Change<K, C, V>] {
        &self.batch_collector
    }

    /// Marks a snapshot as successfully uploaded.
    ///
    /// Call this from a `SnapshotHook` after successfully uploading the snapshot to remote storage.
    /// Only snapshots marked as uploaded will be eligible for auto-cleanup (if configured).
    ///
    /// # Arguments
    ///
    /// * `snapshot_path` - Path to the snapshot file that was uploaded
    ///
    /// # Example
    ///
    /// ```ignore
    /// # use crdt_lite::persist::{PersistedCRDT, PersistConfig, SnapshotHook};
    /// # use std::path::PathBuf;
    /// # use std::sync::{Arc, Mutex};
    /// struct R2Uploader {
    ///     pcrdt: Arc<Mutex<PersistedCRDT<String, String, String>>>,
    /// }
    ///
    /// impl SnapshotHook for R2Uploader {
    ///     fn on_snapshot(&self, snapshot_path: &PathBuf) {
    ///         let path = snapshot_path.clone();
    ///         let pcrdt = self.pcrdt.clone();
    ///
    ///         // Spawn async upload task
    ///         tokio::spawn(async move {
    ///             // Upload to R2...
    ///             // r2.put(snapshot_path, data).await?;
    ///
    ///             // Mark as uploaded for safe cleanup
    ///             pcrdt.lock().unwrap().mark_snapshot_uploaded(path);
    ///         });
    ///     }
    /// }
    /// ```
    pub fn mark_snapshot_uploaded(&mut self, snapshot_path: PathBuf) {
        self.uploaded_snapshots.insert(snapshot_path);
    }

    /// Mark a WAL segment as successfully uploaded.
    ///
    /// Used in conjunction with `cleanup_old_wal_segments(keep_count, true)` to ensure
    /// only uploaded segments are deleted.
    ///
    /// # Example with R2 Upload
    ///
    /// ```ignore
    /// struct R2Uploader {
    ///     pcrdt: Arc<Mutex<PersistedCRDT<String, String, String>>>,
    /// }
    ///
    /// impl WalSegmentHook for R2Uploader {
    ///     fn on_wal_sealed(&self, segment_path: &PathBuf) {
    ///         let path = segment_path.clone();
    ///         let pcrdt = self.pcrdt.clone();
    ///
    ///         // Spawn async upload task
    ///         tokio::spawn(async move {
    ///             // Upload to R2...
    ///             // r2.put(segment_path, data).await?;
    ///
    ///             // Mark as uploaded for safe cleanup
    ///             pcrdt.lock().unwrap().mark_wal_segment_uploaded(path);
    ///         });
    ///     }
    /// }
    /// ```
    pub fn mark_wal_segment_uploaded(&mut self, segment_path: PathBuf) {
        self.uploaded_wal_segments.insert(segment_path);
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
    /// # Arguments
    ///
    /// * `keep_count` - Number of most recent snapshots to keep
    /// * `require_uploaded` - If `true`, only delete snapshots marked via `mark_snapshot_uploaded()`.
    ///                        If `false`, delete ALL old snapshots (used by auto-cleanup).
    ///
    /// # When to Use Each Mode
    ///
    /// **`require_uploaded: false`** (default for auto-cleanup):
    /// - Prevents unbounded disk growth automatically
    /// - Safe when snapshots are expendable (can recover from WAL)
    /// - Used by `auto_cleanup_snapshots` config option
    ///
    /// **`require_uploaded: true`** (for R2 backup workflows):
    /// - Only deletes snapshots that have been successfully uploaded
    /// - Prevents data loss if upload hooks fail
    /// - Call after verifying remote backup success
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crdt_lite::persist::{PersistedCRDT, PersistConfig};
    /// # use std::path::PathBuf;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    /// #     PathBuf::from("./data"), 1, PersistConfig::default())?;
    /// // Auto-cleanup: delete all old snapshots (default behavior)
    /// pcrdt.cleanup_old_snapshots(2, false)?;
    ///
    /// // R2 workflow: only delete uploaded snapshots
    /// // (after marking them with mark_snapshot_uploaded)
    /// pcrdt.cleanup_old_snapshots(2, true)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cleanup_old_snapshots(&mut self, keep_count: usize, require_uploaded: bool) -> Result<(), PersistError> {
        let mut snapshots = Vec::new();

        // Find all snapshot files
        if let Ok(entries) = std::fs::read_dir(&self.base_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                if filename_str.starts_with("snapshot_") && filename_str.ends_with(".bin") {
                    // Include based on require_uploaded setting
                    let should_include = if require_uploaded {
                        self.uploaded_snapshots.contains(&path)
                    } else {
                        true // Include all snapshots
                    };

                    if should_include {
                        if let Some(num_str) = filename_str
                            .strip_prefix("snapshot_")
                            .and_then(|s| s.strip_suffix(".bin"))
                        {
                            if let Ok(version) = num_str.parse::<u64>() {
                                snapshots.push((version, path));
                            }
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
            // Remove from tracking set if present
            self.uploaded_snapshots.remove(path);
        }

        Ok(())
    }

    /// Deletes old WAL segment files, keeping only the N most recent.
    ///
    /// # Arguments
    ///
    /// * `keep_count` - Number of most recent segments to keep
    /// * `require_uploaded` - If `true`, only delete segments marked via `mark_wal_segment_uploaded()`.
    ///                        If `false`, delete ALL old segments (used for cleanup after compaction).
    ///
    /// # When to Use Each Mode
    ///
    /// **`require_uploaded: false`** (for compaction cleanup):
    /// - Deletes all old WAL segments unconditionally
    /// - Safe after snapshot creation (snapshot contains all data)
    /// - Used by `compact_tombstones()` to clean up after compaction
    ///
    /// **`require_uploaded: true`** (for R2 backup workflows):
    /// - Only deletes segments that have been successfully uploaded
    /// - Prevents data loss if upload hooks fail
    /// - Call after verifying remote backup success
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crdt_lite::persist::{PersistedCRDT, PersistConfig};
    /// # use std::path::PathBuf;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    /// #     PathBuf::from("./data"), 1, PersistConfig::default())?;
    /// // Compaction cleanup: delete all old segments (after snapshot)
    /// pcrdt.cleanup_old_wal_segments(0, false)?;
    ///
    /// // R2 workflow: only delete uploaded segments
    /// // (after marking them with mark_wal_segment_uploaded)
    /// pcrdt.cleanup_old_wal_segments(3, true)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn cleanup_old_wal_segments(&mut self, keep_count: usize, require_uploaded: bool) -> Result<(), PersistError> {
        let mut segments = Vec::new();

        // Find all WAL segment files
        if let Ok(entries) = std::fs::read_dir(&self.base_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with("wal_") && filename_str.ends_with(".bin") {
                    // Include based on require_uploaded setting
                    let should_include = if require_uploaded {
                        self.uploaded_wal_segments.contains(&path)
                    } else {
                        true // Include all segments
                    };

                    if should_include {
                        if let Some(num_str) = filename_str
                            .strip_prefix("wal_")
                            .and_then(|s| s.strip_suffix(".bin"))
                        {
                            if let Ok(segment_num) = num_str.parse::<u64>() {
                                segments.push((segment_num, path));
                            }
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
            // Remove from tracking set if present
            self.uploaded_wal_segments.remove(path);
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

        // Sort by segment number (parse numerically for correctness)
        wal_files.sort_by_key(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .strip_prefix("wal_")
                .and_then(|s| s.strip_suffix(".bin"))
                .and_then(|num| num.parse::<u64>().ok())
                .unwrap_or(0)
        });

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
        use std::io::Write;

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

        // Explicitly manage file handle for Windows compatibility
        {
            let mut file = std::fs::File::create(&snapshot_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            // Explicitly drop before any other operations
            drop(file);
        }

        // On Windows, give OS time to release file locks after explicit drop()
        // Windows file system operations are asynchronous at kernel level - even after drop(),
        // the handle may not be fully released immediately, causing subsequent directory ops to fail.
        // 10ms is typically sufficient, but under heavy load this could still fail.
        #[cfg(target_os = "windows")]
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Call snapshot hooks (file is now sealed and immutable)
        for hook in &self.snapshot_hooks {
            hook.on_snapshot(&snapshot_path);
        }

        // Rotate WAL (seals old segments, calls hooks, starts new segment)
        // Note: Old segments are NOT auto-deleted - call cleanup_old_wal_segments() after upload
        let _ = self.wal.rotate(&self.base_path, &self.wal_segment_hooks)?;

        // Auto-cleanup old snapshots if configured
        // Use require_uploaded=false to actually delete old snapshots (prevents disk bloat)
        if let Some(keep_count) = self.config.auto_cleanup_snapshots {
            let _ = self.cleanup_old_snapshots(keep_count, false);
        }

        // Reset counter and timer
        self.changes_since_snapshot = 0;
        self.last_snapshot_time = Instant::now();

        Ok(())
    }

    /// Checks if automatic snapshot threshold is reached and creates snapshot if needed.
    /// Snapshots are triggered by either:
    /// - Change count reaching snapshot_threshold (default: 1000)
    /// - Time elapsed reaching snapshot_interval_secs (default: 300 = 5 minutes)
    fn check_auto_snapshot(&mut self) -> Result<(), PersistError> {
        let should_snapshot =
            // Change count threshold
            self.changes_since_snapshot >= self.config.snapshot_threshold ||
            // Time-based threshold
            self.config.snapshot_interval_secs
                .map(|interval| self.last_snapshot_time.elapsed().as_secs() >= interval)
                .unwrap_or(false);

        if should_snapshot {
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

        // Check if batch size limit exceeded and auto-flush if needed
        if let Some(max_size) = self.config.max_batch_size {
            if self.batch_collector.len() >= max_size {
                // Auto-flush: clear the batch to prevent OOM
                // Applications should call take_batch() before this happens
                self.batch_collector.clear();
            }
        }

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
    /// This method performs atomic tombstone compaction with automatic WAL cleanup
    /// to prevent zombie records. It performs:
    /// 1. Tombstone compaction in memory
    /// 2. Snapshot creation with compacted state
    /// 3. Deletion of old WAL segments (which contain tombstone records)
    ///
    /// **IMPORTANT**: Only call this after verifying all nodes have acknowledged
    /// the `min_acknowledged_version`. Otherwise, deleted records may reappear.
    ///
    /// # Arguments
    ///
    /// * `min_acknowledged_version` - The minimum db_version acknowledged by all nodes
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or `PersistError` if snapshot creation or cleanup fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crdt_lite::persist::{PersistedCRDT, PersistConfig};
    /// # use std::path::PathBuf;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut pcrdt = PersistedCRDT::<String, String, String>::open(
    /// #     PathBuf::from("./data"), 1, PersistConfig::default())?;
    /// // After confirming all nodes have acknowledged version 1000
    /// pcrdt.compact_tombstones(1000)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn compact_tombstones(&mut self, min_acknowledged_version: u64) -> Result<(), PersistError> {
        // Compact tombstones in memory
        self.crdt.compact_tombstones(min_acknowledged_version);

        // Force snapshot after compaction to persist the cleaned state
        self.create_snapshot()?;

        // CRITICAL: Delete old WAL segments that contain tombstone records
        // Failure to do this causes zombie records to reappear on recovery
        // Use require_uploaded=false to unconditionally delete all old segments
        self.cleanup_old_wal_segments(0, false)?;

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
