//! Hook system for post-operation callbacks and file sealing notifications.
//!
//! Hooks enable:
//! - Post-operation broadcasting (notify after successful operations)
//! - Snapshot notifications (for backup/replication)
//! - WAL segment sealing (for archival)
//! - Integration with network layers

use crate::Change;
use std::hash::Hash;
use std::path::Path;

/// Post-operation hook for broadcasting.
///
/// Called after an operation has been successfully applied and **written to WAL**
/// (but before fsync). This timing is intentional for CRDT broadcast semantics:
/// minimizing network propagation delay is more important than local durability.
///
/// ## Timing Guarantees
///
/// When this hook fires:
/// - ✅ Changes applied to in-memory CRDT
/// - ✅ Changes written to WAL file
/// - ✅ WAL buffer flushed to OS page cache
/// - ❌ NOT yet fsynced to disk (happens during snapshot creation)
///
/// **Why before fsync?** For distributed CRDTs:
/// - Faster broadcast = smaller conflict windows
/// - If local node crashes before fsync, peers have the data
/// - On recovery, local node syncs from peers and gets changes back
/// - System-wide convergence maintained despite local data loss
///
/// See module-level docs for full durability discussion.
///
/// # Safety Contract
///
/// Hooks MUST NOT:
/// - Panic (will crash the application)
/// - Block for extended periods (will delay all CRDT operations)
/// - Perform synchronous I/O (use async channels instead)
///
/// # Example
///
/// ```no_run
/// use crdt_lite::persist::PostOpHook;
/// use crdt_lite::Change;
/// use std::sync::mpsc::Sender;
///
/// struct NetworkBroadcast {
///     tx: Sender<Vec<Change<String, String, String>>>,
/// }
///
/// impl PostOpHook<String, String, String> for NetworkBroadcast {
///     fn after_op(&self, changes: &[Change<String, String, String>]) {
///         // Send to async network layer
///         let _ = self.tx.send(changes.to_vec());
///     }
/// }
/// ```
pub trait PostOpHook<K, C, V>: Send + Sync
where
  K: Hash + Eq + Clone,
  C: Hash + Eq + Clone,
  V: Clone,
{
  /// Called after changes have been applied and written (but not fsynced).
  ///
  /// This fires immediately after WAL write to minimize broadcast latency.
  /// Local fsync is deferred to snapshot time for CRDT performance.
  ///
  /// This should be fast and non-blocking. For network I/O, send changes
  /// to an async task rather than blocking here.
  fn after_op(&self, changes: &[Change<K, C, V>]);
}

/// Snapshot hook for backup and replication.
///
/// Called after a snapshot has been created and fsynced to disk.
/// The snapshot file is immutable (sealed) and safe to copy/upload.
///
/// # Safety Contract
///
/// Hooks MUST NOT:
/// - Panic (will crash the application)
/// - Block for extended periods (will delay snapshot creation)
/// - Perform synchronous I/O (use async channels/tasks instead)
///
/// # Example
///
/// ```ignore
/// use crdt_lite::persist::SnapshotHook;
/// use std::path::PathBuf;
///
/// struct R2Uploader {
///     project_id: String,
/// }
///
/// impl SnapshotHook for R2Uploader {
///     fn on_snapshot(&self, snapshot_path: &PathBuf, version: u64) {
///         // Async upload to R2 (spawn task)
///         let path = snapshot_path.clone();
///         let project = self.project_id.clone();
///         tokio::spawn(async move {
///             let data = tokio::fs::read(&path).await.unwrap();
///             // r2.put(format!("projects/{}/snapshots/{}/v{}", project, path.file_name()?, version), data).await
///         });
///     }
/// }
/// ```
pub trait SnapshotHook: Send + Sync {
  /// Called after a snapshot has been created and sealed.
  ///
  /// The snapshot file is immutable and safe for async upload.
  /// This is called synchronously but should spawn async tasks for I/O.
  ///
  /// # Arguments
  ///
  /// * `snapshot_path` - Path to the sealed snapshot file
  /// * `db_version` - The CRDT version (logical clock) at time of snapshot
  fn on_snapshot(&self, snapshot_path: &Path, db_version: u64);
}

/// WAL segment hook for archival.
///
/// Called after a WAL segment has been sealed (rotated out).
/// The segment file is immutable and safe to copy/upload.
///
/// # Safety Contract
///
/// Hooks MUST NOT:
/// - Panic (will crash the application)
/// - Block for extended periods (will delay WAL rotation)
/// - Perform synchronous I/O (use async channels/tasks instead)
///
/// # Example
///
/// ```ignore
/// use crdt_lite::persist::WalSegmentHook;
/// use std::path::PathBuf;
///
/// struct WalArchiver;
///
/// impl WalSegmentHook for WalArchiver {
///     fn on_wal_sealed(&self, segment_path: &PathBuf) {
///         // Async upload sealed segment
///         let path = segment_path.clone();
///         tokio::spawn(async move {
///             let data = tokio::fs::read(&path).await.unwrap();
///             // r2.put(format!("projects/{}/wal/{}", ...), data).await
///         });
///     }
/// }
/// ```
pub trait WalSegmentHook: Send + Sync {
  /// Called after a WAL segment has been sealed (no longer active).
  ///
  /// The segment file is immutable and safe for async upload.
  fn on_wal_sealed(&self, segment_path: &Path);
}

// Convenience implementations for closures

impl<K, C, V, F> PostOpHook<K, C, V> for F
where
  K: Hash + Eq + Clone,
  C: Hash + Eq + Clone,
  V: Clone,
  F: Fn(&[Change<K, C, V>]) + Send + Sync,
{
  fn after_op(&self, changes: &[Change<K, C, V>]) {
    self(changes)
  }
}

impl<F> SnapshotHook for F
where
  F: Fn(&Path, u64) + Send + Sync,
{
  fn on_snapshot(&self, snapshot_path: &Path, db_version: u64) {
    self(snapshot_path, db_version)
  }
}

impl<F> WalSegmentHook for F
where
  F: Fn(&Path) + Send + Sync,
{
  fn on_wal_sealed(&self, segment_path: &Path) {
    self(segment_path)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_hook_closure() {
    let counter = std::sync::Arc::new(std::sync::Mutex::new(0));
    let counter_clone = counter.clone();

    let hook = move |changes: &[Change<String, String, String>]| {
      *counter_clone.lock().unwrap() += changes.len();
    };

    let changes = vec![Change {
      record_id: "rec1".to_string(),
      col_name: Some("field1".to_string()),
      value: Some("value1".to_string()),
      col_version: 1,
      db_version: 1,
      node_id: 1,
      local_db_version: 1,
      flags: 0,
    }];

    hook.after_op(&changes);

    assert_eq!(*counter.lock().unwrap(), 1);
  }
}
