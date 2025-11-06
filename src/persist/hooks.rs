//! Hook system for pre-operation and post-operation callbacks.
//!
//! Hooks enable:
//! - Pre-operation validation (can reject operations)
//! - Post-operation broadcasting (notify after successful operations)
//! - Snapshot notifications (for backup/replication)
//! - WAL segment sealing (for archival)
//! - Integration with network layers
//! - Custom business logic enforcement

use crate::Change;
use std::hash::Hash;
use std::path::PathBuf;

/// Error type for hook rejections.
#[derive(Debug, Clone)]
pub struct HookError {
    pub message: String,
}

impl HookError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for HookError {}

/// Pre-operation hook for validation.
///
/// Called before an operation is applied to the CRDT. If any pre-hook
/// returns an error, the operation is rejected and not applied.
///
/// # Example
///
/// ```
/// use crdt_lite::persist::{PreOpHook, HookError};
/// use crdt_lite::Change;
///
/// struct ValidateNodeId {
///     allowed_nodes: Vec<u64>,
/// }
///
/// impl PreOpHook<String, String, String> for ValidateNodeId {
///     fn before_op(&self, changes: &[Change<String, String, String>]) -> Result<(), HookError> {
///         for change in changes {
///             if !self.allowed_nodes.contains(&change.node_id) {
///                 return Err(HookError::new(format!("Node {} not allowed", change.node_id)));
///             }
///         }
///         Ok(())
///     }
/// }
/// ```
pub trait PreOpHook<K, C, V>
where
    K: Hash + Eq + Clone,
    C: Hash + Eq + Clone,
    V: Clone,
{
    /// Called before changes are applied.
    ///
    /// Return `Ok(())` to allow the operation, or `Err(HookError)` to reject it.
    fn before_op(&self, changes: &[Change<K, C, V>]) -> Result<(), HookError>;
}

/// Post-operation hook for broadcasting.
///
/// Called after an operation has been successfully applied and persisted.
/// Post-hooks cannot reject operations and should not block for long periods.
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
pub trait PostOpHook<K, C, V>
where
    K: Hash + Eq + Clone,
    C: Hash + Eq + Clone,
    V: Clone,
{
    /// Called after changes have been applied and persisted.
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
///     fn on_snapshot(&self, snapshot_path: &PathBuf) {
///         // Async upload to R2 (spawn task)
///         let path = snapshot_path.clone();
///         let project = self.project_id.clone();
///         tokio::spawn(async move {
///             let data = tokio::fs::read(&path).await.unwrap();
///             // r2.put(format!("projects/{}/snapshots/{}", project, path.file_name()?), data).await
///         });
///     }
/// }
/// ```
pub trait SnapshotHook {
    /// Called after a snapshot has been created and sealed.
    ///
    /// The snapshot file is immutable and safe for async upload.
    /// This is called synchronously but should spawn async tasks for I/O.
    fn on_snapshot(&self, snapshot_path: &PathBuf);
}

/// WAL segment hook for archival.
///
/// Called after a WAL segment has been sealed (rotated out).
/// The segment file is immutable and safe to copy/upload.
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
pub trait WalSegmentHook {
    /// Called after a WAL segment has been sealed (no longer active).
    ///
    /// The segment file is immutable and safe for async upload.
    fn on_wal_sealed(&self, segment_path: &PathBuf);
}

// Convenience implementations for closures

impl<K, C, V, F> PreOpHook<K, C, V> for F
where
    K: Hash + Eq + Clone,
    C: Hash + Eq + Clone,
    V: Clone,
    F: Fn(&[Change<K, C, V>]) -> Result<(), HookError>,
{
    fn before_op(&self, changes: &[Change<K, C, V>]) -> Result<(), HookError> {
        self(changes)
    }
}

impl<K, C, V, F> PostOpHook<K, C, V> for F
where
    K: Hash + Eq + Clone,
    C: Hash + Eq + Clone,
    V: Clone,
    F: Fn(&[Change<K, C, V>]),
{
    fn after_op(&self, changes: &[Change<K, C, V>]) {
        self(changes)
    }
}

impl<F> SnapshotHook for F
where
    F: Fn(&PathBuf),
{
    fn on_snapshot(&self, snapshot_path: &PathBuf) {
        self(snapshot_path)
    }
}

impl<F> WalSegmentHook for F
where
    F: Fn(&PathBuf),
{
    fn on_wal_sealed(&self, segment_path: &PathBuf) {
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

    #[test]
    fn test_pre_hook_rejection() {
        let hook = |changes: &[Change<String, String, String>]| {
            for change in changes {
                if change.node_id == 999 {
                    return Err(HookError::new("Invalid node"));
                }
            }
            Ok(())
        };

        let good_change = vec![Change {
            record_id: "rec1".to_string(),
            col_name: Some("field1".to_string()),
            value: Some("value1".to_string()),
            col_version: 1,
            db_version: 1,
            node_id: 1,
            local_db_version: 1,
            flags: 0,
        }];

        let bad_change = vec![Change {
            record_id: "rec1".to_string(),
            col_name: Some("field1".to_string()),
            value: Some("value1".to_string()),
            col_version: 1,
            db_version: 1,
            node_id: 999,
            local_db_version: 1,
            flags: 0,
        }];

        assert!(hook.before_op(&good_change).is_ok());
        assert!(hook.before_op(&bad_change).is_err());
    }
}
