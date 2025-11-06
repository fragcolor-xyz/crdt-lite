//! Write-Ahead Log (WAL) implementation for CRDT persistence.
//!
//! The WAL provides append-only storage for CRDT changes with:
//! - Fast writes without fsync (OS buffering)
//! - Segmented files for efficient rotation
//! - Bincode serialization for compact binary format

use crate::Change;
use bincode::config;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Writer for append-only WAL segments.
pub struct WalWriter<K, C, V>
where
    K: Hash + Eq + Clone,
    C: Hash + Eq + Clone,
    V: Clone,
{
    /// Current WAL segment file
    current_file: BufWriter<File>,
    /// Current segment number
    segment_number: u64,
    /// Base directory path
    _phantom: std::marker::PhantomData<(K, C, V)>,
}

impl<K, C, V> WalWriter<K, C, V>
where
    K: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    C: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Creates a new WAL writer.
    ///
    /// Finds the highest existing segment number and continues from there.
    pub fn new(base_path: PathBuf) -> io::Result<Self> {
        // Find the highest existing segment number
        let mut max_segment = 0;
        if let Ok(entries) = std::fs::read_dir(&base_path) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with("wal_") && filename_str.ends_with(".bin") {
                    // Parse segment number from filename (wal_000001.bin)
                    if let Some(num_str) = filename_str
                        .strip_prefix("wal_")
                        .and_then(|s| s.strip_suffix(".bin"))
                    {
                        if let Ok(num) = num_str.parse::<u64>() {
                            max_segment = max_segment.max(num);
                        }
                    }
                }
            }
        }

        // Start new segment after the highest found
        let segment_number = max_segment + 1;
        let wal_path = Self::segment_path(&base_path, segment_number);

        // Open file in append mode
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path)?;

        Ok(Self {
            current_file: BufWriter::new(file),
            segment_number,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Appends changes to the current WAL segment.
    ///
    /// Changes are written immediately but not fsynced (relies on OS buffering).
    pub fn append(&mut self, changes: &[Change<K, C, V>]) -> io::Result<()> {
        let config = config::standard();

        for change in changes {
            // Serialize and write each change using bincode::serde
            let bytes = bincode::serde::encode_to_vec(change, config)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            // Write length prefix (for reading back)
            let len = bytes.len() as u64;
            self.current_file.write_all(&len.to_le_bytes())?;

            // Write the serialized change
            self.current_file.write_all(&bytes)?;
        }

        // Flush buffer (but don't fsync)
        self.current_file.flush()?;

        Ok(())
    }

    /// Rotates to a new WAL segment, sealing old ones.
    ///
    /// Called after snapshot creation. Fsyncs current segment, calls hooks on sealed segments.
    /// Returns paths of sealed segments. Does NOT delete them - use cleanup_old_wal_segments() after successful upload.
    pub fn rotate(&mut self, base_path: &Path, hooks: &[Box<dyn super::hooks::WalSegmentHook>]) -> io::Result<Vec<PathBuf>> {
        // Flush and fsync current file to ensure data is on disk before hooks fire
        self.current_file.flush()?;
        self.current_file.get_ref().sync_all()?;

        // Collect all existing WAL segment paths (these are now sealed)
        let mut sealed_segments = Vec::new();
        if let Ok(entries) = std::fs::read_dir(base_path) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();
                if filename_str.starts_with("wal_") && filename_str.ends_with(".bin") {
                    sealed_segments.push(entry.path());
                }
            }
        }

        // Create new segment BEFORE deleting old ones (to release the old file handle)
        self.segment_number += 1;
        let new_wal_path = Self::segment_path(base_path, self.segment_number);

        let new_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(new_wal_path)?;

        // Replace the current BufWriter, dropping the old one and closing the old file handle
        self.current_file = BufWriter::new(new_file);

        // Now call hooks on sealed segments (old file is closed, data is fsynced)
        for segment_path in &sealed_segments {
            for hook in hooks {
                hook.on_wal_sealed(segment_path);
            }
        }

        // Return sealed segment paths for tracking
        // NOTE: Segments are NOT deleted here - caller should use cleanup_old_wal_segments()
        // after confirming successful upload to avoid data loss if hooks fail
        Ok(sealed_segments)
    }

    /// Constructs the path for a WAL segment file.
    fn segment_path(base_path: &Path, segment: u64) -> PathBuf {
        base_path.join(format!("wal_{:06}.bin", segment))
    }
}

/// Reads all changes from a WAL file.
pub fn read_wal_file<K, C, V>(path: &Path) -> io::Result<Vec<Change<K, C, V>>>
where
    K: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    C: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let config = config::standard();

    let mut changes = Vec::new();

    // Read changes until EOF
    loop {
        // Read length prefix
        let mut len_bytes = [0u8; 8];
        match reader.read_exact(&mut len_bytes) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let len = u64::from_le_bytes(len_bytes) as usize;

        // Read the serialized change
        let mut buffer = vec![0u8; len];
        reader.read_exact(&mut buffer)?;

        // Deserialize using bincode::serde
        let change: Change<K, C, V> = bincode::serde::decode_from_slice(&buffer, config)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .0;

        changes.push(change);
    }

    Ok(changes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::hooks::WalSegmentHook;
    use crate::Change;

    #[test]
    fn test_wal_write_read() {
        let temp_dir = std::env::temp_dir().join("crdt_wal_test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

        // Create some test changes
        let changes = vec![
            Change {
                record_id: "rec1".to_string(),
                col_name: Some("field1".to_string()),
                value: Some("value1".to_string()),
                col_version: 1,
                db_version: 1,
                node_id: 1,
                local_db_version: 1,
                flags: 0,
            },
            Change {
                record_id: "rec2".to_string(),
                col_name: Some("field2".to_string()),
                value: Some("value2".to_string()),
                col_version: 1,
                db_version: 2,
                node_id: 1,
                local_db_version: 2,
                flags: 0,
            },
        ];

        // Write changes
        let mut writer: WalWriter<String, String, String> = WalWriter::new(temp_dir.clone()).unwrap();
        writer.append(&changes).unwrap();

        // Read them back
        let wal_path = temp_dir.join("wal_000001.bin");
        let read_changes: Vec<Change<String, String, String>> = read_wal_file(&wal_path).unwrap();

        assert_eq!(read_changes.len(), 2);
        assert_eq!(read_changes[0].record_id, "rec1");
        assert_eq!(read_changes[1].record_id, "rec2");

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_wal_rotation() {
        let temp_dir = std::env::temp_dir().join("crdt_wal_rotation_test");
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();

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

        // Write to first segment
        let mut writer: WalWriter<String, String, String> = WalWriter::new(temp_dir.clone()).unwrap();
        writer.append(&changes).unwrap();

        // Verify segment 1 exists
        assert!(temp_dir.join("wal_000001.bin").exists());

        // Rotate (with empty hooks)
        let hooks: Vec<Box<dyn WalSegmentHook>> = Vec::new();
        writer.rotate(&temp_dir, &hooks).unwrap();

        // Old segment should still exist (not auto-deleted), new segment 2 should exist
        assert!(temp_dir.join("wal_000001.bin").exists(), "Old segment should still exist");
        assert!(temp_dir.join("wal_000002.bin").exists(), "New segment should exist");

        // Write to new segment
        writer.append(&changes).unwrap();

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
