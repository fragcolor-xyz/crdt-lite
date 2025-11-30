//! Disk-streaming CRDT with lazy loading for massive datasets.
//!
//! This module provides `StreamingCRDT`, a disk-backed CRDT that:
//! - Loads records on-demand from indexed snapshots
//! - Keeps recent changes in a hot in-memory tier
//! - Uses an LRU cache for frequently accessed cold records
//! - Supports datasets larger than available memory
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────┐
//! │         Hot Tier (Memory)           │  ← Recent writes, always in memory
//! ├─────────────────────────────────────┤
//! │         LRU Cache (Memory)          │  ← Frequently accessed cold records
//! ├─────────────────────────────────────┤
//! │     Indexed Snapshot (Disk)         │  ← All records, loaded on-demand
//! └─────────────────────────────────────┘
//! ```
//!
//! # Indexed Snapshot Format
//!
//! ```text
//! [Header]
//! ├── magic: "CRDS" (4 bytes)
//! ├── version: u8
//! ├── flags: u8
//! ├── node_id: u64
//! ├── clock_value: u64
//! ├── record_count: u64
//! ├── tombstone_count: u64
//! ├── index_offset: u64
//! ├── tombstones_offset: u64
//!
//! [Records Section]
//! ├── record_1 (MessagePack serialized)
//! ├── record_2 (MessagePack serialized)
//! └── ...
//!
//! [Tombstones Section] (loaded at startup - typically small)
//! └── HashMap<K, TombstoneInfo> (MessagePack serialized)
//!
//! [Index Section] (loaded at startup)
//! └── Vec<(K, offset, length)> (MessagePack serialized)
//! ```
//!
//! # Example
//!
//! ```no_run
//! use crdt_lite::persist::streaming::{StreamingCRDT, StreamingConfig};
//! use std::path::PathBuf;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Open with 1000-record LRU cache
//! let mut crdt = StreamingCRDT::<String, String, String>::open(
//!     PathBuf::from("./data"),
//!     1,
//!     StreamingConfig::default(),
//! )?;
//!
//! // Operations work the same as regular CRDT
//! crdt.insert_or_update(&"key1".to_string(), vec![
//!     ("field".to_string(), "value".to_string()),
//! ])?;
//!
//! // Records are loaded on-demand
//! if let Some(record) = crdt.get_record(&"key1".to_string())? {
//!     println!("Found: {:?}", record);
//! }
//! # Ok(())
//! # }
//! ```

use crate::{
    Change, DefaultMergeRule, LogicalClock, NodeId, Record,
    TombstoneInfo, TombstoneStorage, CRDT,
};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hash::Hash;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use super::wal::WalWriter;
use super::{PersistConfig, PersistError, PostOpHook, SnapshotHook, WalSegmentHook};

/// Magic bytes for indexed snapshot format
const SNAPSHOT_MAGIC: &[u8; 4] = b"CRDS";
/// Current snapshot format version
const SNAPSHOT_VERSION: u8 = 1;

/// Default LRU cache capacity (number of records)
pub const DEFAULT_CACHE_CAPACITY: usize = 1000;

/// Configuration for streaming CRDT
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Maximum number of cold records to keep in LRU cache
    pub cache_capacity: usize,
    /// Underlying persistence config (for WAL, snapshots, etc.)
    pub persist_config: PersistConfig,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            persist_config: PersistConfig::default(),
        }
    }
}

/// Entry in the snapshot index
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct IndexEntry<K> {
    key: K,
    offset: u64,
    length: u32,
}


/// Simple LRU cache implementation
struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<K, V>,
    order: Vec<K>, // Most recently used at the end
}

impl<K: Hash + Eq + Clone, V> LruCache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            order: Vec::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if self.map.contains_key(key) {
            // Move to end (most recently used)
            self.order.retain(|k| k != key);
            self.order.push(key.clone());
            self.map.get(key)
        } else {
            None
        }
    }

    fn insert(&mut self, key: K, value: V) {
        if self.map.contains_key(&key) {
            // Update existing
            self.order.retain(|k| k != &key);
        } else if self.map.len() >= self.capacity {
            // Evict oldest
            if let Some(oldest) = self.order.first().cloned() {
                self.order.remove(0);
                self.map.remove(&oldest);
            }
        }
        self.order.push(key.clone());
        self.map.insert(key, value);
    }

    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }
}

/// Cold storage backed by an indexed snapshot file
struct ColdStorage<K, C, V>
where
    K: Ord + Hash + Eq + Clone,
    C: Hash + Eq + Clone,
    V: Clone,
{
    /// The snapshot file (kept open for seeking)
    file: Option<BufReader<File>>,
    /// Key -> (offset, length) index
    index: HashMap<K, (u64, u32)>,
    /// Tombstones (loaded entirely at startup)
    tombstones: TombstoneStorage<K>,
    /// Clock value from snapshot
    clock_value: u64,
    /// Phantom data for type parameters
    _phantom: std::marker::PhantomData<(C, V)>,
}

impl<K, C, V> ColdStorage<K, C, V>
where
    K: Ord + Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    C: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Create empty cold storage (no snapshot file)
    fn empty() -> Self {
        Self {
            file: None,
            index: HashMap::new(),
            tombstones: TombstoneStorage::new(),
            clock_value: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Load cold storage from an indexed snapshot file
    fn from_file(path: &PathBuf) -> Result<Self, PersistError> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and verify magic
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != SNAPSHOT_MAGIC {
            return Err(PersistError::InvalidDirectory(
                "Invalid snapshot magic bytes".to_string(),
            ));
        }

        // Read version and flags
        let mut version_flags = [0u8; 2];
        reader.read_exact(&mut version_flags)?;
        let version = version_flags[0];
        if version != SNAPSHOT_VERSION {
            return Err(PersistError::InvalidDirectory(format!(
                "Unsupported snapshot version: {}",
                version
            )));
        }
        // flags[1] reserved for future use

        // Read header fields
        let mut header_buf = [0u8; 48]; // 6 * u64
        reader.read_exact(&mut header_buf)?;

        let node_id = u64::from_le_bytes(header_buf[0..8].try_into().unwrap());
        let clock_value = u64::from_le_bytes(header_buf[8..16].try_into().unwrap());
        let record_count = u64::from_le_bytes(header_buf[16..24].try_into().unwrap());
        let tombstone_count = u64::from_le_bytes(header_buf[24..32].try_into().unwrap());
        let index_offset = u64::from_le_bytes(header_buf[32..40].try_into().unwrap());
        let tombstones_offset = u64::from_le_bytes(header_buf[40..48].try_into().unwrap());

        let _ = (node_id, record_count, tombstone_count); // Silence unused warnings

        // Load index from end of file
        reader.seek(SeekFrom::Start(index_offset))?;
        let index_entries: Vec<IndexEntry<K>> = rmp_serde::from_read(&mut reader)
            .map_err(PersistError::MsgpackDecode)?;

        let index: HashMap<K, (u64, u32)> = index_entries
            .into_iter()
            .map(|e| (e.key, (e.offset, e.length)))
            .collect();

        // Load tombstones
        reader.seek(SeekFrom::Start(tombstones_offset))?;
        let tombstone_map: HashMap<K, TombstoneInfo> = rmp_serde::from_read(&mut reader)
            .map_err(PersistError::MsgpackDecode)?;

        let mut tombstones = TombstoneStorage::new();
        for (key, info) in tombstone_map {
            tombstones.insert_or_assign(key, info);
        }

        // Reopen file for record access
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        Ok(Self {
            file: Some(reader),
            index,
            tombstones,
            clock_value,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Load a single record from disk
    fn load_record(&mut self, key: &K) -> Result<Option<Record<C, V>>, PersistError> {
        let (offset, length) = match self.index.get(key) {
            Some(&(o, l)) => (o, l),
            None => return Ok(None),
        };

        let reader = match &mut self.file {
            Some(r) => r,
            None => return Ok(None),
        };

        reader.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; length as usize];
        reader.read_exact(&mut buffer)?;

        let record: Record<C, V> =
            rmp_serde::from_slice(&buffer).map_err(PersistError::MsgpackDecode)?;

        Ok(Some(record))
    }

    /// Check if a key exists in cold storage
    fn contains_key(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    /// Check if a record is tombstoned
    fn is_tombstoned(&self, key: &K) -> bool {
        self.tombstones.find(key).is_some()
    }

    /// Get tombstone info
    #[allow(dead_code)]
    fn get_tombstone(&self, key: &K) -> Option<TombstoneInfo> {
        self.tombstones.find(key)
    }

    /// Get all keys in cold storage
    fn keys(&self) -> impl Iterator<Item = &K> {
        self.index.keys()
    }

    /// Get number of records in cold storage
    fn len(&self) -> usize {
        self.index.len()
    }
}

/// A streaming CRDT that loads records on-demand from disk.
///
/// This is designed for datasets too large to fit in memory. Recent changes
/// are kept in a hot in-memory CRDT, while older data is loaded from an
/// indexed snapshot file on-demand.
pub struct StreamingCRDT<K, C, V>
where
    K: Ord + Hash + Eq + Clone,
    C: Hash + Eq + Clone,
    V: Clone,
{
    /// Hot tier: recent changes (always in memory)
    hot: CRDT<K, C, V>,
    /// Keys that exist in hot tier (for quick membership check)
    hot_keys: HashSet<K>,
    /// Keys that have been tombstoned in hot tier
    hot_tombstones: HashSet<K>,

    /// Cold tier: on-disk storage with index
    cold: ColdStorage<K, C, V>,
    /// LRU cache for cold records
    cache: LruCache<K, Record<C, V>>,

    /// WAL writer for persistence
    wal: WalWriter<K, C, V>,
    /// Configuration
    config: StreamingConfig,
    /// Base directory for files
    base_dir: PathBuf,

    /// Changes since last snapshot
    changes_since_snapshot: usize,
    /// Current snapshot version
    snapshot_version: u64,

    /// Hooks
    post_hooks: Vec<Box<dyn PostOpHook<K, C, V>>>,
    snapshot_hooks: Vec<Box<dyn SnapshotHook>>,
    wal_segment_hooks: Vec<Box<dyn WalSegmentHook>>,

    /// Batch collector for network broadcasting
    batch: Vec<Change<K, C, V>>,
    /// Last snapshot time
    last_snapshot_time: std::time::Instant,
}

impl<K, C, V> StreamingCRDT<K, C, V>
where
    K: Ord + Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    C: Ord + Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Opens or creates a streaming CRDT at the specified path.
    ///
    /// If an indexed snapshot exists, it's opened for lazy loading.
    /// Otherwise, attempts to migrate from a regular snapshot.
    pub fn open(
        base_dir: PathBuf,
        node_id: NodeId,
        config: StreamingConfig,
    ) -> Result<Self, PersistError> {
        std::fs::create_dir_all(&base_dir)?;

        // Look for indexed snapshot
        let indexed_snapshot = Self::find_indexed_snapshot(&base_dir)?;

        let (cold, clock_value) = if let Some(path) = indexed_snapshot {
            let cold = ColdStorage::from_file(&path)?;
            let clock = cold.clock_value;
            (cold, clock)
        } else {
            // Try to migrate from regular snapshot
            let migrated = Self::migrate_from_regular_snapshot(&base_dir, node_id)?;
            if let Some((cold, clock)) = migrated {
                (cold, clock)
            } else {
                (ColdStorage::empty(), 0)
            }
        };

        // Create hot CRDT with clock synced to cold
        let mut hot = CRDT::new(node_id, None);
        if clock_value > 0 {
            // Sync hot clock to cold
            hot.get_clock_mut().set_time(clock_value);
        }

        // Create WAL writer
        let wal = WalWriter::new(base_dir.clone())?;

        // Replay WAL on top of cold storage
        let mut streaming = Self {
            hot,
            hot_keys: HashSet::new(),
            hot_tombstones: HashSet::new(),
            cold,
            cache: LruCache::new(config.cache_capacity),
            wal,
            config,
            base_dir,
            changes_since_snapshot: 0,
            snapshot_version: 0,
            post_hooks: Vec::new(),
            snapshot_hooks: Vec::new(),
            wal_segment_hooks: Vec::new(),
            batch: Vec::new(),
            last_snapshot_time: std::time::Instant::now(),
        };

        streaming.replay_wal()?;

        Ok(streaming)
    }

    /// Find the most recent indexed snapshot
    fn find_indexed_snapshot(base_dir: &PathBuf) -> Result<Option<PathBuf>, PersistError> {
        let mut snapshots: Vec<(u64, PathBuf)> = Vec::new();

        if let Ok(entries) = std::fs::read_dir(base_dir) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                // Look for indexed snapshots: snapshot_indexed_NNNNNN.bin
                if filename_str.starts_with("snapshot_indexed_")
                    && filename_str.ends_with(".bin")
                {
                    if let Some(version_str) = filename_str
                        .strip_prefix("snapshot_indexed_")
                        .and_then(|s| s.strip_suffix(".bin"))
                    {
                        if let Ok(version) = version_str.parse::<u64>() {
                            snapshots.push((version, entry.path()));
                        }
                    }
                }
            }
        }

        snapshots.sort_by_key(|(v, _)| *v);
        Ok(snapshots.last().map(|(_, p)| p.clone()))
    }

    /// Migrate from a regular PersistedCRDT snapshot to indexed format
    fn migrate_from_regular_snapshot(
        base_dir: &PathBuf,
        node_id: NodeId,
    ) -> Result<Option<(ColdStorage<K, C, V>, u64)>, PersistError> {
        // Look for existing MessagePack snapshots
        let mut snapshots: Vec<(u64, PathBuf)> = Vec::new();

        if let Ok(entries) = std::fs::read_dir(base_dir) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                if filename_str.starts_with("snapshot_full_")
                    && filename_str.ends_with(".msgpack")
                {
                    if let Some(version_str) = filename_str
                        .strip_prefix("snapshot_full_")
                        .and_then(|s| s.strip_suffix(".msgpack"))
                    {
                        if let Ok(version) = version_str.parse::<u64>() {
                            snapshots.push((version, entry.path()));
                        }
                    }
                }
            }
        }

        if snapshots.is_empty() {
            return Ok(None);
        }

        snapshots.sort_by_key(|(v, _)| *v);
        let (version, path) = snapshots.last().unwrap();

        // Load the regular snapshot
        let bytes = std::fs::read(path)?;

        // Decompress if needed
        #[cfg(feature = "compression")]
        let bytes = if bytes.len() >= 4 && &bytes[0..4] == b"\x28\xb5\x2f\xfd" {
            zstd::decode_all(&bytes[..]).map_err(PersistError::Compression)?
        } else {
            bytes
        };

        #[cfg(not(feature = "compression"))]
        let bytes = bytes;

        // Parse header
        if bytes.len() < 4 {
            return Ok(None);
        }

        let metadata_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if bytes.len() < 4 + metadata_len {
            return Ok(None);
        }

        let crdt_bytes = &bytes[4 + metadata_len..];
        let crdt: CRDT<K, C, V> =
            CRDT::from_msgpack_bytes(crdt_bytes).map_err(PersistError::MsgpackDecode)?;

        let clock_value = crdt.get_clock().current_time();

        // Create indexed snapshot from the loaded CRDT
        let indexed_path = base_dir.join(format!("snapshot_indexed_{:06}.bin", version));
        Self::create_indexed_snapshot_from_crdt(&crdt, node_id, &indexed_path)?;

        // Load the new indexed snapshot
        let cold = ColdStorage::from_file(&indexed_path)?;

        Ok(Some((cold, clock_value)))
    }

    /// Create an indexed snapshot from a CRDT
    fn create_indexed_snapshot_from_crdt(
        crdt: &CRDT<K, C, V>,
        node_id: NodeId,
        path: &PathBuf,
    ) -> Result<(), PersistError> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write magic and version
        writer.write_all(SNAPSHOT_MAGIC)?;
        writer.write_all(&[SNAPSHOT_VERSION, 0])?; // version, flags

        // Placeholder for header (we'll seek back and write it)
        let header_pos = writer.stream_position()?;
        let placeholder = [0u8; 48];
        writer.write_all(&placeholder)?;

        // Write records and build index
        let mut index_entries: Vec<IndexEntry<K>> = Vec::new();
        let records_start = writer.stream_position()?;

        for (key, record) in crdt.get_data().iter() {
            let offset = writer.stream_position()?;
            let record_bytes =
                rmp_serde::to_vec(record).map_err(PersistError::MsgpackEncode)?;
            writer.write_all(&record_bytes)?;

            index_entries.push(IndexEntry {
                key: key.clone(),
                offset,
                length: record_bytes.len() as u32,
            });
        }

        // Write tombstones section
        let tombstones_offset = writer.stream_position()?;
        let tombstone_map: HashMap<K, TombstoneInfo> = crdt
            .get_tombstones()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        let tombstone_bytes =
            rmp_serde::to_vec(&tombstone_map).map_err(PersistError::MsgpackEncode)?;
        writer.write_all(&tombstone_bytes)?;

        // Write index section
        let index_offset = writer.stream_position()?;
        let index_bytes =
            rmp_serde::to_vec(&index_entries).map_err(PersistError::MsgpackEncode)?;
        writer.write_all(&index_bytes)?;

        // Write header
        writer.seek(SeekFrom::Start(header_pos))?;
        writer.write_all(&node_id.to_le_bytes())?;
        writer.write_all(&crdt.get_clock().current_time().to_le_bytes())?;
        writer.write_all(&(index_entries.len() as u64).to_le_bytes())?;
        writer.write_all(&(tombstone_map.len() as u64).to_le_bytes())?;
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&tombstones_offset.to_le_bytes())?;

        writer.flush()?;
        writer.get_ref().sync_all()?;

        let _ = records_start; // Silence unused warning

        Ok(())
    }

    /// Replay WAL files on top of current state
    fn replay_wal(&mut self) -> Result<(), PersistError> {
        let mut wal_files: Vec<_> = std::fs::read_dir(&self.base_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with("wal_") && name.ends_with(".bin")
            })
            .collect();

        wal_files.sort_by_key(|e| {
            e.file_name()
                .to_string_lossy()
                .strip_prefix("wal_")
                .and_then(|s| s.strip_suffix(".bin"))
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0)
        });

        for entry in wal_files {
            let changes: Vec<Change<K, C, V>> = super::wal::read_wal_file(&entry.path())?;
            for change in &changes {
                // Apply to hot tier
                self.hot_keys.insert(change.record_id.clone());
                if change.col_name.is_none() {
                    self.hot_tombstones.insert(change.record_id.clone());
                }
            }
            // Merge the changes
            self.hot.merge_changes(changes, &DefaultMergeRule);
            self.changes_since_snapshot += 1;
        }

        Ok(())
    }

    /// Get a record, checking hot tier first, then cache, then cold storage
    pub fn get_record(&mut self, key: &K) -> Result<Option<Record<C, V>>, PersistError> {
        // 1. Check hot tier first (most recent)
        if self.hot_keys.contains(key) {
            if let Some(record) = self.hot.get_record(key) {
                return Ok(Some(record.clone()));
            }
            // Key is in hot but record was deleted
            if self.hot_tombstones.contains(key) {
                return Ok(None);
            }
        }

        // 2. Check if tombstoned (hot or cold)
        if self.hot_tombstones.contains(key) || self.cold.is_tombstoned(key) {
            return Ok(None);
        }

        // 3. Check LRU cache
        if let Some(record) = self.cache.get(key) {
            return Ok(Some(record.clone()));
        }

        // 4. Load from cold storage
        if let Some(record) = self.cold.load_record(key)? {
            self.cache.insert(key.clone(), record.clone());
            return Ok(Some(record));
        }

        Ok(None)
    }

    /// Check if a record exists (without loading full record)
    pub fn contains_key(&self, key: &K) -> bool {
        if self.hot_tombstones.contains(key) || self.cold.is_tombstoned(key) {
            return false;
        }
        self.hot_keys.contains(key) || self.cache.contains_key(key) || self.cold.contains_key(key)
    }

    /// Check if a record is tombstoned
    pub fn is_tombstoned(&self, key: &K) -> bool {
        self.hot_tombstones.contains(key) || self.cold.is_tombstoned(key)
    }

    /// Insert or update a record
    pub fn insert_or_update<I>(
        &mut self,
        record_id: &K,
        fields: I,
    ) -> Result<Vec<Change<K, C, V>>, PersistError>
    where
        I: IntoIterator<Item = (C, V)>,
    {
        // Check tombstones
        if self.is_tombstoned(record_id) {
            return Ok(Vec::new());
        }

        // If record exists in cold but not hot, load it first
        if !self.hot_keys.contains(record_id) && self.cold.contains_key(record_id) {
            if let Some(cold_record) = self.cold.load_record(record_id)? {
                // Merge cold record into hot
                self.merge_cold_record_to_hot(record_id, cold_record);
            }
        }

        // Apply to hot tier
        let changes = self.hot.insert_or_update(record_id, fields);
        self.hot_keys.insert(record_id.clone());

        // Persist
        self.persist_and_notify(&changes)?;

        Ok(changes)
    }

    /// Delete a record
    pub fn delete_record(&mut self, record_id: &K) -> Result<Option<Change<K, C, V>>, PersistError> {
        if self.is_tombstoned(record_id) {
            return Ok(None);
        }

        let change = self.hot.delete_record(record_id);
        if let Some(ref c) = change {
            self.hot_tombstones.insert(record_id.clone());
            self.persist_and_notify(std::slice::from_ref(c))?;
        }

        Ok(change)
    }

    /// Merge changes from remote nodes
    pub fn merge_changes(
        &mut self,
        changes: Vec<Change<K, C, V>>,
    ) -> Result<Vec<Change<K, C, V>>, PersistError> {
        // Load any cold records that are being updated
        for change in &changes {
            if !self.hot_keys.contains(&change.record_id)
                && self.cold.contains_key(&change.record_id)
            {
                if let Some(cold_record) = self.cold.load_record(&change.record_id)? {
                    self.merge_cold_record_to_hot(&change.record_id, cold_record);
                }
            }
        }

        // Apply to hot tier
        let accepted = self.hot.merge_changes(changes, &DefaultMergeRule);

        // Track keys
        for change in &accepted {
            self.hot_keys.insert(change.record_id.clone());
            if change.col_name.is_none() {
                self.hot_tombstones.insert(change.record_id.clone());
            }
        }

        // Persist
        self.persist_and_notify(&accepted)?;

        Ok(accepted)
    }

    /// Get changes since a version (for sync)
    pub fn get_changes_since(&mut self, last_db_version: u64) -> Result<Vec<Change<K, C, V>>, PersistError> {
        // Hot tier changes
        let mut changes = self.hot.get_changes_since(last_db_version);

        // For cold records not in hot, we'd need to scan - but typically
        // get_changes_since is called for recent changes which are in hot
        // This is a trade-off for streaming: old cold changes require full scan

        // Sort and dedupe
        CRDT::compress_changes(&mut changes);

        Ok(changes)
    }

    /// Create an indexed snapshot of current state
    pub fn create_indexed_snapshot(&mut self) -> Result<PathBuf, PersistError> {
        // Merge hot into a temporary full CRDT for snapshot
        let mut full_crdt: CRDT<K, C, V> = CRDT::new(self.hot.node_id, None);

        // First, load all cold records
        for key in self.cold.keys().cloned().collect::<Vec<_>>() {
            if !self.hot_tombstones.contains(&key) {
                if let Some(record) = self.cold.load_record(&key)? {
                    // Convert record to changes and merge
                    let changes = self.record_to_changes(&key, &record);
                    full_crdt.merge_changes(changes, &DefaultMergeRule);
                }
            }
        }

        // Apply cold tombstones
        for (key, info) in self.cold.tombstones.iter() {
            full_crdt.merge_changes(
                vec![Change::new(
                    key.clone(),
                    None,
                    None,
                    u64::MAX,
                    info.db_version,
                    info.node_id,
                    info.local_db_version,
                    0,
                )],
                &DefaultMergeRule,
            );
        }

        // Merge hot tier on top
        let hot_changes = self.hot.get_changes_since(0);
        full_crdt.merge_changes(hot_changes, &DefaultMergeRule);

        // Create indexed snapshot
        self.snapshot_version += 1;
        let snapshot_path = self.base_dir.join(format!(
            "snapshot_indexed_{:06}.bin",
            self.snapshot_version
        ));

        Self::create_indexed_snapshot_from_crdt(&full_crdt, self.hot.node_id, &snapshot_path)?;

        // Reload cold storage from new snapshot
        self.cold = ColdStorage::from_file(&snapshot_path)?;

        // Clear hot tier
        self.hot = CRDT::new(self.hot.node_id, None);
        self.hot.get_clock_mut().set_time(self.cold.clock_value);
        self.hot_keys.clear();
        self.hot_tombstones.clear();
        self.cache = LruCache::new(self.config.cache_capacity);

        // Rotate WAL and delete old segments (snapshot has all data)
        let _ = self.wal.rotate(&self.base_dir, &self.wal_segment_hooks)?;
        self.cleanup_old_wal_files()?;

        // Reset counters
        self.changes_since_snapshot = 0;
        self.last_snapshot_time = std::time::Instant::now();

        // Call hooks
        let db_version = self.cold.clock_value;
        for hook in &self.snapshot_hooks {
            hook.on_snapshot(&snapshot_path, db_version);
        }

        Ok(snapshot_path)
    }

    /// Get the batch of changes for network broadcasting
    pub fn take_batch(&mut self) -> Vec<Change<K, C, V>> {
        std::mem::take(&mut self.batch)
    }

    /// Add a post-operation hook
    pub fn add_post_hook(&mut self, hook: Box<dyn PostOpHook<K, C, V>>) {
        self.post_hooks.push(hook);
    }

    /// Add a snapshot hook
    pub fn add_snapshot_hook(&mut self, hook: Box<dyn SnapshotHook>) {
        self.snapshot_hooks.push(hook);
    }

    /// Add a WAL segment hook
    pub fn add_wal_segment_hook(&mut self, hook: Box<dyn WalSegmentHook>) {
        self.wal_segment_hooks.push(hook);
    }

    /// Get number of records in hot tier
    pub fn hot_record_count(&self) -> usize {
        self.hot_keys.len()
    }

    /// Get number of records in cold storage
    pub fn cold_record_count(&self) -> usize {
        self.cold.len()
    }

    /// Get current logical clock value
    pub fn get_clock_time(&self) -> u64 {
        self.hot.get_clock().current_time()
    }

    // Helper methods

    fn merge_cold_record_to_hot(&mut self, key: &K, record: Record<C, V>) {
        let changes = self.record_to_changes(key, &record);
        self.hot.merge_changes(changes, &DefaultMergeRule);
        self.hot_keys.insert(key.clone());
    }

    fn record_to_changes(&self, key: &K, record: &Record<C, V>) -> Vec<Change<K, C, V>> {
        let mut changes = Vec::new();
        for (col_name, value) in &record.fields {
            if let Some(col_ver) = record.column_versions.get(col_name) {
                changes.push(Change::new(
                    key.clone(),
                    Some(col_name.clone()),
                    Some(value.clone()),
                    col_ver.col_version,
                    col_ver.db_version,
                    col_ver.node_id,
                    col_ver.local_db_version,
                    0,
                ));
            }
        }
        changes
    }

    /// Delete all WAL files except the current one
    fn cleanup_old_wal_files(&self) -> Result<(), PersistError> {
        let current_segment = self.wal.current_segment();

        if let Ok(entries) = std::fs::read_dir(&self.base_dir) {
            for entry in entries.flatten() {
                let filename = entry.file_name();
                let filename_str = filename.to_string_lossy();

                if filename_str.starts_with("wal_") && filename_str.ends_with(".bin") {
                    // Parse segment number
                    if let Some(num_str) = filename_str
                        .strip_prefix("wal_")
                        .and_then(|s| s.strip_suffix(".bin"))
                    {
                        if let Ok(segment_num) = num_str.parse::<u64>() {
                            // Keep current segment, delete others
                            if segment_num < current_segment {
                                let _ = std::fs::remove_file(entry.path());
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn persist_and_notify(&mut self, changes: &[Change<K, C, V>]) -> Result<(), PersistError> {
        if changes.is_empty() {
            return Ok(());
        }

        // Append to WAL
        self.wal.append(changes)?;
        self.changes_since_snapshot += changes.len();

        // Add to batch
        self.batch.extend_from_slice(changes);

        // Call post hooks
        for hook in &self.post_hooks {
            hook.after_op(changes);
        }

        // Check auto-snapshot
        self.check_auto_snapshot()?;

        Ok(())
    }

    fn check_auto_snapshot(&mut self) -> Result<(), PersistError> {
        let should_snapshot = self.changes_since_snapshot
            >= self.config.persist_config.snapshot_threshold
            || self
                .config
                .persist_config
                .snapshot_interval_secs
                .map(|i| self.last_snapshot_time.elapsed().as_secs() >= i)
                .unwrap_or(false);

        if should_snapshot {
            self.create_indexed_snapshot()?;
        }

        Ok(())
    }
}

// Need to add get_clock_mut and get_tombstones to CRDT - let's do that via a trait extension
// For now, we'll work around it

impl<K: Ord + Hash + Eq + Clone, C: Hash + Eq + Clone, V: Clone> CRDT<K, C, V> {
    /// Mutable access to the logical clock (for internal use)
    pub(crate) fn get_clock_mut(&mut self) -> &mut LogicalClock {
        &mut self.clock
    }

    /// Access to tombstones (for internal use)
    pub(crate) fn get_tombstones(&self) -> &TombstoneStorage<K> {
        &self.tombstones
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_streaming_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        let mut crdt = StreamingCRDT::<String, String, String>::open(
            base_dir.clone(),
            1,
            StreamingConfig::default(),
        )
        .unwrap();

        // Insert
        let changes = crdt
            .insert_or_update(
                &"key1".to_string(),
                vec![("field1".to_string(), "value1".to_string())],
            )
            .unwrap();

        assert_eq!(changes.len(), 1);

        // Get
        let record = crdt.get_record(&"key1".to_string()).unwrap();
        assert!(record.is_some());
        assert_eq!(
            record.unwrap().fields.get("field1").unwrap(),
            "value1"
        );

        // Delete
        let delete = crdt.delete_record(&"key1".to_string()).unwrap();
        assert!(delete.is_some());

        // Get after delete
        let record = crdt.get_record(&"key1".to_string()).unwrap();
        assert!(record.is_none());
    }

    #[test]
    fn test_streaming_snapshot_and_reload() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        // Create and populate
        {
            let mut crdt = StreamingCRDT::<String, String, String>::open(
                base_dir.clone(),
                1,
                StreamingConfig::default(),
            )
            .unwrap();

            for i in 0..100 {
                crdt.insert_or_update(
                    &format!("key{}", i),
                    vec![(format!("field{}", i), format!("value{}", i))],
                )
                .unwrap();
            }

            // Create indexed snapshot
            crdt.create_indexed_snapshot().unwrap();
        }

        // Reopen and verify
        {
            let mut crdt = StreamingCRDT::<String, String, String>::open(
                base_dir.clone(),
                1,
                StreamingConfig::default(),
            )
            .unwrap();

            // Records should be in cold storage
            assert_eq!(crdt.cold_record_count(), 100);
            assert_eq!(crdt.hot_record_count(), 0);

            // Access should work (lazy load)
            let record = crdt.get_record(&"key50".to_string()).unwrap();
            assert!(record.is_some());
            assert_eq!(
                record.unwrap().fields.get("field50").unwrap(),
                "value50"
            );
        }
    }

    #[test]
    fn test_streaming_hot_cold_merge() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        // Create initial data and snapshot
        {
            let mut crdt = StreamingCRDT::<String, String, String>::open(
                base_dir.clone(),
                1,
                StreamingConfig::default(),
            )
            .unwrap();

            crdt.insert_or_update(
                &"key1".to_string(),
                vec![
                    ("field1".to_string(), "cold_value".to_string()),
                    ("field2".to_string(), "unchanged".to_string()),
                ],
            )
            .unwrap();

            crdt.create_indexed_snapshot().unwrap();
        }

        // Reopen and modify
        {
            let mut crdt = StreamingCRDT::<String, String, String>::open(
                base_dir.clone(),
                1,
                StreamingConfig::default(),
            )
            .unwrap();

            // Update one field (should load cold record first)
            crdt.insert_or_update(
                &"key1".to_string(),
                vec![("field1".to_string(), "hot_value".to_string())],
            )
            .unwrap();

            // Verify merged result
            let record = crdt.get_record(&"key1".to_string()).unwrap().unwrap();
            assert_eq!(record.fields.get("field1").unwrap(), "hot_value");
            assert_eq!(record.fields.get("field2").unwrap(), "unchanged");
        }
    }
}
