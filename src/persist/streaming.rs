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
//! ├── node_id: NodeId (8 or 16 bytes depending on feature)
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
//! └── Vec<IndexEntry { key: K, offset: u64, length: u32 }> (MessagePack serialized)
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
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::hash::Hash;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use super::wal::WalWriter;
use super::{PersistConfig, PersistError, PostOpHook, SnapshotHook, WalSegmentHook};

/// Magic bytes for indexed snapshot format
const SNAPSHOT_MAGIC: &[u8; 4] = b"CRDS";
/// Current snapshot format version
const SNAPSHOT_VERSION: u8 = 1;

/// Header size depends on NodeId type
/// Layout: node_id + clock + record_count + tombstone_count + index_offset + tombstones_offset
const HEADER_SIZE: usize = std::mem::size_of::<NodeId>() + 5 * std::mem::size_of::<u64>();

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

/// O(1) LRU cache implementation using HashMap + VecDeque with reference counting
struct LruCache<K, V> {
    capacity: usize,
    map: HashMap<K, V>,
    /// Keys in order of access (oldest at front, newest at back)
    order: VecDeque<K>,
    /// Count how many times each key appears in the order queue
    /// Only evict when count reaches 0
    ref_count: HashMap<K, usize>,
}

impl<K: Hash + Eq + Clone, V> LruCache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1), // Ensure at least 1
            map: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity * 2),
            ref_count: HashMap::with_capacity(capacity),
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        if self.map.contains_key(key) {
            // Mark as recently used by adding to back
            self.order.push_back(key.clone());
            *self.ref_count.entry(key.clone()).or_insert(0) += 1;
            self.map.get(key)
        } else {
            None
        }
    }

    fn insert(&mut self, key: K, value: V) {
        if !self.map.contains_key(&key) {
            // Evict if at capacity
            while self.map.len() >= self.capacity {
                self.evict_one();
            }
        }
        self.order.push_back(key.clone());
        *self.ref_count.entry(key.clone()).or_insert(0) += 1;
        self.map.insert(key, value);
    }

    fn evict_one(&mut self) {
        // Pop from front until we find a key with ref_count == 1 (last reference)
        while let Some(oldest) = self.order.pop_front() {
            let count = self.ref_count.get_mut(&oldest);
            match count {
                Some(c) if *c > 1 => {
                    // Key has newer references, skip this stale one
                    *c -= 1;
                }
                Some(_) => {
                    // Last reference, evict this key
                    self.ref_count.remove(&oldest);
                    self.map.remove(&oldest);
                    break;
                }
                None => {
                    // Key not in map (already evicted), continue
                }
            }
        }
    }

    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    #[allow(dead_code)]
    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
        self.ref_count.clear();
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
        let file_len = file.metadata()?.len();
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
        let mut header_buf = vec![0u8; HEADER_SIZE];
        reader.read_exact(&mut header_buf)?;

        // Parse header - NodeId size varies based on feature flag
        let node_id_size = std::mem::size_of::<NodeId>();
        let mut offset = 0;

        // Skip node_id (we don't need it for loading)
        offset += node_id_size;

        let clock_value = u64::from_le_bytes(header_buf[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let record_count = u64::from_le_bytes(header_buf[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let tombstone_count = u64::from_le_bytes(header_buf[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let index_offset = u64::from_le_bytes(header_buf[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let tombstones_offset = u64::from_le_bytes(header_buf[offset..offset + 8].try_into().unwrap());

        let _ = (record_count, tombstone_count); // Silence unused warnings

        // Validate offsets
        if index_offset == 0 || index_offset >= file_len {
            return Err(PersistError::InvalidDirectory(format!(
                "Invalid index_offset {} (file_len={})", index_offset, file_len
            )));
        }

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

        // Reuse the same reader - just seek when needed for record access
        // No need to reopen the file (fixes file descriptor leak)

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
    fn get_tombstone(&self, key: &K) -> Option<TombstoneInfo> {
        self.tombstones.find(key)
    }

    /// Iterate over keys (without collecting into Vec to save memory)
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

        // Look for indexed snapshot and extract version
        let (indexed_snapshot, snapshot_version) = Self::find_indexed_snapshot(&base_dir)?;

        let (cold, clock_value, snapshot_version) = if let Some(path) = indexed_snapshot {
            let cold = ColdStorage::from_file(&path)?;
            let clock = cold.clock_value;
            (cold, clock, snapshot_version)
        } else {
            // Try to migrate from regular snapshot
            let migrated = Self::migrate_from_regular_snapshot(&base_dir, node_id)?;
            if let Some((cold, clock, version)) = migrated {
                (cold, clock, version)
            } else {
                (ColdStorage::empty(), 0, 0)
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
            snapshot_version,
            post_hooks: Vec::new(),
            snapshot_hooks: Vec::new(),
            wal_segment_hooks: Vec::new(),
            batch: Vec::new(),
            last_snapshot_time: std::time::Instant::now(),
        };

        streaming.replay_wal()?;

        Ok(streaming)
    }

    /// Find the most recent indexed snapshot and return its version
    fn find_indexed_snapshot(base_dir: &PathBuf) -> Result<(Option<PathBuf>, u64), PersistError> {
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
        match snapshots.last() {
            Some((version, path)) => Ok((Some(path.clone()), *version)),
            None => Ok((None, 0)),
        }
    }

    /// Migrate from a regular PersistedCRDT snapshot to indexed format
    fn migrate_from_regular_snapshot(
        base_dir: &PathBuf,
        node_id: NodeId,
    ) -> Result<Option<(ColdStorage<K, C, V>, u64, u64)>, PersistError> {
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
        let version = *version;

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

        // Create indexed snapshot from the loaded CRDT (with atomic write)
        let indexed_path = base_dir.join(format!("snapshot_indexed_{:06}.bin", version));
        Self::create_indexed_snapshot_from_crdt(&crdt, node_id, &indexed_path)?;

        // Load the new indexed snapshot
        let cold = ColdStorage::from_file(&indexed_path)?;

        Ok(Some((cold, clock_value, version)))
    }

    /// Create an indexed snapshot from a CRDT (atomic write via temp file)
    fn create_indexed_snapshot_from_crdt(
        crdt: &CRDT<K, C, V>,
        node_id: NodeId,
        path: &PathBuf,
    ) -> Result<(), PersistError> {
        // Write to a temp file first, then rename atomically
        let temp_path = path.with_extension("bin.tmp");

        // Use unbuffered I/O to avoid issues with seeking and stream_position
        let mut file = File::create(&temp_path)?;

        // Write magic and version
        file.write_all(SNAPSHOT_MAGIC)?;
        file.write_all(&[SNAPSHOT_VERSION, 0])?; // version, flags

        // Placeholder for header (we'll seek back and write it)
        let header_pos = file.stream_position()?;
        let placeholder = vec![0u8; HEADER_SIZE];
        file.write_all(&placeholder)?;

        // Write records and build index
        let mut index_entries: Vec<IndexEntry<K>> = Vec::new();

        for (key, record) in crdt.get_data().iter() {
            let offset = file.stream_position()?;
            let record_bytes =
                rmp_serde::to_vec(record).map_err(PersistError::MsgpackEncode)?;
            file.write_all(&record_bytes)?;

            index_entries.push(IndexEntry {
                key: key.clone(),
                offset,
                length: record_bytes.len() as u32,
            });
        }

        // Write tombstones section
        let tombstones_offset = file.stream_position()?;
        let tombstone_map: HashMap<K, TombstoneInfo> = crdt
            .get_tombstones()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        let tombstone_bytes =
            rmp_serde::to_vec(&tombstone_map).map_err(PersistError::MsgpackEncode)?;
        file.write_all(&tombstone_bytes)?;

        // Write index section
        let index_offset = file.stream_position()?;
        let index_bytes =
            rmp_serde::to_vec(&index_entries).map_err(PersistError::MsgpackEncode)?;
        file.write_all(&index_bytes)?;

        // Write header - seek back and overwrite placeholder
        file.seek(SeekFrom::Start(header_pos))?;
        file.write_all(&node_id.to_le_bytes())?;
        file.write_all(&crdt.get_clock().current_time().to_le_bytes())?;
        file.write_all(&(index_entries.len() as u64).to_le_bytes())?;
        file.write_all(&(tombstone_map.len() as u64).to_le_bytes())?;
        file.write_all(&index_offset.to_le_bytes())?;
        file.write_all(&tombstones_offset.to_le_bytes())?;

        file.sync_all()?;
        drop(file);

        // Atomic rename
        std::fs::rename(&temp_path, path)?;

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
            let num_changes = changes.len();
            for change in &changes {
                // Apply to hot tier
                self.hot_keys.insert(change.record_id.clone());
                if change.col_name.is_none() {
                    self.hot_tombstones.insert(change.record_id.clone());
                }
            }
            // Merge the changes
            self.hot.merge_changes(changes, &DefaultMergeRule);
            self.changes_since_snapshot += num_changes; // Fix: count actual changes
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
                // Directly insert cold record into hot storage (preserves version metadata)
                self.hot.insert_record_direct(record_id.clone(), cold_record);
                self.hot_keys.insert(record_id.clone());
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
                    // Directly insert cold record into hot storage (preserves version metadata)
                    self.hot.insert_record_direct(change.record_id.clone(), cold_record);
                    self.hot_keys.insert(change.record_id.clone());
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
    ///
    /// This scans both hot and cold storage to ensure all changes are included.
    pub fn get_changes_since(&mut self, last_db_version: u64) -> Result<Vec<Change<K, C, V>>, PersistError> {
        // Hot tier changes
        let mut changes = self.hot.get_changes_since(last_db_version);

        // Scan cold storage for records with changes after last_db_version
        // This is necessary to ensure sync completeness
        for key in self.cold.keys().cloned().collect::<Vec<_>>() {
            // Skip if already in hot (hot has newer data)
            if self.hot_keys.contains(&key) {
                continue;
            }
            // Skip if tombstoned
            if self.hot_tombstones.contains(&key) {
                continue;
            }

            if let Some(record) = self.cold.load_record(&key)? {
                // Check if record has changes after last_db_version
                if record.highest_local_db_version > last_db_version {
                    // Convert record to changes
                    for (col_name, value) in &record.fields {
                        if let Some(col_ver) = record.column_versions.get(col_name) {
                            if col_ver.local_db_version > last_db_version {
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
                    }
                }
            }
        }

        // Include cold tombstones if they're after last_db_version
        for (key, info) in self.cold.tombstones.iter() {
            if info.local_db_version > last_db_version && !self.hot_keys.contains(key) {
                changes.push(Change::new(
                    key.clone(),
                    None,
                    None,
                    u64::MAX,
                    info.db_version,
                    info.node_id,
                    info.local_db_version,
                    0,
                ));
            }
        }

        // Sort and dedupe
        CRDT::compress_changes(&mut changes);

        Ok(changes)
    }

    /// Create an indexed snapshot of current state
    pub fn create_indexed_snapshot(&mut self) -> Result<PathBuf, PersistError> {
        // Merge hot into a temporary full CRDT for snapshot
        let mut full_crdt: CRDT<K, C, V> = CRDT::new(self.hot.node_id, None);

        // First, load all cold records (iterate without collecting all keys at once)
        let cold_keys: Vec<K> = self.cold.keys().cloned().collect();
        for key in cold_keys {
            // Skip if hot has this key (hot data supersedes cold)
            if self.hot_keys.contains(&key) {
                continue;
            }
            // Skip if tombstoned in hot
            if self.hot_tombstones.contains(&key) {
                continue;
            }

            if let Some(record) = self.cold.load_record(&key)? {
                // Directly insert record (preserves version metadata)
                full_crdt.insert_record_direct(key, record);
            }
        }

        // Apply cold tombstones ONLY if not superseded by hot records
        for (key, info) in self.cold.tombstones.iter() {
            // Skip if hot has touched this key (hot data supersedes cold tombstones)
            if self.hot_keys.contains(key) {
                continue;
            }
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

        // Get the FINAL clock value after all merges (fixes clock desync)
        let final_clock_value = full_crdt.get_clock().current_time();

        // Create indexed snapshot
        self.snapshot_version += 1;
        let snapshot_path = self.base_dir.join(format!(
            "snapshot_indexed_{:06}.bin",
            self.snapshot_version
        ));

        Self::create_indexed_snapshot_from_crdt(&full_crdt, self.hot.node_id, &snapshot_path)?;

        // Reload cold storage from new snapshot
        self.cold = ColdStorage::from_file(&snapshot_path)?;

        // Clear hot tier with FINAL clock value (fixes clock desync)
        self.hot = CRDT::new(self.hot.node_id, None);
        self.hot.get_clock_mut().set_time(final_clock_value);
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

// Internal CRDT extensions for streaming module
impl<K: Ord + Hash + Eq + Clone, C: Hash + Eq + Clone, V: Clone> CRDT<K, C, V> {
    /// Mutable access to the logical clock (for internal use)
    pub(crate) fn get_clock_mut(&mut self) -> &mut LogicalClock {
        &mut self.clock
    }

    /// Access to tombstones (for internal use)
    pub(crate) fn get_tombstones(&self) -> &TombstoneStorage<K> {
        &self.tombstones
    }

    /// Insert a record directly, preserving its version metadata.
    /// This bypasses the normal change-based API to avoid causality issues
    /// when loading cold records into hot storage.
    pub(crate) fn insert_record_direct(&mut self, key: K, record: Record<C, V>) {
        // Update clock to at least match the record's versions
        for col_ver in record.column_versions.values() {
            if col_ver.db_version > self.clock.current_time() {
                self.clock.set_time(col_ver.db_version);
            }
        }
        self.data.insert(key, record);
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

    #[test]
    fn test_get_changes_since_includes_cold() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        // Create data and snapshot
        {
            let mut crdt = StreamingCRDT::<String, String, String>::open(
                base_dir.clone(),
                1,
                StreamingConfig::default(),
            )
            .unwrap();

            crdt.insert_or_update(
                &"key1".to_string(),
                vec![("field1".to_string(), "value1".to_string())],
            )
            .unwrap();

            crdt.create_indexed_snapshot().unwrap();
        }

        // Reopen and verify get_changes_since includes cold records
        {
            let mut crdt = StreamingCRDT::<String, String, String>::open(
                base_dir.clone(),
                1,
                StreamingConfig::default(),
            )
            .unwrap();

            // Should find changes from cold storage when asking for version 0
            let changes = crdt.get_changes_since(0).unwrap();
            assert!(!changes.is_empty(), "Should find cold changes");

            // Verify the change is for key1
            assert!(
                changes.iter().any(|c| c.record_id == "key1"),
                "Should find change for key1"
            );

            // Should not find changes after the current clock (future)
            let current_clock = crdt.get_clock_time();
            let no_changes = crdt.get_changes_since(current_clock + 100).unwrap();
            assert!(no_changes.is_empty(), "Should not find changes in the future");
        }
    }

    #[test]
    fn test_clock_preserved_across_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path().to_path_buf();

        let clock_before_snapshot;
        let clock_after_snapshot;
        {
            let mut crdt = StreamingCRDT::<String, String, String>::open(
                base_dir.clone(),
                1,
                StreamingConfig::default(),
            )
            .unwrap();

            for i in 0..10 {
                crdt.insert_or_update(
                    &format!("key{}", i),
                    vec![("field".to_string(), format!("value{}", i))],
                )
                .unwrap();
            }

            clock_before_snapshot = crdt.get_clock_time();
            crdt.create_indexed_snapshot().unwrap();
            clock_after_snapshot = crdt.get_clock_time();

            // Clock should be at least as high after snapshot (merge may advance it)
            assert!(
                clock_after_snapshot >= clock_before_snapshot,
                "Clock should not go backwards"
            );
        }

        // Reopen and verify clock is preserved
        {
            let crdt = StreamingCRDT::<String, String, String>::open(
                base_dir.clone(),
                1,
                StreamingConfig::default(),
            )
            .unwrap();

            // Clock should match what it was after snapshot
            assert_eq!(crdt.get_clock_time(), clock_after_snapshot);
        }
    }

    #[test]
    fn test_lru_cache_eviction() {
        let mut cache: LruCache<i32, String> = LruCache::new(3);

        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        // All should be present
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        assert!(cache.contains_key(&3));

        // Access 1 to make it recently used
        let _ = cache.get(&1);

        // Insert 4, should evict 2 (oldest)
        cache.insert(4, "four".to_string());

        assert!(cache.contains_key(&1)); // Still there (was accessed)
        assert!(!cache.contains_key(&2)); // Evicted
        assert!(cache.contains_key(&3));
        assert!(cache.contains_key(&4));
    }
}
