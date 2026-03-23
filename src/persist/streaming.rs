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

use crate::{Change, DefaultMergeRule, NodeId, Record, TombstoneInfo, TombstoneStorage, CRDT};
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

/// Maximum index section size in bytes (security limit to prevent OOM attacks)
/// Allows up to ~10 million index entries (at ~100 bytes per entry estimated)
const MAX_INDEX_SECTION_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB

/// Maximum tombstone section size in bytes (security limit)
const MAX_TOMBSTONE_SECTION_SIZE: u64 = 256 * 1024 * 1024; // 256 MB

/// Result type for migration function to avoid clippy type_complexity warning
type MigrationResult<K, C, V> = Result<Option<(ColdStorage<K, C, V>, u64, u64)>, PersistError>;

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
  /// Highest local_db_version in this record (for efficient sync queries)
  #[cfg_attr(feature = "serde", serde(default))]
  highest_version: u64,
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

/// Initial capacity multiplier for LRU order queue.
/// Uses 2x capacity because each get() adds a duplicate entry - we need headroom
/// before compaction kicks in.
const LRU_ORDER_INITIAL_CAPACITY_MULTIPLIER: usize = 2;

/// Compaction threshold multiplier for LRU order queue.
/// When order.len() exceeds capacity * this value, we compact to remove stale entries.
/// Uses 3x because: 2x for normal duplicates + 1x buffer before we need to compact.
/// Higher values = less frequent compaction but more memory usage.
const LRU_ORDER_COMPACT_THRESHOLD_MULTIPLIER: usize = 3;

impl<K: Hash + Eq + Clone, V> LruCache<K, V> {
  fn new(capacity: usize) -> Self {
    Self {
      capacity: capacity.max(1), // Ensure at least 1
      map: HashMap::with_capacity(capacity),
      order: VecDeque::with_capacity(capacity * LRU_ORDER_INITIAL_CAPACITY_MULTIPLIER),
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
    let is_new = !self.map.contains_key(&key);
    if is_new {
      // Evict if at capacity
      while self.map.len() >= self.capacity {
        self.evict_one();
      }
    }
    // Always mark as recently used (both new and updated keys)
    // Without this, frequently updated keys get evicted despite being "hot"
    self.order.push_back(key.clone());
    *self.ref_count.entry(key.clone()).or_insert(0) += 1;
    // Always update the value
    self.map.insert(key, value);

    // Compact order queue if it gets too large (prevents unbounded growth)
    if self.order.len() > self.capacity * LRU_ORDER_COMPACT_THRESHOLD_MULTIPLIER {
      self.compact();
    }
  }

  /// Rebuild order queue to remove stale entries
  fn compact(&mut self) {
    let mut new_order =
      VecDeque::with_capacity(self.capacity * LRU_ORDER_INITIAL_CAPACITY_MULTIPLIER);
    let mut new_ref_count: HashMap<K, usize> = HashMap::with_capacity(self.capacity);

    // Keep only the most recent entry for each key
    for key in self.order.drain(..).rev() {
      if self.map.contains_key(&key) && !new_ref_count.contains_key(&key) {
        new_ref_count.insert(key.clone(), 1);
        new_order.push_front(key);
      }
    }

    self.order = new_order;
    self.ref_count = new_ref_count;
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
}

/// Cold storage backed by an indexed snapshot file
struct ColdStorage<K, C, V>
where
  K: Ord + Hash + Eq + Clone,
  C: Hash + Eq + Clone,
  V: Clone,
{
  /// Path to snapshot file (opened on-demand to avoid FD exhaustion)
  snapshot_path: Option<PathBuf>,
  /// Key -> (offset, length, highest_local_db_version) index
  /// The version is used for efficient get_changes_since queries
  index: HashMap<K, (u64, u32, u64)>,
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
      snapshot_path: None,
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

    // Validate offsets
    if index_offset == 0 || index_offset >= file_len {
      return Err(PersistError::InvalidDirectory(format!(
        "Invalid index_offset {} (file_len={})",
        index_offset, file_len
      )));
    }
    if tombstones_offset == 0 || tombstones_offset >= file_len || tombstones_offset > index_offset {
      return Err(PersistError::InvalidDirectory(format!(
        "Invalid tombstones_offset {} (index_offset={}, file_len={})",
        tombstones_offset, index_offset, file_len
      )));
    }

    // Security: Validate section sizes to prevent OOM attacks from malicious snapshots
    let index_section_size = file_len - index_offset;
    if index_section_size > MAX_INDEX_SECTION_SIZE {
      return Err(PersistError::InvalidDirectory(format!(
        "Index section too large: {} bytes (max {})",
        index_section_size, MAX_INDEX_SECTION_SIZE
      )));
    }

    let tombstone_section_size = index_offset - tombstones_offset;
    if tombstone_section_size > MAX_TOMBSTONE_SECTION_SIZE {
      return Err(PersistError::InvalidDirectory(format!(
        "Tombstone section too large: {} bytes (max {})",
        tombstone_section_size, MAX_TOMBSTONE_SECTION_SIZE
      )));
    }

    // Load index from end of file
    reader.seek(SeekFrom::Start(index_offset))?;
    let index_entries: Vec<IndexEntry<K>> =
      rmp_serde::from_read(&mut reader).map_err(PersistError::MsgpackDecode)?;

    // Validate header record_count against actual index entries
    if (index_entries.len() as u64) != record_count {
      return Err(PersistError::InvalidDirectory(format!(
        "Header record_count ({}) doesn't match actual index entries ({})",
        record_count,
        index_entries.len()
      )));
    }

    let index: HashMap<K, (u64, u32, u64)> = index_entries
      .into_iter()
      .map(|e| (e.key, (e.offset, e.length, e.highest_version)))
      .collect();

    // Load tombstones
    reader.seek(SeekFrom::Start(tombstones_offset))?;
    let tombstone_map: HashMap<K, TombstoneInfo> =
      rmp_serde::from_read(&mut reader).map_err(PersistError::MsgpackDecode)?;

    // Validate header tombstone_count against actual tombstones
    if (tombstone_map.len() as u64) != tombstone_count {
      return Err(PersistError::InvalidDirectory(format!(
        "Header tombstone_count ({}) doesn't match actual tombstones ({})",
        tombstone_count,
        tombstone_map.len()
      )));
    }

    let mut tombstones = TombstoneStorage::new();
    for (key, info) in tombstone_map {
      tombstones.insert_or_assign(key, info);
    }

    // Store path for on-demand file access (avoids FD exhaustion with many CRDTs)
    Ok(Self {
      snapshot_path: Some(path.clone()),
      index,
      tombstones,
      clock_value,
      _phantom: std::marker::PhantomData,
    })
  }

  /// Load a single record from disk (opens file on-demand)
  fn load_record(&self, key: &K) -> Result<Option<Record<C, V>>, PersistError> {
    let (offset, length, _version) = match self.index.get(key) {
      Some(&(o, l, v)) => (o, l, v),
      None => return Ok(None),
    };

    let path = match &self.snapshot_path {
      Some(p) => p,
      None => return Ok(None),
    };

    // Open file on-demand, seek to record, read, close
    // This avoids keeping file descriptors open indefinitely
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
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

  /// Iterate over keys (without collecting into Vec to save memory)
  fn keys(&self) -> impl Iterator<Item = &K> {
    self.index.keys()
  }

  /// Get keys with changes after a specific version (for efficient sync)
  fn keys_with_changes_since(&self, last_db_version: u64) -> impl Iterator<Item = &K> {
    self
      .index
      .iter()
      .filter(move |(_, (_, _, highest_version))| *highest_version > last_db_version)
      .map(|(k, _)| k)
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
///
/// # Thread Safety
///
/// **`StreamingCRDT` is NOT thread-safe.** All operations require `&mut self`
/// (exclusive access) and must be performed from a single thread or protected
/// by external synchronization (e.g., `Mutex<StreamingCRDT>`).
///
/// File operations:
/// - Files are opened on-demand for each `load_record()` call (no persistent FDs)
/// - Snapshot files are opened, read, and closed within a single operation
/// - WAL writes use a single `BufWriter` that is not thread-safe
///
/// If you need concurrent access from multiple threads:
/// 1. Wrap the `StreamingCRDT` in a `Mutex` or `RwLock`
/// 2. Use separate `StreamingCRDT` instances per thread (not recommended for shared data)
/// 3. Use a message-passing architecture with a single owner thread
///
/// # File Descriptor Usage
///
/// Unlike some database designs, `StreamingCRDT` does NOT keep snapshot files open.
/// Files are opened on-demand for each cold record load and closed immediately.
/// This prevents FD exhaustion when running many `StreamingCRDT` instances.
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

  /// Snapshots that have been successfully uploaded (for safe cleanup)
  uploaded_snapshots: HashSet<PathBuf>,
  /// WAL segments that have been successfully uploaded (for safe cleanup)
  uploaded_wal_segments: HashSet<PathBuf>,
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
      uploaded_snapshots: HashSet::new(),
      uploaded_wal_segments: HashSet::new(),
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
        if filename_str.starts_with("snapshot_indexed_") && filename_str.ends_with(".bin") {
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

  /// Migrate from a regular PersistedCRDT snapshot to indexed format.
  ///
  /// Handles both full and incremental snapshots: loads the latest full snapshot,
  /// applies any incremental snapshots that build on it (matching PersistedCRDT
  /// recovery logic), then converts to indexed format.
  fn migrate_from_regular_snapshot(
    base_dir: &PathBuf,
    node_id: NodeId,
  ) -> MigrationResult<K, C, V> {
    // Use PersistedCRDT's snapshot discovery to find full + incremental snapshots
    let (latest_full, incrementals) =
      super::PersistedCRDT::<K, C, V>::discover_snapshots(base_dir)?;

    let (full_path, full_version) = match latest_full {
      Some(v) => v,
      None => return Ok(None),
    };

    // Load the full snapshot
    let bytes = std::fs::read(&full_path)?;

    // Decompress if needed
    #[cfg(feature = "compression")]
    let bytes = if bytes.len() >= 4 && &bytes[0..4] == b"\x28\xb5\x2f\xfd" {
      zstd::decode_all(&bytes[..]).map_err(PersistError::Compression)?
    } else {
      bytes
    };

    // Parse header with detailed error messages
    if bytes.len() < 4 {
      return Err(PersistError::InvalidDirectory(format!(
        "Snapshot file too small to contain header: {} bytes (path: {})",
        bytes.len(),
        full_path.display()
      )));
    }

    let metadata_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if bytes.len() < 4 + metadata_len {
      return Err(PersistError::InvalidDirectory(format!(
        "Snapshot truncated: header says {} bytes metadata but file only has {} bytes after header (path: {})",
        metadata_len,
        bytes.len().saturating_sub(4),
        full_path.display()
      )));
    }

    let crdt_bytes = &bytes[4 + metadata_len..];
    let mut crdt: CRDT<K, C, V> =
      CRDT::from_msgpack_bytes(crdt_bytes).map_err(PersistError::MsgpackDecode)?;

    // Apply incremental snapshots in order (matching PersistedCRDT recovery logic).
    // Without this, records stored only in incrementals would be silently lost,
    // since the WAL only contains changes since the last incremental snapshot.
    for (incr_path, _) in incrementals {
      let incr_bytes = std::fs::read(&incr_path)?;
      let incr_decompressed = {
        #[cfg(feature = "compression")]
        {
          if incr_bytes.len() >= 4 && &incr_bytes[0..4] == b"\x28\xb5\x2f\xfd" {
            zstd::decode_all(&incr_bytes[..]).map_err(PersistError::Compression)?
          } else {
            incr_bytes
          }
        }
        #[cfg(not(feature = "compression"))]
        {
          incr_bytes
        }
      };

      let incremental: super::IncrementalSnapshot<K, C, V> =
        rmp_serde::from_slice(&incr_decompressed)?;

      // Convert incremental records to changes and apply
      let mut changes = Vec::new();
      for (key, record) in incremental.changed_records {
        for (col_name, value) in record.fields {
          if let Some(col_version) = record.column_versions.get(&col_name) {
            changes.push(Change {
              record_id: key.clone(),
              col_name: Some(col_name),
              value: Some(value),
              col_version: col_version.col_version,
              db_version: col_version.db_version,
              node_id: col_version.node_id,
              local_db_version: col_version.local_db_version,
              flags: 0,
            });
          }
        }
      }

      // Add tombstone changes
      for (key, tombstone) in incremental.new_tombstones {
        changes.push(Change {
          record_id: key,
          col_name: None,
          value: None,
          col_version: u64::MAX, // TOMBSTONE_COL_VERSION
          db_version: tombstone.db_version,
          node_id: tombstone.node_id,
          local_db_version: tombstone.local_db_version,
          flags: 0,
        });
      }

      crdt.merge_changes(changes, &DefaultMergeRule);
    }

    let clock_value = crdt.get_clock().current_time();
    let version = full_version;

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
    let clock_time = crdt.get_clock().current_time();

    for (key, record) in crdt.get_data().iter() {
      // Clock consistency: record versions should not exceed CRDT clock
      debug_assert!(
        record.highest_local_db_version <= clock_time,
        "Record has version {} but CRDT clock is {} - clock inconsistency",
        record.highest_local_db_version,
        clock_time
      );

      let offset = file.stream_position()?;
      let record_bytes = rmp_serde::to_vec(record).map_err(PersistError::MsgpackEncode)?;

      // Validate record size fits in u32 (index uses u32 for length)
      if record_bytes.len() > u32::MAX as usize {
        return Err(PersistError::InvalidDirectory(format!(
          "Record too large: {} bytes (max {} bytes)",
          record_bytes.len(),
          u32::MAX
        )));
      }

      file.write_all(&record_bytes)?;

      index_entries.push(IndexEntry {
        key: key.clone(),
        offset,
        length: record_bytes.len() as u32,
        highest_version: record.highest_local_db_version,
      });
    }

    // Write tombstones section
    let tombstones_offset = file.stream_position()?;
    let tombstone_map: HashMap<K, TombstoneInfo> = crdt
      .get_tombstones()
      .iter()
      .map(|(k, v)| (k.clone(), *v))
      .collect();
    let tombstone_bytes = rmp_serde::to_vec(&tombstone_map).map_err(PersistError::MsgpackEncode)?;
    file.write_all(&tombstone_bytes)?;

    // Write index section
    let index_offset = file.stream_position()?;
    let index_bytes = rmp_serde::to_vec(&index_entries).map_err(PersistError::MsgpackEncode)?;
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

  /// Create a streaming snapshot without loading all records into memory.
  /// Returns the final clock value for the snapshot.
  fn create_streaming_snapshot(&mut self, path: &PathBuf) -> Result<u64, PersistError> {
    let temp_path = path.with_extension("bin.tmp");
    let mut file = File::create(&temp_path)?;

    // Write magic and version
    file.write_all(SNAPSHOT_MAGIC)?;
    file.write_all(&[SNAPSHOT_VERSION, 0])?;

    // Placeholder for header
    let header_pos = file.stream_position()?;
    let placeholder = vec![0u8; HEADER_SIZE];
    file.write_all(&placeholder)?;

    // Track highest clock value seen
    let mut max_clock = self.hot.get_clock().current_time();

    // Build index as we write
    let mut index_entries: Vec<IndexEntry<K>> = Vec::new();

    // 1. Stream cold records that are NOT superseded by hot tier
    //    We iterate directly and re-check filters at write time for consistency
    //    (avoids TOCTOU issues and is clearer about intent)
    for key in self.cold.keys().cloned().collect::<Vec<_>>() {
      // Re-check filter at write time (ensures consistency)
      if self.hot_keys.contains(&key) || self.hot_tombstones.contains(&key) {
        continue;
      }

      // Load and write individual record (only one in memory at a time)
      if let Some(record) = self.cold.load_record(&key)? {
        // Clock consistency: record versions should not exceed our clock
        debug_assert!(
          record.highest_local_db_version <= max_clock,
          "Cold record has version {} but max_clock is {} - clock inconsistency",
          record.highest_local_db_version,
          max_clock
        );
        max_clock = max_clock.max(record.highest_local_db_version);

        let offset = file.stream_position()?;
        let record_bytes = rmp_serde::to_vec(&record).map_err(PersistError::MsgpackEncode)?;

        // Validate record size fits in u32 (index uses u32 for length)
        if record_bytes.len() > u32::MAX as usize {
          return Err(PersistError::InvalidDirectory(format!(
            "Record too large: {} bytes (max {} bytes)",
            record_bytes.len(),
            u32::MAX
          )));
        }

        file.write_all(&record_bytes)?;

        index_entries.push(IndexEntry {
          key,
          offset,
          length: record_bytes.len() as u32,
          highest_version: record.highest_local_db_version,
        });
      }
    }

    // 2. Write hot tier records (already in memory, no extra load)
    for (key, record) in self.hot.get_data().iter() {
      // Clock consistency: hot record versions should not exceed our clock
      debug_assert!(
        record.highest_local_db_version <= max_clock,
        "Hot record has version {} but max_clock is {} - clock inconsistency",
        record.highest_local_db_version,
        max_clock
      );
      max_clock = max_clock.max(record.highest_local_db_version);

      let offset = file.stream_position()?;
      let record_bytes = rmp_serde::to_vec(record).map_err(PersistError::MsgpackEncode)?;

      // Validate record size fits in u32 (index uses u32 for length)
      if record_bytes.len() > u32::MAX as usize {
        return Err(PersistError::InvalidDirectory(format!(
          "Record too large: {} bytes (max {} bytes)",
          record_bytes.len(),
          u32::MAX
        )));
      }

      file.write_all(&record_bytes)?;

      index_entries.push(IndexEntry {
        key: key.clone(),
        offset,
        length: record_bytes.len() as u32,
        highest_version: record.highest_local_db_version,
      });
    }

    // 3. Merge tombstones from cold and hot
    let mut tombstone_map: HashMap<K, TombstoneInfo> = HashMap::new();

    // Add cold tombstones not superseded by hot
    // A cold tombstone should be included unless the hot tier has explicitly
    // handled this key (either as a live record or as its own tombstone).
    // Checking hot_keys alone is insufficient: if a key is in hot_keys but
    // has no live record and no hot tombstone, the cold tombstone would be
    // lost, causing zombie record resurrection on next recovery.
    for (key, info) in self.cold.tombstones.iter() {
      if !self.hot_keys.contains(key) || !self.hot_tombstones.contains(key) {
        // If key is not in hot at all, preserve cold tombstone.
        // If key is in hot_keys but also in hot_tombstones, the hot tombstone
        // will be added below (and may supersede this one via HashMap insert).
        if !self.hot_keys.contains(key) {
          tombstone_map.insert(key.clone(), *info);
        } else if !self.hot_tombstones.contains(key) {
          // Key is in hot_keys but has no hot tombstone and no hot record —
          // this is an inconsistent state. Preserve the cold tombstone to
          // prevent zombie records.
          if self.hot.get_record(key).is_none() {
            tombstone_map.insert(key.clone(), *info);
          }
        }
      }
    }

    // Add hot tombstones
    for key in &self.hot_tombstones {
      if let Some(info) = self.hot.get_tombstones().find(key) {
        tombstone_map.insert(key.clone(), info);
      }
    }

    // Write tombstones section
    let tombstones_offset = file.stream_position()?;
    let tombstone_bytes = rmp_serde::to_vec(&tombstone_map).map_err(PersistError::MsgpackEncode)?;
    file.write_all(&tombstone_bytes)?;

    // Write index section
    let index_offset = file.stream_position()?;
    let index_bytes = rmp_serde::to_vec(&index_entries).map_err(PersistError::MsgpackEncode)?;
    file.write_all(&index_bytes)?;

    // Write header
    file.seek(SeekFrom::Start(header_pos))?;
    file.write_all(&self.hot.node_id().to_le_bytes())?;
    file.write_all(&max_clock.to_le_bytes())?;
    file.write_all(&(index_entries.len() as u64).to_le_bytes())?;
    file.write_all(&(tombstone_map.len() as u64).to_le_bytes())?;
    file.write_all(&index_offset.to_le_bytes())?;
    file.write_all(&tombstones_offset.to_le_bytes())?;

    file.sync_all()?;
    drop(file);

    // Atomic rename
    std::fs::rename(&temp_path, path)?;

    Ok(max_clock)
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

      // Filter out changes for records that are tombstoned in cold storage.
      // Without this, WAL entries predating the current snapshot could resurrect
      // records that were deleted after the WAL was written (zombie resurrection).
      // This happens when auto_cleanup_wal=false (WAL hooks registered) and old
      // WAL segments survive across snapshot cycles.
      let changes: Vec<Change<K, C, V>> = changes
        .into_iter()
        .filter(|c| !self.cold.is_tombstoned(&c.record_id))
        .collect();

      // Merge first, then track only ACCEPTED changes in hot_keys/hot_tombstones.
      // Tracking before merge would pollute hot_keys with rejected changes, causing
      // create_streaming_snapshot to skip valid cold records (silent data loss).
      let accepted = self.hot.merge_changes(changes, &DefaultMergeRule);
      for change in &accepted {
        self.hot_keys.insert(change.record_id.clone());
        if change.col_name.is_none() {
          self.hot_tombstones.insert(change.record_id.clone());
        }
      }
      self.changes_since_snapshot += accepted.len();
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
      // Key is tracked in hot but has no live record — either it's tombstoned
      // or in an inconsistent state. Either way, don't fall through to cold.
      return Ok(None);
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

  /// Check if a record exists (without loading full record from disk).
  ///
  /// This is a fast membership check that:
  /// - Returns `false` if the record is tombstoned (deleted)
  /// - Returns `true` if the key exists in hot tier, cache, or cold storage index
  ///
  /// Unlike `get_record()`, this does NOT load the full record from disk,
  /// making it efficient for existence checks on large datasets.
  pub fn contains_key(&self, key: &K) -> bool {
    if self.hot_tombstones.contains(key) || self.cold.is_tombstoned(key) {
      return false;
    }
    self.hot_keys.contains(key) || self.cache.contains_key(key) || self.cold.contains_key(key)
  }

  /// Check if a record has been deleted (tombstoned).
  ///
  /// Returns `true` if the record was deleted in either hot or cold tier.
  /// Tombstoned records cannot be updated until compacted.
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
        self
          .hot
          .insert_record_direct(record_id.clone(), cold_record);
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
  ///
  /// This correctly handles records that exist only in cold storage by loading
  /// them into the hot tier first before creating the tombstone.
  pub fn delete_record(&mut self, record_id: &K) -> Result<Option<Change<K, C, V>>, PersistError> {
    if self.is_tombstoned(record_id) {
      return Ok(None);
    }

    // If record exists in cold but not hot, load it first so delete_record works
    // Without this, deleting a cold-only record would silently fail (no tombstone created)
    if !self.hot_keys.contains(record_id) && self.cold.contains_key(record_id) {
      if let Some(cold_record) = self.cold.load_record(record_id)? {
        self
          .hot
          .insert_record_direct(record_id.clone(), cold_record);
        self.hot_keys.insert(record_id.clone());
      }
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
    // Load any cold records that are being updated (skip tombstoned records
    // to prevent zombie resurrection — a field update for a deleted record
    // must not bring it back to life)
    for change in &changes {
      if !self.hot_keys.contains(&change.record_id)
        && !self.is_tombstoned(&change.record_id)
        && self.cold.contains_key(&change.record_id)
      {
        if let Some(cold_record) = self.cold.load_record(&change.record_id)? {
          // Directly insert cold record into hot storage (preserves version metadata)
          self
            .hot
            .insert_record_direct(change.record_id.clone(), cold_record);
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
  ///
  /// # Performance
  ///
  /// Uses an indexed lookup to find cold records with changes after `last_db_version`,
  /// avoiding a full scan of all records. Only records that actually have changes
  /// after the specified version are loaded from disk.
  ///
  /// For optimal performance, call `get_changes_since` with the highest version
  /// you've already synced to minimize the number of records loaded.
  pub fn get_changes_since(
    &mut self,
    last_db_version: u64,
  ) -> Result<Vec<Change<K, C, V>>, PersistError> {
    // Hot tier changes
    let mut changes = self.hot.get_changes_since(last_db_version);

    // Use indexed lookup for cold records with changes after last_db_version
    // This avoids scanning ALL cold records - only those with matching versions
    let cold_keys_to_check: Vec<K> = self
      .cold
      .keys_with_changes_since(last_db_version)
      .filter(|k| !self.hot_keys.contains(*k) && !self.hot_tombstones.contains(*k))
      .cloned()
      .collect();

    for key in cold_keys_to_check {
      if let Some(record) = self.cold.load_record(&key)? {
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

    // Include cold tombstones if they're after last_db_version
    // Skip if hot has already tombstoned or touched this key
    for (key, info) in self.cold.tombstones.iter() {
      if info.local_db_version > last_db_version
        && !self.hot_keys.contains(key)
        && !self.hot_tombstones.contains(key)
      {
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
  ///
  /// This streams records directly to the snapshot file without loading all
  /// cold records into memory, making it suitable for large datasets.
  pub fn create_indexed_snapshot(&mut self) -> Result<PathBuf, PersistError> {
    self.snapshot_version += 1;
    let snapshot_path = self
      .base_dir
      .join(format!("snapshot_indexed_{:06}.bin", self.snapshot_version));

    // Stream snapshot directly to file (avoids memory explosion)
    let final_clock_value = self.create_streaming_snapshot(&snapshot_path)?;

    // Reload cold storage from new snapshot
    self.cold = ColdStorage::from_file(&snapshot_path)?;

    // Clear hot tier with final clock value
    self.hot = CRDT::new(self.hot.node_id(), None);
    self.hot.get_clock_mut().set_time(final_clock_value);
    self.hot_keys.clear();
    self.hot_tombstones.clear();
    self.cache = LruCache::new(self.config.cache_capacity);

    // Rotate WAL (creates new segment, fires hooks on sealed segments)
    let _ = self.wal.rotate(&self.base_dir, &self.wal_segment_hooks)?;

    // Auto-cleanup old WAL segments that are now captured in the snapshot.
    // This is necessary for correctness - without it, WAL replay would re-apply
    // changes that are already in the snapshot, causing hot_keys to be polluted.
    //
    // When WAL segment hooks are registered, auto-cleanup is disabled by default
    // to prevent deleting WAL files before async upload hooks complete.
    // Override with auto_cleanup_wal: Some(true/false) in config.
    let auto_cleanup = self
      .config
      .persist_config
      .auto_cleanup_wal
      .unwrap_or(self.wal_segment_hooks.is_empty());
    if auto_cleanup {
      self.cleanup_old_wal_segments_internal()?;
    }

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

  /// Compact tombstones older than the specified version.
  ///
  /// **CRITICAL**: Only call this after ALL nodes have acknowledged the version.
  /// Premature compaction can cause deleted records to reappear (zombie records).
  ///
  /// This compacts tombstones in both hot and cold tiers, creates a new snapshot
  /// capturing the compacted state, and unconditionally deletes old WAL segments.
  ///
  /// # Arguments
  ///
  /// * `min_acknowledged_version` - The minimum version acknowledged by all nodes.
  ///   Tombstones with `db_version < min_acknowledged_version` will be removed.
  ///
  /// # Returns
  ///
  /// The number of tombstones removed.
  pub fn compact_tombstones(
    &mut self,
    min_acknowledged_version: u64,
  ) -> Result<usize, PersistError> {
    // Compact hot tier tombstones
    let hot_compacted = self.hot.compact_tombstones(min_acknowledged_version);

    // Remove from hot_tombstones AND hot_keys tracking sets for compacted tombstones.
    // Removing from hot_keys is critical: if a key stays in hot_keys after its hot
    // tombstone is compacted, create_streaming_snapshot will skip the cold tombstone
    // for that key (assuming hot supersedes it), causing the tombstone to vanish
    // entirely from the snapshot — leading to zombie record resurrection.
    let keys_to_remove: Vec<K> = self
      .hot_tombstones
      .iter()
      .filter(|k| self.hot.get_tombstones().find(*k).is_none())
      .cloned()
      .collect();

    for key in &keys_to_remove {
      self.hot_tombstones.remove(key);
      self.hot_keys.remove(key);
    }

    // Compact cold tombstones in-memory BEFORE creating snapshot,
    // so the snapshot captures the compacted state.
    let cold_compacted = self.cold.tombstones.compact(min_acknowledged_version);

    let total = hot_compacted + cold_compacted;

    // Always create snapshot and clean up WAL unconditionally, matching
    // PersistedCRDT::compact_tombstones behavior. Even when total==0, this
    // ensures consistent state and WAL cleanup.
    self.create_indexed_snapshot()?;

    // CRITICAL: Delete old WAL segments that contain tombstone records.
    // Failure to do this causes zombie records to reappear on recovery.
    // Use internal cleanup to unconditionally delete all old segments
    // regardless of auto_cleanup_wal config.
    self.cleanup_old_wal_segments_internal()?;

    Ok(total)
  }

  /// Add a post-operation hook.
  ///
  /// The hook is called after each operation is written to WAL (and flushed).
  /// Use for real-time broadcasting of changes to other nodes.
  ///
  /// Note: Hook fires AFTER WAL flush but BEFORE fsync. Changes are in OS
  /// page cache, safe from process crashes but not power failures.
  pub fn add_post_hook(&mut self, hook: Box<dyn PostOpHook<K, C, V>>) {
    self.post_hooks.push(hook);
  }

  /// Add a snapshot hook.
  ///
  /// The hook is called after a snapshot has been created and fsynced.
  /// Use for uploading snapshots to cloud storage (S3, R2, etc.).
  ///
  /// The hook receives the snapshot path and the db_version (logical clock value).
  pub fn add_snapshot_hook(&mut self, hook: Box<dyn SnapshotHook>) {
    self.snapshot_hooks.push(hook);
  }

  /// Add a WAL segment hook.
  ///
  /// The hook is called when a WAL segment is sealed (rotated).
  /// Use for archiving WAL segments to cloud storage.
  ///
  /// Note: Sealed segments are NOT auto-deleted. Call `cleanup_old_wal_segments()`
  /// after confirming successful upload.
  pub fn add_wal_segment_hook(&mut self, hook: Box<dyn WalSegmentHook>) {
    self.wal_segment_hooks.push(hook);
  }

  /// Get number of records currently in the hot tier.
  ///
  /// The hot tier contains recently modified records that haven't been
  /// flushed to a snapshot yet. This count includes tombstoned records.
  ///
  /// Note: A record may exist in both hot and cold tiers (hot supersedes cold).
  pub fn hot_record_count(&self) -> usize {
    self.hot_keys.len()
  }

  /// Get number of records in cold storage (the indexed snapshot).
  ///
  /// This is the number of records persisted in the last snapshot.
  /// Does not include records added after the snapshot.
  pub fn cold_record_count(&self) -> usize {
    self.cold.len()
  }

  /// Get total number of active (non-tombstoned) records.
  ///
  /// This computes the unique active record count across hot and cold tiers:
  /// Note: Records in hot tier that supersede cold records are counted once.
  /// Tombstoned records are not counted.
  pub fn total_record_count(&self) -> usize {
    // Cold records not in hot and not tombstoned
    let cold_only = self
      .cold
      .index
      .keys()
      .filter(|k| !self.hot_keys.contains(*k) && !self.hot_tombstones.contains(*k))
      .filter(|k| !self.cold.is_tombstoned(*k))
      .count();
    // Hot records minus hot tombstones
    let hot_active = self
      .hot_keys
      .len()
      .saturating_sub(self.hot_tombstones.len());
    cold_only + hot_active
  }

  /// Get current logical clock value
  pub fn get_clock_time(&self) -> u64 {
    self.hot.get_clock().current_time()
  }

  /// Returns the node ID of this CRDT instance.
  pub fn node_id(&self) -> NodeId {
    self.hot.node_id()
  }

  /// Explicitly create a snapshot of the current state.
  ///
  /// This is an alias for `create_indexed_snapshot()` that matches the
  /// `PersistedCRDT::snapshot()` API for consistency.
  pub fn snapshot(&mut self) -> Result<PathBuf, PersistError> {
    self.create_indexed_snapshot()
  }

  /// Get the current number of changes since the last snapshot.
  pub fn changes_since_snapshot(&self) -> usize {
    self.changes_since_snapshot
  }

  /// Peek at the current batch without consuming it.
  pub fn peek_batch(&self) -> &[Change<K, C, V>] {
    &self.batch
  }

  /// Delete a specific field from a record.
  ///
  /// If the record only exists in cold storage, it is loaded into the hot
  /// tier first. Returns `None` if the record or field doesn't exist, or
  /// if the record is tombstoned.
  pub fn delete_field(
    &mut self,
    record_id: &K,
    field_name: &C,
  ) -> Result<Option<Change<K, C, V>>, PersistError> {
    if self.is_tombstoned(record_id) {
      return Ok(None);
    }

    // Load cold record into hot if needed
    if !self.hot_keys.contains(record_id) && self.cold.contains_key(record_id) {
      if let Some(cold_record) = self.cold.load_record(record_id)? {
        self
          .hot
          .insert_record_direct(record_id.clone(), cold_record);
        self.hot_keys.insert(record_id.clone());
      }
    }

    let change = self.hot.delete_field(record_id, field_name);
    if let Some(ref c) = change {
      self.hot_keys.insert(record_id.clone());
      self.persist_and_notify(std::slice::from_ref(c))?;
    }

    Ok(change)
  }

  /// Get changes since a version, excluding changes from specific nodes.
  ///
  /// Useful for syncing with peers — exclude the peer's own node ID to avoid
  /// echoing their changes back to them.
  pub fn get_changes_since_excluding(
    &mut self,
    last_db_version: u64,
    excluding: &HashSet<NodeId>,
  ) -> Result<Vec<Change<K, C, V>>, PersistError> {
    let mut changes = self.get_changes_since(last_db_version)?;
    changes.retain(|c| !excluding.contains(&c.node_id));
    Ok(changes)
  }

  /// Mark a snapshot as successfully uploaded to cloud storage.
  ///
  /// Call this after your snapshot hook has successfully uploaded the file.
  /// This enables safe cleanup with `cleanup_old_snapshots(keep, true)`.
  pub fn mark_snapshot_uploaded(&mut self, path: PathBuf) {
    self.uploaded_snapshots.insert(path);
  }

  /// Mark a WAL segment as successfully uploaded to cloud storage.
  ///
  /// Call this after your WAL segment hook has successfully uploaded the file.
  /// This enables safe cleanup with `cleanup_old_wal_segments(keep, true)`.
  pub fn mark_wal_segment_uploaded(&mut self, path: PathBuf) {
    self.uploaded_wal_segments.insert(path);
  }

  /// Cleanup old snapshots, keeping the specified number of most recent ones.
  ///
  /// # Arguments
  ///
  /// * `keep` - Number of most recent snapshots to keep
  /// * `require_uploaded` - If true, only delete snapshots marked as uploaded via
  ///   `mark_snapshot_uploaded()`. This prevents data loss if upload hooks fail.
  ///
  /// # Returns
  ///
  /// Number of snapshots deleted.
  pub fn cleanup_old_snapshots(
    &mut self,
    keep: usize,
    require_uploaded: bool,
  ) -> Result<usize, PersistError> {
    let mut snapshots: Vec<(u64, PathBuf)> = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&self.base_dir) {
      for entry in entries.flatten() {
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();

        if filename_str.starts_with("snapshot_indexed_") && filename_str.ends_with(".bin") {
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

    // Sort by version (ascending) - oldest first
    snapshots.sort_by_key(|(v, _)| *v);

    let mut deleted = 0;
    let delete_count = snapshots.len().saturating_sub(keep);

    for (_, path) in snapshots.into_iter().take(delete_count) {
      if require_uploaded && !self.uploaded_snapshots.contains(&path) {
        continue; // Skip - not uploaded yet
      }
      if std::fs::remove_file(&path).is_ok() {
        self.uploaded_snapshots.remove(&path);
        deleted += 1;
      }
    }

    Ok(deleted)
  }

  /// Cleanup old WAL segments, keeping the specified number of most recent ones.
  ///
  /// # Arguments
  ///
  /// * `keep` - Number of most recent WAL segments to keep (plus current segment)
  /// * `require_uploaded` - If true, only delete segments marked as uploaded via
  ///   `mark_wal_segment_uploaded()`. This prevents data loss if upload hooks fail.
  ///
  /// # Returns
  ///
  /// Number of WAL segments deleted.
  pub fn cleanup_old_wal_segments(
    &mut self,
    keep: usize,
    require_uploaded: bool,
  ) -> Result<usize, PersistError> {
    let current_segment = self.wal.current_segment();
    let mut segments: Vec<(u64, PathBuf)> = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&self.base_dir) {
      for entry in entries.flatten() {
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();

        if filename_str.starts_with("wal_") && filename_str.ends_with(".bin") {
          if let Some(num_str) = filename_str
            .strip_prefix("wal_")
            .and_then(|s| s.strip_suffix(".bin"))
          {
            if let Ok(segment_num) = num_str.parse::<u64>() {
              // Never delete current segment
              if segment_num < current_segment {
                segments.push((segment_num, entry.path()));
              }
            }
          }
        }
      }
    }

    // Sort by segment number (ascending) - oldest first
    segments.sort_by_key(|(n, _)| *n);

    let mut deleted = 0;
    let delete_count = segments.len().saturating_sub(keep);

    for (_, path) in segments.into_iter().take(delete_count) {
      if require_uploaded && !self.uploaded_wal_segments.contains(&path) {
        continue; // Skip - not uploaded yet
      }
      if std::fs::remove_file(&path).is_ok() {
        self.uploaded_wal_segments.remove(&path);
        deleted += 1;
      }
    }

    Ok(deleted)
  }

  /// Internal WAL cleanup (unconditional, no upload tracking)
  fn cleanup_old_wal_segments_internal(&self) -> Result<(), PersistError> {
    let current_segment = self.wal.current_segment();

    let entries = std::fs::read_dir(&self.base_dir)?;
    for entry in entries.flatten() {
      let filename = entry.file_name();
      let filename_str = filename.to_string_lossy();

      if filename_str.starts_with("wal_") && filename_str.ends_with(".bin") {
        if let Some(num_str) = filename_str
          .strip_prefix("wal_")
          .and_then(|s| s.strip_suffix(".bin"))
        {
          if let Ok(segment_num) = num_str.parse::<u64>() {
            // Keep current segment, delete others
            if segment_num < current_segment {
              std::fs::remove_file(entry.path())?;
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

    // Append to WAL (includes internal flush)
    self.wal.append(changes)?;
    self.changes_since_snapshot += changes.len();

    // Explicit flush before hooks to guarantee changes are in OS page cache
    // This ensures hooks (which may broadcast to network) see written data
    self.wal.flush()?;

    // Add to batch
    self.batch.extend_from_slice(changes);

    // Auto-flush batch if exceeds max size (prevents OOM)
    // Keeps only the most recent half to preserve some data for late consumers
    if let Some(max_size) = self.config.persist_config.max_batch_size {
      if self.batch.len() >= max_size {
        let keep_from = self.batch.len() / 2;
        self.batch.drain(..keep_from);
      }
    }

    // Call post hooks (WAL is flushed, safe to broadcast)
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
    assert_eq!(record.unwrap().fields.get("field1").unwrap(), "value1");

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
        crdt
          .insert_or_update(
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
      assert_eq!(record.unwrap().fields.get("field50").unwrap(), "value50");
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

      crdt
        .insert_or_update(
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
      crdt
        .insert_or_update(
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

      crdt
        .insert_or_update(
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
      assert!(
        no_changes.is_empty(),
        "Should not find changes in the future"
      );
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
        crdt
          .insert_or_update(
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

  /// Test that deleting a record that only exists in cold storage works correctly.
  /// This was a critical bug where cold-only records couldn't be deleted.
  #[test]
  fn test_delete_cold_record() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    // Create record and snapshot (moves to cold)
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      crdt
        .insert_or_update(
          &"cold_key".to_string(),
          vec![("field".to_string(), "value".to_string())],
        )
        .unwrap();

      crdt.create_indexed_snapshot().unwrap();
    }

    // Reopen (record is now cold-only) and delete
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      // Verify record exists in cold storage
      assert_eq!(crdt.cold_record_count(), 1);
      assert_eq!(crdt.hot_record_count(), 0);
      assert!(crdt.contains_key(&"cold_key".to_string()));

      // Delete the cold-only record
      let delete_change = crdt.delete_record(&"cold_key".to_string()).unwrap();
      assert!(
        delete_change.is_some(),
        "Delete should return a change for cold-only record"
      );

      // Verify tombstone exists and record is gone
      assert!(crdt.is_tombstoned(&"cold_key".to_string()));
      assert!(!crdt.contains_key(&"cold_key".to_string()));
      let record = crdt.get_record(&"cold_key".to_string()).unwrap();
      assert!(record.is_none(), "Deleted record should not be retrievable");
    }

    // Reopen and verify record doesn't reappear (zombie prevention)
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      // Record should still be tombstoned after WAL replay
      assert!(
        crdt.is_tombstoned(&"cold_key".to_string()),
        "Tombstone should persist after reopen"
      );
      let record = crdt.get_record(&"cold_key".to_string()).unwrap();
      assert!(
        record.is_none(),
        "Deleted record should not reappear after reopen"
      );
    }
  }

  /// Test that clock doesn't jump when loading cold records from remote nodes.
  /// This was a bug where db_version was used instead of local_db_version.
  #[test]
  fn test_clock_causality_on_cold_load() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    // Create a CRDT and manually inject a record with high db_version
    // (simulating a record synced from a node with higher clock)
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      // Insert a few records to advance local clock
      for i in 0..5 {
        crdt
          .insert_or_update(
            &format!("key{}", i),
            vec![("field".to_string(), format!("value{}", i))],
          )
          .unwrap();
      }

      let clock_after_inserts = crdt.get_clock_time();
      assert!(
        clock_after_inserts > 0 && clock_after_inserts <= 10,
        "Clock should be reasonable after 5 inserts"
      );

      crdt.create_indexed_snapshot().unwrap();
    }

    // Reopen and update a cold record
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      let clock_after_reopen = crdt.get_clock_time();

      // Update a cold record (triggers insert_record_direct)
      crdt
        .insert_or_update(
          &"key0".to_string(),
          vec![("field".to_string(), "updated".to_string())],
        )
        .unwrap();

      let clock_after_update = crdt.get_clock_time();

      // Clock should have advanced by a small amount (1-2), not jumped wildly
      let clock_diff = clock_after_update - clock_after_reopen;
      assert!(
        clock_diff <= 5,
        "Clock should not jump excessively: before={} after={} diff={}",
        clock_after_reopen,
        clock_after_update,
        clock_diff
      );
    }
  }

  /// Test upload tracking and cleanup methods
  #[test]
  fn test_upload_tracking_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    let mut crdt = StreamingCRDT::<String, String, String>::open(
      base_dir.clone(),
      1,
      StreamingConfig::default(),
    )
    .unwrap();

    // Create some data and snapshots
    for i in 0..3 {
      crdt
        .insert_or_update(
          &format!("key{}", i),
          vec![("field".to_string(), format!("value{}", i))],
        )
        .unwrap();
      crdt.create_indexed_snapshot().unwrap();
    }

    // Should have 3 snapshots
    let snapshot_count = std::fs::read_dir(&base_dir)
      .unwrap()
      .filter(|e| {
        e.as_ref()
          .unwrap()
          .file_name()
          .to_string_lossy()
          .starts_with("snapshot_indexed_")
      })
      .count();
    assert_eq!(snapshot_count, 3);

    // Cleanup without upload tracking (require_uploaded=false)
    let deleted = crdt.cleanup_old_snapshots(1, false).unwrap();
    assert_eq!(deleted, 2, "Should delete 2 old snapshots");

    // Only 1 snapshot should remain
    let remaining = std::fs::read_dir(&base_dir)
      .unwrap()
      .filter(|e| {
        e.as_ref()
          .unwrap()
          .file_name()
          .to_string_lossy()
          .starts_with("snapshot_indexed_")
      })
      .count();
    assert_eq!(remaining, 1);
  }

  /// Test that LRU cache correctly handles updates to existing keys.
  /// Previously, updating an existing key didn't mark it as recently used,
  /// causing frequently-updated records to be evicted.
  #[test]
  fn test_lru_cache_update_marks_recent() {
    let mut cache: LruCache<i32, String> = LruCache::new(3);

    cache.insert(1, "one".to_string());
    cache.insert(2, "two".to_string());
    cache.insert(3, "three".to_string());

    // Update key 1 (should mark it as recently used)
    cache.insert(1, "one_updated".to_string());

    // Insert 4 — should evict 2 (oldest untouched), not 1 (recently updated)
    cache.insert(4, "four".to_string());

    assert!(cache.contains_key(&1), "Updated key should not be evicted");
    assert_eq!(cache.get(&1).unwrap(), "one_updated");
    assert!(
      !cache.contains_key(&2),
      "Oldest untouched key should be evicted"
    );
    assert!(cache.contains_key(&3));
    assert!(cache.contains_key(&4));
  }

  /// Test that merge_changes does not resurrect tombstoned cold records.
  #[test]
  fn test_merge_changes_no_zombie_resurrection() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    // Create a record, snapshot it (goes cold), then delete it
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      crdt
        .insert_or_update(
          &"victim".to_string(),
          vec![("field".to_string(), "value".to_string())],
        )
        .unwrap();

      crdt.create_indexed_snapshot().unwrap();
    }

    // Reopen: record is cold. Delete it, then try to merge a field update for it.
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      // Delete the cold record
      crdt.delete_record(&"victim".to_string()).unwrap();
      assert!(crdt.is_tombstoned(&"victim".to_string()));

      // Simulate a remote node sending a field update for the deleted record
      let remote_change = Change::new(
        "victim".to_string(),
        Some("field".to_string()),
        Some("resurrected".to_string()),
        1,  // col_version
        50, // db_version (high, from remote)
        99, // node_id (different node)
        50, // local_db_version
        0,
      );

      let accepted = crdt.merge_changes(vec![remote_change]).unwrap();
      // The change may or may not be accepted by the merge rule,
      // but the record should remain tombstoned
      let _ = accepted;

      // Record must still be gone
      let record = crdt.get_record(&"victim".to_string()).unwrap();
      assert!(
        record.is_none(),
        "Tombstoned record should not be resurrected by merge"
      );
    }
  }

  /// Test delete_field on cold records
  #[test]
  fn test_delete_field_cold_record() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    // Create record with two fields and snapshot
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      crdt
        .insert_or_update(
          &"key1".to_string(),
          vec![
            ("name".to_string(), "Alice".to_string()),
            ("email".to_string(), "alice@example.com".to_string()),
          ],
        )
        .unwrap();

      crdt.create_indexed_snapshot().unwrap();
    }

    // Reopen and delete one field
    {
      let mut crdt = StreamingCRDT::<String, String, String>::open(
        base_dir.clone(),
        1,
        StreamingConfig::default(),
      )
      .unwrap();

      assert_eq!(crdt.hot_record_count(), 0);

      let change = crdt
        .delete_field(&"key1".to_string(), &"email".to_string())
        .unwrap();
      assert!(
        change.is_some(),
        "Should return a change for field deletion"
      );

      // Record should still exist with remaining field
      let record = crdt.get_record(&"key1".to_string()).unwrap().unwrap();
      assert_eq!(record.fields.get("name").unwrap(), "Alice");
      assert!(
        record.fields.get("email").is_none(),
        "Deleted field should be gone"
      );
    }
  }

  /// Test node_id(), snapshot() alias, changes_since_snapshot(), peek_batch()
  #[test]
  fn test_api_parity_methods() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    let mut crdt = StreamingCRDT::<String, String, String>::open(
      base_dir.clone(),
      42,
      StreamingConfig::default(),
    )
    .unwrap();

    assert_eq!(crdt.node_id(), 42);
    assert_eq!(crdt.changes_since_snapshot(), 0);
    assert!(crdt.peek_batch().is_empty());

    crdt
      .insert_or_update(
        &"key1".to_string(),
        vec![("f".to_string(), "v".to_string())],
      )
      .unwrap();

    assert_eq!(crdt.changes_since_snapshot(), 1);
    assert_eq!(crdt.peek_batch().len(), 1);

    // take_batch clears it
    let batch = crdt.take_batch();
    assert_eq!(batch.len(), 1);
    assert!(crdt.peek_batch().is_empty());

    // snapshot() alias works
    let path = crdt.snapshot().unwrap();
    assert!(path.exists());
    assert_eq!(crdt.changes_since_snapshot(), 0);
  }

  /// Test get_changes_since_excluding
  #[test]
  fn test_get_changes_since_excluding() {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    let mut crdt = StreamingCRDT::<String, String, String>::open(
      base_dir.clone(),
      1,
      StreamingConfig::default(),
    )
    .unwrap();

    crdt
      .insert_or_update(
        &"key1".to_string(),
        vec![("f".to_string(), "v".to_string())],
      )
      .unwrap();

    // Should find changes from node 1
    let all = crdt.get_changes_since(0).unwrap();
    assert!(!all.is_empty());

    // Excluding node 1 should return nothing
    let excluding = HashSet::from([1 as NodeId]);
    let filtered = crdt.get_changes_since_excluding(0, &excluding).unwrap();
    assert!(filtered.is_empty(), "Should exclude node 1's changes");

    // Excluding a different node should return all changes
    let other = HashSet::from([99 as NodeId]);
    let not_filtered = crdt.get_changes_since_excluding(0, &other).unwrap();
    assert_eq!(not_filtered.len(), all.len());
  }
}
