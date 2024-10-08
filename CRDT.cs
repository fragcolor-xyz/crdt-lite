using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

// Namespace for CRDT implementation
namespace ForSync.CRDT {
  /// <summary>
  /// Represents a logical clock for maintaining causality.
  /// </summary>
  public class LogicalClock {
    private ulong _time;

    public LogicalClock() {
      _time = 0;
    }

    /// <summary>
    /// Increments the clock for a local event.
    /// </summary>
    public ulong Tick() {
      _time++;
      return _time;
    }

    /// <summary>
    /// Updates the clock based on a received time.
    /// </summary>
    public ulong Update(ulong receivedTime) {
      _time = Math.Max(_time, receivedTime);
      _time++;
      return _time;
    }

    /// <summary>
    /// Sets the logical clock to a specific time.
    /// </summary>
    public void SetTime(ulong t) {
      _time = t;
    }

    /// <summary>
    /// Retrieves the current time.
    /// </summary>
    public ulong CurrentTime => _time;
  }

  /// <summary>
  /// Represents the version information for a column.
  /// </summary>
  public struct ColumnVersion {
    public ulong ColVersion { get; set; }
    public ulong DbVersion { get; set; }
    public ulong NodeId { get; set; }
    public ulong LocalDbVersion { get; set; }

    public ColumnVersion(ulong c, ulong d, ulong n, ulong ldbVer = 0) {
      ColVersion = c;
      DbVersion = d;
      NodeId = n;
      LocalDbVersion = ldbVer;
    }
  }

  // Add this new struct definition
  public struct ColName<K> {
    public string Name { get; }
    public K? AssociatedId { get; }

    public ColName(string name, K? associatedId = default) {
      Name = name;
      AssociatedId = associatedId;
    }

    public override string ToString() => AssociatedId != null ? $"{Name}:{AssociatedId}" : Name;

    // Implement equality members
    public override bool Equals(object? obj) {
      return obj is ColName<K> other &&
             Name == other.Name &&
             EqualityComparer<K?>.Default.Equals(AssociatedId, other.AssociatedId);
    }

    public override int GetHashCode() {
      return HashCode.Combine(Name, AssociatedId);
    }

    public static bool operator ==(ColName<K> left, ColName<K> right) {
      return left.Equals(right);
    }

    public static bool operator !=(ColName<K> left, ColName<K> right) {
      return !(left == right);
    }
  }

  /// <summary>
  /// Represents a record in the CRDT.
  /// </summary>
  public class Record<K, V> {
    public Dictionary<ColName<K>, V> Fields { get; set; }
    public Dictionary<ColName<K>, ColumnVersion> ColumnVersions { get; set; }

    public Record() {
      Fields = new Dictionary<ColName<K>, V>();
      ColumnVersions = new Dictionary<ColName<K>, ColumnVersion>();
    }

    public Record(Dictionary<ColName<K>, V> fields, Dictionary<ColName<K>, ColumnVersion> columnVersions) {
      Fields = fields;
      ColumnVersions = columnVersions;
    }

    /// <summary>
    /// Compares two Record<V> instances.
    /// </summary>
    public override bool Equals(object obj) {
      if (obj is not Record<K, V> other)
        return false;

      if (Fields.Count != other.Fields.Count)
        return false;

      foreach (var kvp in Fields) {
        if (!other.Fields.TryGetValue(kvp.Key, out var otherValue) || !EqualityComparer<V>.Default.Equals(kvp.Value, otherValue))
          return false;
      }

      // ColumnVersions are not compared as per original C++ code
      return true;
    }

    public override int GetHashCode() {
      // Simplistic hash code, can be improved
      int hash = 17;
      foreach (var kvp in Fields) {
        hash = hash * 23 + kvp.Key.GetHashCode();
        hash = hash * 23 + (kvp.Value?.GetHashCode() ?? 0);
      }
      return hash;
    }
  }

  /// <summary>
  /// Represents a single change in the CRDT.
  /// </summary>
  public class Change<K, V> {
    public K RecordId { get; set; }
    public ColName<K>? ColName { get; set; } // null represents tombstone of the record
    public V? Value { get; set; }       // null represents deletion of the column, not the record
    public ulong ColVersion { get; set; }
    public ulong DbVersion { get; set; }
    public ulong NodeId { get; set; }
    public ulong LocalDbVersion { get; set; }

    public Change() {
      RecordId = default!;
    }

    public Change(K recordId, ColName<K>? colName, V? value, ulong colVersion, ulong dbVersion, ulong nodeId, ulong localDbVersion = 0) {
      RecordId = recordId;
      ColName = colName;
      Value = value;
      ColVersion = colVersion;
      DbVersion = dbVersion;
      NodeId = nodeId;
      LocalDbVersion = localDbVersion;
    }
  }

  /// <summary>
  /// Comparator for sorting Changes.
  /// </summary>
  public class ChangeComparer<K, V> : IComparer<Change<K, V>> {
    public int Compare(Change<K, V>? a, Change<K, V>? b) {
      if (a == null || b == null)
        throw new ArgumentException("Cannot compare null Changes.");

      int recordCompare = Comparer<K>.Default.Compare(a.RecordId, b.RecordId);
      if (recordCompare != 0)
        return recordCompare;

      bool aHasCol = a.ColName != null;
      bool bHasCol = b.ColName != null;

      if (aHasCol != bHasCol)
        return bHasCol ? -1 : 1; // Deletions (null) come last

      if (a.ColName != b.ColName) {
        if (a.ColName == null) return -1;
        if (b.ColName == null) return 1;
        return string.Compare(a.ColName.Value.ToString(), b.ColName.Value.ToString(), StringComparison.Ordinal);
      }

      if (a.ColVersion != b.ColVersion)
        return a.ColVersion > b.ColVersion ? -1 : 1;

      if (a.DbVersion != b.DbVersion)
        return a.DbVersion > b.DbVersion ? -1 : 1;

      if (a.NodeId != b.NodeId)
        return a.NodeId > b.NodeId ? -1 : 1;

      return 0; // Consider equal if all fields match
    }
  }

  /// <summary>
  /// Represents the CRDT structure, generic over key (K) and value (V) types.
  /// </summary>
  public class CRDT<K, V> {
    private ulong _nodeId;
    private LogicalClock _clock;
    private Dictionary<K, Record<K, V>> _data;
    private HashSet<K> _tombstones;

    private CRDT<K, V>? _parent;
    private ulong _baseVersion;

    public ulong NodeId => _nodeId;
    public ulong Clock => _clock.CurrentTime;
    public ulong BaseVersion => _baseVersion;

    public CRDT(ulong nodeId, CRDT<K, V>? parent = null) {
      _nodeId = nodeId;
      _clock = new LogicalClock();
      _data = new Dictionary<K, Record<K, V>>();
      _tombstones = new HashSet<K>();
      _parent = parent;
      _baseVersion = parent != null ? parent._clock.CurrentTime : 0;

      if (_parent != null) {
        _clock = new LogicalClock {
          // Assuming LogicalClock has a copy constructor or equivalent
          // If not, you might need to implement a method to copy the clock state
          // For simplicity, setting the clock's time to parent's current time
          // and assuming that LogicalClock can be assigned directly
          // Otherwise, implement a proper copy mechanism
        };
        _clock.SetTime(_parent._clock.CurrentTime);
      }
    }

    /// <summary>
    /// Creates a CRDT from a list of changes (e.g., loaded from disk).
    /// </summary>
    public CRDT(ulong nodeId, List<Change<K, V>> changes) {
      _nodeId = nodeId;
      _clock = new LogicalClock();
      _data = new Dictionary<K, Record<K, V>>();
      _tombstones = new HashSet<K>();

      ApplyChanges(changes);
    }

    /// <summary>
    /// Resets the CRDT to a state as if it was constructed with the given changes.
    /// </summary>
    public void Reset(List<Change<K, V>> changes) {
      _data.Clear();
      _tombstones.Clear();
      _clock = new LogicalClock();

      ApplyChanges(changes);
    }

    /// <summary>
    /// Generates inverse changes for a given set of changes based on the parent state.
    /// </summary>
    public List<Change<K, V>> InvertChanges(List<Change<K, V>> changes, bool useParent = false) {
      List<Change<K, V>> inverseChanges = new List<Change<K, V>>();

      foreach (var change in changes) {
        K recordId = change.RecordId;
        ColName<K>? colName = change.ColName;
        V? value = change.Value;

        if (colName == null) {
          // The change was a record deletion (tombstone)
          Record<K, V>? recordPtr = useParent ? GetRecordPtr(recordId, true) : GetRecordPtr(recordId);

          if (recordPtr != null) {
            // Restore all fields from the record
            foreach (var kvp in recordPtr.Fields) {
              inverseChanges.Add(new Change<K, V>(
                  recordId,
                  kvp.Key,
                  kvp.Value,
                  recordPtr.ColumnVersions[kvp.Key].ColVersion,
                  recordPtr.ColumnVersions[kvp.Key].DbVersion,
                  _nodeId));
            }

            // Remove the tombstone
            inverseChanges.Add(new Change<K, V>(
                recordId,
                null,
                default,
                0, // Column version 0 signifies removal of tombstone
                _clock.CurrentTime,
                _nodeId));
          }
        } else {
          // The change was an insertion or update of a column
          ColName<K> col = colName.Value;
          Record<K, V>? recordPtr = useParent ? GetRecordPtr(recordId, true) : GetRecordPtr(recordId);

          if (recordPtr != null) {
            if (recordPtr.Fields.TryGetValue(col, out V existingValue)) {
              // The record has a value for this column; set it back to the record's value
              inverseChanges.Add(new Change<K, V>(
                  recordId,
                  col,
                  existingValue,
                  recordPtr.ColumnVersions[col].ColVersion,
                  recordPtr.ColumnVersions[col].DbVersion,
                  _nodeId));
            } else {
              // The record does not have this column; delete it to revert
              inverseChanges.Add(new Change<K, V>(
                  recordId,
                  col,
                  default,
                  0, // Column version 0 signifies deletion
                  _clock.CurrentTime,
                  _nodeId));
            }
          } else {
            // The record does not exist; remove the entire record to revert
            inverseChanges.Add(new Change<K, V>(
                recordId,
                null,
                default,
                0, // Column version 0 signifies a tombstone
                _clock.CurrentTime,
                _nodeId));
          }
        }
      }

      return inverseChanges;
    }

    /// <summary>
    /// Reverts all changes made by this CRDT since it was created from the parent.
    /// </summary>
    public List<Change<K, V>> Revert() {
      if (_parent == null)
        throw new InvalidOperationException("Cannot revert without a parent CRDT.");

      // Step 1: Retrieve all changes made by the child since base_version_
      List<Change<K, V>> childChanges = GetChangesSince(_baseVersion);

      // Step 2: Generate inverse changes using the generalized function
      List<Change<K, V>> inverseChanges = InvertChanges(childChanges, useParent: true);

      return inverseChanges;
    }

    /// <summary>
    /// Inserts a new record or updates an existing record in the CRDT.
    /// </summary>
    public List<Change<K, V>> InsertOrUpdate(K recordId, Dictionary<ColName<K>, V> fields) {
      List<Change<K, V>> changes = new List<Change<K, V>>();
      ulong dbVersion = _clock.Tick();

      // Check if the record is tombstoned
      if (IsRecordTombstoned(recordId)) {
        return changes;
      }

      Record<K, V> record = GetOrCreateRecordUnchecked(recordId);

      foreach (var kvp in fields) {
        ColName<K> colName = kvp.Key;
        V value = kvp.Value;

        ulong colVersion;
        if (record.ColumnVersions.TryGetValue(colName, out ColumnVersion colInfo)) {
          colVersion = ++colInfo.ColVersion;
          colInfo.DbVersion = dbVersion;
          colInfo.NodeId = _nodeId;
          record.ColumnVersions[colName] = colInfo;
        } else {
          colVersion = 1;
          record.ColumnVersions[colName] = new ColumnVersion(colVersion, dbVersion, _nodeId, dbVersion);
        }

        record.Fields[colName] = value;

        changes.Add(new Change<K, V>(
            recordId,
            colName,
            value,
            colVersion,
            dbVersion,
            _nodeId,
            dbVersion));
      }

      return changes;
    }

    /// <summary>
    /// Deletes a record by marking it as tombstoned.
    /// </summary>
    public List<Change<K, V>> DeleteRecord(K recordId) {
      List<Change<K, V>> changes = new List<Change<K, V>>();

      if (IsRecordTombstoned(recordId)) {
        return changes;
      }

      ulong dbVersion = _clock.Tick();

      // Mark as tombstone and remove data
      _tombstones.Add(recordId);
      _data.Remove(recordId);

      // Insert deletion clock info
      Dictionary<ColName<K>, ColumnVersion> deletionClock = new Dictionary<ColName<K>, ColumnVersion>
      {
                { new ColName<K>("__deleted__"), new ColumnVersion(1, dbVersion, _nodeId, dbVersion) }
            };

      // Store deletion info in the data map
      _data[recordId] = new Record<K, V>(new Dictionary<ColName<K>, V>(), deletionClock);

      changes.Add(new Change<K, V>(
          recordId,
          null,
          default,
          1,
          dbVersion,
          _nodeId,
          dbVersion));

      return changes;
    }

    /// <summary>
    /// Retrieves all changes since a given lastDbVersion.
    /// </summary>
    public List<Change<K, V>> GetChangesSince(ulong lastDbVersion) {
      List<Change<K, V>> changes = new List<Change<K, V>>();

      // Get changes from parent
      if (_parent != null) {
        List<Change<K, V>> parentChanges = _parent.GetChangesSince(lastDbVersion);
        changes.AddRange(parentChanges);
      }

      foreach (var recordKvp in _data) {
        K recordId = recordKvp.Key;
        Record<K, V> record = recordKvp.Value;

        foreach (var colKvp in record.ColumnVersions) {
          ColName<K> colName = colKvp.Key;
          ColumnVersion colVersion = colKvp.Value;

          if (colVersion.LocalDbVersion > lastDbVersion) {
            ColName<K>? name = colName.Name != "__deleted__" ? colName : null;
            V? value = colName.Name != "__deleted__" && record.Fields.TryGetValue(colName, out V tempVal) ? tempVal : default;

            changes.Add(new Change<K, V>(
                recordId,
                name,
                value,
                colVersion.ColVersion,
                colVersion.DbVersion,
                colVersion.NodeId,
                colVersion.LocalDbVersion));
          }
        }
      }

      if (_parent != null) {
        // Since we merge from the parent, we need to also run a compression pass
        CompressChanges(changes);
      }

      return changes;
    }

    /// <summary>
    /// Merges a set of incoming changes into the CRDT.
    /// </summary>
    public List<Change<K, V>> MergeChanges(List<Change<K, V>> changes, bool ignoreParent = false) {
      List<Change<K, V>> acceptedChanges = new List<Change<K, V>>();

      if (changes.Count == 0)
        return acceptedChanges;

      foreach (var change in changes) {
        K recordId = change.RecordId;
        ColName<K>? colName = change.ColName;
        ulong remoteColVersion = change.ColVersion;
        ulong remoteDbVersion = change.DbVersion;
        ulong remoteNodeId = change.NodeId;
        V? remoteValue = change.Value;

        // Update the logical clock
        ulong newLocalDbVersion = _clock.Update(remoteDbVersion);

        // Retrieve local column version information
        Record<K, V>? recordPtr = GetRecordPtr(recordId, ignoreParent);
        ColumnVersion? localColInfo = null;

        if (recordPtr != null) {
          ColName<K> key = colName ?? new ColName<K>("__deleted__");
          if (recordPtr.ColumnVersions.TryGetValue(key, out ColumnVersion tempColInfo)) {
            localColInfo = tempColInfo;
          }
        }

        // Determine whether to accept the remote change
        bool shouldAccept = false;

        if (localColInfo == null) {
          // No local version exists; accept the remote change
          shouldAccept = true;
        } else {
          if (remoteColVersion > localColInfo.Value.ColVersion) {
            // Remote change is newer; accept it
            shouldAccept = true;
          } else if (remoteColVersion < localColInfo.Value.ColVersion) {
            // Remote change is older; reject it
            shouldAccept = false;
          } else {
            // col_version is equal; use db_version as the next tiebreaker
            if (remoteDbVersion > localColInfo.Value.DbVersion) {
              shouldAccept = true;
            } else if (remoteDbVersion < localColInfo.Value.DbVersion) {
              shouldAccept = false;
            } else {
              // db_version is equal; use node_id for final tiebreaking
              shouldAccept = remoteNodeId > localColInfo.Value.NodeId;
            }
          }
        }

        if (shouldAccept) {
          if (colName == null) {
            // Handle deletion
            _tombstones.Add(recordId);
            _data.Remove(recordId);

            // Update deletion clock info
            Dictionary<ColName<K>, ColumnVersion> deletionClock = new Dictionary<ColName<K>, ColumnVersion>
            {
                            { new ColName<K>("__deleted__"), new ColumnVersion(remoteColVersion, remoteDbVersion, remoteNodeId, newLocalDbVersion) }
                        };

            // Store deletion info in the data map
            _data[recordId] = new Record<K, V>(new Dictionary<ColName<K>, V>(), deletionClock);

            acceptedChanges.Add(new Change<K, V>(
                recordId,
                null,
                default,
                remoteColVersion,
                remoteDbVersion,
                remoteNodeId,
                newLocalDbVersion));
          } else if (!IsRecordTombstoned(recordId, ignoreParent)) {
            // Handle insertion or update
            Record<K, V> record = GetOrCreateRecordUnchecked(recordId, ignoreParent);

            if (remoteValue != null) {
              record.Fields[colName.Value] = remoteValue;
            } else {
              // If remoteValue is null, remove the field
              record.Fields.Remove(colName.Value);
            }

            // Update the column version info
            record.ColumnVersions[colName.Value] = new ColumnVersion(remoteColVersion, remoteDbVersion, remoteNodeId, newLocalDbVersion);

            acceptedChanges.Add(new Change<K, V>(
                recordId,
                colName,
                remoteValue,
                remoteColVersion,
                remoteDbVersion,
                remoteNodeId,
                newLocalDbVersion));
          }
        }
      }

      return acceptedChanges;
    }

    /// <summary>
    /// Compresses a list of changes in-place by removing redundant changes that overwrite each other.
    /// </summary>
    public static void CompressChanges(List<Change<K, V>> changes, bool sorted = false) {
      if (changes.Count == 0)
        return;

      if (!sorted) {
        changes.Sort(new ChangeComparer<K, V>());
      }

      int writeIndex = 0;

      for (int readIndex = 1; readIndex < changes.Count; readIndex++) {
        var writeChange = changes[writeIndex];
        var readChange = changes[readIndex];

        if (!EqualityComparer<K>.Default.Equals(writeChange.RecordId, readChange.RecordId)) {
          writeIndex++;
          changes[writeIndex] = readChange;
        } else if (readChange.ColName == null && writeChange.ColName != null) {
          // Current read is a deletion, keep it and skip all previous changes for this record
          writeIndex++;
          changes[writeIndex] = readChange;
        } else if (writeChange.ColName != readChange.ColName) {
          // New column for the same record
          writeIndex++;
          changes[writeIndex] = readChange;
        }
        // Else: same record and column, keep the existing one (which is the most recent due to sorting)
      }

      // Remove the redundant changes
      changes.RemoveRange(writeIndex + 1, changes.Count - (writeIndex + 1));
    }

    /// <summary>
    /// Prints the current data and tombstones for debugging purposes.
    /// </summary>
    public void PrintData(Action<string> logAction) {
      logAction($"Node {_nodeId} Data:");
      foreach (var recordKvp in _data) {
        K recordId = recordKvp.Key;
        Record<K, V> record = recordKvp.Value;

        if (_tombstones.Contains(recordId))
          continue; // Skip tombstoned records

        logAction($"ID: {recordId}");
        foreach (var fieldKvp in record.Fields) {
          logAction($"  {fieldKvp.Key}: {fieldKvp.Value}");
        }
      }

      string tombstones = string.Join(" ", _tombstones);
      logAction($"Tombstones: {tombstones}");
    }

    /// <summary>
    /// Retrieves the logical clock.
    /// </summary>
    public LogicalClock GetClock() => _clock;

    /// <summary>
    /// Retrieves all data, optionally combining with parent data.
    /// </summary>
    public Dictionary<K, Record<K, V>> GetData() {
      if (_parent == null)
        return new Dictionary<K, Record<K, V>>(_data);

      Dictionary<K, Record<K, V>> combinedData = new Dictionary<K, Record<K, V>>(_parent.GetData());
      foreach (var kvp in _data) {
        combinedData[kvp.Key] = kvp.Value;
      }
      return combinedData;
    }

    /// <summary>
    /// Synchronizes two CRDT nodes.
    /// Retrieves changes from the source since lastDbVersion and merges them into
    /// the target. Updates lastDbVersion to prevent reprocessing the same
    /// changes.
    /// </summary>
    public static void SyncNodes(CRDT<K, V> source, CRDT<K, V> target, ref ulong lastDbVersion) {
      List<Change<K, V>> changes = source.GetChangesSince(lastDbVersion);

      // Update lastDbVersion to the current max db_version in source
      ulong maxVersion = changes.Any() ? changes.Max(c => c.DbVersion) : lastDbVersion;
      if (maxVersion > lastDbVersion)
        lastDbVersion = maxVersion;

      target.MergeChanges(changes);
    }

    #region Private Methods

    /// <summary>
    /// Applies a list of changes to reconstruct the CRDT state.
    /// </summary>
    private void ApplyChanges(List<Change<K, V>> changes) {
      // Determine the maximum db_version from the changes
      ulong maxDbVersion = changes.Any() ? changes.Max(c => c.DbVersion) : 0;

      // Set the logical clock to the maximum db_version
      _clock.SetTime(maxDbVersion);

      // Apply each change to reconstruct the CRDT state
      foreach (var change in changes) {
        K recordId = change.RecordId;
        ColName<K>? colName = change.ColName;
        ulong remoteColVersion = change.ColVersion;
        ulong remoteDbVersion = change.DbVersion;
        ulong remoteNodeId = change.NodeId;
        ulong remoteLocalDbVersion = change.LocalDbVersion;
        V? remoteValue = change.Value;

        if (colName == null) {
          // Handle deletion
          _tombstones.Add(recordId);
          _data.Remove(recordId);

          // Insert deletion clock info
          Dictionary<ColName<K>, ColumnVersion> deletionClock = new Dictionary<ColName<K>, ColumnVersion>
          {
                        { new ColName<K>("__deleted__"), new ColumnVersion(remoteColVersion, remoteDbVersion, remoteNodeId, remoteLocalDbVersion) }
                    };

          // Store deletion info in the data map
          _data[recordId] = new Record<K, V>(new Dictionary<ColName<K>, V>(), deletionClock);
        } else {
          if (!IsRecordTombstoned(recordId)) {
            // Handle insertion or update
            Record<K, V> record = GetOrCreateRecordUnchecked(recordId);

            if (remoteValue != null) {
              record.Fields[colName.Value] = remoteValue;
            }

            // Update the column version info
            record.ColumnVersions[colName.Value] = new ColumnVersion(remoteColVersion, remoteDbVersion, remoteNodeId, remoteLocalDbVersion);
          }
        }
      }
    }

    /// <summary>
    /// Checks if a record is tombstoned.
    /// </summary>
    private bool IsRecordTombstoned(K recordId, bool ignoreParent = false) {
      if (_tombstones.Contains(recordId))
        return true;

      if (_data.TryGetValue(recordId, out var record) &&
          record.ColumnVersions.ContainsKey(new ColName<K>("__deleted__")))
        return true;

      if (_parent != null && !ignoreParent)
        return _parent.IsRecordTombstoned(recordId);

      return false;
    }

    /// <summary>
    /// Retrieves a record pointer.
    /// </summary>
    private Record<K, V>? GetRecordPtr(K recordId, bool ignoreParent = false) {
      if (_data.TryGetValue(recordId, out Record<K, V>? record))
        return record;

      if (_parent != null && !ignoreParent)
        return _parent.GetRecordPtr(recordId);

      return null;
    }

    /// <summary>
    /// Retrieves or creates a record without checking tombstones.
    /// </summary>
    private Record<K, V> GetOrCreateRecordUnchecked(K recordId, bool ignoreParent = false) {
      if (!_data.TryGetValue(recordId, out Record<K, V>? record)) {
        _data[recordId] = new Record<K, V>();

        if (_parent != null && !ignoreParent) {
          Record<K, V>? parentRecord = _parent.GetRecordPtr(recordId);
          if (parentRecord != null)
            _data[recordId] = new Record<K, V>(new Dictionary<ColName<K>, V>(parentRecord.Fields),
                                           new Dictionary<ColName<K>, ColumnVersion>(parentRecord.ColumnVersions));
        }
      }

      return _data[recordId];
    }

    #endregion
  }
}
