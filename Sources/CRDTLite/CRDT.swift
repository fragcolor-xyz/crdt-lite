// CRDT.swift
// A lightweight, column-based CRDT implementation in Swift
//
// This library provides a generic CRDT with last-write-wins semantics, supporting:
// - Generic key and value types
// - Logical clock for causality tracking
// - Tombstone-based deletion
// - Parent-child CRDT hierarchies
// - Custom merge rules
// - Change compression
// - JSON serialization via Codable

import Foundation

// MARK: - Type Aliases

/// Type alias for node IDs
/// Use UInt64 for compatibility with Rust/C++ implementations
public typealias NodeId = UInt64

/// Type alias for column keys (field names)
public typealias ColumnKey = String

/// Column version used for tombstone changes
/// Using UInt64.max ensures tombstones are treated as having the highest possible version
private let TOMBSTONE_COL_VERSION: UInt64 = UInt64.max

// MARK: - Logical Clock

/// Represents a logical clock for maintaining causality.
public struct LogicalClock: Codable, Equatable {
    private var time: UInt64

    /// Creates a new logical clock starting at 0
    public init() {
        self.time = 0
    }

    /// Increments the clock for a local event and returns the new time
    public mutating func tick() -> UInt64 {
        time += 1
        return time
    }

    /// Updates the clock based on a received time and returns the new time
    public mutating func update(_ receivedTime: UInt64) -> UInt64 {
        time = max(time, receivedTime)
        time += 1
        return time
    }

    /// Sets the logical clock to a specific time
    public mutating func setTime(_ newTime: UInt64) {
        time = newTime
    }

    /// Retrieves the current time
    public func currentTime() -> UInt64 {
        return time
    }
}

// MARK: - Column Version

/// Represents version information for a column.
public struct ColumnVersion: Codable, Equatable {
    public var colVersion: UInt64
    public var dbVersion: UInt64
    public var nodeId: NodeId
    public var localDbVersion: UInt64

    public init(colVersion: UInt64, dbVersion: UInt64, nodeId: NodeId, localDbVersion: UInt64) {
        self.colVersion = colVersion
        self.dbVersion = dbVersion
        self.nodeId = nodeId
        self.localDbVersion = localDbVersion
    }
}

// MARK: - Tombstone Info

/// Minimal version information for tombstones.
///
/// Stores essential data: dbVersion for conflict resolution, nodeId for sync exclusion,
/// and localDbVersion for sync.
public struct TombstoneInfo: Codable, Equatable {
    public var dbVersion: UInt64
    public var nodeId: NodeId
    public var localDbVersion: UInt64

    public init(dbVersion: UInt64, nodeId: NodeId, localDbVersion: UInt64) {
        self.dbVersion = dbVersion
        self.nodeId = nodeId
        self.localDbVersion = localDbVersion
    }

    /// Helper to create a ColumnVersion for comparison with regular columns
    public func asColumnVersion() -> ColumnVersion {
        return ColumnVersion(
            colVersion: TOMBSTONE_COL_VERSION,
            dbVersion: dbVersion,
            nodeId: nodeId,
            localDbVersion: localDbVersion
        )
    }
}

// MARK: - Change

/// Represents a single change in the CRDT.
///
/// A change can represent:
/// - An insertion or update of a column value (when `colName` is non-nil)
/// - A deletion of a specific column (when `colName` is non-nil and `value` is nil)
/// - A deletion of an entire record (when `colName` is nil)
public struct Change<K: Hashable & Codable, C: Hashable & Codable, V: Codable>: Codable, Equatable where K: Equatable, C: Equatable, V: Equatable {
    public var recordId: K
    /// `nil` represents tombstone of the record
    public var colName: C?
    /// `nil` represents deletion of the column (not the record)
    public var value: V?
    public var colVersion: UInt64
    public var dbVersion: UInt64
    public var nodeId: NodeId
    /// Local dbVersion when the change was created (useful for `getChangesSince`)
    public var localDbVersion: UInt64
    /// Optional flags to indicate the type of change (ephemeral, not stored)
    public var flags: UInt32

    public init(
        recordId: K,
        colName: C?,
        value: V?,
        colVersion: UInt64,
        dbVersion: UInt64,
        nodeId: NodeId,
        localDbVersion: UInt64,
        flags: UInt32 = 0
    ) {
        self.recordId = recordId
        self.colName = colName
        self.value = value
        self.colVersion = colVersion
        self.dbVersion = dbVersion
        self.nodeId = nodeId
        self.localDbVersion = localDbVersion
        self.flags = flags
    }
}

// MARK: - Record

/// Represents a record in the CRDT.
public struct Record<C: Hashable & Codable, V: Codable>: Codable where C: Equatable {
    public var fields: [C: V]
    public var columnVersions: [C: ColumnVersion]

    /// Track version boundaries for efficient filtering
    public var lowestLocalDbVersion: UInt64
    public var highestLocalDbVersion: UInt64

    public init() {
        self.fields = [:]
        self.columnVersions = [:]
        self.lowestLocalDbVersion = UInt64.max
        self.highestLocalDbVersion = 0
    }

    /// Creates a record from existing fields and column versions
    public init(fields: [C: V], columnVersions: [C: ColumnVersion]) {
        self.fields = fields
        self.columnVersions = columnVersions

        var lowest = UInt64.max
        var highest: UInt64 = 0

        for version in columnVersions.values {
            if version.localDbVersion < lowest {
                lowest = version.localDbVersion
            }
            if version.localDbVersion > highest {
                highest = version.localDbVersion
            }
        }

        self.lowestLocalDbVersion = lowest
        self.highestLocalDbVersion = highest
    }
}

extension Record: Equatable where V: Equatable {
    public static func == (lhs: Record<C, V>, rhs: Record<C, V>) -> Bool {
        // Compare only fields, not columnVersions (those will differ per node)
        return lhs.fields == rhs.fields
    }
}

// MARK: - Tombstone Storage

/// Storage for tombstones (deleted records).
///
/// Uses a Dictionary for efficient lookups and supports compaction.
public struct TombstoneStorage<K: Hashable & Codable>: Codable {
    private var entries: [K: TombstoneInfo]

    public init() {
        self.entries = [:]
    }

    public mutating func insertOrAssign(_ key: K, _ info: TombstoneInfo) {
        entries[key] = info
    }

    public func find(_ key: K) -> TombstoneInfo? {
        return entries[key]
    }

    public mutating func erase(_ key: K) -> Bool {
        return entries.removeValue(forKey: key) != nil
    }

    public mutating func clear() {
        entries.removeAll()
    }

    public func makeIterator() -> Dictionary<K, TombstoneInfo>.Iterator {
        return entries.makeIterator()
    }

    public var count: Int {
        return entries.count
    }

    public var isEmpty: Bool {
        return entries.isEmpty
    }

    /// Compact tombstones older than the specified version.
    ///
    /// Returns the number of tombstones removed.
    public mutating func compact(minAcknowledgedVersion: UInt64) -> Int {
        let initialCount = entries.count
        entries = entries.filter { $0.value.dbVersion >= minAcknowledgedVersion }
        return initialCount - entries.count
    }
}

// MARK: - Merge Rule Protocol

/// Protocol for merge rules that determine conflict resolution.
///
/// Implementations should return `true` if the remote change should be accepted,
/// `false` otherwise.
public protocol MergeRule {
    /// Determines whether to accept a remote change over a local one
    func shouldAccept(
        localCol: UInt64,
        localDb: UInt64,
        localNode: NodeId,
        remoteCol: UInt64,
        remoteDb: UInt64,
        remoteNode: NodeId
    ) -> Bool
}

// MARK: - Default Merge Rule

/// Default merge rule implementing last-write-wins semantics.
///
/// Comparison priority:
/// 1. Column version (higher wins)
/// 2. DB version (higher wins)
/// 3. Node ID (higher wins as tiebreaker)
public struct DefaultMergeRule: MergeRule {
    public init() {}

    public func shouldAccept(
        localCol: UInt64,
        localDb: UInt64,
        localNode: NodeId,
        remoteCol: UInt64,
        remoteDb: UInt64,
        remoteNode: NodeId
    ) -> Bool {
        if remoteCol > localCol {
            return true
        } else if remoteCol < localCol {
            return false
        } else {
            if remoteDb > localDb {
                return true
            } else if remoteDb < localDb {
                return false
            } else {
                return remoteNode > localNode
            }
        }
    }
}

// MARK: - Change Comparator Protocol

/// Protocol for change comparators used in sorting and compression.
public protocol ChangeComparator {
    associatedtype K: Hashable & Comparable
    associatedtype C: Hashable & Comparable
    associatedtype V

    func compare(_ a: Change<K, C, V>, _ b: Change<K, C, V>) -> ComparisonResult
}

// MARK: - Default Change Comparator

/// Default change comparator.
///
/// Sorts by:
/// 1. Record ID (ascending)
/// 2. Column name presence (deletions/tombstones last)
/// 3. Column name (ascending)
/// 4. Column version (descending - most recent first)
/// 5. DB version (descending)
/// 6. Node ID (descending)
public struct DefaultChangeComparator<K: Hashable & Comparable, C: Hashable & Comparable, V>: ChangeComparator {
    public init() {}

    public func compare(_ a: Change<K, C, V>, _ b: Change<K, C, V>) -> ComparisonResult {
        // Compare record IDs
        if a.recordId < b.recordId {
            return .orderedAscending
        } else if a.recordId > b.recordId {
            return .orderedDescending
        }

        // Deletions (nil) come last for each record
        switch (a.colName, b.colName) {
        case (nil, nil):
            break
        case (nil, .some):
            return .orderedDescending
        case (.some, nil):
            return .orderedAscending
        case let (.some(aCol), .some(bCol)):
            if aCol < bCol {
                return .orderedAscending
            } else if aCol > bCol {
                return .orderedDescending
            }
        }

        // Compare versions (descending - most recent first)
        if b.colVersion < a.colVersion {
            return .orderedAscending
        } else if b.colVersion > a.colVersion {
            return .orderedDescending
        }

        if b.dbVersion < a.dbVersion {
            return .orderedAscending
        } else if b.dbVersion > a.dbVersion {
            return .orderedDescending
        }

        if b.nodeId < a.nodeId {
            return .orderedAscending
        } else if b.nodeId > a.nodeId {
            return .orderedDescending
        }

        return .orderedSame
    }
}

// MARK: - CRDT Class

/// Main CRDT structure, generic over key (K), column (C), and value (V) types.
///
/// This implements a column-based CRDT with last-write-wins semantics.
public final class CRDT<K: Hashable & Codable, C: Hashable & Codable, V: Codable> where K: Equatable, C: Equatable {
    private var nodeId: NodeId
    private var clock: LogicalClock
    private var data: [K: Record<C, V>]
    private var tombstones: TombstoneStorage<K>
    private var parent: CRDT<K, C, V>?
    private var baseVersion: UInt64

    /// Creates a new empty CRDT.
    ///
    /// - Parameters:
    ///   - nodeId: Unique identifier for this CRDT node
    ///   - parent: Optional parent CRDT for hierarchical structures
    public init(nodeId: NodeId, parent: CRDT<K, C, V>? = nil) {
        self.nodeId = nodeId
        self.parent = parent

        if let parent = parent {
            self.clock = parent.clock
            self.baseVersion = parent.clock.currentTime()
        } else {
            self.clock = LogicalClock()
            self.baseVersion = 0
        }

        self.data = [:]
        self.tombstones = TombstoneStorage()
    }

    /// Creates a CRDT from a list of changes (e.g., loaded from disk).
    ///
    /// - Parameters:
    ///   - nodeId: The unique identifier for this CRDT node
    ///   - changes: A list of changes to apply to reconstruct the CRDT state
    public convenience init(nodeId: NodeId, changes: [Change<K, C, V>]) where V: Equatable {
        self.init(nodeId: nodeId, parent: nil)
        self.applyChanges(changes)
    }

    /// Resets the CRDT to a state as if it was constructed with the given changes.
    ///
    /// - Parameter changes: A list of changes to apply to reconstruct the CRDT state
    public func reset(changes: [Change<K, C, V>]) where V: Equatable {
        data.removeAll()
        tombstones.clear()
        clock = LogicalClock()
        applyChanges(changes)
    }

    // MARK: - Insert/Update Operations

    /// Inserts a new record or updates an existing record in the CRDT.
    ///
    /// - Parameters:
    ///   - recordId: The unique identifier for the record
    ///   - fields: A dictionary of (column_name, value) pairs
    /// - Returns: An array of changes created by this operation
    @discardableResult
    public func insertOrUpdate(_ recordId: K, fields: [(C, V)]) -> [Change<K, C, V>] where V: Equatable {
        return insertOrUpdateWithFlags(recordId, flags: 0, fields: fields)
    }

    /// Inserts a new record or updates an existing record with flags.
    ///
    /// - Parameters:
    ///   - recordId: The unique identifier for the record
    ///   - flags: Flags to indicate the type of change
    ///   - fields: A dictionary of (column_name, value) pairs
    /// - Returns: An array of changes created by this operation
    @discardableResult
    public func insertOrUpdateWithFlags(_ recordId: K, flags: UInt32, fields: [(C, V)]) -> [Change<K, C, V>] where V: Equatable {
        let dbVersion = clock.tick()

        // Check if the record is tombstoned
        if isRecordTombstoned(recordId, ignoreParent: false) {
            return []
        }

        var changes: [Change<K, C, V>] = []
        let currentNodeId = nodeId
        var record = getOrCreateRecordUnchecked(recordId, ignoreParent: false)

        for (colName, value) in fields {
            let colVersion: UInt64

            if var colInfo = record.columnVersions[colName] {
                colInfo.colVersion += 1
                colInfo.dbVersion = dbVersion
                colInfo.nodeId = currentNodeId
                colInfo.localDbVersion = dbVersion
                colVersion = colInfo.colVersion
                record.columnVersions[colName] = colInfo
            } else {
                colVersion = 1
                record.columnVersions[colName] = ColumnVersion(
                    colVersion: colVersion,
                    dbVersion: dbVersion,
                    nodeId: currentNodeId,
                    localDbVersion: dbVersion
                )
            }

            // Update record version boundaries
            if dbVersion < record.lowestLocalDbVersion {
                record.lowestLocalDbVersion = dbVersion
            }
            if dbVersion > record.highestLocalDbVersion {
                record.highestLocalDbVersion = dbVersion
            }

            record.fields[colName] = value
            changes.append(Change(
                recordId: recordId,
                colName: colName,
                value: value,
                colVersion: colVersion,
                dbVersion: dbVersion,
                nodeId: currentNodeId,
                localDbVersion: dbVersion,
                flags: flags
            ))
        }

        // Persist the modified record back to the data dictionary
        data[recordId] = record

        return changes
    }

    // MARK: - Delete Operations

    /// Deletes a record by marking it as tombstoned.
    ///
    /// - Parameter recordId: The unique identifier for the record
    /// - Returns: An optional Change representing the deletion
    @discardableResult
    public func deleteRecord(_ recordId: K) -> Change<K, C, V>? {
        return deleteRecordWithFlags(recordId, flags: 0)
    }

    /// Deletes a record with flags.
    ///
    /// - Parameters:
    ///   - recordId: The unique identifier for the record
    ///   - flags: Flags to indicate the type of change
    /// - Returns: An optional Change representing the deletion
    @discardableResult
    public func deleteRecordWithFlags(_ recordId: K, flags: UInt32) -> Change<K, C, V>? {
        if isRecordTombstoned(recordId, ignoreParent: false) {
            return nil
        }

        let dbVersion = clock.tick()

        // Mark as tombstone and remove data
        data.removeValue(forKey: recordId)

        // Store deletion information in tombstones
        tombstones.insertOrAssign(
            recordId,
            TombstoneInfo(dbVersion: dbVersion, nodeId: nodeId, localDbVersion: dbVersion)
        )

        return Change(
            recordId: recordId,
            colName: nil,
            value: nil,
            colVersion: TOMBSTONE_COL_VERSION,
            dbVersion: dbVersion,
            nodeId: nodeId,
            localDbVersion: dbVersion,
            flags: flags
        )
    }

    /// Deletes a specific field from a record.
    ///
    /// - Parameters:
    ///   - recordId: The unique identifier for the record
    ///   - fieldName: The name of the field to delete
    /// - Returns: An optional Change representing the field deletion. Returns nil if:
    ///   - The record is tombstoned
    ///   - The record doesn't exist
    ///   - The field doesn't exist in the record
    @discardableResult
    public func deleteField(_ recordId: K, fieldName: C) -> Change<K, C, V>? where V: Equatable {
        return deleteFieldWithFlags(recordId, fieldName: fieldName, flags: 0)
    }

    /// Deletes a specific field from a record with flags.
    ///
    /// - Parameters:
    ///   - recordId: The unique identifier for the record
    ///   - fieldName: The name of the field to delete
    ///   - flags: Flags to indicate the type of change
    /// - Returns: An optional Change representing the field deletion
    @discardableResult
    public func deleteFieldWithFlags(_ recordId: K, fieldName: C, flags: UInt32) -> Change<K, C, V>? where V: Equatable {
        // Check if the record is tombstoned
        if isRecordTombstoned(recordId, ignoreParent: false) {
            return nil
        }

        // Get the record (return nil if it doesn't exist)
        guard var record = data[recordId] else {
            return nil
        }

        // Check if the field exists
        guard record.fields[fieldName] != nil else {
            return nil
        }

        let dbVersion = clock.tick()

        // Get or create column version and increment it
        let colVersion: UInt64

        if var colInfo = record.columnVersions[fieldName] {
            colInfo.colVersion += 1
            colInfo.dbVersion = dbVersion
            colInfo.nodeId = nodeId
            colInfo.localDbVersion = dbVersion
            colVersion = colInfo.colVersion
            record.columnVersions[fieldName] = colInfo
        } else {
            // This shouldn't happen if the field exists, but handle it gracefully
            colVersion = 1
            record.columnVersions[fieldName] = ColumnVersion(
                colVersion: colVersion,
                dbVersion: dbVersion,
                nodeId: nodeId,
                localDbVersion: dbVersion
            )
        }

        // Update record version boundaries
        if dbVersion < record.lowestLocalDbVersion {
            record.lowestLocalDbVersion = dbVersion
        }
        if dbVersion > record.highestLocalDbVersion {
            record.highestLocalDbVersion = dbVersion
        }

        // Remove the field from the record (but keep the ColumnVersion as field tombstone)
        record.fields.removeValue(forKey: fieldName)

        // Persist the modified record back to the data dictionary
        data[recordId] = record

        return Change(
            recordId: recordId,
            colName: fieldName,
            value: nil,
            colVersion: colVersion,
            dbVersion: dbVersion,
            nodeId: nodeId,
            localDbVersion: dbVersion,
            flags: flags
        )
    }

    // MARK: - Merge Operations

    /// Merges incoming changes into the CRDT.
    ///
    /// - Parameters:
    ///   - changes: Array of changes to merge
    ///   - mergeRule: The merge rule to use for conflict resolution
    /// - Returns: Array of accepted changes
    @discardableResult
    public func mergeChanges<R: MergeRule>(_ changes: [Change<K, C, V>], mergeRule: R) -> [Change<K, C, V>] where V: Equatable {
        return mergeChangesImpl(changes, ignoreParent: false, mergeRule: mergeRule)
    }

    private func mergeChangesImpl<R: MergeRule>(
        _ changes: [Change<K, C, V>],
        ignoreParent: Bool,
        mergeRule: R
    ) -> [Change<K, C, V>] where V: Equatable {
        var acceptedChanges: [Change<K, C, V>] = []

        if changes.isEmpty {
            return acceptedChanges
        }

        for change in changes {
            let recordId = change.recordId
            let colName = change.colName
            let remoteValue = change.value
            let remoteColVersion = change.colVersion
            let remoteDbVersion = change.dbVersion
            let remoteNodeId = change.nodeId
            let flags = change.flags

            // Always update the logical clock to maintain causal consistency
            let newLocalDbVersion = clock.update(remoteDbVersion)

            // Skip all changes for tombstoned records
            if isRecordTombstoned(recordId, ignoreParent: ignoreParent) {
                continue
            }

            // Retrieve local column version information
            let localColInfo: ColumnVersion?

            if colName == nil {
                // For deletions, check tombstones
                localColInfo = tombstones.find(recordId)?.asColumnVersion()
            } else if let col = colName {
                // For column updates, check the record
                localColInfo = getRecordPtr(recordId, ignoreParent: ignoreParent)?.columnVersions[col]
            } else {
                localColInfo = nil
            }

            // Determine whether to accept the remote change
            let shouldAccept: Bool

            if let localInfo = localColInfo {
                shouldAccept = mergeRule.shouldAccept(
                    localCol: localInfo.colVersion,
                    localDb: localInfo.dbVersion,
                    localNode: localInfo.nodeId,
                    remoteCol: remoteColVersion,
                    remoteDb: remoteDbVersion,
                    remoteNode: remoteNodeId
                )
            } else {
                shouldAccept = true
            }

            if shouldAccept {
                if let colKey = colName {
                    // Handle insertion or update
                    var record = getOrCreateRecordUnchecked(recordId, ignoreParent: ignoreParent)

                    // Update field value
                    if let value = remoteValue {
                        record.fields[colKey] = value
                    } else {
                        // If remoteValue is nil, remove the field
                        record.fields.removeValue(forKey: colKey)
                    }

                    // Update the column version info and record version boundaries
                    record.columnVersions[colKey] = ColumnVersion(
                        colVersion: remoteColVersion,
                        dbVersion: remoteDbVersion,
                        nodeId: remoteNodeId,
                        localDbVersion: newLocalDbVersion
                    )

                    // Update version boundaries
                    if newLocalDbVersion < record.lowestLocalDbVersion {
                        record.lowestLocalDbVersion = newLocalDbVersion
                    }
                    if newLocalDbVersion > record.highestLocalDbVersion {
                        record.highestLocalDbVersion = newLocalDbVersion
                    }

                    // Persist the modified record back to the data dictionary
                    data[recordId] = record

                    acceptedChanges.append(Change(
                        recordId: recordId,
                        colName: colKey,
                        value: remoteValue,
                        colVersion: remoteColVersion,
                        dbVersion: remoteDbVersion,
                        nodeId: remoteNodeId,
                        localDbVersion: newLocalDbVersion,
                        flags: flags
                    ))
                } else {
                    // Handle deletion
                    data.removeValue(forKey: recordId)

                    // Store deletion information in tombstones
                    tombstones.insertOrAssign(
                        recordId,
                        TombstoneInfo(
                            dbVersion: remoteDbVersion,
                            nodeId: remoteNodeId,
                            localDbVersion: newLocalDbVersion
                        )
                    )

                    acceptedChanges.append(Change(
                        recordId: recordId,
                        colName: nil,
                        value: nil,
                        colVersion: remoteColVersion,
                        dbVersion: remoteDbVersion,
                        nodeId: remoteNodeId,
                        localDbVersion: newLocalDbVersion,
                        flags: flags
                    ))
                }
            }
        }

        return acceptedChanges
    }

    // MARK: - Sync Operations

    /// Retrieves all changes since a given `lastDbVersion`.
    ///
    /// - Parameter lastDbVersion: The database version to retrieve changes since
    /// - Returns: An array of changes
    public func getChangesSince(_ lastDbVersion: UInt64) -> [Change<K, C, V>] where K: Comparable, C: Comparable {
        return getChangesSinceExcluding(lastDbVersion, excluding: Set<NodeId>())
    }

    /// Retrieves all changes since a given `lastDbVersion`, excluding specific nodes.
    ///
    /// - Parameters:
    ///   - lastDbVersion: The database version to retrieve changes since
    ///   - excluding: Set of node IDs to exclude from the results
    /// - Returns: An array of changes
    public func getChangesSinceExcluding(
        _ lastDbVersion: UInt64,
        excluding: Set<NodeId>
    ) -> [Change<K, C, V>] where K: Comparable, C: Comparable {
        var changes: [Change<K, C, V>] = []

        // Get changes from parent
        if let parent = parent {
            let parentChanges = parent.getChangesSinceExcluding(lastDbVersion, excluding: excluding)
            changes.append(contentsOf: parentChanges)
        }

        // Get changes from regular records
        for (recordId, record) in data {
            // Skip records that haven't changed since lastDbVersion
            if record.highestLocalDbVersion <= lastDbVersion {
                continue
            }

            for (colName, clockInfo) in record.columnVersions {
                if clockInfo.localDbVersion > lastDbVersion && !excluding.contains(clockInfo.nodeId) {
                    let value = record.fields[colName]

                    changes.append(Change(
                        recordId: recordId,
                        colName: colName,
                        value: value,
                        colVersion: clockInfo.colVersion,
                        dbVersion: clockInfo.dbVersion,
                        nodeId: clockInfo.nodeId,
                        localDbVersion: clockInfo.localDbVersion,
                        flags: 0
                    ))
                }
            }
        }

        // Get deletion changes from tombstones
        for (recordId, tombstoneInfo) in tombstones.makeIterator() {
            if tombstoneInfo.localDbVersion > lastDbVersion && !excluding.contains(tombstoneInfo.nodeId) {
                changes.append(Change(
                    recordId: recordId,
                    colName: nil,
                    value: nil,
                    colVersion: TOMBSTONE_COL_VERSION,
                    dbVersion: tombstoneInfo.dbVersion,
                    nodeId: tombstoneInfo.nodeId,
                    localDbVersion: tombstoneInfo.localDbVersion,
                    flags: 0
                ))
            }
        }

        if parent != nil {
            // Compress changes to remove redundant operations
            CRDT.compressChanges(&changes)
        }

        return changes
    }

    // MARK: - Change Compression

    /// Compresses an array of changes in-place by removing redundant changes.
    ///
    /// Changes are sorted and then compressed using a two-pointer technique.
    public static func compressChanges(_ changes: inout [Change<K, C, V>]) where K: Comparable, C: Comparable, V: Equatable {
        if changes.isEmpty {
            return
        }

        // Sort changes using the DefaultChangeComparator
        let comparator = DefaultChangeComparator<K, C, V>()
        changes.sort { comparator.compare($0, $1) == .orderedAscending }

        // Use two-pointer technique to compress in-place
        var write = 0
        for read in 1..<changes.count {
            if changes[read].recordId != changes[write].recordId {
                // New record, always keep it
                write += 1
                if write != read {
                    changes[write] = changes[read]
                }
            } else if changes[read].colName == nil && changes[write].colName != nil {
                // Current read is a deletion, backtrack to first change for this record
                // and replace it with the deletion, effectively discarding all field updates
                var firstPos = write
                while firstPos > 0 && changes[firstPos - 1].recordId == changes[read].recordId {
                    firstPos -= 1
                }
                changes[firstPos] = changes[read]
                write = firstPos
            } else if changes[read].colName != changes[write].colName && changes[write].colName != nil {
                // New column for the same record
                write += 1
                if write != read {
                    changes[write] = changes[read]
                }
            }
            // Else: same record and column, keep the existing one (most recent due to sorting)
        }

        changes.removeSubrange((write + 1)..<changes.count)
    }

    // MARK: - Query Operations

    /// Retrieves a reference to a record if it exists.
    ///
    /// - Parameter recordId: The unique identifier for the record
    /// - Returns: An optional Record
    public func getRecord(_ recordId: K) -> Record<C, V>? {
        return getRecordPtr(recordId, ignoreParent: false)
    }

    /// Checks if a record is tombstoned.
    ///
    /// - Parameter recordId: The unique identifier for the record
    /// - Returns: True if the record is tombstoned, false otherwise
    public func isTombstoned(_ recordId: K) -> Bool {
        return isRecordTombstoned(recordId, ignoreParent: false)
    }

    /// Gets tombstone information for a record.
    ///
    /// - Parameter recordId: The unique identifier for the record
    /// - Returns: Optional TombstoneInfo containing the tombstone version information
    public func getTombstone(_ recordId: K) -> TombstoneInfo? {
        if let info = tombstones.find(recordId) {
            return info
        }

        if let parent = parent {
            return parent.getTombstone(recordId)
        }

        return nil
    }

    /// Gets the current logical clock.
    public func getClock() -> LogicalClock {
        return clock
    }

    /// Gets a reference to the internal data map.
    public func getData() -> [K: Record<C, V>] {
        return data
    }

    // MARK: - Tombstone Management

    /// Removes tombstones older than the specified version.
    ///
    /// **IMPORTANT**: Only call this method when ALL participating nodes have acknowledged
    /// the `minAcknowledgedVersion`. Compacting too early may cause deleted records to
    /// reappear on nodes that haven't received the deletion yet.
    ///
    /// - Parameter minAcknowledgedVersion: Tombstones with dbVersion < this value will be removed
    /// - Returns: The number of tombstones removed
    @discardableResult
    public func compactTombstones(minAcknowledgedVersion: UInt64) -> Int {
        return tombstones.compact(minAcknowledgedVersion: minAcknowledgedVersion)
    }

    /// Gets the number of tombstones currently stored.
    public func tombstoneCount() -> Int {
        return tombstones.count
    }

    // MARK: - Helper Methods

    private func applyChanges(_ changes: [Change<K, C, V>]) where V: Equatable {
        // Determine the maximum dbVersion from the changes
        let maxDbVersion = changes.reduce(0) { max($0, max($1.dbVersion, $1.localDbVersion)) }

        // Set the logical clock to the maximum dbVersion
        clock.setTime(maxDbVersion)

        // Apply each change to reconstruct the CRDT state
        for change in changes {
            let recordId = change.recordId
            let colName = change.colName
            let remoteColVersion = change.colVersion
            let remoteDbVersion = change.dbVersion
            let remoteNodeId = change.nodeId
            let remoteLocalDbVersion = change.localDbVersion
            let remoteValue = change.value

            if colName == nil {
                // Handle deletion
                data.removeValue(forKey: recordId)

                // Store deletion information in tombstones
                tombstones.insertOrAssign(
                    recordId,
                    TombstoneInfo(
                        dbVersion: remoteDbVersion,
                        nodeId: remoteNodeId,
                        localDbVersion: remoteLocalDbVersion
                    )
                )
            } else if let colKey = colName {
                // Handle insertion or update
                if !isRecordTombstoned(recordId, ignoreParent: false) {
                    var record = getOrCreateRecordUnchecked(recordId, ignoreParent: false)

                    // Insert or update the field value
                    if let value = remoteValue {
                        record.fields[colKey] = value
                    }

                    // Update the column version info
                    record.columnVersions[colKey] = ColumnVersion(
                        colVersion: remoteColVersion,
                        dbVersion: remoteDbVersion,
                        nodeId: remoteNodeId,
                        localDbVersion: remoteLocalDbVersion
                    )

                    // Update version boundaries
                    if remoteLocalDbVersion < record.lowestLocalDbVersion {
                        record.lowestLocalDbVersion = remoteLocalDbVersion
                    }
                    if remoteLocalDbVersion > record.highestLocalDbVersion {
                        record.highestLocalDbVersion = remoteLocalDbVersion
                    }

                    // Persist the mutated record to the data dictionary
                    data[recordId] = record
                }
            }
        }
    }

    private func isRecordTombstoned(_ recordId: K, ignoreParent: Bool) -> Bool {
        if tombstones.find(recordId) != nil {
            return true
        }

        if !ignoreParent, let parent = parent {
            return parent.isRecordTombstoned(recordId, ignoreParent: false)
        }

        return false
    }

    private func getOrCreateRecordUnchecked(_ recordId: K, ignoreParent: Bool) -> Record<C, V> {
        if let existing = data[recordId] {
            return existing
        }

        let record: Record<C, V>

        if !ignoreParent, let parent = parent, let parentRecord = parent.getRecordPtr(recordId, ignoreParent: false) {
            record = parentRecord
        } else {
            record = Record()
        }

        data[recordId] = record
        return record
    }

    private func getRecordPtr(_ recordId: K, ignoreParent: Bool) -> Record<C, V>? {
        if let record = data[recordId] {
            return record
        }

        if !ignoreParent, let parent = parent {
            return parent.getRecordPtr(recordId, ignoreParent: false)
        }

        return nil
    }
}

// MARK: - Codable Support

extension CRDT: Codable where K: Codable, C: Codable, V: Codable {
    private enum CodingKeys: String, CodingKey {
        case nodeId
        case clock
        case data
        case tombstones
        case baseVersion
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(nodeId, forKey: .nodeId)
        try container.encode(clock, forKey: .clock)
        try container.encode(data, forKey: .data)
        try container.encode(tombstones, forKey: .tombstones)
        try container.encode(baseVersion, forKey: .baseVersion)
        // Note: parent is not serialized
    }

    public convenience init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let nodeId = try container.decode(NodeId.self, forKey: .nodeId)
        let clock = try container.decode(LogicalClock.self, forKey: .clock)
        let data = try container.decode([K: Record<C, V>].self, forKey: .data)
        let tombstones = try container.decode(TombstoneStorage<K>.self, forKey: .tombstones)
        let baseVersion = try container.decode(UInt64.self, forKey: .baseVersion)

        self.init(nodeId: nodeId, parent: nil)
        self.clock = clock
        self.data = data
        self.tombstones = tombstones
        self.baseVersion = baseVersion
        // Note: parent is always nil after deserialization
    }
}

// MARK: - JSON Convenience Methods

extension CRDT {
    /// Serializes the CRDT to a JSON string.
    ///
    /// Note: The parent relationship is not serialized and must be rebuilt after deserialization.
    public func toJSON() throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = .prettyPrinted
        let data = try encoder.encode(self)
        guard let json = String(data: data, encoding: .utf8) else {
            throw NSError(domain: "CRDTLite", code: -1, userInfo: [NSLocalizedDescriptionKey: "Failed to convert data to string"])
        }
        return json
    }

    /// Deserializes a CRDT from a JSON string.
    ///
    /// Note: The parent relationship is not deserialized and will be `nil`.
    /// Applications must rebuild parent-child relationships if needed.
    public static func fromJSON(_ json: String) throws -> CRDT<K, C, V> {
        guard let data = json.data(using: .utf8) else {
            throw NSError(domain: "CRDTLite", code: -1, userInfo: [NSLocalizedDescriptionKey: "Failed to convert string to data"])
        }
        let decoder = JSONDecoder()
        return try decoder.decode(CRDT<K, C, V>.self, from: data)
    }
}
