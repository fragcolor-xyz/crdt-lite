// CRDTTests.swift
// Comprehensive test suite for CRDT implementation
// Tests match the Rust and C++ test suites for compatibility

import XCTest
@testable import CRDTLite

final class CRDTTests: XCTestCase {

    // MARK: - Logical Clock Tests

    func testLogicalClock() {
        var clock = LogicalClock()
        XCTAssertEqual(clock.currentTime(), 0)

        let t1 = clock.tick()
        XCTAssertEqual(t1, 1)
        XCTAssertEqual(clock.currentTime(), 1)

        let t2 = clock.update(5)
        XCTAssertEqual(t2, 6)
        XCTAssertEqual(clock.currentTime(), 6)

        // Test that update with lower value still increments
        let t3 = clock.update(3)
        XCTAssertEqual(t3, 7)
        XCTAssertEqual(clock.currentTime(), 7)
    }

    // MARK: - Tombstone Storage Tests

    func testTombstoneStorage() {
        var storage = TombstoneStorage<String>()
        let info = TombstoneInfo(dbVersion: 10, nodeId: 1, localDbVersion: 10)

        storage.insertOrAssign("key1", info)
        XCTAssertEqual(storage.count, 1)

        XCTAssertEqual(storage.find("key1"), info)
        XCTAssertNil(storage.find("key2"))

        let removed = storage.compact(minAcknowledgedVersion: 15)
        XCTAssertEqual(removed, 1)
        XCTAssertEqual(storage.count, 0)
    }

    func testTombstoneStorageCompaction() {
        var storage = TombstoneStorage<String>()

        storage.insertOrAssign("old1", TombstoneInfo(dbVersion: 5, nodeId: 1, localDbVersion: 5))
        storage.insertOrAssign("old2", TombstoneInfo(dbVersion: 8, nodeId: 1, localDbVersion: 8))
        storage.insertOrAssign("new1", TombstoneInfo(dbVersion: 15, nodeId: 1, localDbVersion: 15))
        storage.insertOrAssign("new2", TombstoneInfo(dbVersion: 20, nodeId: 1, localDbVersion: 20))

        XCTAssertEqual(storage.count, 4)

        // Compact tombstones older than version 10
        let removed = storage.compact(minAcknowledgedVersion: 10)
        XCTAssertEqual(removed, 2)
        XCTAssertEqual(storage.count, 2)

        // Old tombstones should be gone
        XCTAssertNil(storage.find("old1"))
        XCTAssertNil(storage.find("old2"))

        // New tombstones should remain
        XCTAssertNotNil(storage.find("new1"))
        XCTAssertNotNil(storage.find("new2"))
    }

    // MARK: - Basic Insert Tests

    func testBasicInsert() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let fields = [
            ("name", "Alice"),
            ("age", "30")
        ]

        let changes = crdt.insertOrUpdate("user1", fields: fields)

        XCTAssertEqual(changes.count, 2)
        XCTAssertEqual(crdt.getData().count, 1)

        let record = crdt.getRecord("user1")
        XCTAssertNotNil(record)
        XCTAssertEqual(record?.fields["name"], "Alice")
        XCTAssertEqual(record?.fields["age"], "30")
    }

    func testInsertMultipleRecords() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice")])
        let _ = crdt.insertOrUpdate("user2", fields: [("name", "Bob")])
        let _ = crdt.insertOrUpdate("user3", fields: [("name", "Charlie")])

        XCTAssertEqual(crdt.getData().count, 3)
        XCTAssertEqual(crdt.getRecord("user1")?.fields["name"], "Alice")
        XCTAssertEqual(crdt.getRecord("user2")?.fields["name"], "Bob")
        XCTAssertEqual(crdt.getRecord("user3")?.fields["name"], "Charlie")
    }

    func testUpdateExistingRecord() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice"), ("age", "30")])
        let _ = crdt.insertOrUpdate("user1", fields: [("age", "31")])

        let record = crdt.getRecord("user1")
        XCTAssertEqual(record?.fields["name"], "Alice")
        XCTAssertEqual(record?.fields["age"], "31")

        // Check that column version was incremented
        let ageVersion = record?.columnVersions["age"]
        XCTAssertEqual(ageVersion?.colVersion, 2)
    }

    // MARK: - Delete Tests

    func testDeleteRecord() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user2", fields: [("name", "Bob")])

        let deleteChange = crdt.deleteRecord("user2")
        XCTAssertNotNil(deleteChange)
        XCTAssertTrue(crdt.isTombstoned("user2"))
        XCTAssertEqual(crdt.getData().count, 0)
        XCTAssertEqual(crdt.tombstoneCount(), 1)
    }

    func testDeleteNonExistentRecord() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let deleteChange = crdt.deleteRecord("nonexistent")
        XCTAssertNotNil(deleteChange)
        XCTAssertTrue(crdt.isTombstoned("nonexistent"))
    }

    func testInsertAfterDelete() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice")])
        let _ = crdt.deleteRecord("user1")

        // Try to insert after delete - should be ignored
        let changes = crdt.insertOrUpdate("user1", fields: [("name", "Bob")])
        XCTAssertEqual(changes.count, 0)
        XCTAssertTrue(crdt.isTombstoned("user1"))
        XCTAssertNil(crdt.getRecord("user1"))
    }

    func testDeleteField() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice"), ("age", "30"), ("city", "NYC")])

        let deleteChange = crdt.deleteField("user1", fieldName: "age")
        XCTAssertNotNil(deleteChange)

        let record = crdt.getRecord("user1")
        XCTAssertEqual(record?.fields["name"], "Alice")
        XCTAssertNil(record?.fields["age"])
        XCTAssertEqual(record?.fields["city"], "NYC")

        // Column version should still exist (field tombstone)
        XCTAssertNotNil(record?.columnVersions["age"])
    }

    func testDeleteFieldFromTombstonedRecord() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice")])
        let _ = crdt.deleteRecord("user1")

        let deleteChange = crdt.deleteField("user1", fieldName: "name")
        XCTAssertNil(deleteChange)
    }

    // MARK: - Merge Tests

    func testMergeChanges() {
        let crdt1 = CRDT<String, String, String>(nodeId: 1)
        let crdt2 = CRDT<String, String, String>(nodeId: 2)

        let changes1 = crdt1.insertOrUpdate("record1", fields: [("tag", "Node1")])
        let changes2 = crdt2.insertOrUpdate("record1", fields: [("tag", "Node2")])

        let mergeRule = DefaultMergeRule()
        crdt1.mergeChanges(changes2, mergeRule: mergeRule)
        crdt2.mergeChanges(changes1, mergeRule: mergeRule)

        // Node2 has higher nodeId, so its value should win
        XCTAssertEqual(crdt1.getRecord("record1")?.fields["tag"], "Node2")
        XCTAssertEqual(crdt2.getRecord("record1")?.fields["tag"], "Node2")
    }

    func testMergeConcurrentUpdates() {
        let crdt1 = CRDT<String, String, String>(nodeId: 1)
        let crdt2 = CRDT<String, String, String>(nodeId: 2)

        // Both nodes update different fields of the same record
        let changes1 = crdt1.insertOrUpdate("user1", fields: [("name", "Alice"), ("age", "30")])
        let changes2 = crdt2.insertOrUpdate("user1", fields: [("city", "NYC")])

        let mergeRule = DefaultMergeRule()
        crdt1.mergeChanges(changes2, mergeRule: mergeRule)
        crdt2.mergeChanges(changes1, mergeRule: mergeRule)

        // Both CRDTs should have all fields
        let record1 = crdt1.getRecord("user1")
        XCTAssertEqual(record1?.fields["name"], "Alice")
        XCTAssertEqual(record1?.fields["age"], "30")
        XCTAssertEqual(record1?.fields["city"], "NYC")

        let record2 = crdt2.getRecord("user1")
        XCTAssertEqual(record2?.fields["name"], "Alice")
        XCTAssertEqual(record2?.fields["age"], "30")
        XCTAssertEqual(record2?.fields["city"], "NYC")
    }

    func testMergeConflictResolution() {
        let crdt1 = CRDT<String, String, String>(nodeId: 1)
        let crdt2 = CRDT<String, String, String>(nodeId: 2)

        // Both nodes update the same field
        let changes1 = crdt1.insertOrUpdate("user1", fields: [("status", "online")])
        let changes2 = crdt2.insertOrUpdate("user1", fields: [("status", "offline")])

        let mergeRule = DefaultMergeRule()
        crdt1.mergeChanges(changes2, mergeRule: mergeRule)
        crdt2.mergeChanges(changes1, mergeRule: mergeRule)

        // Node2 should win (higher node ID with same version)
        XCTAssertEqual(crdt1.getRecord("user1")?.fields["status"], "offline")
        XCTAssertEqual(crdt2.getRecord("user1")?.fields["status"], "offline")
    }

    func testMergeDeletePropagation() {
        let crdt1 = CRDT<String, String, String>(nodeId: 1)
        let crdt2 = CRDT<String, String, String>(nodeId: 2)

        // Node 1 creates a record
        let changes1 = crdt1.insertOrUpdate("user1", fields: [("name", "Alice")])

        // Node 2 receives the record
        let mergeRule = DefaultMergeRule()
        crdt2.mergeChanges(changes1, mergeRule: mergeRule)

        // Node 1 deletes the record
        if let deleteChange = crdt1.deleteRecord("user1") {
            // Node 2 receives the deletion
            crdt2.mergeChanges([deleteChange], mergeRule: mergeRule)
        }

        // Both should have the record tombstoned
        XCTAssertTrue(crdt1.isTombstoned("user1"))
        XCTAssertTrue(crdt2.isTombstoned("user1"))
        XCTAssertNil(crdt2.getRecord("user1"))
    }

    func testMergeFieldDeletion() {
        let crdt1 = CRDT<String, String, String>(nodeId: 1)
        let crdt2 = CRDT<String, String, String>(nodeId: 2)

        // Both create the same record
        let _ = crdt1.insertOrUpdate("user1", fields: [("name", "Alice"), ("age", "30")])
        let _ = crdt2.insertOrUpdate("user1", fields: [("name", "Alice"), ("age", "30")])

        // Node 1 deletes a field
        if let deleteChange = crdt1.deleteField("user1", fieldName: "age") {
            // Node 2 receives the deletion
            let mergeRule = DefaultMergeRule()
            crdt2.mergeChanges([deleteChange], mergeRule: mergeRule)
        }

        // Both should have the field deleted
        XCTAssertNil(crdt1.getRecord("user1")?.fields["age"])
        XCTAssertNil(crdt2.getRecord("user1")?.fields["age"])
        XCTAssertEqual(crdt1.getRecord("user1")?.fields["name"], "Alice")
        XCTAssertEqual(crdt2.getRecord("user1")?.fields["name"], "Alice")
    }

    // MARK: - Sync Tests

    func testGetChangesSince() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice")])
        let initialVersion = crdt.getClock().currentTime()

        let _ = crdt.insertOrUpdate("user2", fields: [("name", "Bob")])
        let _ = crdt.insertOrUpdate("user3", fields: [("name", "Charlie")])

        let changes = crdt.getChangesSince(initialVersion)

        // Should get changes for user2 and user3 only
        XCTAssertEqual(changes.count, 2)

        let recordIds = Set(changes.map { $0.recordId })
        XCTAssertTrue(recordIds.contains("user2"))
        XCTAssertTrue(recordIds.contains("user3"))
        XCTAssertFalse(recordIds.contains("user1"))
    }

    func testGetChangesSinceExcluding() {
        let crdt1 = CRDT<String, String, String>(nodeId: 1)
        let crdt2 = CRDT<String, String, String>(nodeId: 2)

        let changes1 = crdt1.insertOrUpdate("record1", fields: [("from", "node1")])
        let changes2 = crdt2.insertOrUpdate("record2", fields: [("from", "node2")])

        let mergeRule = DefaultMergeRule()
        crdt1.mergeChanges(changes2, mergeRule: mergeRule)

        // Get changes excluding node 2's changes
        let changes = crdt1.getChangesSinceExcluding(0, excluding: Set([2]))

        // Should only get node 1's changes
        XCTAssertEqual(changes.count, 1)
        XCTAssertEqual(changes.first?.recordId, "record1")
    }

    func testSyncBetweenNodes() {
        let crdt1 = CRDT<String, String, String>(nodeId: 1)
        let crdt2 = CRDT<String, String, String>(nodeId: 2)

        // Node 1 makes some changes
        let _ = crdt1.insertOrUpdate("user1", fields: [("name", "Alice")])
        let _ = crdt1.insertOrUpdate("user2", fields: [("name", "Bob")])

        // Sync to node 2
        var lastSyncVersion: UInt64 = 0
        let changes1 = crdt1.getChangesSince(lastSyncVersion)
        let mergeRule = DefaultMergeRule()
        crdt2.mergeChanges(changes1, mergeRule: mergeRule)
        lastSyncVersion = crdt1.getClock().currentTime()

        // Node 2 makes some changes
        let _ = crdt2.insertOrUpdate("user3", fields: [("name", "Charlie")])

        // Sync back to node 1
        let changes2 = crdt2.getChangesSince(lastSyncVersion)
        crdt1.mergeChanges(changes2, mergeRule: mergeRule)

        // Both nodes should have all records
        XCTAssertEqual(crdt1.getData().count, 3)
        XCTAssertEqual(crdt2.getData().count, 3)
        XCTAssertNotNil(crdt1.getRecord("user3"))
        XCTAssertNotNil(crdt2.getRecord("user1"))
    }

    // MARK: - Change Compression Tests

    func testCompressChanges() {
        var changes: [Change<String, String, String>] = [
            Change(recordId: "user1", colName: "name", value: "Alice", colVersion: 1, dbVersion: 1, nodeId: 1, localDbVersion: 1),
            Change(recordId: "user1", colName: "name", value: "Bob", colVersion: 2, dbVersion: 2, nodeId: 1, localDbVersion: 2),
            Change(recordId: "user1", colName: "age", value: "30", colVersion: 1, dbVersion: 3, nodeId: 1, localDbVersion: 3),
        ]

        CRDT<String, String, String>.compressChanges(&changes)

        // Should compress to 2 changes (latest name update and age update)
        XCTAssertEqual(changes.count, 2)

        // Latest name change should be kept
        let nameChange = changes.first { $0.colName == "name" }
        XCTAssertEqual(nameChange?.value, "Bob")
        XCTAssertEqual(nameChange?.colVersion, 2)

        // Age change should be kept
        let ageChange = changes.first { $0.colName == "age" }
        XCTAssertEqual(ageChange?.value, "30")
    }

    func testCompressChangesWithDeletion() {
        var changes: [Change<String, String, String>] = [
            Change(recordId: "user1", colName: "name", value: "Alice", colVersion: 1, dbVersion: 1, nodeId: 1, localDbVersion: 1),
            Change(recordId: "user1", colName: "age", value: "30", colVersion: 1, dbVersion: 2, nodeId: 1, localDbVersion: 2),
            Change(recordId: "user1", colName: nil, value: nil, colVersion: 1, dbVersion: 3, nodeId: 1, localDbVersion: 3),
        ]

        CRDT<String, String, String>.compressChanges(&changes)

        // Should compress to 1 change (just the deletion)
        XCTAssertEqual(changes.count, 1)
        XCTAssertNil(changes.first?.colName)
    }

    func testCompressChangesMultipleRecords() {
        var changes: [Change<String, String, String>] = [
            Change(recordId: "user1", colName: "name", value: "Alice", colVersion: 1, dbVersion: 1, nodeId: 1, localDbVersion: 1),
            Change(recordId: "user2", colName: "name", value: "Bob", colVersion: 1, dbVersion: 2, nodeId: 1, localDbVersion: 2),
            Change(recordId: "user1", colName: "name", value: "Alicia", colVersion: 2, dbVersion: 3, nodeId: 1, localDbVersion: 3),
        ]

        CRDT<String, String, String>.compressChanges(&changes)

        // Should keep latest change for each record
        XCTAssertEqual(changes.count, 2)

        let user1Changes = changes.filter { $0.recordId == "user1" }
        XCTAssertEqual(user1Changes.count, 1)
        XCTAssertEqual(user1Changes.first?.value, "Alicia")

        let user2Changes = changes.filter { $0.recordId == "user2" }
        XCTAssertEqual(user2Changes.count, 1)
        XCTAssertEqual(user2Changes.first?.value, "Bob")
    }

    // MARK: - JSON Serialization Tests

    func testJSONSerialization() throws {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        // Add some data
        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice"), ("age", "30")])
        let _ = crdt.insertOrUpdate("user2", fields: [("name", "Bob")])
        let _ = crdt.deleteRecord("user2")

        // Serialize to JSON
        let json = try crdt.toJSON()

        // Deserialize
        let deserialized = try CRDT<String, String, String>.fromJSON(json)

        // Verify data is preserved
        XCTAssertEqual(crdt.getData().count, deserialized.getData().count)
        XCTAssertEqual(crdt.getRecord("user1")?.fields, deserialized.getRecord("user1")?.fields)

        // Verify tombstones are preserved
        XCTAssertEqual(crdt.tombstoneCount(), deserialized.tombstoneCount())
        XCTAssertTrue(deserialized.isTombstoned("user2"))

        // Verify clock is preserved
        XCTAssertEqual(crdt.getClock().currentTime(), deserialized.getClock().currentTime())
    }

    func testJSONSerializationWithComplexTypes() throws {
        struct UserData: Codable, Equatable {
            let name: String
            let age: Int
        }

        let crdt = CRDT<String, String, UserData>(nodeId: 1)

        let alice = UserData(name: "Alice", age: 30)
        let bob = UserData(name: "Bob", age: 25)

        let _ = crdt.insertOrUpdate("user1", fields: [("data", alice)])
        let _ = crdt.insertOrUpdate("user2", fields: [("data", bob)])

        // Serialize to JSON
        let json = try crdt.toJSON()

        // Deserialize
        let deserialized = try CRDT<String, String, UserData>.fromJSON(json)

        // Verify data is preserved
        XCTAssertEqual(crdt.getData().count, deserialized.getData().count)
        XCTAssertEqual(crdt.getRecord("user1")?.fields["data"], deserialized.getRecord("user1")?.fields["data"])
        XCTAssertEqual(crdt.getRecord("user2")?.fields["data"], deserialized.getRecord("user2")?.fields["data"])
    }

    func testCodableRoundTrip() throws {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice"), ("age", "30")])
        let _ = crdt.insertOrUpdate("user2", fields: [("name", "Bob")])

        // Encode
        let encoder = JSONEncoder()
        let data = try encoder.encode(crdt)

        // Decode
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(CRDT<String, String, String>.self, from: data)

        // Verify
        XCTAssertEqual(crdt.getData().count, decoded.getData().count)
        XCTAssertEqual(crdt.getRecord("user1")?.fields, decoded.getRecord("user1")?.fields)
        XCTAssertEqual(crdt.getRecord("user2")?.fields, decoded.getRecord("user2")?.fields)
    }

    // MARK: - Edge Cases and Error Conditions

    func testEmptyChangesArray() {
        let crdt = CRDT<String, String, String>(nodeId: 1)
        let mergeRule = DefaultMergeRule()

        let accepted = crdt.mergeChanges([], mergeRule: mergeRule)
        XCTAssertEqual(accepted.count, 0)
    }

    func testCompressEmptyChanges() {
        var changes: [Change<String, String, String>] = []
        CRDT<String, String, String>.compressChanges(&changes)
        XCTAssertEqual(changes.count, 0)
    }

    func testClockUpdateWithLowerValue() {
        var clock = LogicalClock()
        let _ = clock.tick()
        let _ = clock.tick()
        let _ = clock.tick()

        XCTAssertEqual(clock.currentTime(), 3)

        // Update with lower value should still increment
        let result = clock.update(1)
        XCTAssertEqual(result, 4)
        XCTAssertEqual(clock.currentTime(), 4)
    }

    func testMultipleFieldUpdates() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [
            ("name", "Alice"),
            ("age", "30"),
            ("city", "NYC"),
            ("country", "USA")
        ])

        let record = crdt.getRecord("user1")
        XCTAssertEqual(record?.fields.count, 4)
        XCTAssertEqual(record?.columnVersions.count, 4)
    }

    func testVersionBoundaries() {
        let crdt = CRDT<String, String, String>(nodeId: 1)

        let _ = crdt.insertOrUpdate("user1", fields: [("name", "Alice")])
        let _ = crdt.insertOrUpdate("user1", fields: [("age", "30")])

        let record = crdt.getRecord("user1")

        // Verify version boundaries are tracked
        XCTAssertGreaterThan(record!.highestLocalDbVersion, 0)
        XCTAssertLessThanOrEqual(record!.lowestLocalDbVersion, record!.highestLocalDbVersion)
    }
}
