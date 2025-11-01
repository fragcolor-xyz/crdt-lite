// SwiftExample.swift
// Example usage of CRDT-Lite Swift implementation

import Foundation
import CRDTLite

// MARK: - Basic Usage Example

func basicExample() {
    print("=== Basic CRDT Example ===\n")

    // Create a CRDT instance
    let crdt = CRDT<String, String, String>(nodeId: 1)

    // Insert a record
    let changes = crdt.insertOrUpdate("user1", fields: [
        ("name", "Alice"),
        ("age", "30"),
        ("city", "New York")
    ])

    print("Created \(changes.count) changes")

    // Query the record
    if let record = crdt.getRecord("user1") {
        print("Record fields:")
        for (field, value) in record.fields {
            print("  \(field): \(value)")
        }
    }

    // Update a field
    let _ = crdt.insertOrUpdate("user1", fields: [("age", "31")])
    print("\nUpdated age to: \(crdt.getRecord("user1")?.fields["age"] ?? "")")

    // Delete a field
    let _ = crdt.deleteField("user1", fieldName: "city")
    print("Deleted city field")
    print("Remaining fields: \(crdt.getRecord("user1")?.fields.keys.joined(separator: ", "))")

    print("\n")
}

// MARK: - Sync Between Nodes Example

func syncExample() {
    print("=== Sync Between Nodes Example ===\n")

    // Create two nodes
    let node1 = CRDT<String, String, String>(nodeId: 1)
    let node2 = CRDT<String, String, String>(nodeId: 2)

    // Node 1 creates some records
    let _ = node1.insertOrUpdate("doc1", fields: [("title", "Document 1"), ("author", "Alice")])
    let _ = node1.insertOrUpdate("doc2", fields: [("title", "Document 2"), ("author", "Bob")])

    print("Node 1 has \(node1.getData().count) records")

    // Sync to Node 2
    var lastSyncVersion: UInt64 = 0
    let changes1 = node1.getChangesSince(lastSyncVersion)
    print("Syncing \(changes1.count) changes from Node 1 to Node 2")

    let mergeRule = DefaultMergeRule()
    let _ = node2.mergeChanges(changes1, mergeRule: mergeRule)
    lastSyncVersion = node1.getClock().currentTime()

    print("Node 2 now has \(node2.getData().count) records")

    // Node 2 makes changes
    let _ = node2.insertOrUpdate("doc3", fields: [("title", "Document 3"), ("author", "Charlie")])
    let _ = node2.insertOrUpdate("doc1", fields: [("status", "reviewed")])

    // Sync back to Node 1
    let changes2 = node2.getChangesSince(lastSyncVersion)
    print("\nSyncing \(changes2.count) changes from Node 2 to Node 1")

    let _ = node1.mergeChanges(changes2, mergeRule: mergeRule)

    print("Node 1 now has \(node1.getData().count) records")
    print("doc1 on Node 1 has fields: \(node1.getRecord("doc1")?.fields.keys.joined(separator: ", ") ?? "")")

    print("\n")
}

// MARK: - Conflict Resolution Example

func conflictExample() {
    print("=== Conflict Resolution Example ===\n")

    // Create two nodes
    let node1 = CRDT<String, String, String>(nodeId: 1)
    let node2 = CRDT<String, String, String>(nodeId: 2)

    // Both nodes start with the same document
    let initialChanges = node1.insertOrUpdate("doc1", fields: [("status", "draft")])
    let mergeRule = DefaultMergeRule()
    let _ = node2.mergeChanges(initialChanges, mergeRule: mergeRule)

    print("Initial status: \(node1.getRecord("doc1")?.fields["status"] ?? "")")

    // Both nodes update the same field (conflict!)
    let _ = node1.insertOrUpdate("doc1", fields: [("status", "review")])
    let _ = node2.insertOrUpdate("doc1", fields: [("status", "published")])

    print("\nNode 1 status: \(node1.getRecord("doc1")?.fields["status"] ?? "")")
    print("Node 2 status: \(node2.getRecord("doc1")?.fields["status"] ?? "")")

    // Exchange changes
    let changes1 = node1.getChangesSince(0)
    let changes2 = node2.getChangesSince(0)

    let _ = node1.mergeChanges(changes2, mergeRule: mergeRule)
    let _ = node2.mergeChanges(changes1, mergeRule: mergeRule)

    // Both nodes should converge (Node 2 wins with higher node ID)
    print("\nAfter sync:")
    print("Node 1 status: \(node1.getRecord("doc1")?.fields["status"] ?? "")")
    print("Node 2 status: \(node2.getRecord("doc1")?.fields["status"] ?? "")")

    print("\n")
}

// MARK: - JSON Serialization Example

func serializationExample() {
    print("=== JSON Serialization Example ===\n")

    // Create and populate a CRDT
    let crdt = CRDT<String, String, String>(nodeId: 1)
    let _ = crdt.insertOrUpdate("user1", fields: [
        ("name", "Alice"),
        ("email", "alice@example.com")
    ])
    let _ = crdt.insertOrUpdate("user2", fields: [
        ("name", "Bob"),
        ("email", "bob@example.com")
    ])

    // Delete one record
    let _ = crdt.deleteRecord("user2")

    print("Original CRDT:")
    print("  Records: \(crdt.getData().count)")
    print("  Tombstones: \(crdt.tombstoneCount())")
    print("  Clock: \(crdt.getClock().currentTime())")

    do {
        // Serialize to JSON
        let json = try crdt.toJSON()
        print("\nJSON size: \(json.count) characters")

        // Deserialize
        let restored = try CRDT<String, String, String>.fromJSON(json)

        print("\nRestored CRDT:")
        print("  Records: \(restored.getData().count)")
        print("  Tombstones: \(restored.tombstoneCount())")
        print("  Clock: \(restored.getClock().currentTime())")

        // Verify data
        if let record = restored.getRecord("user1") {
            print("\nuser1 data preserved:")
            print("  name: \(record.fields["name"] ?? "")")
            print("  email: \(record.fields["email"] ?? "")")
        }

        print("\nuser2 is tombstoned: \(restored.isTombstoned("user2"))")

    } catch {
        print("Serialization error: \(error)")
    }

    print("\n")
}

// MARK: - Tombstone Compaction Example

func tombstoneCompactionExample() {
    print("=== Tombstone Compaction Example ===\n")

    let crdt = CRDT<String, String, String>(nodeId: 1)

    // Create and delete several records
    for i in 1...10 {
        let _ = crdt.insertOrUpdate("doc\(i)", fields: [("content", "Document \(i)")])
    }

    // Delete half of them
    for i in 1...5 {
        let _ = crdt.deleteRecord("doc\(i)")
    }

    print("Records: \(crdt.getData().count)")
    print("Tombstones: \(crdt.tombstoneCount())")

    // Simulate all nodes acknowledging up to version 10
    let minAcknowledgedVersion: UInt64 = 10
    let removedCount = crdt.compactTombstones(minAcknowledgedVersion: minAcknowledgedVersion)

    print("\nAfter compaction (min version: \(minAcknowledgedVersion)):")
    print("Tombstones removed: \(removedCount)")
    print("Remaining tombstones: \(crdt.tombstoneCount())")

    print("\n")
}

// MARK: - Change Compression Example

func compressionExample() {
    print("=== Change Compression Example ===\n")

    let crdt = CRDT<String, String, String>(nodeId: 1)

    // Make many updates to the same record
    for i in 1...5 {
        let _ = crdt.insertOrUpdate("doc1", fields: [("version", "\(i)")])
    }

    var changes = crdt.getChangesSince(0)
    print("Uncompressed changes: \(changes.count)")

    // Compress changes
    CRDT<String, String, String>.compressChanges(&changes)
    print("Compressed changes: \(changes.count)")

    // The compressed changes should only contain the latest version
    if let lastChange = changes.first {
        print("Latest version: \(lastChange.value ?? "")")
    }

    print("\n")
}

// MARK: - Complex Data Types Example

func complexTypesExample() {
    print("=== Complex Data Types Example ===\n")

    // Define a complex data type
    struct UserProfile: Codable, Equatable {
        let name: String
        let age: Int
        let tags: [String]
    }

    // Create a CRDT with complex values
    let crdt = CRDT<String, String, UserProfile>(nodeId: 1)

    let alice = UserProfile(name: "Alice", age: 30, tags: ["developer", "swift"])
    let bob = UserProfile(name: "Bob", age: 25, tags: ["designer", "ui"])

    let _ = crdt.insertOrUpdate("user1", fields: [("profile", alice)])
    let _ = crdt.insertOrUpdate("user2", fields: [("profile", bob)])

    print("Stored \(crdt.getData().count) user profiles")

    if let aliceRecord = crdt.getRecord("user1") {
        if let profile = aliceRecord.fields["profile"] {
            print("\nAlice's profile:")
            print("  Name: \(profile.name)")
            print("  Age: \(profile.age)")
            print("  Tags: \(profile.tags.joined(separator: ", "))")
        }
    }

    print("\n")
}

// MARK: - Main

print("CRDT-Lite Swift Examples\n")
print("========================\n\n")

basicExample()
syncExample()
conflictExample()
serializationExample()
tombstoneCompactionExample()
compressionExample()
complexTypesExample()

print("All examples completed!")
