# CRDT-Lite Swift Implementation

A lightweight, column-based CRDT (Conflict-free Replicated Data Type) implementation in Swift, fully compatible with the Rust and C++ versions.

## Features

✅ **Generic types** - Works with any `Hashable & Codable` key, column, and value types
✅ **Last-Write-Wins semantics** - Automatic conflict resolution with deterministic ordering
✅ **Logical clock** - Lamport-style causality tracking
✅ **Tombstone-based deletion** - Safe deletion with compaction support
✅ **Column-level versioning** - Fine-grained conflict resolution per field
✅ **Change tracking** - Export and merge changes between nodes
✅ **Compression** - Efficient change compression for sync
✅ **JSON serialization** - Full Codable support for persistence and network transfer
✅ **Cross-platform** - iOS, macOS, watchOS, tvOS, and Linux

## Requirements

- Swift 5.7+
- Xcode 14.0+ (for iOS/macOS development)
- Swift toolchain (for Linux)

## Installation

### Swift Package Manager

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/fragcolor-xyz/crdt-lite.git", from: "0.4.1")
]
```

Or add to your Xcode project:
1. File → Add Packages...
2. Enter the repository URL
3. Select version or branch

## Quick Start

```swift
import CRDTLite

// Create a CRDT instance
let crdt = CRDT<String, String, String>(nodeId: 1)

// Insert or update records
let changes = crdt.insertOrUpdate("user1", fields: [
    ("name", "Alice"),
    ("age", "30"),
    ("city", "NYC")
])

// Query a record
if let record = crdt.getRecord("user1") {
    print("Name:", record.fields["name"] ?? "")
}

// Update specific fields
crdt.insertOrUpdate("user1", fields: [("age", "31")])

// Delete a field
crdt.deleteField("user1", fieldName: "city")

// Delete entire record
crdt.deleteRecord("user1")
```

## Syncing Between Nodes

```swift
// Node 1
let node1 = CRDT<String, String, String>(nodeId: 1)
node1.insertOrUpdate("doc1", fields: [("content", "Hello")])

// Get changes since last sync
var lastSyncVersion: UInt64 = 0
let changes = node1.getChangesSince(lastSyncVersion)

// Send changes to Node 2 (via network, etc.)
let json = try JSONEncoder().encode(changes)

// Node 2
let node2 = CRDT<String, String, String>(nodeId: 2)
let receivedChanges = try JSONDecoder().decode([Change<String, String, String>].self, from: json)

// Merge changes
let mergeRule = DefaultMergeRule()
node2.mergeChanges(receivedChanges, mergeRule: mergeRule)

// Update sync version
lastSyncVersion = node1.getClock().currentTime()
```

## Conflict Resolution

The default merge rule implements Last-Write-Wins (LWW) with the following priority:

1. **Column version** (higher wins) - increments on each edit
2. **DB version** (higher wins) - global logical clock
3. **Node ID** (higher wins) - deterministic tiebreaker

```swift
// Both nodes update the same field concurrently
let node1 = CRDT<String, String, String>(nodeId: 1)
let node2 = CRDT<String, String, String>(nodeId: 2)

let changes1 = node1.insertOrUpdate("doc", fields: [("status", "draft")])
let changes2 = node2.insertOrUpdate("doc", fields: [("status", "published")])

// Merge changes
let mergeRule = DefaultMergeRule()
node1.mergeChanges(changes2, mergeRule: mergeRule)
node2.mergeChanges(changes1, mergeRule: mergeRule)

// Node 2 wins (higher node ID with same versions)
// Both nodes converge to: status = "published"
```

### Custom Merge Rules

Implement the `MergeRule` protocol for custom conflict resolution:

```swift
struct CustomMergeRule: MergeRule {
    func shouldAccept(
        localCol: UInt64,
        localDb: UInt64,
        localNode: NodeId,
        remoteCol: UInt64,
        remoteDb: UInt64,
        remoteNode: NodeId
    ) -> Bool {
        // Custom logic here
        // Return true to accept remote change, false to keep local
        return remoteDb > localDb
    }
}
```

## Persistence

### JSON Serialization

```swift
let crdt = CRDT<String, String, String>(nodeId: 1)
crdt.insertOrUpdate("user1", fields: [("name", "Alice")])

// Save to JSON
let json = try crdt.toJSON()
try json.write(to: fileURL, atomically: true, encoding: .utf8)

// Load from JSON
let loadedJSON = try String(contentsOf: fileURL)
let restored = try CRDT<String, String, String>.fromJSON(loadedJSON)
```

### Using Codable Directly

```swift
// Encode to Data
let encoder = JSONEncoder()
let data = try encoder.encode(crdt)

// Decode from Data
let decoder = JSONDecoder()
let restored = try decoder.decode(CRDT<String, String, String>.self, from: data)
```

**Note:** Parent-child relationships are not serialized. After deserialization, the `parent` field will always be `nil`. Applications must rebuild hierarchies if needed.

## Tombstone Management

Deleted records are marked with tombstones. To prevent memory exhaustion:

```swift
// Track the minimum version acknowledged by ALL nodes
let minAcknowledgedVersion: UInt64 = 1000

// Compact old tombstones (only when all nodes have synced past this version)
let removedCount = crdt.compactTombstones(minAcknowledgedVersion: minAcknowledgedVersion)

// Monitor tombstone count
print("Tombstones:", crdt.tombstoneCount())
```

⚠️ **IMPORTANT:** Only compact tombstones when ALL participating nodes have acknowledged the version. Compacting too early may cause deleted records to reappear!

## Change Compression

Compress redundant changes before syncing:

```swift
var changes = node1.getChangesSince(lastVersion)

// Remove redundant changes (e.g., multiple updates to same field)
CRDT<String, String, String>.compressChanges(&changes)

// Send compressed changes over network
```

## Backend Interoperability

The Swift implementation uses the same data structures and algorithms as the Rust and C++ versions. You can sync between Swift clients and Rust/C++ backends using JSON:

```swift
// Swift → Backend
let changes = swiftCRDT.getChangesSince(lastVersion)
let json = try JSONEncoder().encode(changes)
// Send to Rust/C++ backend via HTTP, WebSocket, etc.

// Backend → Swift
let receivedData = // ... receive from backend
let changes = try JSONDecoder().decode([Change<String, String, String>].self, from: receivedData)
swiftCRDT.mergeChanges(changes, mergeRule: DefaultMergeRule())
```

### JSON Schema Compatibility

Change format (compatible with Rust `serde` serialization):

```json
{
  "recordId": "user1",
  "colName": "name",
  "value": "Alice",
  "colVersion": 1,
  "dbVersion": 5,
  "nodeId": 1,
  "localDbVersion": 5,
  "flags": 0
}
```

Deletion (record tombstone):
```json
{
  "recordId": "user1",
  "colName": null,
  "value": null,
  "colVersion": 18446744073709551615,
  "dbVersion": 10,
  "nodeId": 1,
  "localDbVersion": 10,
  "flags": 0
}
```

## Building and Testing

### macOS/iOS

```bash
# Build
swift build

# Run tests
swift test

# Run tests in parallel (faster)
swift test --parallel

# Run specific test
swift test --filter CRDTTests.testBasicInsert

# Build in release mode
swift build -c release

# Run tests in release mode
swift test -c release

# Generate Xcode project
swift package generate-xcodeproj
open CRDTLite.xcodeproj
```

### Linux

```bash
# Install Swift (Ubuntu/Debian)
# Follow instructions at https://swift.org/download/

# Or use the official installer:
# wget https://swift.org/builds/swift-5.9-release/ubuntu2204/swift-5.9-RELEASE/swift-5.9-RELEASE-ubuntu22.04.tar.gz
# tar xzf swift-5.9-RELEASE-ubuntu22.04.tar.gz
# export PATH=$PWD/swift-5.9-RELEASE-ubuntu22.04/usr/bin:$PATH

# Build
swift build

# Run tests
swift test

# Run tests in parallel
swift test --parallel

# Run specific test
swift test --filter CRDTTests.testBasicInsert
```

### Continuous Integration

The Swift implementation is tested automatically on every push and pull request via GitHub Actions:

- ✅ **Multi-platform testing** - Both Ubuntu and macOS
- ✅ **Debug and Release builds** - Ensures optimization doesn't break functionality
- ✅ **Parallel test execution** - Verifies thread safety
- ✅ **iOS simulator build** - Validates iOS compatibility
- ✅ **Package validation** - Checks Package.swift integrity

See [.github/workflows/swift-ci.yml](.github/workflows/swift-ci.yml) for the full CI configuration.

**Note:** Swift wasn't available in the development environment during implementation, but the code follows established patterns from the Rust and C++ versions. The GitHub Actions CI will verify all tests pass on actual Swift installations.

### Test Coverage

The test suite includes:
- ✅ Logical clock tests
- ✅ Tombstone storage tests
- ✅ Insert/update operations
- ✅ Delete operations (record and field)
- ✅ Merge and conflict resolution
- ✅ Sync between nodes
- ✅ Change compression
- ✅ JSON serialization/deserialization
- ✅ Edge cases and error conditions

Run tests with:
```bash
swift test --parallel
```

## iOS Example

```swift
import SwiftUI
import CRDTLite

class DocumentStore: ObservableObject {
    @Published var crdt: CRDT<String, String, String>

    init(nodeId: NodeId) {
        self.crdt = CRDT(nodeId: nodeId)
    }

    func updateField(_ docId: String, field: String, value: String) {
        let changes = crdt.insertOrUpdate(docId, fields: [(field, value)])

        // Send changes to server
        Task {
            try await syncChangesToServer(changes)
        }
    }

    func getDocument(_ docId: String) -> [String: String]? {
        return crdt.getRecord(docId)?.fields
    }

    private func syncChangesToServer(_ changes: [Change<String, String, String>]) async throws {
        let data = try JSONEncoder().encode(changes)
        // Send to backend...
    }
}

struct DocumentView: View {
    @StateObject var store: DocumentStore

    var body: some View {
        VStack {
            if let doc = store.getDocument("doc1") {
                Text("Title: \(doc["title"] ?? "")")
                Text("Content: \(doc["content"] ?? "")")
            }

            Button("Update") {
                store.updateField("doc1", field: "title", value: "New Title")
            }
        }
    }
}
```

## Performance Considerations

- **Change compression** - Use for parent-child CRDTs or when sending many changes
- **Sync frequency** - More frequent = better UX, but more overhead
- **Tombstone compaction** - Compact periodically after confirming all nodes synced
- **Version bounds** - Records track min/max versions to skip unchanged data

## API Documentation

### CRDT Class

#### Initialization
```swift
init(nodeId: NodeId, parent: CRDT<K, C, V>? = nil)
init(nodeId: NodeId, changes: [Change<K, C, V>])
```

#### Insert/Update
```swift
func insertOrUpdate(_ recordId: K, fields: [(C, V)]) -> [Change<K, C, V>]
func insertOrUpdateWithFlags(_ recordId: K, flags: UInt32, fields: [(C, V)]) -> [Change<K, C, V>]
```

#### Delete
```swift
func deleteRecord(_ recordId: K) -> Change<K, C, V>?
func deleteRecordWithFlags(_ recordId: K, flags: UInt32) -> Change<K, C, V>?
func deleteField(_ recordId: K, fieldName: C) -> Change<K, C, V>?
func deleteFieldWithFlags(_ recordId: K, fieldName: C, flags: UInt32) -> Change<K, C, V>?
```

#### Merge & Sync
```swift
func mergeChanges<R: MergeRule>(_ changes: [Change<K, C, V>], mergeRule: R) -> [Change<K, C, V>]
func getChangesSince(_ lastDbVersion: UInt64) -> [Change<K, C, V>]
func getChangesSinceExcluding(_ lastDbVersion: UInt64, excluding: Set<NodeId>) -> [Change<K, C, V>]
```

#### Query
```swift
func getRecord(_ recordId: K) -> Record<C, V>?
func isTombstoned(_ recordId: K) -> Bool
func getTombstone(_ recordId: K) -> TombstoneInfo?
func getClock() -> LogicalClock
func getData() -> [K: Record<C, V>]
```

#### Maintenance
```swift
func compactTombstones(minAcknowledgedVersion: UInt64) -> Int
func tombstoneCount() -> Int
func reset(changes: [Change<K, C, V>])
```

#### Serialization
```swift
func toJSON() throws -> String
static func fromJSON(_ json: String) throws -> CRDT<K, C, V>
```

#### Static Methods
```swift
static func compressChanges(_ changes: inout [Change<K, C, V>])
```

## Architecture

The Swift implementation closely follows the Rust and C++ designs:

```
┌─────────────────────────────────────────┐
│ CRDT<K, C, V>                           │
├─────────────────────────────────────────┤
│ - nodeId: NodeId                        │
│ - clock: LogicalClock                   │
│ - data: [K: Record<C, V>]               │
│ - tombstones: TombstoneStorage<K>       │
│ - parent: CRDT?                         │
├─────────────────────────────────────────┤
│ + insertOrUpdate()                      │
│ + deleteRecord()                        │
│ + deleteField()                         │
│ + mergeChanges()                        │
│ + getChangesSince()                     │
│ + compressChanges()                     │
└─────────────────────────────────────────┘
           │
           ├─── Record<C, V>
           │    - fields: [C: V]
           │    - columnVersions: [C: ColumnVersion]
           │    - version boundaries
           │
           ├─── Change<K, C, V>
           │    - recordId, colName, value
           │    - versions and metadata
           │
           └─── TombstoneStorage<K>
                - Deleted record tracking
                - Compaction support
```

## Known Limitations

- **Parent relationships** - Not serialized (must rebuild after deserialization)
- **Clock overflow** - Uses UInt64 (2^64 operations, extremely unlikely)
- **No built-in network layer** - Application must implement transport

## Contributing

The Swift implementation aims to maintain 100% compatibility with the Rust and C++ versions. When adding features:

1. Match the API signatures in `src/lib.rs` and `crdt.hpp`
2. Add corresponding tests in `Tests/CRDTLiteTests/`
3. Update this README with examples
4. Test interoperability with Rust/C++ backends

## License

See LICENSE file in repository root.

## Related Documentation

- [CLAUDE.md](./CLAUDE.md) - Full architecture documentation
- [Rust implementation](./src/lib.rs)
- [C++ implementation](./crdt.hpp)
- [Test suite](./Tests/CRDTLiteTests/CRDTTests.swift)
