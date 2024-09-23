# CRDT-Lite

> [!WARNING]
> This project is in early development and not intended for production use.

CRDT-Lite is a lightweight implementation of Conflict-free Replicated Data Types (CRDTs) in both Rust and C++. It provides a generic CRDT structure suitable for distributed systems requiring eventual consistency. CRDT-Lite is currently being used in [Formabble](https://formabble.com), a collaborative game engine, and will be integrated into a derived product that we will announce soon.

## Table of Contents

- [CRDT-Lite](#crdt-lite)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Usage](#usage)
  - [Quick Start](#quick-start)
    - [Rust Implementation](#rust-implementation)
    - [C++ Implementation](#c-implementation)
  - [Implementation Details](#implementation-details)
    - [Core Components](#core-components)
    - [Simplified Clock Mechanism](#simplified-clock-mechanism)
    - [Conflict Resolution](#conflict-resolution)
    - [Efficient Change Propagation](#efficient-change-propagation)
    - [Tombstone Handling](#tombstone-handling)
    - [Merge Operation](#merge-operation)
  - [External Version Tracking](#external-version-tracking)
  - [Design Considerations](#design-considerations)
    - [Clock Mechanism](#clock-mechanism)
    - [Conflict Resolution](#conflict-resolution-1)
    - [Efficiency](#efficiency)
    - [Scalability](#scalability)
  - [Limitations](#limitations)
  - [FAQ](#faq)
  - [Future Enhancements](#future-enhancements)
  - [Contributing](#contributing)
  - [License](#license)

## Features

- **Generic CRDT Structure:** Supports custom key and value types.
- **Logical Clock:** Maintains causality across events.
- **Fine-Grained Conflict Resolution:** Based on column versions, site IDs, and sequence numbers.
- **CRUD Operations:** Insert, update, and delete operations with tombstone handling.
- **Efficient Merging:** Synchronizes state across nodes effectively.
- **Multi-Language Support:** Implemented in both Rust and C++ for flexibility and performance.
- **External Version Tracking:** Robust synchronization management without requiring identical logical clocks across nodes.

## Usage

CRDT-Lite is actively used in [Formabble](https://formabble.com), a collaborative game engine designed to facilitate real-time collaboration in game development. By leveraging CRDT-Lite, Formabble ensures consistent and conflict-free data synchronization across multiple users and instances. Additionally, CRDT-Lite will be integrated into a derived product that we will announce soon, further demonstrating its versatility and effectiveness in real-world applications.

## Quick Start

### Rust Implementation

1. **Install Rust:** If you don't have Rust installed, get it from [rustup.rs](https://rustup.rs/).
2. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/crdt-lite.git
   cd crdt-lite
   ```
3. **Build and Run Tests:**
   ```bash
   cargo test
   ```

### C++ Implementation

1. **Ensure a C++20 Compatible Compiler:** Make sure you have a compiler that supports C++20.
2. **Compile and Run:**
   ```bash
   g++ -std=c++20 -o crdt crdt.cpp && ./crdt
   ```

## Implementation Details

CRDT-Lite's Rust and C++ implementations share similar core concepts and design principles, ensuring consistency across both languages.

### Core Components

1. **LogicalClock:** Manages causality across events.
   ```cpp
   class LogicalClock {
     uint64_t time_;
   public:
     uint64_t tick() { return ++time_; }
     uint64_t update(uint64_t received_time) {
       time_ = std::max(time_, received_time);
       return ++time_;
     }
   };
   ```
2. **ColumnVersion:** Tracks version information for each column in a record.
   ```cpp
   struct ColumnVersion {
     uint64_t col_version;
     uint64_t db_version;
     uint64_t site_id;
     uint64_t seq;
   };
   ```
3. **Record:** Represents individual records with fields and their version information.
4. **CRDT:** The main structure managing overall state, including data records and tombstones.
5. **External Version Tracking:** Maintains synchronization state between nodes.

### Simplified Clock Mechanism

CRDT-Lite employs a straightforward approach to manage causality without the complexity of vector clocks:

- **Logical Clock:** Each node maintains a single logical clock that increments with each operation.
- **Column Version:** Each field in a record has its own version information, replacing the need for vector clocks.
- **External Version Tracking:** Tracks `last_db_version` and `last_local_version_at_remote_integration` to manage synchronization efficiently.

### Conflict Resolution

Conflicts are resolved deterministically using a combination of:

1. **Column Version:** Tracks changes at the field level.
2. **Database Version (`db_version`):** Provides a global ordering of changes.
3. **Site ID:** Breaks ties between concurrent changes from different nodes.
4. **Sequence Number (`seq`):** Orders changes from the same site.

The merge algorithm prioritizes these factors in the above order to ensure consistent conflict resolution across all nodes.

### Efficient Change Propagation

Each operation (insert, update, delete) generates a `Change` object for incremental updates:

```cpp
template <typename K, typename V>
struct Change {
  K record_id;
  std::string col_name;
  std::optional<V> value;
  uint64_t col_version;
  uint64_t db_version;
  uint64_t site_id;
  uint64_t seq;
};
```

This design minimizes bandwidth usage by transmitting only the necessary changes during synchronization.

### Tombstone Handling

Deleted records are marked with tombstones to prevent their accidental resurrection during merges:

```cpp
std::unordered_set<K> tombstones_;
```

Future updates may include garbage collection mechanisms for old tombstones to optimize storage.

### Merge Operation

The merge process ensures eventual consistency by:

1. **Comparing Incoming Changes:** Each incoming change is evaluated against the local version.
2. **Applying Changes:** If the incoming change is newer or wins the conflict resolution, it is applied.
3. **Updating Local Versions:** Local version information is updated accordingly.

This deterministic process guarantees that all nodes reach a consistent state.

## External Version Tracking

CRDT-Lite utilizes external version tracking to manage synchronization without requiring identical logical clocks across nodes:

1. **Last Version Integrated from Remote (`last_remote_version`):** Tracks the highest `db_version` received from a specific remote node.
2. **Last Local Version at Integration (`last_local_version_at_remote_integration`):** Records the local `db_version` when the last set of remote changes was integrated.

This mechanism ensures that only relevant changes are considered during merges, optimizing the synchronization process.

## Design Considerations

CRDT-Lite is crafted with a focus on simplicity, efficiency, and scalability, addressing common challenges in distributed systems.

### Clock Mechanism

- **Logical Clock:** Combines with external version tracking to maintain causality without synchronized clocks.
- **Efficiency:** Handles clock drift between nodes effectively.

### Conflict Resolution

- **Fine-Grained:** Operates at the field level for precise conflict management.
- **Deterministic:** Uses a combination of column version, database version, site ID, and sequence number to resolve conflicts predictably.

### Efficiency

- **Minimal Storage Overhead:** Stores only the current state without historical data.
- **Change-Based Operations:** Facilitates efficient incremental updates and synchronization.

### Scalability

- **Supports Many Nodes:** Utilizes UUIDs for site IDs to handle a large or unpredictable number of nodes.
- **Optimized for Multiple Fields:** Efficiently manages records with numerous fields.

## Limitations

- **Thread Safety:** The current implementation is not thread-safe. Concurrency support is not planned.
- **Network Transport Layer:** Not included. Users must implement their own synchronization mechanisms.

## FAQ

**Q: How does CRDT-Lite handle tombstones for deleted records?**  
A: Tombstones are maintained to properly handle deletions across the distributed system. Future updates may include a garbage collection mechanism for old tombstones.

**Q: Is there support for composite fields or atomic updates to multiple related fields?**  
A: Currently, fields are treated individually. A Redis-like approach with long keys (e.g., `fbl/pose/translation`) can handle composite data. Full support for composite fields may be added in future updates.

**Q: How does the system ensure security?**  
A: CRDT-Lite focuses on data consistency and conflict resolution. Security measures like encryption should be implemented at the network layer or application layer as appropriate for your use case.

**Q: Can the system handle large numbers of concurrent updates?**  
A: Yes, the conflict resolution mechanism ensures that all nodes will eventually reach a consistent state, even with many concurrent updates. The resolution is deterministic, using site ID and sequence number for tiebreaking when necessary.

**Q: How efficient is the synchronization process?**  
A: CRDT-Lite uses a change-based operation system, allowing for efficient incremental updates between frequently communicating nodes. This significantly reduces overhead in lively connected systems.

## Future Enhancements

- **Tombstone Garbage Collection:** Implement mechanisms to remove old tombstones.
- **Bandwidth Optimization:** Enhance efficiency for large datasets.
- **Custom Merge Functions:** Allow users to define specific merge behaviors for unique use cases.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. Your contributions help improve CRDT-Lite for everyone.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

CRDT-Lite offers a streamlined approach to conflict-free replicated data types, balancing simplicity and efficiency without compromising on the core benefits of CRDTs. By focusing on a simplified clock mechanism and robust conflict resolution, CRDT-Lite is well-suited for applications that prioritize scalability and low overhead in distributed environments.