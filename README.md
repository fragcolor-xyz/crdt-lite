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
  - [Extras](#extras)
    - [Detailed Merge Algorithm and Conflict Resolution](#detailed-merge-algorithm-and-conflict-resolution)
      - [Overview](#overview)
      - [Conflict Resolution Attributes](#conflict-resolution-attributes)
      - [Merge Algorithm Steps](#merge-algorithm-steps)
      - [Why the Algorithm Is Sound](#why-the-algorithm-is-sound)
      - [Importance of Attribute Comparison Order](#importance-of-attribute-comparison-order)
        - [1. Column Version (`col_version`) Before Database Version (`db_version`)](#1-column-version-col_version-before-database-version-db_version)
        - [2. Database Version (`db_version`) Before Node ID (`node_id`)](#2-database-version-db_version-before-node-id-node_id)
        - [3. Node ID (`node_id`) Before Sequence Number (`seq`)](#3-node-id-node_id-before-sequence-number-seq)
      - [Example Scenarios](#example-scenarios)
        - [Scenario 1: Concurrent Updates to the Same Field](#scenario-1-concurrent-updates-to-the-same-field)
        - [Scenario 2: Deletion vs. Update](#scenario-2-deletion-vs-update)
      - [Why Comparing `col_version` Before `db_version` Matters](#why-comparing-col_version-before-db_version-matters)
      - [Key Takeaways](#key-takeaways)

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
   g++ -std=c++20 -o crdt tests.cpp && ./crdt
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
  CrdtString col_name;
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
CrdtSet<K> tombstones_;
```

Future updates may include garbage collection mechanisms for old tombstones to optimize storage.

### Merge Operation

The merge process ensures eventual consistency by:

1. **Comparing Incoming Changes:** Each incoming change is evaluated against the local version.
2. **Applying Changes:** If the incoming change is newer or wins the conflict resolution, it is applied.
3. **Updating Local Versions:** Local version information is updated accordingly.

This deterministic process guarantees that all nodes reach a consistent state.

## External Version Tracking

**External version tracking** in CRDT-Lite is designed to be managed by the users of the library. This means that while CRDT-Lite provides the mechanisms for tracking versions, it does not store or manage these versions internally. Users are responsible for maintaining and persisting the following version information:

1. **Last Version Integrated from Remote (`last_remote_version`):** Tracks the highest `db_version` received from a specific remote node.
2. **Last Local Version at Integration (`last_local_version_at_remote_integration`):** Records the local `db_version` when the last set of remote changes was integrated.

**Important:** CRDT-Lite does not handle the storage or retrieval of these version numbers. Users must implement their own storage solutions (e.g., databases, files, in-memory structures) to persist and manage these version tracking values. This external management allows for flexibility in how synchronization state is maintained across different environments and use cases.

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

## Extras

### Detailed Merge Algorithm and Conflict Resolution

#### Overview

The merge operation in CRDT-Lite is designed to ensure **eventual consistency** across distributed nodes by deterministically resolving conflicts when changes from different nodes are integrated. The core of this process lies in the **conflict resolution algorithm**, which decides whether to accept or reject incoming changes based on a set of versioning attributes.

#### Conflict Resolution Attributes

The conflict resolution algorithm uses the following attributes, in order of priority:

1. **Column Version (`col_version`)**: Represents the version of a specific column (field) within a record. It increments with each change to that column.
2. **Database Version (`db_version`)**: A logical clock that provides a causal ordering of changes across the entire database.
3. **Node ID (`node_id`)**: A unique identifier for each node (site) in the distributed system. It helps break ties when `col_version` and `db_version` are equal.
4. **Sequence Number (`seq`)**: A counter that increments with each change from the same node, ensuring ordering of changes from the same site.

#### Merge Algorithm Steps

When integrating incoming changes, the algorithm processes each change individually using the following steps:

1. **Update Logical Clock**: The local logical clock (`db_version`) is updated to be at least as high as the incoming `db_version`.
2. **Retrieve Local Version Information**: For the record and column in question, retrieve the local `col_version`, `db_version`, `node_id`, and `seq`.
3. **Determine Acceptance of Change**: Compare the incoming change's attributes with the local attributes in the following order:
   - **Column Version (`col_version`)**:
     - **If incoming `col_version` > local `col_version`**: Accept the change.
     - **If incoming `col_version` < local `col_version`**: Reject the change.
     - **If equal**, proceed to the next attribute.
   - **Database Version (`db_version`)**:
     - **If incoming `db_version` > local `db_version`**: Accept the change.
     - **If incoming `db_version` < local `db_version`**: Reject the change.
     - **If equal**, proceed to the next attribute.
   - **Node ID (`node_id`)**:
     - **If incoming `node_id` > local `node_id`**: Accept the change.
     - **If incoming `node_id` < local `node_id`**: Reject the change.
     - **If equal**, proceed to the next attribute.
   - **Sequence Number (`seq`)**:
     - **If incoming `seq` > local `seq`**: Accept the change.
     - **If incoming `seq` <= local `seq`**: Reject the change.
4. **Apply Accepted Changes**:
   - **If accepted**:
     - **Updates and Insertions**: Update the field's value and version information.
     - **Deletions**: Mark the record as deleted (tombstoned) and remove its data.
   - **If rejected**: Discard the incoming change.

#### Why the Algorithm Is Sound

The merge algorithm is sound because it ensures that:

- **Determinism**: All nodes apply the same conflict resolution logic, leading to the same final state when all changes have been propagated.
- **Consistency**: The use of versioning attributes provides a total ordering of changes, allowing nodes to agree on which changes are more recent or authoritative.
- **Eventual Consistency**: By deterministically resolving conflicts and accepting the most recent changes based on the defined attributes, all nodes will eventually converge to the same state.

#### Importance of Attribute Comparison Order

The order in which attributes are compared is critical for maintaining the correctness and soundness of the conflict resolution. Here's why each attribute is compared before the next:

##### 1. Column Version (`col_version`) Before Database Version (`db_version`)

- **Field-Level Granularity**: `col_version` provides a version number specific to each field (column) within a record. By prioritizing `col_version`, we ensure that changes to individual fields are accurately tracked and resolved.
- **Independent Field Updates**: Different fields in the same record can be updated independently on different nodes. Comparing `col_version` first allows the algorithm to correctly resolve changes to specific fields without being affected by unrelated changes elsewhere in the database.
- **Avoiding Unnecessary Rejections**: If we compared `db_version` first, a change with a higher `db_version` but lower `col_version` might incorrectly overwrite a more recent field-specific change, leading to data loss or inconsistency.

##### 2. Database Version (`db_version`) Before Node ID (`node_id`)

- **Causal Ordering**: `db_version` acts as a logical clock that captures the causal history of changes across the entire database. By comparing `db_version` after `col_version`, we respect the causal relationships between changes.
- **Global Consistency**: Using `db_version` ensures that changes are ordered consistently across nodes, even when `col_version` is the same.

##### 3. Node ID (`node_id`) Before Sequence Number (`seq`)

- **Tie-Breaking Between Nodes**: When `col_version` and `db_version` are equal, `node_id` provides a deterministic way to break ties between changes originating from different nodes.
- **Sequence Within a Node**: `seq` is used to order changes from the same node when all other attributes are equal.

#### Example Scenarios

##### Scenario 1: Concurrent Updates to the Same Field

- **Node A** updates field `F` in record `R`:
  - `col_version = 2`, `db_version = 5`, `node_id = 1`, `seq = 10`.
- **Node B** updates field `F` in record `R` concurrently:
  - `col_version = 2`, `db_version = 6`, `node_id = 2`, `seq = 7`.

**Conflict Resolution**:

1. **Compare `col_version`**:
   - Both are `2`; proceed to `db_version`.
2. **Compare `db_version`**:
   - Node B's `db_version` (`6`) > Node A's `db_version` (`5`); accept Node B's change.

##### Scenario 2: Deletion vs. Update

- **Node A** deletes record `R`:
  - Updates `"__deleted__"` column with `col_version = 3`, `db_version = 8`, `node_id = 1`, `seq = 15`.
- **Node B** updates field `F` in record `R`:
  - `col_version = 2`, `db_version = 7`, `node_id = 2`, `seq = 12`.

**Conflict Resolution** for field `F`:

1. **Compare `col_version`**:
   - Node B's `col_version` (`2`) < Node A's `col_version` (`3` for `"__deleted__"`); reject Node B's change.

**Conflict Resolution** for `"__deleted__"`:

- Since only Node A has a change to `"__deleted__"`, and Node B doesn't have a local version for it, Node B will accept Node A's deletion.

#### Why Comparing `col_version` Before `db_version` Matters

- **Preservation of Field-Specific Updates**: By comparing `col_version` first, we ensure that the most recent changes to a field are preserved, even if the overall `db_version` is lower.
- **Avoiding Overwrites from Unrelated Changes**: A higher `db_version` does not necessarily mean that a change to a specific field is more recent. If we compared `db_version` first, a change that didn't affect a particular field could incorrectly overwrite a more recent change to that field.
- **Correct Handling of Deletions**: Deletions are treated as changes to the `"__deleted__"` column. By comparing `col_version` first, we can correctly resolve conflicts between deletions and updates to other fields.

#### Key Takeaways

- **Soundness**: The merge algorithm is sound because it ensures deterministic conflict resolution based on a well-defined ordering of versioning attributes.
- **Importance of Order**: Comparing `col_version` before `db_version` is crucial for accurately resolving conflicts at the field level and maintaining data integrity.
- **Uniform Application**: The conflict resolution logic applies uniformly to all columns, including the `"__deleted__"` column used for deletions, simplifying the algorithm and avoiding special cases.
