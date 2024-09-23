# CRDT-Lite

> [!WARNING]
> This project is in early development and not intended for production use.

CRDT-Lite is a lightweight implementation of Conflict-free Replicated Data Types (CRDTs) in both Rust and C++. It provides a generic CRDT structure that can be used for distributed systems requiring eventual consistency.

## Features

- Generic CRDT structure supporting custom key and value types
- Logical clock for maintaining causality
- Fine-grained conflict resolution based on column versions, site IDs, and sequence numbers
- Support for insert, update, and delete operations with tombstone handling
- Efficient merging mechanism for synchronizing state across nodes
- Implementations in both Rust and C++ for flexibility and performance

## Rust Implementation

### Quick Start

1. Ensure you have Rust installed. If not, get it from [rustup.rs](https://rustup.rs/).
2. Clone this repository:
   ```
   git clone https://github.com/yourusername/crdt-lite.git
   cd crdt-lite
   ```
3. Build and run tests:
   ```
   cargo test
   ```

## C++ Implementation

### Quick Start

1. Ensure you have a C++20 compatible compiler.
2. Compile and run:
   ```
   g++ -std=c++20 -o crdt crdt.cpp && ./crdt
   ```

## Implementation Details

Both Rust and C++ implementations share similar core concepts:

1. **LogicalClock**: Manages causality across events.
2. **ColumnVersion**: Tracks version information for each column in a record.
3. **Record**: Represents individual records with fields and their version information.
4. **CRDT**: The main structure managing overall state, including data records and tombstones.

Key operations include:
- Insert: Adds new records
- Update: Modifies existing records
- Delete: Marks records as tombstoned
- Merge: Synchronizes state between nodes, resolving conflicts

## Limitations and Future Work

- The current implementation is not thread-safe. Concurrency support is planned for future versions.
- Network transport layer is not included. Users must implement their own synchronization mechanism.
- Performance optimizations for large datasets are yet to be implemented.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.