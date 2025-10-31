// record_id_types.hpp
// Record ID type system for CRDT-SQLite
// Supports both int64_t (auto-increment rowid) and __uint128_t (UUID-like distributed IDs)

#ifndef RECORD_ID_TYPES_HPP
#define RECORD_ID_TYPES_HPP

#include <cstdint>
#include <string>
#include <sstream>
#include <iomanip>
#include <random>
#include <stdexcept>
#include <sqlite3.h>

// Type traits for different record ID types
template <typename T>
struct RecordIdTraits;

// Specialization for int64_t (SQLite rowid)
template <>
struct RecordIdTraits<int64_t> {
  using type = int64_t;

  static std::string to_string(int64_t id) {
    return std::to_string(id);
  }

  static int64_t from_string(const std::string &s) {
    return std::stoll(s);
  }

  static void bind_to_sqlite(sqlite3_stmt *stmt, int index, int64_t id) {
    sqlite3_bind_int64(stmt, index, id);
  }

  static int64_t from_sqlite(sqlite3_stmt *stmt, int index) {
    return sqlite3_column_int64(stmt, index);
  }

  static int64_t from_sqlite_value(sqlite3_value *val) {
    return sqlite3_value_int64(val);
  }

  static constexpr const char* sql_type() {
    return "INTEGER";
  }

  static constexpr bool is_auto_increment() {
    return true;  // Can use SQLite rowid
  }
};

// 128-bit unsigned integer support
#ifdef __SIZEOF_INT128__
using uint128_t = __uint128_t;

// Specialization for __uint128_t (distributed UUIDs)
template <>
struct RecordIdTraits<uint128_t> {
  using type = uint128_t;

  // Convert to hex string (32 chars)
  static std::string to_string(uint128_t id) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');

    uint64_t high = static_cast<uint64_t>(id >> 64);
    uint64_t low = static_cast<uint64_t>(id & 0xFFFFFFFFFFFFFFFF);

    oss << std::setw(16) << high << std::setw(16) << low;
    return oss.str();
  }

  // Parse from hex string
  static uint128_t from_string(const std::string &s) {
    if (s.length() != 32) {
      throw std::invalid_argument("uint128_t hex string must be 32 chars");
    }

    uint64_t high = std::stoull(s.substr(0, 16), nullptr, 16);
    uint64_t low = std::stoull(s.substr(16, 16), nullptr, 16);

    return (static_cast<uint128_t>(high) << 64) | low;
  }

  // Bind as BLOB (16 bytes) for efficiency
  static void bind_to_sqlite(sqlite3_stmt *stmt, int index, uint128_t id) {
    // Store as 16-byte blob in big-endian format
    unsigned char bytes[16];
    for (int i = 0; i < 16; i++) {
      bytes[15 - i] = static_cast<unsigned char>((id >> (i * 8)) & 0xFF);
    }
    sqlite3_bind_blob(stmt, index, bytes, 16, SQLITE_TRANSIENT);
  }

  static uint128_t from_sqlite(sqlite3_stmt *stmt, int index) {
    const unsigned char *blob = static_cast<const unsigned char*>(
      sqlite3_column_blob(stmt, index)
    );
    int size = sqlite3_column_bytes(stmt, index);

    if (size != 16) {
      throw std::invalid_argument(
        "Invalid uint128_t blob size: expected 16 bytes, got " + std::to_string(size)
      );
    }

    uint128_t result = 0;
    for (int i = 0; i < 16; i++) {
      result = (result << 8) | blob[i];
    }
    return result;
  }

  static uint128_t from_sqlite_value(sqlite3_value *val) {
    const unsigned char *blob = static_cast<const unsigned char*>(
      sqlite3_value_blob(val)
    );
    int size = sqlite3_value_bytes(val);

    if (size != 16) {
      throw std::invalid_argument(
        "Invalid uint128_t blob size: expected 16 bytes, got " + std::to_string(size)
      );
    }

    uint128_t result = 0;
    for (int i = 0; i < 16; i++) {
      result = (result << 8) | blob[i];
    }
    return result;
  }

  static constexpr const char* sql_type() {
    return "BLOB";  // 16 bytes
  }

  static constexpr bool is_auto_increment() {
    return false;  // Must generate manually
  }

  /// Generate random 128-bit ID
  ///
  /// Collision probability (birthday paradox):
  /// - 2^32 IDs:  ~2^-65 chance of collision (negligible)
  /// - 2^64 IDs:  ~50% chance of collision (avoid this scale!)
  ///
  /// Note: No timestamp component. Collisions rely purely on randomness.
  /// For distributed systems with many nodes, prefer generate_with_node()
  /// to partition the ID space by node.
  ///
  /// User responsibility: If you provide your own IDs instead of using this
  /// function, collision avoidance is entirely your responsibility.
  static uint128_t generate() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    uint64_t high = dis(gen);
    uint64_t low = dis(gen);

    return (static_cast<uint128_t>(high) << 64) | low;
  }

  /// Generate with node ID in high bits (RECOMMENDED for distributed systems)
  ///
  /// Partitions the 128-bit space: high 64 bits = node_id, low 64 bits = random.
  /// This eliminates cross-node collisions entirely - only same-node IDs can collide.
  ///
  /// Collision probability PER NODE:
  /// - 2^32 IDs:  ~2^-33 chance (negligible)
  /// - 2^64 IDs:  impossible (exceeds node's 64-bit space)
  ///
  /// Recommendation: Use this method for multi-node systems where node_id is unique.
  ///
  /// @param node_id Unique node identifier (must fit in 63 bits to avoid sign issues)
  /// @throws std::invalid_argument if node_id >= 2^63 (reserved for safe signed/unsigned conversion)
  static uint128_t generate_with_node(uint64_t node_id) {
    // SECURITY: Validate node_id fits in 63 bits
    // This prevents issues when node_id originates from signed int64_t (CrdtNodeId)
    // and avoids potential sign bit complications in cross-platform serialization
    constexpr uint64_t MAX_NODE_ID = (1ULL << 63) - 1;  // 2^63 - 1
    if (node_id > MAX_NODE_ID) {
      throw std::invalid_argument(
        "node_id must fit in 63 bits (max " + std::to_string(MAX_NODE_ID) +
        "), got " + std::to_string(node_id)
      );
    }

    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    // High 64 bits: node_id
    // Low 64 bits: random
    uint64_t low = dis(gen);

    return (static_cast<uint128_t>(node_id) << 64) | low;
  }
};

// Helper class for ID generation
template <typename RecordId>
class RecordIdGenerator {
public:
  static RecordId generate() {
    if constexpr (RecordIdTraits<RecordId>::is_auto_increment()) {
      // For int64_t, return 0 to signal auto-increment
      return 0;
    } else {
      // For uint128_t, generate random
      return RecordIdTraits<RecordId>::generate();
    }
  }

  static RecordId generate_with_node(uint64_t node_id) {
    if constexpr (RecordIdTraits<RecordId>::is_auto_increment()) {
      return 0;
    } else {
      return RecordIdTraits<RecordId>::generate_with_node(node_id);
    }
  }
};

// Hash function for uint128_t (required for std::unordered_set/map)
// Newer versions of libc++ (macOS 15+) provide this, skip if already defined
// Define std::hash<uint128_t> if not already provided by standard library
//
// Modern libc++ (19.0+) may provide std::hash<__uint128_t> automatically.
// To prevent redefinition errors, users can define CRDT_HAS_UINT128_HASH
// if their standard library already provides it.
//
// Override detection: #define CRDT_HAS_UINT128_HASH before including this header
#ifndef CRDT_HAS_UINT128_HASH
  // Auto-detect: libc++ 19.0+ provides hash, older versions don't
  #if defined(_LIBCPP_VERSION) && _LIBCPP_VERSION >= 190000
    #define CRDT_HAS_UINT128_HASH 1
  #else
    #define CRDT_HAS_UINT128_HASH 0
  #endif
#endif

#if !CRDT_HAS_UINT128_HASH
namespace std {
  template<>
  struct hash<uint128_t> {
    size_t operator()(const uint128_t& val) const noexcept {
      // Use boost-style hash combine to avoid collision vulnerabilities
      // Simple XOR creates collisions: (A,B) and (B,A) hash to same value
      uint64_t high = static_cast<uint64_t>(val >> 64);
      uint64_t low = static_cast<uint64_t>(val & 0xFFFFFFFFFFFFFFFF);

      size_t seed = std::hash<uint64_t>{}(high);
      // Boost hash_combine formula: seed ^= hash(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2)
      seed ^= std::hash<uint64_t>{}(low) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      return seed;
    }
  };
}
#endif

#endif // __SIZEOF_INT128__

// Compile-time configuration
// Define CRDT_SQLITE_USE_UUID to use 128-bit IDs by default
#ifdef CRDT_SQLITE_USE_UUID
  #ifdef __SIZEOF_INT128__
    using DefaultRecordId = uint128_t;
  #else
    #error "CRDT_SQLITE_USE_UUID requires 128-bit integer support"
  #endif
#else
  using DefaultRecordId = int64_t;
#endif

#endif // RECORD_ID_TYPES_HPP
