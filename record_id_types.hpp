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
      return 0;  // Invalid blob
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
      return 0;  // Invalid blob
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

  // Generate random 128-bit ID
  static uint128_t generate() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    uint64_t high = dis(gen);
    uint64_t low = dis(gen);

    return (static_cast<uint128_t>(high) << 64) | low;
  }

  // Generate with node ID in high bits (prevents collisions)
  static uint128_t generate_with_node(uint64_t node_id) {
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
namespace std {
  template<>
  struct hash<uint128_t> {
    size_t operator()(const uint128_t& val) const noexcept {
      // XOR high and low 64 bits
      uint64_t high = static_cast<uint64_t>(val >> 64);
      uint64_t low = static_cast<uint64_t>(val & 0xFFFFFFFFFFFFFFFF);
      return std::hash<uint64_t>{}(high) ^ std::hash<uint64_t>{}(low);
    }
  };
}

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
