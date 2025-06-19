// crdt.hpp
#ifndef CRDT_HPP
#define CRDT_HPP

#include <cstdint>

// Define this if you want to override the default collection types
// Basically define these before including this header and ensure this define is set before this header is included
// in any other files that include this file
#ifndef CRDT_COLLECTIONS_DEFINED
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <set>
#include <vector>

template <typename T> using CrdtVector = std::vector<T>;

using CrdtKey = std::string;

template <typename K, typename V, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using CrdtMap = std::unordered_map<K, V, Hash, KeyEqual>;

template <typename K, typename V, typename Comparator = std::less<K>> using CrdtSortedMap = std::map<K, V, Comparator>;

template <typename K, typename Hash = std::hash<K>, typename KeyEqual = std::equal_to<K>>
using CrdtSet = std::unordered_set<K, Hash, KeyEqual>;

template <typename T, typename Comparator> using CrdtSortedSet = std::set<T, Comparator>;
using CrdtNodeId = uint64_t;
#endif

#include <algorithm>
#include <iostream>
#include <optional>
#include <memory>
#include <type_traits>
#include <functional>
#include <variant>

// Add this helper struct at the beginning of the file, outside of the CRDT class

// Helper struct to check if a container has emplace_back method
template <typename T, typename = void> struct has_emplace_back : std::false_type {};

template <typename T>
struct has_emplace_back<T, std::void_t<decltype(std::declval<T>().emplace_back(std::declval<typename T::value_type>()))>>
    : std::true_type {};

// Helper function to add an element to a container
template <typename Container, typename Element> void add_to_container(Container &container, Element &&element) {
  if constexpr (has_emplace_back<Container>::value) {
    container.emplace_back(std::forward<Element>(element));
  } else {
    container.emplace(std::forward<Element>(element));
  }
}

/// Represents a single change in the CRDT.
template <typename K, typename V> struct Change {
  K record_id;
  std::optional<CrdtKey> col_name; // std::nullopt represents tombstone of the record
  std::optional<V> value;          // note std::nullopt represents deletion of the column, not the record
  uint64_t col_version;
  uint64_t db_version;
  CrdtNodeId node_id;

  // this field is useful only locally when doing things like get_changes_since
  // we record the local db_version when the change was created
  uint64_t local_db_version;

  // those optional flags are used to indicate the type of change, they are not stored in the records, users should manage them.
  // they are very ephemeral and set only during insert_or_update, delete_record and merge_changes
  uint32_t flags;

  Change() = default;

  Change(K rid, std::optional<CrdtKey> cname, std::optional<V> val, uint64_t cver, uint64_t dver, CrdtNodeId nid,
         uint64_t ldb_ver = 0, uint32_t f = 0)
      : record_id(std::move(rid)), col_name(std::move(cname)), value(std::move(val)), col_version(cver), db_version(dver),
        node_id(nid), local_db_version(ldb_ver), flags(f) {}
};

// Update the MergeRule concept to properly handle void context
template <typename Rule, typename K, typename V, typename Context = void>
concept MergeRule =
    // Case 1: No context (void)
    (std::is_void_v<Context> &&
     requires(Rule r, const Change<K, V> &local, const Change<K, V> &remote) {
       { r(local, remote) } -> std::convertible_to<bool>;
     }) ||
    // Case 2: With context
    (!std::is_void_v<Context> && requires(Rule r, const Change<K, V> &local, const Change<K, V> &remote, const Context &ctx) {
      { r(local, remote, ctx) } -> std::convertible_to<bool>;
    });

// Default merge rule with proper void handling
template <typename K, typename V, typename Context = void> struct DefaultMergeRule {
  // Primary version with scalar values
  constexpr bool operator()(uint64_t local_col, uint64_t local_db, const CrdtNodeId &local_node, uint64_t remote_col,
                            uint64_t remote_db, const CrdtNodeId &remote_node) const {
    if (remote_col > local_col) {
      return true;
    } else if (remote_col < local_col) {
      return false;
    } else {
      if (remote_db > local_db) {
        return true;
      } else if (remote_db < local_db) {
        return false;
      } else {
        return (remote_node > local_node);
      }
    }
  }

  // Adapter for Change objects
  constexpr bool operator()(const Change<K, V> &local, const Change<K, V> &remote) const {
    return (*this)(local.col_version, local.db_version, local.node_id, remote.col_version, remote.db_version, remote.node_id);
  }
};

// Specialization for non-void context
template <typename K, typename V, typename Context>
  requires(!std::is_void_v<Context>)
struct DefaultMergeRule<K, V, Context> {
  // Primary version with scalar values
  constexpr bool operator()(uint64_t local_col, uint64_t local_db, uint64_t local_node, uint64_t remote_col, uint64_t remote_db,
                            uint64_t remote_node, const Context &) const {
    DefaultMergeRule<K, V, void> default_rule;
    return default_rule(local_col, local_db, local_node, remote_col, remote_db, remote_node);
  }

  // Adapter for Change objects
  constexpr bool operator()(const Change<K, V> &local, const Change<K, V> &remote, const Context &ctx) const {
    return (*this)(local.col_version, local.db_version, local.node_id, remote.col_version, remote.db_version, remote.node_id,
                   ctx);
  }
};

// Define a concept for a custom change comparator
template <typename Comparator, typename K, typename V>
concept ChangeComparator = requires(Comparator c, const Change<K, V> &a, const Change<K, V> &b) {
  { c(a, b) } -> std::convertible_to<bool>;
};

// Default change comparator (current behavior)
template <typename K, typename V> struct DefaultChangeComparator {
  constexpr bool operator()(const Change<K, V> &a, const Change<K, V> &b) const {
    if (a.record_id != b.record_id)
      return a.record_id < b.record_id;
    if (a.col_name.has_value() != b.col_name.has_value())
      return b.col_name.has_value(); // Deletions (nullopt) come last for each record
    if (a.col_name != b.col_name)
      return a.col_name < b.col_name;
    if (a.col_version != b.col_version)
      return a.col_version > b.col_version;
    if (a.db_version != b.db_version)
      return a.db_version > b.db_version;
    if (a.node_id != b.node_id)
      return a.node_id > b.node_id;
    return false; // Consider equal if all fields match
  }
};

/// Represents a default sort function.
struct DefaultSort {
  template <typename Iterator, typename Comparator>
  constexpr void operator()(Iterator begin, Iterator end, Comparator comp) const {
    std::sort(begin, end, comp);
  }
};

/// Represents a logical clock for maintaining causality.
class LogicalClock {
public:
  LogicalClock() : time_(0) {}

  /// Increments the clock for a local event.
  constexpr uint64_t tick() { return ++time_; }

  /// Updates the clock based on a received time.
  constexpr uint64_t update(uint64_t received_time) {
    time_ = std::max(time_, received_time);
    return ++time_;
  }

  /// Sets the logical clock to a specific time.
  constexpr void set_time(uint64_t t) { time_ = t; }

  /// Retrieves the current time.
  constexpr uint64_t current_time() const { return time_; }

private:
  uint64_t time_;
};

/// Represents the version information for a column.
struct ColumnVersion {
  uint64_t col_version;
  uint64_t db_version;
  CrdtNodeId node_id;

  // this field is useful only locally when doing things like get_changes_since
  // we record the local db_version when the change was created
  uint64_t local_db_version;

  constexpr ColumnVersion(uint64_t c, uint64_t d, CrdtNodeId n, uint64_t ldb_ver = 0)
      : col_version(c), db_version(d), node_id(n), local_db_version(ldb_ver) {}
};

/// Compact tombstone information structure to reduce memory usage
/// Maintains full 64-bit precision to avoid overflow issues
struct TombstoneInfo {
  uint64_t col_version;
  uint64_t db_version;
  CrdtNodeId node_id;
  uint64_t local_db_version;
  
  constexpr TombstoneInfo(uint64_t c, uint64_t d, CrdtNodeId n, uint64_t ldb_ver = 0)
      : col_version(c), db_version(d), node_id(n), local_db_version(ldb_ver) {}
        
  // Convert back to ColumnVersion when needed
  constexpr ColumnVersion to_column_version() const {
    return ColumnVersion(col_version, db_version, node_id, local_db_version);
  }
};

// Alternative compact storage for tombstones using sorted vector
// More memory efficient than hash map for large numbers of tombstones
template<typename K>
struct CompactTombstoneEntry {
  K record_id;
  TombstoneInfo info;
  
  constexpr CompactTombstoneEntry(K id, TombstoneInfo inf) 
    : record_id(std::move(id)), info(inf) {}
};

template<typename K>
class CompactTombstoneStorage {
public:
  using Entry = CompactTombstoneEntry<K>;
  
private:
  CrdtVector<Entry> entries_;
  
public:
  void insert_or_assign(const K& key, const TombstoneInfo& info) {
    auto it = std::lower_bound(entries_.begin(), entries_.end(), key,
      [](const Entry& entry, const K& key) { return entry.record_id < key; });
    
    if (it != entries_.end() && it->record_id == key) {
      it->info = info;
    } else {
      entries_.emplace(it, key, info);
    }
  }
  
  std::optional<TombstoneInfo> find(const K& key) const {
    auto it = std::lower_bound(entries_.begin(), entries_.end(), key,
      [](const Entry& entry, const K& key) { return entry.record_id < key; });
    
    if (it != entries_.end() && it->record_id == key) {
      return it->info;
    }
    return std::nullopt;
  }
  
  bool erase(const K& key) {
    auto it = std::lower_bound(entries_.begin(), entries_.end(), key,
      [](const Entry& entry, const K& key) { return entry.record_id < key; });
    
    if (it != entries_.end() && it->record_id == key) {
      entries_.erase(it);
      return true;
    }
    return false;
  }
  
  void clear() { entries_.clear(); }
  
  auto begin() const { return entries_.begin(); }
  auto end() const { return entries_.end(); }
  size_t size() const { return entries_.size(); }
};

/// Represents a record in the CRDT.
template <typename V> struct Record {
  CrdtMap<CrdtKey, V> fields;
  CrdtMap<CrdtKey, ColumnVersion> column_versions;

  // Track version boundaries for efficient filtering
  uint64_t lowest_local_db_version = UINT64_MAX;
  uint64_t highest_local_db_version = 0;

  Record() = default;

  Record(CrdtMap<CrdtKey, V> &&f, CrdtMap<CrdtKey, ColumnVersion> &&cv) : fields(std::move(f)), column_versions(std::move(cv)) {
    // Initialize version boundaries
    for (const auto &[_, ver] : column_versions) {
      if (ver.local_db_version < lowest_local_db_version) {
        lowest_local_db_version = ver.local_db_version;
      }
      if (ver.local_db_version > highest_local_db_version) {
        highest_local_db_version = ver.local_db_version;
      }
    }
  }
};

// Free function to compare two Record<V> instances
template <typename V> constexpr bool operator==(const Record<V> &lhs, const Record<V> &rhs) {
  // Compare fields
  if (lhs.fields.size() != rhs.fields.size())
    return false;
  for (const auto &[key, value] : lhs.fields) {
    auto it = rhs.fields.find(key);
    if (it == rhs.fields.end() || it->second != value)
      return false;
  }

  // We don't care about column_versions, as those will be different for each node

  return true;
}

// Concept for map-like containers
template <typename Container, typename Key, typename Value>
concept MapLike = requires(Container c, Key k, Value v) {
  typename Container::key_type;
  typename Container::mapped_type;
  typename Container::value_type;
  typename Container::iterator;
  { c[k] } -> std::convertible_to<Value &>;
  { c.find(k) } -> std::convertible_to<typename Container::iterator>;
  { c.emplace(k, v) };
  { c.try_emplace(k, v) } -> std::same_as<std::pair<typename Container::iterator, bool>>;
  { c.insert_or_assign(k, v) } -> std::same_as<std::pair<typename Container::iterator, bool>>;
  { c.clear() } -> std::same_as<void>;
  { c.erase(k) };
};

/// Represents the CRDT structure, generic over key (`K`) and value (`V`) types.
template <typename K, typename V, typename SortFunctionType = DefaultSort, MapLike<K, Record<V>> MapType = CrdtMap<K, Record<V>>>
class CRDT : public std::enable_shared_from_this<CRDT<K, V, SortFunctionType, MapType>> {
protected:
  CrdtNodeId node_id_;
  LogicalClock clock_;
  MapType data_;

  // Separate storage for tombstones - maps record IDs to their deletion information
  // Use compact storage for better memory efficiency with large numbers of tombstones
  CompactTombstoneStorage<K> tombstones_;

  // our clock won't be shared with the parent
  // we optionally allow to merge from the parent or push to the parent
  std::shared_ptr<CRDT> parent_;
  uint64_t base_version_; // Tracks the parent's db_version at the time of child creation
  SortFunctionType sort_func_;

public:
  // Create a new empty CRDT
  // Complexity: O(1)
  CRDT(CrdtNodeId node_id, std::shared_ptr<CRDT> parent = nullptr, SortFunctionType sort_func = SortFunctionType())
      : node_id_(node_id), clock_(), data_(), tombstones_(), parent_(parent), sort_func_(std::move(sort_func)) {
    if (parent_) {
      clock_ = parent_->clock_;
      base_version_ = parent_->clock_.current_time();
    } else {
      base_version_ = 0;
    }
  }

  /// Create a CRDT from a list of changes (e.g., loaded from disk).
  ///
  /// # Arguments
  ///
  /// * `node_id` - The unique identifier for this CRDT node.
  /// * `changes` - A list of changes to apply to reconstruct the CRDT state.
  ///
  /// Complexity: O(n), where n is the number of changes
  constexpr CRDT(CrdtNodeId node_id, CrdtVector<Change<K, V>> &&changes) : node_id_(node_id), clock_(), data_(), tombstones_() {
    apply_changes(std::move(changes));
  }

  /// Resets the CRDT to a state as if it was constructed with the given changes.
  ///
  /// # Arguments
  ///
  /// * `changes` - A list of changes to apply to reconstruct the CRDT state.
  ///
  /// Complexity: O(n), where n is the number of changes
  constexpr void reset(CrdtVector<Change<K, V>> &&changes) {
    // Clear existing data
    data_.clear();
    tombstones_.clear();

    // Reset the logical clock
    clock_ = LogicalClock();

    apply_changes(std::move(changes));
  }

  /// Reverts all changes made by this CRDT since it was created from the parent.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the inverse changes needed to undo the child's changes.
  ///
  /// # Complexity
  ///
  /// O(c), where c is the number of changes since `base_version_`
  constexpr CrdtVector<Change<K, V>> revert() {
    if (!parent_) {
      throw std::runtime_error("Cannot revert without a parent CRDT.");
    }

    // Step 1: Retrieve all changes made by the child since base_version_
    CrdtVector<Change<K, V>> child_changes = this->get_changes_since(base_version_);

    // Step 2: Generate inverse changes using the parent CRDT
    return invert_changes(child_changes, *parent_);
  }

  /// Computes the difference between this CRDT and another CRDT.
  ///
  /// # Arguments
  ///
  /// * `other` - The CRDT to compare against.
  ///
  /// # Returns
  ///
  /// A vector of `Change` objects representing the changes needed to transform this CRDT into the other CRDT.
  ///
  /// # Complexity
  ///
  /// O(c), where c is the number of changes since the common ancestor
  constexpr CrdtVector<Change<K, V>> diff(const CRDT &other) const {
    // Find the common ancestor (lowest common db_version)
    uint64_t common_version = std::min(clock_.current_time(), other.clock_.current_time());

    // Get changes from this CRDT since the common ancestor
    CrdtVector<Change<K, V>> this_changes = this->get_changes_since(common_version);

    // Get changes from the other CRDT since the common ancestor
    CrdtVector<Change<K, V>> other_changes = other.get_changes_since(common_version);

    // Invert the changes from this CRDT
    CrdtVector<Change<K, V>> inverted_this_changes = invert_changes(this_changes, other);

    // Combine the inverted changes from this CRDT with the changes from the other CRDT
    CrdtVector<Change<K, V>> diff_changes;
    diff_changes.reserve(inverted_this_changes.size() + other_changes.size());
    diff_changes.insert(diff_changes.end(), inverted_this_changes.begin(), inverted_this_changes.end());
    diff_changes.insert(diff_changes.end(), other_changes.begin(), other_changes.end());

    // Compress the changes to remove redundant operations
    compress_changes(diff_changes);

    return diff_changes;
  }

  /// Inserts a new record or updates an existing record in the CRDT.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `fields` - A variadic list of field name-value pairs.
  ///
  /// Complexity: O(n), where n is the number of fields in the input
  template <typename... Pairs> constexpr void insert_or_update(const K &record_id, Pairs &&...pairs) {
    insert_or_update_impl<CrdtVector<Change<K, V>>>(record_id, 0, nullptr, std::forward<Pairs>(pairs)...);
  }

  /// Inserts a new record or updates an existing record in the CRDT, and stores changes.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `changes` - A reference to a container to store the changes.
  /// * `fields` - A variadic list of field name-value pairs.
  ///
  /// Complexity: O(n), where n is the number of fields in the input
  template <typename ChangeContainer, typename... Pairs>
  constexpr void insert_or_update(const K &record_id, ChangeContainer &changes, Pairs &&...pairs) {
    insert_or_update_impl(record_id, 0, &changes, std::forward<Pairs>(pairs)...);
  }

  /// Inserts a new record or updates an existing record in the CRDT.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `flags` - A set of flags to indicate the type of change.
  /// * `fields` - A variadic list of field name-value pairs.
  ///
  /// Complexity: O(n), where n is the number of fields in the input
  template <typename... Pairs> constexpr void insert_or_update(const K &record_id, uint32_t flags, Pairs &&...pairs) {
    insert_or_update_impl<CrdtVector<Change<K, V>>>(record_id, flags, nullptr, std::forward<Pairs>(pairs)...);
  }

  /// Inserts a new record or updates an existing record in the CRDT, and stores changes.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `flags` - A set of flags to indicate the type of change.
  /// * `changes` - A reference to a container to store the changes.
  /// * `fields` - A variadic list of field name-value pairs.
  ///
  /// Complexity: O(n), where n is the number of fields in the input
  template <typename ChangeContainer, typename... Pairs>
  constexpr void insert_or_update(const K &record_id, uint32_t flags, ChangeContainer &changes, Pairs &&...pairs) {
    insert_or_update_impl(record_id, flags, &changes, std::forward<Pairs>(pairs)...);
  }

  /// Inserts a new record or updates an existing record in the CRDT using an iterable container of field-value pairs.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `fields` - An iterable container of field name-value pairs (will be consumed).
  ///
  /// Complexity: O(n), where n is the number of fields in the input
  template <typename Container> constexpr void insert_or_update_from_container(const K &record_id, Container &&fields) {
    insert_or_update_from_container_impl<CrdtVector<Change<K, V>>>(record_id, 0, std::forward<Container>(fields), nullptr);
  }

  /// Inserts a new record or updates an existing record in the CRDT using an iterable container of field-value pairs.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `flags` - A set of flags to indicate the type of change.
  /// * `fields` - An iterable container of field name-value pairs (will be consumed).
  ///
  /// Complexity: O(n), where n is the number of fields in the input
  template <typename Container>
  constexpr void insert_or_update_from_container(const K &record_id, uint32_t flags, Container &&fields) {
    insert_or_update_from_container_impl<CrdtVector<Change<K, V>>>(record_id, flags, std::forward<Container>(fields), nullptr);
  }

  /// Inserts a new record or updates an existing record in the CRDT using an iterable container of field-value pairs,
  /// and stores changes.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `fields` - An iterable container of field name-value pairs (will be consumed).
  /// * `changes` - A reference to a container to store the changes.
  ///
  /// Complexity: O(n), where n is the number of fields in the input
  template <typename Container, typename ChangeContainer>
  constexpr void insert_or_update_from_container(const K &record_id, Container &&fields, ChangeContainer &changes) {
    insert_or_update_from_container_impl(record_id, 0, std::forward<Container>(fields), &changes);
  }

  /// Inserts a new record or updates an existing record in the CRDT using an iterable container of field-value pairs,
  /// and stores changes.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `flags` - A set of flags to indicate the type of change.
  /// * `fields` - An iterable container of field name-value pairs (will be consumed).
  /// * `changes - A reference to a container to store the changes.
  ///
  /// Complexity: O(n), where n is the number of fields in the input
  template <typename Container, typename ChangeContainer>
  constexpr void insert_or_update_from_container(const K &record_id, uint32_t flags, Container &&fields,
                                                 ChangeContainer &changes) {
    insert_or_update_from_container_impl(record_id, flags, std::forward<Container>(fields), &changes);
  }

  /// Deletes a record by marking it as tombstoned.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  ///
  /// Complexity: O(1)
  virtual void delete_record(const K &record_id, uint32_t flags = 0) {
    delete_record_impl<CrdtVector<Change<K, V>>>(record_id, flags, nullptr);
  }

  /// Deletes a record by marking it as tombstoned, and stores the change.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `changes` - A reference to a container to store the change.
  ///
  /// Complexity: O(1)
  template <typename ChangeContainer> void delete_record(const K &record_id, ChangeContainer &changes, uint32_t flags = 0) {
    delete_record_impl(record_id, flags, &changes);
  }

  /// Retrieves all changes since a given `last_db_version`.
  ///
  /// # Arguments
  ///
  /// * `last_db_version` - The database version to retrieve changes since.
  ///
  /// # Returns
  ///
  /// A vector of changes.
  ///
  /// Complexity: O(n), where n is the number of records (optimized with version bounds)
  virtual CrdtVector<Change<K, V>> get_changes_since(uint64_t last_db_version, CrdtSet<CrdtNodeId> excluding = {}) const {
    CrdtVector<Change<K, V>> changes;

    // Get changes from parent
    if (parent_) {
      auto parent_changes = parent_->get_changes_since(last_db_version);
      changes.insert(changes.end(), parent_changes.begin(), parent_changes.end());
    }

    // Get changes from regular records
    for (const auto &[record_id, record] : data_) {
      // Skip records that haven't changed since last_db_version
      if (record.highest_local_db_version <= last_db_version) {
        continue;
      }

      for (const auto &[col_name, clock_info] : record.column_versions) {
        if (clock_info.local_db_version > last_db_version && !excluding.contains(clock_info.node_id)) {
          std::optional<V> value = std::nullopt;
          std::optional<CrdtKey> name = col_name;

          auto field_it = record.fields.find(col_name);
          if (field_it != record.fields.end()) {
            value = field_it->second;
          }

          changes.emplace_back(Change<K, V>(record_id, std::move(name), std::move(value), clock_info.col_version,
                                            clock_info.db_version, clock_info.node_id, clock_info.local_db_version));
        }
      }
    }

    // Get deletion changes from tombstones
    for (const auto &entry : tombstones_) {
      const auto &record_id = entry.record_id;
      const auto clock_info = entry.info.to_column_version();
      if (clock_info.local_db_version > last_db_version && !excluding.contains(clock_info.node_id)) {
        changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt, clock_info.col_version,
                                          clock_info.db_version, clock_info.node_id, clock_info.local_db_version));
      }
    }

    if (parent_) {
      // Since we merge from the parent, we need to also run a compression pass
      // to remove changes that have been overwritten by top level changes
      // since we compare at first by col_version, it's fine even if our db_version is lower
      // since we merge from the parent, we know that the changes are applied in order and col_version should always be increasing
      compress_changes(changes);
    }

    return changes;
  }

  /// Merges a set of incoming changes into the CRDT.
  ///
  /// # Arguments
  ///
  /// * `changes` - A vector of changes to merge.
  /// * `merge_rule` - The merge rule to use for conflict resolution.
  /// * `context` - Optional context for the merge rule.
  ///
  /// # Returns
  ///
  /// If `ReturnAcceptedChanges` is `true`, returns a vector of accepted changes.
  /// Otherwise, returns `void`.
  ///
  /// Complexity: O(c), where c is the number of changes to merge
  template <bool ReturnAcceptedChanges = false, typename MergeContext = void,
            MergeRule<K, V, MergeContext> MergeRuleType = DefaultMergeRule<K, V, MergeContext>>
  std::conditional_t<ReturnAcceptedChanges, CrdtVector<Change<K, V>>, void>
  merge_changes(CrdtVector<Change<K, V>> &&changes, bool ignore_parent = false, MergeRuleType merge_rule = MergeRuleType(),
                std::conditional_t<std::is_void_v<MergeContext>,
                                   std::monostate, // Use monostate for void case
                                   MergeContext>
                    context = {}) {
    CrdtVector<Change<K, V>> accepted_changes;

    if (changes.empty()) {
      if constexpr (ReturnAcceptedChanges) {
        return accepted_changes;
      } else {
        return;
      }
    }

    for (auto &&change : changes) {
      const K &record_id = change.record_id;
      std::optional<CrdtKey> col_name = std::move(change.col_name);
      uint64_t remote_col_version = change.col_version;
      uint64_t remote_db_version = change.db_version;
      CrdtNodeId remote_node_id = change.node_id;
      std::optional<V> remote_value = std::move(change.value);
      uint32_t flags = change.flags;

      // Always update the logical clock to maintain causal consistency,
      // prevent clock drift, and ensure accurate conflict resolution.
      // This reflects the node's knowledge of global progress, even for
      // non-accepted changes.
      uint64_t new_local_db_version = clock_.update(remote_db_version);

      // Skip all changes for tombstoned records
      if (is_record_tombstoned(record_id, ignore_parent)) {
        continue;
      }

      // Retrieve local column version information
      const Record<V> *record_ptr = get_record_ptr(record_id, ignore_parent);
      const ColumnVersion *local_col_info = nullptr;
      
      if (!col_name) {
        // For deletions, check tombstones
        if (auto tombstone_info = tombstones_.find(record_id)) {
          static thread_local ColumnVersion temp_col_version{0, 0, 0, 0};
          temp_col_version = tombstone_info->to_column_version();
          local_col_info = &temp_col_version;
        }
      } else if (record_ptr != nullptr) {
        // For column updates, check the record
        auto col_it = record_ptr->column_versions.find(*col_name);
        if (col_it != record_ptr->column_versions.end()) {
          local_col_info = &col_it->second;
        }
      }

      // Determine whether to accept the remote change
      bool should_accept = false;

      if (local_col_info == nullptr) {
        should_accept = true;
      } else {
        // Use the provided merge rule with context handling
        if constexpr (std::is_void_v<MergeContext>) {
          should_accept = merge_rule(local_col_info->col_version, local_col_info->db_version, local_col_info->node_id,
                                     remote_col_version, remote_db_version, remote_node_id);
        } else {
          should_accept = merge_rule(local_col_info->col_version, local_col_info->db_version, local_col_info->node_id,
                                     remote_col_version, remote_db_version, remote_node_id, context);
        }
      }

      if (should_accept) {
        if (!col_name) {
          // Handle deletion
          data_.erase(record_id);

          // Store deletion information in tombstones
          tombstones_.insert_or_assign(record_id, TombstoneInfo(remote_col_version, remote_db_version, remote_node_id, new_local_db_version));

          if constexpr (ReturnAcceptedChanges) {
            accepted_changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt, remote_col_version,
                                                       remote_db_version, remote_node_id, new_local_db_version, flags));
          }
        } else {
          // Handle insertion or update
          Record<V> &record = get_or_create_record_unchecked(record_id, ignore_parent);

          // Update field value
          if (remote_value.has_value()) {
            if constexpr (ReturnAcceptedChanges) {
              record.fields[*col_name] = *remote_value;
            } else {
              record.fields[*col_name] = std::move(*remote_value);
            }
          } else {
            // If remote_value is std::nullopt, remove the field
            record.fields.erase(*col_name);
          }

          // Update the column version info and record version boundaries
          if constexpr (ReturnAcceptedChanges) {
            record.column_versions.insert_or_assign(
                *col_name, ColumnVersion(remote_col_version, remote_db_version, remote_node_id, new_local_db_version));

            // Update version boundaries
            if (new_local_db_version < record.lowest_local_db_version) {
              record.lowest_local_db_version = new_local_db_version;
            }
            if (new_local_db_version > record.highest_local_db_version) {
              record.highest_local_db_version = new_local_db_version;
            }

            accepted_changes.emplace_back(Change<K, V>(record_id, std::move(col_name), std::move(remote_value),
                                                       remote_col_version, remote_db_version, remote_node_id,
                                                       new_local_db_version, flags));
          } else {
            record.column_versions.insert_or_assign(
                std::move(*col_name), ColumnVersion(remote_col_version, remote_db_version, remote_node_id, new_local_db_version));

            // Update version boundaries
            if (new_local_db_version < record.lowest_local_db_version) {
              record.lowest_local_db_version = new_local_db_version;
            }
            if (new_local_db_version > record.highest_local_db_version) {
              record.highest_local_db_version = new_local_db_version;
            }
          }
        }
      }
    }

    if constexpr (ReturnAcceptedChanges) {
      return accepted_changes;
    }
  }

  /// Compresses a vector of changes in-place by removing redundant changes that overwrite each other.
  ///
  /// # Arguments
  ///
  /// * `changes` - A vector of changes to compress (will be modified in-place).
  ///
  /// Complexity: O(n log n), where n is the number of changes
  template <bool Sorted = false> static void compress_changes(CrdtVector<Change<K, V>> &changes) {
    if (changes.empty())
      return;

    auto new_end = compress_changes<Sorted>(changes.begin(), changes.end());
    changes.erase(new_end, changes.end());
  }

  /// Compresses a range of changes by removing redundant changes that overwrite each other.
  ///
  /// # Arguments
  ///
  /// * `begin` - Iterator to the beginning of the range.
  /// * `end` - Iterator to the end of the range.
  ///
  /// # Returns
  ///
  /// Iterator to the new end of the range after compression.
  ///
  /// Complexity: O(n log n), where n is the number of changes
  template <bool Sorted = false, typename Iterator> static Iterator compress_changes(Iterator begin, Iterator end) {
    if (begin == end)
      return end;

    if constexpr (!Sorted) {
      // Sort changes using the DefaultChangeComparator
      SortFunctionType()(begin, end, DefaultChangeComparator<K, V>());
    }

    // Use two-pointer technique to compress in-place
    Iterator write = begin;
    for (Iterator read = std::next(begin); read != end; ++read) {
      if (read->record_id != write->record_id) {
        // New record, always keep it
        ++write;
        if (write != read) {
          *write = std::move(*read);
        }
      } else if (!read->col_name.has_value() && write->col_name.has_value()) {
        // Current read is a deletion, keep it and skip all previous changes for this record
        *write = std::move(*read);

      } // Check if new column name, but make sure it's not a deletion
      else if (read->col_name != write->col_name && write->col_name.has_value()) {
        // New column for the same record
        ++write;
        if (write != read) {
          *write = std::move(*read);
        }
      }
      // Else: same record and column, keep the existing one (which is the most recent due to sorting)
    }

    return std::next(write);
  }

/// Prints the current data and tombstones for debugging purposes.
///
/// Complexity: O(n * m), where n is the number of records and m is the average number of fields per record
#ifndef NDEBUG
  constexpr void print_data() const {
    std::cout << "Node " << node_id_ << " Data:" << std::endl;
    for (const auto &[record_id, record] : data_) {
      std::cout << "ID: ";
      print_value(record_id);
      std::cout << std::endl;
      for (const auto &[key, value] : record.fields) {
        std::cout << "  ";
        print_value(key);
        std::cout << ": ";
        print_value(value);
        std::cout << std::endl;
      }
    }
  }
#else
  constexpr void print_data() const {}
#endif

  // Complexity: O(1)
  constexpr const LogicalClock &get_clock() const { return clock_; }

  constexpr CrdtMap<K, Record<V>> get_data_combined() const {
    if (!parent_) {
      return data_;
    }

    CrdtMap<K, Record<V>> combined_data = parent_->get_data_combined();
    
    // Remove any records that are tombstoned in this CRDT
    for (const auto &entry : tombstones_) {
      combined_data.erase(entry.record_id);
    }
    
    // Add records from this CRDT
    for (const auto &[key, record] : data_) {
      combined_data[key] = record;
    }
    
    return combined_data;
  }

  constexpr auto &get_data() { return data_; }

  /// Retrieves a pointer to a record if it exists, or nullptr if it doesn't.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `ignore_parent` - If true, only checks the current CRDT instance, ignoring the parent.
  ///
  /// # Returns
  ///
  /// A pointer to the Record<V> if found, or nullptr if not found.
  ///
  /// Complexity: O(1) average case for hash table lookup
  constexpr Record<V> *get_record(const K &record_id, bool ignore_parent = false) {
    return get_record_ptr(record_id, ignore_parent);
  }

  constexpr const Record<V> *get_record(const K &record_id, bool ignore_parent = false) const {
    return get_record_ptr(record_id, ignore_parent);
  }

  // Add this public method to the CRDT class
  /// Checks if a record is tombstoned.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `ignore_parent` - If true, only checks the current CRDT instance, ignoring the parent.
  ///
  /// # Returns
  ///
  /// True if the record is tombstoned, false otherwise.
  ///
  /// Complexity: O(1) average case for hash table lookup
  constexpr bool is_tombstoned(const K &record_id, bool ignore_parent = false) const {
    return is_record_tombstoned(record_id, ignore_parent);
  }

  /// Gets tombstone information for a record.
  ///
  /// # Arguments
  ///
  /// * `record_id` - The unique identifier for the record.
  /// * `ignore_parent` - If true, only checks the current CRDT instance, ignoring the parent.
  ///
  /// # Returns
  ///
  /// std::optional<ColumnVersion> containing the tombstone version information if the record is tombstoned,
  /// or std::nullopt if the record is not tombstoned.
  ///
  /// Complexity: O(1) average case for hash table lookup
  constexpr std::optional<ColumnVersion> get_tombstone(const K &record_id, bool ignore_parent = false) const {
    if (auto tombstone_info = tombstones_.find(record_id)) {
      return tombstone_info->to_column_version();
    }

    if (parent_ && !ignore_parent) {
      return parent_->get_tombstone(record_id);
    }
    
    return std::nullopt;
  }

  /// Query records matching a predicate.
  ///
  /// # Arguments
  ///
  /// * `pred` - A predicate function that takes a key and record and returns a boolean.
  ///
  /// # Returns
  ///
  /// A vector of key-record pairs for records that match the predicate.
  ///
  /// Complexity: O(n), where n is the number of records
  template <typename Predicate> CrdtVector<std::pair<K, Record<V>>> query_records(Predicate &&pred) const {
    CrdtVector<std::pair<K, Record<V>>> results;
    for (const auto &[key, record] : data_) {
      if (!is_tombstoned(key) && pred(key, record)) {
        results.emplace_back(key, record);
      }
    }
    return results;
  }

  /// Projection to extract specific columns only.
  ///
  /// # Arguments
  ///
  /// * `pred` - A predicate function that takes a key and record and returns a boolean.
  /// * `proj` - A projection function that takes a key and record and returns the desired result type.
  ///
  /// # Returns
  ///
  /// A vector of projected results for records that match the predicate.
  ///
  /// Complexity: O(n), where n is the number of records
  template <typename Predicate, typename Projection> auto query_with_projection(Predicate &&pred, Projection &&proj) const {
    using ResultType = std::invoke_result_t<Projection, K, Record<V>>;
    CrdtVector<ResultType> results;
    for (const auto &[key, record] : data_) {
      if (!is_tombstoned(key) && pred(key, record)) {
        results.push_back(proj(key, record));
      }
    }
    return results;
  }

  // Add this constructor to the CRDT class
  CRDT(const CRDT &other)
      : node_id_(other.node_id_), clock_(other.clock_), data_(other.data_), tombstones_(other.tombstones_), parent_(other.parent_),
        base_version_(other.base_version_), sort_func_(other.sort_func_) {
    // Note: This creates a shallow copy of the parent pointer
  }

  CRDT &operator=(const CRDT &other) {
    if (this != &other) {
      node_id_ = other.node_id_;
      clock_ = other.clock_;
      data_ = other.data_;
      tombstones_ = other.tombstones_;
      parent_ = other.parent_;
      base_version_ = other.base_version_;
      sort_func_ = other.sort_func_;
    }
    return *this;
  }

  // Move constructor
  CRDT(CRDT &&other) noexcept
      : node_id_(other.node_id_), clock_(std::move(other.clock_)), data_(std::move(other.data_)), tombstones_(std::move(other.tombstones_)),
        parent_(std::move(other.parent_)), base_version_(other.base_version_), sort_func_(std::move(other.sort_func_)) {}

  // Move assignment operator
  CRDT &operator=(CRDT &&other) noexcept {
    if (this != &other) {
      node_id_ = other.node_id_;
      clock_ = std::move(other.clock_);
      data_ = std::move(other.data_);
      tombstones_ = std::move(other.tombstones_);
      parent_ = std::move(other.parent_);
      base_version_ = other.base_version_;
      sort_func_ = std::move(other.sort_func_);
    }
    return *this;
  }

protected:
  // Helper function to print values
  template <typename T> static void print_value(const T &value) {
    if constexpr (std::is_same_v<T, std::string> || std::is_arithmetic_v<T>) {
      std::cout << value;
    } else {
      std::cout << "[non-printable]";
    }
  }

  /// Applies a list of changes to reconstruct the CRDT state.
  ///
  /// # Arguments
  ///
  /// * `changes` - A list of changes to apply.
  ///
  /// Complexity: O(n), where n is the number of changes
  void apply_changes(CrdtVector<Change<K, V>> &&changes) {
    // Determine the maximum db_version from the changes
    uint64_t max_db_version = 0;
    for (const auto &change : changes) {
      if (change.db_version > max_db_version) {
        max_db_version = change.db_version;
      }
      // also consider the local db_version if it's higher
      if (change.local_db_version > max_db_version) {
        max_db_version = change.local_db_version;
      }
    }

    // Set the logical clock to the maximum db_version
    clock_.set_time(max_db_version);

    // Apply each change to reconstruct the CRDT state
    for (auto &&change : changes) {
      const K &record_id = change.record_id;
      std::optional<CrdtKey> col_name = std::move(change.col_name);
      uint64_t remote_col_version = change.col_version;
      uint64_t remote_db_version = change.db_version;
      CrdtNodeId remote_node_id = change.node_id;
      uint64_t remote_local_db_version = change.local_db_version;
      std::optional<V> remote_value = std::move(change.value);

      if (!col_name.has_value()) {
        // Handle deletion
        data_.erase(record_id);

        // Store deletion information in tombstones
        tombstones_.insert_or_assign(record_id, TombstoneInfo(remote_col_version, remote_db_version, remote_node_id, remote_local_db_version));
      } else {
        if (!is_record_tombstoned(record_id)) {
          // Handle insertion or update
          Record<V> &record = get_or_create_record_unchecked(record_id);

          // Insert or update the field value
          if (remote_value.has_value()) {
            record.fields[*col_name] = std::move(remote_value.value());
          }

          // Update the column version info
          record.column_versions.insert_or_assign(std::move(*col_name), ColumnVersion(remote_col_version, remote_db_version,
                                                                                      remote_node_id, remote_local_db_version));

          // Update version boundaries
          if (remote_local_db_version < record.lowest_local_db_version) {
            record.lowest_local_db_version = remote_local_db_version;
          }
          if (remote_local_db_version > record.highest_local_db_version) {
            record.highest_local_db_version = remote_local_db_version;
          }
        }
      }
    }
  }

  constexpr bool is_record_tombstoned(const K &record_id, bool ignore_parent = false) const {
    if (tombstones_.find(record_id).has_value()) {
      return true;
    }

    if (parent_ && !ignore_parent) {
      return parent_->is_record_tombstoned(record_id);
    }
    return false;
  }

  // Notice that this will not check if the record is tombstoned! Such check should be done by the caller
  constexpr Record<V> &get_or_create_record_unchecked(const K &record_id, bool ignore_parent = false) {
    auto [it, inserted] = data_.try_emplace(record_id, Record<V>());
    if (inserted && parent_ && !ignore_parent) {
      if (auto parent_record = parent_->get_record_ptr(record_id)) {
        it->second = *parent_record;
      }
    }
    return it->second;
  }

  constexpr Record<V> *get_record_ptr(const K &record_id, bool ignore_parent = false) {
    auto it = data_.find(record_id);
    if (it != data_.end()) {
      return &(it->second);
    }
    if (ignore_parent) {
      return nullptr;
    } else {
      return parent_ ? parent_->get_record_ptr(record_id) : nullptr;
    }
  }

  constexpr const Record<V> *get_record_ptr(const K &record_id, bool ignore_parent = false) const {
    auto it = data_.find(record_id);
    if (it != data_.end()) {
      return &(it->second);
    }
    if (ignore_parent) {
      return nullptr;
    } else {
      return parent_ ? parent_->get_record_ptr(record_id) : nullptr;
    }
  }

  /// Generates inverse changes for a given set of changes based on a reference CRDT state.
  ///
  /// # Arguments
  ///
  /// * `changes` - A vector of changes to invert.
  /// * `reference_crdt` - A reference CRDT to use as the base state for inversion.
  ///
  /// # Returns
  ///
  /// A vector of inverse `Change` objects.
  CrdtVector<Change<K, V>> invert_changes(const CrdtVector<Change<K, V>> &changes, const CRDT &reference_crdt) const {
    CrdtVector<Change<K, V>> inverse_changes;

    for (const auto &change : changes) {
      const K &record_id = change.record_id;
      const std::optional<CrdtKey> &col_name = change.col_name;

      if (!col_name.has_value()) {
        // The change was a record deletion (tombstone)
        // To revert, restore the record's state from the reference CRDT
        auto record_ptr = reference_crdt.get_record(record_id);
        if (record_ptr) {
          // Restore all fields from the record, sorted by db_version
          std::vector<std::pair<CrdtKey, V>> sorted_fields(record_ptr->fields.begin(), record_ptr->fields.end());
          std::sort(sorted_fields.begin(), sorted_fields.end(), [&](const auto &a, const auto &b) {
            return record_ptr->column_versions.at(a.first).db_version < record_ptr->column_versions.at(b.first).db_version;
          });
          for (const auto &[ref_col, ref_val] : sorted_fields) {
            inverse_changes.emplace_back(Change<K, V>(record_id, ref_col, ref_val,
                                                      record_ptr->column_versions.at(ref_col).col_version,
                                                      record_ptr->column_versions.at(ref_col).db_version, node_id_,
                                                      record_ptr->column_versions.at(ref_col).local_db_version));
          }
        }
      } else {
        // The change was an insertion or update of a column
        CrdtKey col = *col_name;
        auto record_ptr = reference_crdt.get_record(record_id);
        if (record_ptr) {
          auto field_it = record_ptr->fields.find(col);
          if (field_it != record_ptr->fields.end()) {
            // The record has a value for this column in the reference; set it back to the reference's value
            inverse_changes.emplace_back(Change<K, V>(
                record_id, col, field_it->second, record_ptr->column_versions.at(col).col_version,
                record_ptr->column_versions.at(col).db_version, node_id_, record_ptr->column_versions.at(col).local_db_version));
          } else {
            // The record does not have this column in the reference; delete it to revert
            inverse_changes.emplace_back(Change<K, V>(record_id, col,
                                                      std::nullopt, // Indicates deletion
                                                      0,            // Column version 0 signifies deletion
                                                      clock_.current_time(), node_id_));
          }
        } else {
          // The record does not exist in the reference; remove the entire record to revert
          inverse_changes.emplace_back(Change<K, V>(record_id, std::nullopt, std::nullopt,
                                                    0, // Column version 0 signifies a tombstone
                                                    clock_.current_time(), node_id_));
        }
      }
    }

    return inverse_changes;
  }

  /// Implementation of insert_or_update
  template <typename ChangeContainer, typename... Pairs>
  constexpr void insert_or_update_impl(const K &record_id, uint32_t flags, ChangeContainer *changes, Pairs &&...pairs) {
    uint64_t db_version = clock_.tick();

    // Check if the record is tombstoned
    if (is_record_tombstoned(record_id)) {
      return;
    }

    Record<V> &record = get_or_create_record_unchecked(record_id);

    // Helper function to process each pair
    auto process_pair = [&](const auto &pair) {
      const auto &col_name = pair.first;
      const auto &value = pair.second;

      uint64_t col_version;
      auto col_it = record.column_versions.find(col_name);
      if (col_it != record.column_versions.end()) {
        col_version = ++col_it->second.col_version;
        col_it->second.db_version = db_version;
        col_it->second.node_id = node_id_;
        col_it->second.local_db_version = db_version;
      } else {
        col_version = 1;
        record.column_versions.emplace(col_name, ColumnVersion(col_version, db_version, node_id_, db_version));
      }

      // Update record version boundaries
      if (db_version < record.lowest_local_db_version) {
        record.lowest_local_db_version = db_version;
      }
      if (db_version > record.highest_local_db_version) {
        record.highest_local_db_version = db_version;
      }

      if (changes) {
        record.fields[col_name] = value;
        add_to_container(*changes, Change<K, V>(record_id, std::move(col_name), std::move(value), col_version, db_version,
                                                node_id_, db_version, flags));
      } else {
        record.fields[std::move(col_name)] = std::move(value);
      }
    };

    // Process all pairs
    (process_pair(std::forward<Pairs>(pairs)), ...);
  }

  /// Implementation of insert_or_update_from_container
  template <typename ChangeContainer, typename Container>
  constexpr void insert_or_update_from_container_impl(const K &record_id, uint32_t flags, Container &&fields,
                                                      ChangeContainer *changes) {
    uint64_t db_version = clock_.tick();

    // Check if the record is tombstoned
    if (is_record_tombstoned(record_id)) {
      return;
    }

    Record<V> &record = get_or_create_record_unchecked(record_id);

    // Process each field-value pair in the container
    for (auto &&[col_name, value] : std::forward<Container>(fields)) {
      uint64_t col_version;
      auto col_it = record.column_versions.find(col_name);
      if (col_it != record.column_versions.end()) {
        col_version = ++col_it->second.col_version;
        col_it->second.db_version = db_version;
        col_it->second.node_id = node_id_;
        col_it->second.local_db_version = db_version;
      } else {
        col_version = 1;
        record.column_versions.emplace(col_name, ColumnVersion(col_version, db_version, node_id_, db_version));
      }

      // Update record version boundaries
      if (db_version < record.lowest_local_db_version) {
        record.lowest_local_db_version = db_version;
      }
      if (db_version > record.highest_local_db_version) {
        record.highest_local_db_version = db_version;
      }

      if (changes) {
        record.fields[col_name] = value;
        add_to_container(*changes, Change<K, V>(record_id, std::move(col_name), std::move(value), col_version, db_version,
                                                node_id_, db_version, flags));
      } else {
        record.fields[std::move(col_name)] = std::move(value);
      }
    }
  }

  /// Implementation of delete_record
  template <typename ChangeContainer> void delete_record_impl(const K &record_id, uint32_t flags, ChangeContainer *changes) {
    if (is_record_tombstoned(record_id)) {
      return;
    }

    uint64_t db_version = clock_.tick();

    // Mark as tombstone and remove data
    data_.erase(record_id);

    // Store deletion information in tombstones
    tombstones_.insert_or_assign(record_id, TombstoneInfo(1, db_version, node_id_, db_version));

    if (changes) {
      add_to_container(*changes, Change<K, V>(record_id, std::nullopt, std::nullopt, 1, db_version, node_id_, db_version, flags));
    }
  }
};

/// Synchronizes two CRDT nodes.
/// Retrieves changes from the source since last_db_version and merges them into
/// the target. Updates last_db_version to prevent reprocessing the same
/// changes.
///
/// Complexity: O(c + m), where c is the number of changes since last_db_version,
/// and m is the complexity of merge_changes
template <typename K, typename V, typename SortFunctionType = DefaultSort, MapLike<K, Record<V>> MapType = CrdtMap<K, Record<V>>,
          typename MergeContext = void, MergeRule<K, V, MergeContext> MergeRuleType = DefaultMergeRule<K, V, MergeContext>>
constexpr void sync_nodes(CRDT<K, V, SortFunctionType, MapType> &source, CRDT<K, V, SortFunctionType, MapType> &target,
                          uint64_t &last_db_version, MergeRuleType merge_rule = MergeRuleType(),
                          std::conditional_t<std::is_void_v<MergeContext>,
                                             std::monostate, // Use monostate for void case
                                             MergeContext>
                              context = {}) {
  auto changes = source.get_changes_since(last_db_version);

  // Update last_db_version to the current max db_version in source
  uint64_t max_version = last_db_version;
  for (const auto &change : changes) {
    if (change.db_version > max_version) {
      max_version = change.db_version;
    }
  }
  if (max_version > last_db_version) {
    last_db_version = max_version;
  }

  target.merge_changes(std::move(changes), false, merge_rule, context);
}

#endif // CRDT_HPP