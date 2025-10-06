// text_crdt.hpp
#ifndef TEXT_CRDT_HPP
#define TEXT_CRDT_HPP

#include <cstdint>
#include <vector>
#include <map>
#include <unordered_map>
#include <optional>
#include <string>
#include <algorithm>
#include <memory>
#include <compare>

// Reuse core concepts from crdt.hpp
#ifndef CRDT_COLLECTIONS_DEFINED
using CrdtNodeId = uint64_t;
#endif

/// Represents a position in the document using fractional indexing with array-based approach.
/// Position is a path of integers that defines total ordering.
/// Between any two positions, we can always generate a new position.
struct FractionalPosition {
  std::vector<uint64_t> path;

  // Default constructor creates invalid/sentinel position
  FractionalPosition() = default;

  // Construct from path
  explicit FractionalPosition(std::vector<uint64_t> p) : path(std::move(p)) {}

  // C++20 spaceship operator for total ordering
  auto operator<=>(const FractionalPosition &other) const = default;
  bool operator==(const FractionalPosition &other) const = default;

  // Check if position is valid
  bool is_valid() const { return !path.empty(); }

  // Constants for position generation
  static constexpr uint64_t INITIAL_SPACING = 10000;
  static constexpr uint64_t MIN_SPACING = 10;

  /// Generate the first position in the document
  static FractionalPosition first() { return FractionalPosition({INITIAL_SPACING}); }

  /// Generate position after all others (for append)
  static FractionalPosition after(const FractionalPosition &pos) {
    if (!pos.is_valid()) {
      return first();
    }
    auto new_path = pos.path;
    new_path.back() += INITIAL_SPACING;
    return FractionalPosition(std::move(new_path));
  }

  /// Generate position before all others (for prepend)
  static FractionalPosition before(const FractionalPosition &pos) {
    if (!pos.is_valid() || pos.path.empty()) {
      return FractionalPosition({INITIAL_SPACING / 2});
    }
    if (pos.path[0] >= MIN_SPACING) {
      auto new_path = pos.path;
      new_path[0] -= MIN_SPACING;
      return FractionalPosition(std::move(new_path));
    }
    // If first element is too small, prepend a level
    std::vector<uint64_t> new_path = {INITIAL_SPACING / 2};
    return FractionalPosition(std::move(new_path));
  }

  /// Generate position between two positions
  /// This is the core algorithm - must handle all cases correctly
  static FractionalPosition between(const FractionalPosition &a, const FractionalPosition &b) {
    // Handle edge cases
    if (!a.is_valid() && !b.is_valid()) {
      return first();
    }
    if (!a.is_valid()) {
      return before(b);
    }
    if (!b.is_valid()) {
      return after(a);
    }

    // Find the first position where paths differ
    size_t min_len = std::min(a.path.size(), b.path.size());
    size_t diff_idx = 0;
    while (diff_idx < min_len && a.path[diff_idx] == b.path[diff_idx]) {
      diff_idx++;
    }

    // Case 1: One path is prefix of the other
    if (diff_idx == min_len) {
      // a is shorter or equal - extend a's path
      std::vector<uint64_t> new_path = a.path;
      if (a.path.size() < b.path.size()) {
        // a is prefix of b, insert between by taking b's next element and halving
        new_path.push_back(b.path[diff_idx] / 2);
      } else {
        // a == b (shouldn't happen in practice, but handle it)
        new_path.push_back(INITIAL_SPACING);
      }
      return FractionalPosition(std::move(new_path));
    }

    // Case 2: Paths differ at diff_idx
    uint64_t a_val = a.path[diff_idx];
    uint64_t b_val = b.path[diff_idx];

    // If there's room between the values, use midpoint
    if (b_val - a_val > 1) {
      std::vector<uint64_t> new_path(a.path.begin(), a.path.begin() + diff_idx + 1);
      new_path[diff_idx] = a_val + (b_val - a_val) / 2;
      return FractionalPosition(std::move(new_path));
    }

    // No room - extend a's path at next level
    std::vector<uint64_t> new_path = a.path;
    if (new_path.size() == diff_idx + 1) {
      // a ends at diff_idx, append a new level
      new_path.push_back(INITIAL_SPACING);
    } else {
      // a continues beyond diff_idx, increment at next level
      new_path[diff_idx + 1] += INITIAL_SPACING;
    }
    return FractionalPosition(std::move(new_path));
  }
};

/// Version information for a line
struct VersionInfo {
  uint64_t line_version;     // Per-line version (increments on edit)
  uint64_t db_version;       // Global logical clock
  CrdtNodeId node_id;        // Node that made this change
  uint64_t local_db_version; // Local clock when change was applied

  VersionInfo(uint64_t lv = 1, uint64_t db = 0, CrdtNodeId nid = 0, uint64_t local_db = 0)
      : line_version(lv), db_version(db), node_id(nid), local_db_version(local_db) {}
};

/// Represents a concurrent version of content (for Both-Writes-Win)
template <typename V = std::string> struct ConflictVersion {
  V content;
  VersionInfo version;

  ConflictVersion(V c, VersionInfo v) : content(std::move(c)), version(std::move(v)) {}

  // Order by version for deterministic conflict presentation
  bool operator<(const ConflictVersion &other) const {
    if (version.db_version != other.version.db_version)
      return version.db_version < other.version.db_version;
    return version.node_id < other.version.node_id;
  }
};

/// Data stored for each line in the document
template <typename K, typename V = std::string> struct LineData {
  K id;                      // Stable UUID identity
  FractionalPosition position; // Current position in document (can change if moved)
  V content;                 // Line content (current/displayed version)
  VersionInfo version;       // Version metadata

  // For Both-Writes-Win: store concurrent conflicting versions
  std::optional<std::vector<ConflictVersion<V>>> conflicts;

  // For auto-merge: store base content (before local edit)
  // This enables 3-way merge when concurrent edits are detected
  std::optional<V> base_content;

  LineData() = default;

  LineData(K line_id, FractionalPosition pos, V c, VersionInfo v)
      : id(std::move(line_id)), position(std::move(pos)), content(std::move(c)), version(std::move(v)) {}

  // Check if line has unresolved conflicts
  bool has_conflicts() const { return conflicts.has_value() && !conflicts->empty(); }

  // Get all versions (main content + conflicts) for BWW
  std::vector<ConflictVersion<V>> get_all_versions() const {
    std::vector<ConflictVersion<V>> all;
    all.emplace_back(content, version);
    if (conflicts.has_value()) {
      all.insert(all.end(), conflicts->begin(), conflicts->end());
    }
    return all;
  }
};

/// Change types for text operations
enum class TextChangeType {
  Insert,  // New line inserted
  Edit,    // Line content changed
  Delete,  // Line deleted
  Move     // Line position changed (future)
};

/// Represents a change to the text document
template <typename K, typename V = std::string> struct TextChange {
  TextChangeType type;
  K line_id;
  std::optional<FractionalPosition> position; // For inserts/moves
  std::optional<V> content;                   // For inserts/edits
  VersionInfo version;

  TextChange() = default;

  TextChange(TextChangeType t, K id, VersionInfo v)
      : type(t), line_id(std::move(id)), version(std::move(v)) {}

  TextChange(TextChangeType t, K id, FractionalPosition pos, V c, VersionInfo v)
      : type(t), line_id(std::move(id)), position(std::move(pos)),
        content(std::move(c)), version(std::move(v)) {}
};

/// Logical clock for causality tracking (reused from crdt.hpp concepts)
class LogicalClock {
public:
  LogicalClock() : time_(0) {}

  uint64_t tick() { return ++time_; }

  uint64_t update(uint64_t received_time) {
    time_ = std::max(time_, received_time);
    return ++time_;
  }

  void set_time(uint64_t t) { time_ = t; }

  uint64_t current_time() const { return time_; }

private:
  uint64_t time_;
};

/// Text CRDT with line-based granularity and fractional positioning
///
/// Template parameters:
/// - K: Line ID type (default: std::string, could be UUID type)
/// - V: Content type (default: std::string)
///
/// Features:
/// - Dual indexing: fast iteration by position, fast lookup by ID
/// - Fractional positioning: infinite density between any two lines
/// - Both-Writes-Win support: preserve concurrent edits
/// - Logical clocks: maintain causality
template <typename K = std::string, typename V = std::string> class TextCRDT {
protected:
  CrdtNodeId node_id_;
  LogicalClock clock_;

  // Dual indexing for efficient operations
  std::map<FractionalPosition, K> position_to_id_;           // Ordered by position
  std::unordered_map<K, LineData<K, V>> lines_;              // Lookup by ID

  // Tombstones for deleted lines (track deletion metadata)
  std::unordered_map<K, VersionInfo> tombstones_;

public:
  /// Create a new empty text CRDT
  explicit TextCRDT(CrdtNodeId node_id) : node_id_(node_id), clock_() {}

  /// Insert a line at the beginning of the document
  K insert_line_at_start(const V &content) {
    uint64_t db_version = clock_.tick();
    K line_id = generate_line_id();

    FractionalPosition pos;
    if (position_to_id_.empty()) {
      pos = FractionalPosition::first();
    } else {
      pos = FractionalPosition::before(position_to_id_.begin()->first);
    }

    LineData<K, V> line(line_id, pos, content, VersionInfo(1, db_version, node_id_, db_version));

    position_to_id_[pos] = line_id;
    lines_[line_id] = std::move(line);

    return line_id;
  }

  /// Insert a line at the end of the document
  K insert_line_at_end(const V &content) {
    uint64_t db_version = clock_.tick();
    K line_id = generate_line_id();

    FractionalPosition pos;
    if (position_to_id_.empty()) {
      pos = FractionalPosition::first();
    } else {
      pos = FractionalPosition::after(position_to_id_.rbegin()->first);
    }

    LineData<K, V> line(line_id, pos, content, VersionInfo(1, db_version, node_id_, db_version));

    position_to_id_[pos] = line_id;
    lines_[line_id] = std::move(line);

    return line_id;
  }

  /// Insert a line after a specific position
  /// If after_id is nullopt or invalid, inserts at end
  K insert_line_after(const std::optional<K> &after_id, const V &content) {
    if (!after_id.has_value()) {
      return insert_line_at_end(content);
    }

    auto it = lines_.find(*after_id);
    if (it == lines_.end()) {
      return insert_line_at_end(content);
    }

    uint64_t db_version = clock_.tick();
    K line_id = generate_line_id();

    // Find position after the given line
    const FractionalPosition &after_pos = it->second.position;
    auto pos_it = position_to_id_.find(after_pos);

    FractionalPosition new_pos;
    if (pos_it != position_to_id_.end()) {
      auto next_it = std::next(pos_it);
      if (next_it != position_to_id_.end()) {
        // Insert between after_pos and next
        new_pos = FractionalPosition::between(after_pos, next_it->first);
      } else {
        // Insert after last element
        new_pos = FractionalPosition::after(after_pos);
      }
    } else {
      new_pos = FractionalPosition::after(after_pos);
    }

    LineData<K, V> line(line_id, new_pos, content, VersionInfo(db_version, node_id_, db_version));

    position_to_id_[new_pos] = line_id;
    lines_[line_id] = std::move(line);

    return line_id;
  }

  /// Insert a line before a specific position
  K insert_line_before(const std::optional<K> &before_id, const V &content) {
    if (!before_id.has_value()) {
      return insert_line_at_start(content);
    }

    auto it = lines_.find(*before_id);
    if (it == lines_.end()) {
      return insert_line_at_start(content);
    }

    uint64_t db_version = clock_.tick();
    K line_id = generate_line_id();

    // Find position before the given line
    const FractionalPosition &before_pos = it->second.position;
    auto pos_it = position_to_id_.find(before_pos);

    FractionalPosition new_pos;
    if (pos_it != position_to_id_.end() && pos_it != position_to_id_.begin()) {
      auto prev_it = std::prev(pos_it);
      // Insert between prev and before_pos
      new_pos = FractionalPosition::between(prev_it->first, before_pos);
    } else {
      // Insert before first element
      new_pos = FractionalPosition::before(before_pos);
    }

    LineData<K, V> line(line_id, new_pos, content, VersionInfo(db_version, node_id_, db_version));

    position_to_id_[new_pos] = line_id;
    lines_[line_id] = std::move(line);

    return line_id;
  }

  /// Edit a line's content (Last-Write-Wins by default)
  bool edit_line(const K &line_id, const V &new_content) {
    auto it = lines_.find(line_id);
    if (it == lines_.end() || is_tombstoned(line_id)) {
      return false;
    }

    uint64_t db_version = clock_.tick();
    uint64_t new_line_version = it->second.version.line_version + 1;

    // Save base content for potential 3-way merge
    if (!it->second.base_content.has_value()) {
      it->second.base_content = it->second.content;
    }

    it->second.content = new_content;
    it->second.version = VersionInfo(new_line_version, db_version, node_id_, db_version);
    // Clear conflicts on new edit (user resolved)
    it->second.conflicts.reset();

    return true;
  }

  /// Delete a line (tombstone it)
  bool delete_line(const K &line_id) {
    auto it = lines_.find(line_id);
    if (it == lines_.end() || is_tombstoned(line_id)) {
      return false;
    }

    uint64_t db_version = clock_.tick();

    // Remove from position index
    position_to_id_.erase(it->second.position);

    // Remove from lines and add tombstone
    tombstones_[line_id] = VersionInfo(1, db_version, node_id_, db_version);
    lines_.erase(it);

    return true;
  }

  /// Get a line by ID
  std::optional<LineData<K, V>> get_line(const K &line_id) const {
    auto it = lines_.find(line_id);
    if (it != lines_.end() && !is_tombstoned(line_id)) {
      return it->second;
    }
    return std::nullopt;
  }

  /// Check if a line is tombstoned (deleted)
  bool is_tombstoned(const K &line_id) const {
    return tombstones_.find(line_id) != tombstones_.end();
  }

  /// Get all lines in document order
  std::vector<LineData<K, V>> get_all_lines() const {
    std::vector<LineData<K, V>> result;
    result.reserve(lines_.size());

    for (const auto &[pos, id] : position_to_id_) {
      auto it = lines_.find(id);
      if (it != lines_.end()) {
        result.push_back(it->second);
      }
    }

    return result;
  }

  /// Get line count
  size_t line_count() const { return lines_.size(); }

  /// Get current clock time
  uint64_t current_version() const { return clock_.current_time(); }

  /// Get node ID
  CrdtNodeId node_id() const { return node_id_; }

  /// Get all changes since a specific version
  std::vector<TextChange<K, V>> get_changes_since(uint64_t last_db_version) const {
    std::vector<TextChange<K, V>> changes;

    // Collect changes from active lines
    for (const auto &[id, line] : lines_) {
      if (line.version.local_db_version > last_db_version) {
        // This line was modified since last sync
        TextChange<K, V> change(
          TextChangeType::Insert, // Could be insert or edit, we treat same
          id,
          line.position,
          line.content,
          line.version
        );
        changes.push_back(change);
      }
    }

    // Collect deletions from tombstones
    for (const auto &[id, tomb_version] : tombstones_) {
      if (tomb_version.local_db_version > last_db_version) {
        TextChange<K, V> change(TextChangeType::Delete, id, tomb_version);
        changes.push_back(change);
      }
    }

    return changes;
  }

  /// Merge changes from remote node using Last-Write-Wins (default)
  void merge_changes(const std::vector<TextChange<K, V>> &changes) {
    for (const auto &change : changes) {
      // Update logical clock to maintain causality
      uint64_t new_local_db_version = clock_.update(change.version.db_version);

      if (change.type == TextChangeType::Delete) {
        // Handle deletion
        merge_delete(change, new_local_db_version);
      } else {
        // Handle insert or edit (same logic for both)
        merge_insert_or_edit(change, new_local_db_version);
      }
    }
  }

  /// Merge changes with custom merge rule (for BWW, auto-merge, etc.)
  template <typename MergeRule>
  void merge_changes_with_rule(const std::vector<TextChange<K, V>> &changes, MergeRule merge_rule) {
    for (const auto &change : changes) {
      uint64_t new_local_db_version = clock_.update(change.version.db_version);

      if (change.type == TextChangeType::Delete) {
        merge_delete(change, new_local_db_version);
      } else {
        merge_insert_or_edit_with_rule(change, new_local_db_version, merge_rule);
      }
    }
  }

  // TODO: Implement Both-Writes-Win merge rule
  // TODO: Implement auto-merge for non-overlapping edits
  // TODO: Implement move operations

protected:
  /// Generate a unique line ID (placeholder - needs proper UUID generation)
  K generate_line_id() {
    // TODO: Replace with proper UUID generation
    // For now, use timestamp + node_id + counter
    static uint64_t counter = 0;
    if constexpr (std::is_same_v<K, std::string>) {
      return std::to_string(clock_.current_time()) + "_" +
             std::to_string(node_id_) + "_" +
             std::to_string(++counter);
    } else {
      // For other types, user must provide ID externally or specialize
      return K{};
    }
  }

  /// Merge a delete operation
  void merge_delete(const TextChange<K, V> &change, uint64_t new_local_db_version) {
    const K &line_id = change.line_id;

    // Check if already tombstoned
    auto tomb_it = tombstones_.find(line_id);
    if (tomb_it != tombstones_.end()) {
      // Already deleted - use LWW to decide which deletion wins
      if (should_accept_lww(tomb_it->second, change.version)) {
        tomb_it->second = change.version;
        tomb_it->second.local_db_version = new_local_db_version;
      }
      return;
    }

    // Check if line exists
    auto it = lines_.find(line_id);
    if (it != lines_.end()) {
      // Line exists - remove it and add tombstone
      position_to_id_.erase(it->second.position);
      lines_.erase(it);
    }

    // Add tombstone
    VersionInfo tomb_version = change.version;
    tomb_version.local_db_version = new_local_db_version;
    tombstones_[line_id] = tomb_version;
  }

  /// Merge an insert or edit operation (LWW)
  void merge_insert_or_edit(const TextChange<K, V> &change, uint64_t new_local_db_version) {
    const K &line_id = change.line_id;

    // Skip if tombstoned
    if (is_tombstoned(line_id)) {
      return;
    }

    auto it = lines_.find(line_id);
    if (it == lines_.end()) {
      // New line - insert it
      if (!change.position.has_value() || !change.content.has_value()) {
        return; // Invalid change
      }

      VersionInfo new_version = change.version;
      new_version.local_db_version = new_local_db_version;

      LineData<K, V> new_line(line_id, *change.position, *change.content, new_version);

      // Check for position collision
      auto pos_it = position_to_id_.find(*change.position);
      if (pos_it != position_to_id_.end() && pos_it->second != line_id) {
        // Position collision! Adjust position slightly using node_id for determinism
        FractionalPosition adjusted_pos = *change.position;
        // Append node_id as tie-breaker to make position unique
        adjusted_pos.path.push_back(change.version.node_id);
        new_line.position = adjusted_pos;
        position_to_id_[adjusted_pos] = line_id;
      } else {
        position_to_id_[*change.position] = line_id;
      }

      lines_[line_id] = std::move(new_line);
    } else {
      // Line exists - check if we should accept the change (LWW)
      if (should_accept_lww(it->second.version, change.version)) {
        // Accept the change
        if (change.content.has_value()) {
          it->second.content = *change.content;
        }

        VersionInfo new_version = change.version;
        new_version.local_db_version = new_local_db_version;
        it->second.version = new_version;
        it->second.conflicts.reset(); // Clear conflicts on new write
      }
    }
  }

  /// Merge an insert or edit with custom merge rule
  template <typename MergeRule>
  void merge_insert_or_edit_with_rule(const TextChange<K, V> &change,
                                      uint64_t new_local_db_version,
                                      MergeRule merge_rule) {
    const K &line_id = change.line_id;

    if (is_tombstoned(line_id)) {
      return;
    }

    auto it = lines_.find(line_id);
    if (it == lines_.end()) {
      // New line - always insert
      if (!change.position.has_value() || !change.content.has_value()) {
        return;
      }

      VersionInfo new_version = change.version;
      new_version.local_db_version = new_local_db_version;

      LineData<K, V> new_line(line_id, *change.position, *change.content, new_version);

      // Check for position collision
      auto pos_it = position_to_id_.find(*change.position);
      if (pos_it != position_to_id_.end() && pos_it->second != line_id) {
        // Position collision! Adjust position slightly using node_id for determinism
        FractionalPosition adjusted_pos = *change.position;
        adjusted_pos.path.push_back(change.version.node_id);
        new_line.position = adjusted_pos;
        position_to_id_[adjusted_pos] = line_id;
      } else {
        position_to_id_[*change.position] = line_id;
      }

      lines_[line_id] = std::move(new_line);
    } else {
      // Line exists - use custom merge rule
      if (!change.content.has_value()) {
        return;
      }

      merge_rule(*this, it->second, change, new_local_db_version);
    }
  }

  /// Last-Write-Wins comparison: returns true if remote should win
  bool should_accept_lww(const VersionInfo &local, const VersionInfo &remote) const {
    // Compare line_version first (activity on this specific line)
    if (remote.line_version > local.line_version) {
      return true;
    } else if (remote.line_version < local.line_version) {
      return false;
    }

    // Same line_version - compare db_version (global time)
    if (remote.db_version > local.db_version) {
      return true;
    } else if (remote.db_version < local.db_version) {
      return false;
    }

    // Same db_version - use node_id as tie-breaker
    return remote.node_id > local.node_id;
  }
};

/// Word-level diff helpers for auto-merge
///
/// ⚠️  EXPERIMENTAL - These functions have known bugs (see AutoMergingTextRule docs)
/// The merge_diffs() and apply_diff() functions do not correctly handle
/// merging overlapping operations, leading to incorrect merge results.
namespace TextDiff {
  /// Split text into words (by whitespace)
  inline std::vector<std::string> split_words(const std::string &text) {
    std::vector<std::string> words;
    std::string current;

    for (char c : text) {
      if (std::isspace(c)) {
        if (!current.empty()) {
          words.push_back(current);
          current.clear();
        }
      } else {
        current += c;
      }
    }

    if (!current.empty()) {
      words.push_back(current);
    }

    return words;
  }

  /// Represent a word diff operation
  struct DiffOp {
    enum Type { Keep, Insert, Delete } type;
    size_t position; // Position in base
    std::string word; // For Insert operations

    DiffOp(Type t, size_t pos, std::string w = "")
      : type(t), position(pos), word(std::move(w)) {}
  };

  /// Simple word-level diff (finds insertions and deletions)
  inline std::vector<DiffOp> diff_words(const std::vector<std::string> &base,
                                        const std::vector<std::string> &modified) {
    std::vector<DiffOp> ops;

    // Simple diff: find what changed
    size_t base_idx = 0, mod_idx = 0;

    while (base_idx < base.size() || mod_idx < modified.size()) {
      if (base_idx >= base.size()) {
        // Remaining are insertions
        ops.emplace_back(DiffOp::Insert, base_idx, modified[mod_idx]);
        mod_idx++;
      } else if (mod_idx >= modified.size()) {
        // Remaining are deletions
        ops.emplace_back(DiffOp::Delete, base_idx);
        base_idx++;
      } else if (base[base_idx] == modified[mod_idx]) {
        // Same word, keep it
        ops.emplace_back(DiffOp::Keep, base_idx);
        base_idx++;
        mod_idx++;
      } else {
        // Words differ - check if it's insertion or replacement
        // Look ahead to see if base word appears later in modified
        bool found_later = false;
        for (size_t i = mod_idx + 1; i < modified.size() && i < mod_idx + 5; i++) {
          if (base[base_idx] == modified[i]) {
            // It's an insertion
            ops.emplace_back(DiffOp::Insert, base_idx, modified[mod_idx]);
            mod_idx++;
            found_later = true;
            break;
          }
        }

        if (!found_later) {
          // Treat as replacement (delete + insert)
          ops.emplace_back(DiffOp::Delete, base_idx);
          ops.emplace_back(DiffOp::Insert, base_idx, modified[mod_idx]);
          base_idx++;
          mod_idx++;
        }
      }
    }

    return ops;
  }

  /// Check if two diff operation lists overlap
  inline bool diffs_overlap(const std::vector<DiffOp> &diff1, const std::vector<DiffOp> &diff2) {
    // Check if any operation positions conflict
    for (const auto &op1 : diff1) {
      if (op1.type == DiffOp::Keep) continue;

      for (const auto &op2 : diff2) {
        if (op2.type == DiffOp::Keep) continue;

        // Same position with different operations = conflict
        if (op1.position == op2.position) {
          return true;
        }
      }
    }
    return false;
  }

  /// Apply diff operations to base text
  inline std::string apply_diff(const std::vector<std::string> &base, const std::vector<DiffOp> &ops) {
    std::vector<std::string> result;
    size_t base_idx = 0;

    for (const auto &op : ops) {
      // Add words from base up to this operation
      while (base_idx < op.position && base_idx < base.size()) {
        result.push_back(base[base_idx++]);
      }

      switch (op.type) {
        case DiffOp::Keep:
          if (base_idx < base.size()) {
            result.push_back(base[base_idx++]);
          }
          break;
        case DiffOp::Insert:
          result.push_back(op.word);
          break;
        case DiffOp::Delete:
          base_idx++; // Skip this word from base
          break;
      }
    }

    // Add any remaining base words
    while (base_idx < base.size()) {
      result.push_back(base[base_idx++]);
    }

    // Join words back with spaces
    std::string output;
    for (size_t i = 0; i < result.size(); i++) {
      if (i > 0) output += " ";
      output += result[i];
    }
    return output;
  }

  /// Merge two diffs if they don't overlap
  inline std::vector<DiffOp> merge_diffs(const std::vector<DiffOp> &diff1, const std::vector<DiffOp> &diff2) {
    std::vector<DiffOp> merged;

    // Combine and sort by position
    merged.insert(merged.end(), diff1.begin(), diff1.end());
    merged.insert(merged.end(), diff2.begin(), diff2.end());

    std::sort(merged.begin(), merged.end(), [](const DiffOp &a, const DiffOp &b) {
      return a.position < b.position;
    });

    return merged;
  }
}

/// Both-Writes-Win merge rule: Preserve all concurrent edits
template <typename K, typename V>
struct BothWritesWinMergeRule {
  void operator()(TextCRDT<K, V> &crdt,
                  LineData<K, V> &local_line,
                  const TextChange<K, V> &remote_change,
                  uint64_t new_local_db_version) const {

    // Check if this is a concurrent edit (same line_version, different nodes)
    bool is_concurrent = (local_line.version.line_version == remote_change.version.line_version &&
                         local_line.version.node_id != remote_change.version.node_id);

    if (is_concurrent) {
      // Concurrent edit detected - preserve both versions
      if (!local_line.conflicts.has_value()) {
        local_line.conflicts = std::vector<ConflictVersion<V>>();
      }

      // Add remote version to conflicts
      local_line.conflicts->emplace_back(*remote_change.content, remote_change.version);

      // Sort conflicts for deterministic ordering
      std::sort(local_line.conflicts->begin(), local_line.conflicts->end());

      // Update local_db_version to track that we've seen this change
      VersionInfo new_version = remote_change.version;
      new_version.local_db_version = new_local_db_version;

      // Keep the highest version metadata (but don't change content)
      if (new_version.db_version > local_line.version.db_version) {
        local_line.version.db_version = new_version.db_version;
        local_line.version.local_db_version = new_local_db_version;
      }
    } else {
      // Not concurrent - use LWW
      bool should_accept = false;

      if (remote_change.version.line_version > local_line.version.line_version) {
        should_accept = true;
      } else if (remote_change.version.line_version < local_line.version.line_version) {
        should_accept = false;
      } else {
        // Same line_version - compare db_version
        if (remote_change.version.db_version > local_line.version.db_version) {
          should_accept = true;
        } else if (remote_change.version.db_version < local_line.version.db_version) {
          should_accept = false;
        } else {
          should_accept = remote_change.version.node_id > local_line.version.node_id;
        }
      }

      if (should_accept) {
        local_line.content = *remote_change.content;
        VersionInfo new_version = remote_change.version;
        new_version.local_db_version = new_local_db_version;
        local_line.version = new_version;
        local_line.conflicts.reset(); // Clear conflicts - this is a newer version
      }
    }
  }
};

/// Auto-Merging merge rule: 3-way merge for non-overlapping concurrent edits
///
/// ⚠️  EXPERIMENTAL / INCOMPLETE - See issues below before using!
///
/// INTENDED BEHAVIOR:
/// Uses base content tracking to perform Git-style 3-way merge:
/// - Detects truly non-overlapping changes (different word positions)
/// - Automatically merges them without conflicts
/// - Falls back to deterministic BWW for overlapping changes
///
/// Example successful auto-merge:
///   Base:   "The quick brown fox"
///   Local:  "The FAST brown fox"  (changed word 1)
///   Remote: "The quick brown CAT" (changed word 3)
///   Merge:  "The FAST brown CAT"  (both changes applied!)
///
/// KNOWN ISSUES:
/// 1. Diff merge algorithm is broken - merge_diffs() naively concatenates diffs
///    instead of properly combining them. This causes duplicate KEEP operations
///    and incorrect merge results.
///
/// 2. apply_diff() doesn't correctly handle merged diffs with overlapping positions.
///    Example: When merging [DELETE pos=1, INSERT pos=1] with [KEEP pos=1],
///    it should take the DELETE+INSERT, but currently applies both.
///
/// 3. Non-deterministic convergence - Two nodes merging the same concurrent edits
///    may produce different results due to the broken diff merge logic.
///    THIS VIOLATES THE CRDT CONVERGENCE GUARANTEE!
///
/// 4. Currently falls back to BWW for most cases, making it equivalent to
///    BothWritesWinMergeRule in practice.
///
/// TO FIX:
/// - Implement proper diff merge algorithm that:
///   a) Deduplicates KEEP operations at same position
///   b) Resolves conflicts between DELETE/INSERT and KEEP at same position
///   c) Maintains position order correctly after merging
/// - Ensure apply_diff() correctly applies the merged result
/// - Add comprehensive tests for all edge cases
/// - Verify deterministic convergence in all scenarios
///
/// RECOMMENDED: Use BothWritesWinMergeRule instead until this is fixed.
template <typename K>
struct AutoMergingTextRule {
  void operator()(TextCRDT<K, std::string> &crdt,
                  LineData<K, std::string> &local_line,
                  const TextChange<K, std::string> &remote_change,
                  uint64_t new_local_db_version) const {

    // Check if this is a concurrent edit
    bool is_concurrent = (local_line.version.line_version == remote_change.version.line_version &&
                         local_line.version.node_id != remote_change.version.node_id);

    if (!is_concurrent) {
      // Not concurrent - use LWW
      bool should_accept = false;
      if (remote_change.version.line_version > local_line.version.line_version) {
        should_accept = true;
      } else if (remote_change.version.line_version < local_line.version.line_version) {
        should_accept = false;
      } else {
        if (remote_change.version.db_version > local_line.version.db_version) {
          should_accept = true;
        } else if (remote_change.version.db_version < local_line.version.db_version) {
          should_accept = false;
        } else {
          should_accept = remote_change.version.node_id > local_line.version.node_id;
        }
      }

      if (should_accept) {
        local_line.content = *remote_change.content;
        VersionInfo new_version = remote_change.version;
        new_version.local_db_version = new_local_db_version;
        local_line.version = new_version;
        local_line.conflicts.reset();
      }
      return;
    }

    // Concurrent edit detected - try 3-way merge!

    // Do we have the base content?
    if (!local_line.base_content.has_value()) {
      // No base - fall back to deterministic BWW
      goto fallback_bww;
    }

    {
      const std::string &base = *local_line.base_content;
      const std::string &local = local_line.content;
      const std::string &remote = *remote_change.content;

      // 3-way merge using word-level diff
      auto base_words = TextDiff::split_words(base);
      auto local_words = TextDiff::split_words(local);
      auto remote_words = TextDiff::split_words(remote);

      // Compute diffs from base
      auto local_diff = TextDiff::diff_words(base_words, local_words);
      auto remote_diff = TextDiff::diff_words(base_words, remote_words);

      // Check if diffs overlap
      if (TextDiff::diffs_overlap(local_diff, remote_diff)) {
        // Overlapping changes - cannot auto-merge
        goto fallback_bww;
      }

      // Non-overlapping! Merge both diffs
      auto merged_diff = TextDiff::merge_diffs(local_diff, remote_diff);
      std::string merged_content = TextDiff::apply_diff(base_words, merged_diff);

      // Success! Apply the merged content
      local_line.content = merged_content;
      local_line.base_content.reset(); // Merge successful, clear base

      // Update version
      if (remote_change.version.db_version > local_line.version.db_version) {
        local_line.version.db_version = remote_change.version.db_version;
        local_line.version.local_db_version = new_local_db_version;
      }

      return; // Successfully merged!
    }

fallback_bww:
    // Overlapping changes or no base - use deterministic BWW
    bool local_wins = local_line.version.node_id < remote_change.version.node_id;

    if (!local_line.conflicts.has_value()) {
      local_line.conflicts = std::vector<ConflictVersion<std::string>>();
      // Add current local content as first conflict
      local_line.conflicts->emplace_back(local_line.content, local_line.version);
    }

    // Add remote content to conflicts
    local_line.conflicts->emplace_back(*remote_change.content, remote_change.version);

    // Sort conflicts deterministically
    std::sort(local_line.conflicts->begin(), local_line.conflicts->end());

    // Set displayed content to deterministic winner (lowest node_id)
    if (!local_wins) {
      local_line.content = *remote_change.content;
    }

    // Update version
    if (remote_change.version.db_version > local_line.version.db_version) {
      local_line.version.db_version = remote_change.version.db_version;
      local_line.version.local_db_version = new_local_db_version;
    }
  }
};

/// Helper function to sync two text CRDTs
template <typename K, typename V>
void sync_text_crdts(TextCRDT<K, V> &source, TextCRDT<K, V> &target, uint64_t &last_sync_version) {
  auto changes = source.get_changes_since(last_sync_version);

  if (!changes.empty()) {
    // Merge changes into target
    target.merge_changes(changes);

    // Update sync version to source's current version AFTER merging
    last_sync_version = source.current_version();
  }
}

#endif // TEXT_CRDT_HPP
