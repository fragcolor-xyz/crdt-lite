// Work in progress, need values serialized to bytes

#ifndef CRDT_URKEL_MAP_HPP
#define CRDT_URKEL_MAP_HPP

// Add these forward declarations for urkel types
extern "C" {
struct urkel_s;
struct urkel_tx_s;
struct urkel_iter_s;

typedef struct urkel_s urkel_t;
typedef struct urkel_tx_s urkel_tx_t;
typedef struct urkel_iter_s urkel_iter_t;
}

#include <memory>
#include <optional>
#include <functional>
#include <cstring>
#include <stdexcept>
#include <iterator>
#include <urkel.h>
#include <vector>

// Forward declarations for iterator types
template <typename K, typename V> class UrkelMapIterator;
template <typename K, typename V> class UrkelMapConstIterator;

// Urkel Tree wrapper that implements the map interface
template <typename K, typename V> class UrkelMap {
public:
  using key_type = K;
  using mapped_type = V;
  using value_type = std::pair<const K, V>;
  using iterator = UrkelMapIterator<K, V>;
  using const_iterator = UrkelMapConstIterator<K, V>;

  // Default constructor - doesn't initialize the tree
  UrkelMap() : tree_(nullptr), tx_(nullptr) {}

  // Initialize the tree with a given path
  bool init(const char *prefix) {
    if (tree_) {
      return false; // Already initialized
    }

    tree_ = std::shared_ptr<urkel_t>(urkel_open(prefix), urkel_close);
    return tree_ != nullptr;
  }

  // Check if the tree is initialized
  bool is_initialized() const { return tree_ != nullptr; }

  V &operator[](const K &key) {
    if (!is_initialized()) {
      throw std::runtime_error("UrkelMap not initialized");
    }
    if (!tx_) {
      tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
      if (!tx_) {
        throw std::runtime_error("Failed to create transaction");
      }
    }

    size_t size;
    auto *key_bytes = reinterpret_cast<const unsigned char *>(&key);

    // Static buffer for small values, heap allocation for larger ones
    static constexpr size_t STACK_BUF_SIZE = 1024;
    unsigned char stack_buf[STACK_BUF_SIZE];
    std::unique_ptr<unsigned char[]> heap_buf;
    unsigned char *value_buf = stack_buf;

    int result = urkel_tx_get(tx_.get(), value_buf, &size, key_bytes);

    if (result == 1) {
      // Key exists, return existing value
      return *reinterpret_cast<V *>(value_buf);
    }

    // Key doesn't exist, create new entry
    V new_value{};
    result = urkel_tx_insert(tx_.get(), key_bytes, reinterpret_cast<const unsigned char *>(&new_value), sizeof(V));

    if (result != 1) {
      throw std::runtime_error("Failed to insert value");
    }

    // Store the new value in our buffer and return it
    std::memcpy(value_buf, &new_value, sizeof(V));
    return *reinterpret_cast<V *>(value_buf);
  }

  bool contains(const K &key) const {
    if (!is_initialized()) {
      throw std::runtime_error("UrkelMap not initialized");
    }
    if (!tx_) {
      const_cast<UrkelMap *>(this)->tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }
    return urkel_tx_has(tx_.get(), reinterpret_cast<const unsigned char *>(&key)) == 1;
  }

  void insert(const K &key, const V &value) {
    if (!is_initialized()) {
      throw std::runtime_error("UrkelMap not initialized");
    }
    if (!tx_) {
      tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }

    int result = urkel_tx_insert(tx_.get(), reinterpret_cast<const unsigned char *>(&key),
                                 reinterpret_cast<const unsigned char *>(&value), sizeof(V));

    if (result != 1) {
      throw std::runtime_error("Failed to insert value");
    }
  }

  void commit() {
    if (tx_) {
      if (urkel_tx_commit(tx_.get()) != 1) {
        throw std::runtime_error("Failed to commit transaction");
      }
      tx_.reset();
    }
  }

  iterator find(const K &key) {
    if (!tx_) {
      tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }

    size_t size;
    auto *key_bytes = reinterpret_cast<const unsigned char *>(&key);
    unsigned char value_buf[sizeof(V)];

    if (urkel_tx_get(tx_.get(), value_buf, &size, key_bytes) == 1) {
      return iterator(tx_.get(), key);
    }
    return end();
  }

  const_iterator find(const K &key) const {
    if (!tx_) {
      const_cast<UrkelMap *>(this)->tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }

    size_t size;
    auto *key_bytes = reinterpret_cast<const unsigned char *>(&key);
    unsigned char value_buf[sizeof(V)];

    if (urkel_tx_get(tx_.get(), value_buf, &size, key_bytes) == 1) {
      return const_iterator(tx_.get(), key);
    }
    return end();
  }

  void emplace(const K &key, const V &value) { insert(key, value); }

  void clear() {
    tx_.reset();
    tree_ = std::shared_ptr<urkel_t>(urkel_open("db"), urkel_close);
  }

  void erase(const K &key) {
    if (!tx_) {
      tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }

    urkel_tx_remove(tx_.get(), reinterpret_cast<const unsigned char *>(&key));
  }

  iterator begin() {
    if (!tx_) {
      tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }
    return iterator(tx_.get());
  }

  iterator end() { return iterator(); }

  const_iterator begin() const {
    if (!tx_) {
      const_cast<UrkelMap *>(this)->tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }
    return const_iterator(tx_.get());
  }

  const_iterator end() const { return const_iterator(); }

  // Get the current root hash of the tree
  std::vector<unsigned char> get_root_hash() const {
    std::vector<unsigned char> hash(32); // Urkel uses 32-byte hashes
    if (tx_) {
      urkel_tx_root(tx_.get(), hash.data());
    } else {
      urkel_root(tree_.get(), hash.data());
    }
    return hash;
  }

  // Inject a specific root hash to validate against
  bool inject_root_hash(const std::vector<unsigned char> &hash) {
    if (!tx_) {
      tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }
    return urkel_tx_inject(tx_.get(), hash.data()) == 1;
  }

  // Generate proof for a key
  std::vector<unsigned char> prove(const K &key) const {
    if (!tx_) {
      const_cast<UrkelMap *>(this)->tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }

    unsigned char *proof_raw;
    size_t proof_len;

    int result = urkel_tx_prove(tx_.get(), &proof_raw, &proof_len, reinterpret_cast<const unsigned char *>(&key));

    if (result != 1) {
      return std::vector<unsigned char>();
    }

    std::vector<unsigned char> proof(proof_raw, proof_raw + proof_len);
    urkel_free(proof_raw);
    return proof;
  }

  // Verify a proof for a key against a root hash
  static bool verify_proof(const std::vector<unsigned char> &proof, const K &key, const std::vector<unsigned char> &root_hash,
                           std::optional<V> &value_out) {
    int exists;
    unsigned char value_buf[sizeof(V)];
    size_t value_len;

    int result = urkel_verify(&exists, value_buf, &value_len, proof.data(), proof.size(),
                              reinterpret_cast<const unsigned char *>(&key), root_hash.data());

    if (result == 1 && exists) {
      value_out = *reinterpret_cast<V *>(value_buf);
      return true;
    }

    value_out = std::nullopt;
    return result == 1;
  }

  // Add destructor to commit any pending transactions
  ~UrkelMap() {
    if (tx_) {
      try {
        commit();
      } catch (...) {
        // Silently fail in destructor
      }
    }
  }

  std::pair<iterator, bool> try_emplace(const K &key, const V &value) {
    if (!tx_) {
      tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }

    // Check if key exists
    if (contains(key)) {
      return {find(key), false};
    }

    // Key doesn't exist, insert new value
    int result = urkel_tx_insert(tx_.get(), reinterpret_cast<const unsigned char *>(&key),
                                 reinterpret_cast<const unsigned char *>(&value), sizeof(V));

    if (result != 1) {
      throw std::runtime_error("Failed to insert value");
    }

    return {find(key), true};
  }

  std::pair<iterator, bool> insert_or_assign(const K &key, const V &value) {
    if (!tx_) {
      tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }

    bool existed = contains(key);

    // Insert or update the value
    int result = urkel_tx_insert(tx_.get(), reinterpret_cast<const unsigned char *>(&key),
                                 reinterpret_cast<const unsigned char *>(&value), sizeof(V));

    if (result != 1) {
      throw std::runtime_error("Failed to insert or assign value");
    }

    return {find(key), !existed};
  }

  bool empty() const {
    if (!tx_) {
      const_cast<UrkelMap *>(this)->tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }
    auto iter = urkel_iter_create(tx_.get());
    if (!iter)
      return true;

    unsigned char key_buf[sizeof(K)];
    unsigned char value_buf[sizeof(V)];
    size_t size;
    bool is_empty = urkel_iter_next(iter, key_buf, value_buf, &size) != 0;
    urkel_iter_destroy(iter);
    return is_empty;
  }

  size_t size() const {
    if (!tx_) {
      const_cast<UrkelMap *>(this)->tx_ = std::shared_ptr<urkel_tx_t>(urkel_tx_create(tree_.get(), nullptr), urkel_tx_destroy);
    }
    size_t count = 0;
    auto iter = urkel_iter_create(tx_.get());
    if (!iter)
      return count;

    unsigned char key_buf[sizeof(K)];
    unsigned char value_buf[sizeof(V)];
    size_t value_size;
    while (urkel_iter_next(iter, key_buf, value_buf, &value_size) == 0) {
      count++;
    }
    urkel_iter_destroy(iter);
    return count;
  }

private:
  std::shared_ptr<urkel_t> tree_;
  std::shared_ptr<urkel_tx_t> tx_;
};

// Iterator implementation
template <typename K, typename V> class UrkelMapIterator {
public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = std::pair<const K, V>;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type *;
  using reference = value_type &;

  // Default constructor - create empty iterator
  UrkelMapIterator() : tx_(nullptr), iter_(nullptr) {}

  // Constructor with transaction
  explicit UrkelMapIterator(urkel_tx_t *tx) : tx_(tx), iter_(tx ? urkel_iter_create(tx) : nullptr) {
    if (iter_) {
      advance();
    }
  }

  // Constructor with key
  UrkelMapIterator(urkel_tx_t *tx, const K &key) : tx_(tx), iter_(nullptr) {
    size_t size;
    unsigned char value_buf[sizeof(V)];
    if (urkel_tx_get(tx_, value_buf, &size, reinterpret_cast<const unsigned char *>(&key)) == 0) {
      // Construct the pair in-place using placement new
      new (&current_) value_type(key, *reinterpret_cast<V *>(value_buf));
    }
  }

  ~UrkelMapIterator() {
    if (iter_) {
      urkel_iter_destroy(iter_);
    }
  }

  reference operator*() { return current_; }
  pointer operator->() { return &current_; }

  UrkelMapIterator &operator++() {
    advance();
    return *this;
  }

  UrkelMapIterator operator++(int) {
    UrkelMapIterator tmp = *this;
    advance();
    return tmp;
  }

  bool operator==(const UrkelMapIterator &other) const {
    if (!iter_ && !other.iter_)
      return true;
    if (!iter_ || !other.iter_)
      return false;
    return current_.first == other.current_.first;
  }

  bool operator!=(const UrkelMapIterator &other) const { return !(*this == other); }

private:
  void advance() {
    if (!iter_)
      return;

    unsigned char key_buf[sizeof(K)];
    unsigned char value_buf[sizeof(V)];
    size_t size;

    if (urkel_iter_next(iter_, key_buf, value_buf, &size) != 0) {
      urkel_iter_destroy(iter_);
      iter_ = nullptr;
      return;
    }

    // Create new pair in-place using placement new
    K key = *reinterpret_cast<K *>(key_buf);
    V value = *reinterpret_cast<V *>(value_buf);
    new (&current_) value_type(std::move(key), std::move(value));
  }

  urkel_tx_t *tx_;
  urkel_iter_t *iter_;
  alignas(value_type) char current_storage_[sizeof(value_type)]; // Storage for current_
  value_type &current_ = *reinterpret_cast<value_type *>(current_storage_);
};

template <typename K, typename V> class UrkelMapConstIterator : public UrkelMapIterator<K, V> {
public:
  using parent = UrkelMapIterator<K, V>;
  using parent::parent;
};

#endif // CRDT_URKEL_MAP_HPP