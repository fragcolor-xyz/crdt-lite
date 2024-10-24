#include <iostream>
#include <cstdlib>
#include <ctime>
#include <limits>
#include <memory>
#include <cstddef>
#include <functional>

template <typename T> class MemoryPool {
private:
  struct Block {
    Block *next;
    // Ensure data is the last member
    alignas(T) unsigned char data[sizeof(T)];
  };
  Block *freeList;

public:
  MemoryPool() : freeList(nullptr) {}

  ~MemoryPool() {
    while (freeList) {
      Block *next = freeList->next;
      ::operator delete(freeList);
      freeList = next;
    }
  }

  void *allocate() {
    if (freeList) {
      Block *block = freeList;
      freeList = freeList->next;
      return block->data;
    } else {
      Block *newBlock = static_cast<Block *>(::operator new(sizeof(Block)));
      return newBlock->data;
    }
  }

  void deallocate(void *p) {
    if (p) {
      Block *block = reinterpret_cast<Block *>(reinterpret_cast<char *>(p) - offsetof(Block, data));
      block->next = freeList;
      freeList = block;
    }
  }
};

template <typename Key, typename Value, typename Compare = std::less<Key>> class SkipListMap {
private:
  struct Node;

  static const int MAX_LEVEL = 16;

  struct Node {
    std::pair<const Key, Value> data;
    Node *forward[MAX_LEVEL + 1];
    Node *backward;

    template <typename... Args> Node(Args &&...args) : data(std::forward<Args>(args)...), backward(nullptr) {
      for (int i = 0; i <= MAX_LEVEL; ++i)
        forward[i] = nullptr;
    }
  };

  float probability;
  int level;
  Node *header;
  Compare less;
  MemoryPool<Node> nodePool;

  int randomLevel() {
    int lvl = 0;
    while (((double)std::rand() / RAND_MAX) < probability && lvl < MAX_LEVEL)
      ++lvl;
    return lvl;
  }

public:
  // Bidirectional Iterator
  class iterator {
  public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = std::pair<const Key, Value>;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type *;
    using reference = value_type &;

    iterator(Node *node = nullptr) : node_(node) {}

    reference operator*() const { return node_->data; }

    pointer operator->() const { return &(node_->data); }

    iterator &operator++() {
      node_ = node_->forward[0];
      return *this;
    }

    iterator operator++(int) {
      iterator tmp = *this;
      node_ = node_->forward[0];
      return tmp;
    }

    iterator &operator--() {
      if (node_) {
        node_ = node_->backward;
      }
      return *this;
    }

    iterator operator--(int) {
      iterator tmp = *this;
      if (node_) {
        node_ = node_->backward;
      }
      return tmp;
    }

    bool operator==(const iterator &other) const { return node_ == other.node_; }

    bool operator!=(const iterator &other) const { return node_ != other.node_; }

  private:
    Node *node_;
  };

  // Const Iterator
  class const_iterator {
  public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = const std::pair<const Key, Value>;
    using difference_type = std::ptrdiff_t;
    using pointer = const value_type *;
    using reference = const value_type &;

    const_iterator(const Node *node = nullptr) : node_(node) {}

    reference operator*() const { return node_->data; }

    pointer operator->() const { return &(node_->data); }

    const_iterator &operator++() {
      node_ = node_->forward[0];
      return *this;
    }

    const_iterator operator++(int) {
      const_iterator tmp = *this;
      node_ = node_->forward[0];
      return tmp;
    }

    const_iterator &operator--() {
      if (node_) {
        node_ = node_->backward;
      }
      return *this;
    }

    const_iterator operator--(int) {
      const_iterator tmp = *this;
      if (node_) {
        node_ = node_->backward;
      }
      return tmp;
    }

    bool operator==(const const_iterator &other) const { return node_ == other.node_; }

    bool operator!=(const const_iterator &other) const { return node_ != other.node_; }

  private:
    const Node *node_;
  };

  explicit SkipListMap(float prob = 0.5, const Compare &comp = Compare()) : probability(prob), level(0), less(comp) {
    std::srand(std::time(nullptr));
    void *nodeMem = nodePool.allocate();
    header = new (nodeMem) Node();
  }

  ~SkipListMap() {
    Node *node = header;
    while (node) {
      Node *next = node->forward[0];
      node->~Node(); // Manually call destructor
      nodePool.deallocate(node);
      node = next;
    }
  }

  std::pair<iterator, bool> insert(const std::pair<Key, Value> &kv) { return emplace(kv.first, kv.second); }

  std::pair<iterator, bool> insert_or_assign(const Key &key, const Value &value) {
    auto result = emplace(key, value);
    if (!result.second) {
      result.first->second = value;
    }
    return result;
  }

  template <typename... Args> std::pair<iterator, bool> try_emplace(const Key &key, Args &&...args) {
    Node *update[MAX_LEVEL + 1];
    Node *x = header;

    for (int i = level; i >= 0; --i) {
      while (x->forward[i] && less(x->forward[i]->data.first, key))
        x = x->forward[i];
      update[i] = x;
    }

    x = x->forward[0];

    if (x && !less(key, x->data.first) && !less(x->data.first, key)) {
      // Key already exists
      return {iterator(x), false};
    } else {
      int lvl = randomLevel();
      if (lvl > level) {
        for (int i = level + 1; i <= lvl; ++i)
          update[i] = header;
        level = lvl;
      }

      void *nodeMem = nodePool.allocate();
      x = new (nodeMem)
          Node(std::piecewise_construct, std::forward_as_tuple(key), std::forward_as_tuple(std::forward<Args>(args)...));
      x->backward = update[0] != header ? update[0] : nullptr;
      for (int i = 0; i <= lvl; ++i) {
        x->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = x;
      }
      if (x->forward[0])
        x->forward[0]->backward = x;
      return {iterator(x), true};
    }
  }

  template <typename... Args> std::pair<iterator, bool> emplace(Args &&...args) {
    Node *update[MAX_LEVEL + 1];
    Node *x = header;

    // Extract key from arguments
    auto key = std::get<0>(std::forward_as_tuple(args...));

    for (int i = level; i >= 0; --i) {
      while (x->forward[i] && less(x->forward[i]->data.first, key))
        x = x->forward[i];
      update[i] = x;
    }

    x = x->forward[0];

    if (x && !less(key, x->data.first) && !less(x->data.first, key)) {
      // Key already exists
      return {iterator(x), false};
    } else {
      int lvl = randomLevel();
      if (lvl > level) {
        for (int i = level + 1; i <= lvl; ++i)
          update[i] = header;
        level = lvl;
      }

      void *nodeMem = nodePool.allocate();
      x = new (nodeMem) Node(std::forward<Args>(args)...);
      x->backward = update[0] != header ? update[0] : nullptr;
      for (int i = 0; i <= lvl; ++i) {
        x->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = x;
      }
      if (x->forward[0])
        x->forward[0]->backward = x;
      return {iterator(x), true};
    }
  }

  iterator find(const Key &key) {
    Node *x = header;
    for (int i = level; i >= 0; --i) {
      while (x->forward[i] && less(x->forward[i]->data.first, key))
        x = x->forward[i];
    }
    x = x->forward[0];
    if (x && !less(key, x->data.first) && !less(x->data.first, key)) {
      return iterator(x);
    }
    return end();
  }

  const_iterator find(const Key &key) const {
    Node *x = header;
    for (int i = level; i >= 0; --i) {
      while (x->forward[i] && less(x->forward[i]->data.first, key))
        x = x->forward[i];
    }
    x = x->forward[0];
    if (x && !less(key, x->data.first) && !less(x->data.first, key)) {
      return const_iterator(x);
    }
    return cend();
  }

  size_t erase(const Key &key) {
    Node *update[MAX_LEVEL + 1];
    Node *x = header;
    for (int i = level; i >= 0; --i) {
      while (x->forward[i] && less(x->forward[i]->data.first, key))
        x = x->forward[i];
      update[i] = x;
    }

    x = x->forward[0];

    if (x && !less(key, x->data.first) && !less(x->data.first, key)) {
      for (int i = 0; i <= level; ++i) {
        if (update[i]->forward[i] != x)
          break;
        update[i]->forward[i] = x->forward[i];
      }
      if (x->forward[0])
        x->forward[0]->backward = x->backward;
      x->~Node(); // Manually call destructor
      nodePool.deallocate(x);
      while (level > 0 && header->forward[level] == nullptr)
        --level;
      return 1;
    }
    return 0;
  }

  Value &operator[](const Key &key) {
    auto result = emplace(key, Value());
    return result.first->second;
  }

  // Iterator functions
  iterator begin() { return iterator(header->forward[0]); }

  iterator end() { return iterator(nullptr); }

  const_iterator begin() const { return const_iterator(header->forward[0]); }

  const_iterator end() const { return const_iterator(nullptr); }

  const_iterator cbegin() const { return const_iterator(header->forward[0]); }

  const_iterator cend() const { return const_iterator(nullptr); }

  bool empty() const { return header->forward[0] == nullptr; }

  size_t size() const {
    size_t count = 0;
    Node *x = header->forward[0];
    while (x) {
      ++count;
      x = x->forward[0];
    }
    return count;
  }
};
