#include <string>
#include <vector>
#include <unordered_set>
#include <map>
#include <set>
#include <array>
#include "crdt_urkel_map.hpp"

#include "crdt.hpp"
#include <chrono>
#include <iostream>
#include <random>
#include <filesystem>

// Add this near the top of the file, after the includes but before the CRDT type definitions
namespace std {
template <> struct hash<std::array<unsigned char, 32>> {
  size_t operator()(const std::array<unsigned char, 32> &arr) const {
    // Use first 8 bytes as a 64-bit hash
    size_t result = 0;
    for (size_t i = 0; i < sizeof(size_t) && i < arr.size(); ++i) {
      result = (result << 8) | arr[i];
    }
    return result;
  }
};
} // namespace std

// Define types for the CRDT
using RecordId = std::array<unsigned char, 32>;
using FieldValue = std::string;
using MyCRDT = CRDT<RecordId, FieldValue, void, DefaultMergeRule<RecordId, FieldValue>,
                    DefaultChangeComparator<RecordId, FieldValue>, DefaultSort, UrkelMap<RecordId, Record<FieldValue>>>;

class UrkelCrdtBenchmark {
public:
  UrkelCrdtBenchmark(CrdtNodeId node1_id, CrdtNodeId node2_id)
      : node1(node1_id, nullptr, DefaultMergeRule<RecordId, FieldValue>(), DefaultChangeComparator<RecordId, FieldValue>(),
              DefaultSort(), std::monostate()),
        node2(node2_id, nullptr, DefaultMergeRule<RecordId, FieldValue>(), DefaultChangeComparator<RecordId, FieldValue>(),
              DefaultSort(), std::monostate()),
        last_db_version_node1(0), last_db_version_node2(0) {
    // Create directories for Urkel databases if they don't exist
    std::filesystem::create_directories("node1_db");
    std::filesystem::create_directories("node2_db");
    node1.get_data().init("node1_db");
    node2.get_data().init("node2_db");
  }

  ~UrkelCrdtBenchmark() {
    // Clean up database directories after benchmark
    std::filesystem::remove_all("node1_db");
    std::filesystem::remove_all("node2_db");
  }

  void runBenchmark() {
    std::cout << "\nStarting Urkel CRDT Benchmark...\n" << std::endl;

    measureOperation("Insert", [this]() { insertRecords(1000); });
    measureOperation("Update", [this]() { updateRecords(1000, std::chrono::seconds(5)); });
    measureOperation("Delete", [this]() { deleteRecords(500); });
    measureOperation("Sync", [this]() { synchronizeNodes(); });

    std::cout << "\nUrkel CRDT Benchmark completed.\n" << std::endl;
  }

private:
  MyCRDT node1;
  MyCRDT node2;
  uint64_t last_db_version_node1;
  uint64_t last_db_version_node2;
  std::vector<RecordId> record_ids;

  template <typename F> void measureOperation(const std::string &name, F operation) {
    std::cout << "Starting " << name << " operation..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    operation();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << name << " operation completed in " << duration.count() << " ms\n" << std::endl;
  }

  std::string generate_random_string(size_t length) {
    const char charset[] = "0123456789"
                           "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                           "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = sizeof(charset) - 2;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, max_index);

    std::string str(length, 0);
    for (size_t i = 0; i < length; ++i) {
      str[i] = charset[dist(gen)];
    }
    return str;
  }

  RecordId hash_key(const std::string &input) {
    RecordId result;
    urkel_hash(result.data(), input.c_str(), input.length());
    return result;
  }

  void insertRecords(size_t count) {
    CrdtVector<Change<RecordId, FieldValue>> changes;
    size_t batch_size = 100;
    size_t batches = count / batch_size;

    for (size_t batch = 0; batch < batches; ++batch) {
      for (size_t i = 0; i < batch_size; ++i) {
        std::string key_str = "record_" + std::to_string(batch * batch_size + i) + "_" + generate_random_string(8);
        RecordId rid = hash_key(key_str);
        record_ids.push_back(rid);

        node1.insert_or_update(rid, changes, std::make_pair("field1", "value1_" + std::to_string(i)),
                               std::make_pair("field2", "value2_" + std::to_string(i)));
      }
      // Commit after each batch
      node1.get_data().commit();
    }

    std::cout << "Inserted " << count << " records in batches of " << batch_size << std::endl;
  }

  void updateRecords(size_t count, std::chrono::seconds duration) {
    auto start = std::chrono::high_resolution_clock::now();
    size_t updates = 0;
    std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, record_ids.size() - 1);
    std::uniform_int_distribution<int> field_dist(1, 2);

    CrdtVector<Change<RecordId, FieldValue>> changes;
    size_t batch_size = 100;
    size_t batch_count = 0;

    while (std::chrono::high_resolution_clock::now() - start < duration && updates < count) {
      size_t index = dist(gen);
      RecordId rid = record_ids[index];
      int field_num = field_dist(gen);

      node1.insert_or_update(rid, changes,
                             std::make_pair("field" + std::to_string(field_num), "updated_value_" + std::to_string(updates)));

      updates++;
      batch_count++;

      if (batch_count >= batch_size) {
        node1.get_data().commit();
        batch_count = 0;
      }
    }

    // Commit any remaining changes
    if (batch_count > 0) {
      node1.get_data().commit();
    }

    std::cout << "Performed " << updates << " updates" << std::endl;
  }

  void deleteRecords(size_t count) {
    CrdtVector<Change<RecordId, FieldValue>> changes;
    size_t batch_size = 100;
    size_t batches = count / batch_size;
    size_t deletions = 0;

    for (size_t batch = 0; batch < batches && batch * batch_size < record_ids.size(); ++batch) {
      for (size_t i = 0; i < batch_size && batch * batch_size + i < record_ids.size(); ++i) {
        RecordId rid = record_ids[batch * batch_size + i];
        node1.delete_record(rid, changes);
        deletions++;
      }
      // Commit after each batch
      node1.get_data().commit();
    }

    std::cout << "Deleted " << deletions << " records in batches of " << batch_size << std::endl;
  }

  void synchronizeNodes() {
    // Get root hash from node1
    auto root_hash1 = node1.get_data().get_root_hash();

    // Sync from node1 to node2
    sync_nodes(node1, node2, last_db_version_node1);
    node2.get_data().commit();

    // Get root hash from node2 after sync
    auto root_hash2 = node2.get_data().get_root_hash();

    // Verify root hashes match
    bool hashes_match = (root_hash1 == root_hash2);
    std::cout << "Node synchronization " << (hashes_match ? "successful" : "failed") << std::endl;

    if (!hashes_match) {
      std::cout << "Warning: Root hashes don't match after synchronization!" << std::endl;
    }
  }

  // Add this as a friend function declaration inside the class
  friend void print_value(const RecordId &value) {
    std::cout << std::hex;
    for (size_t i = 0; i < 4; ++i) {
      std::cout << static_cast<int>(value[i]);
    }
    std::cout << "..." << std::dec;
  }
};

int main() {
  try {
    CrdtNodeId node1_id = 1;
    CrdtNodeId node2_id = 2;

    UrkelCrdtBenchmark benchmark(node1_id, node2_id);
    benchmark.runBenchmark();
  } catch (const std::exception &e) {
    std::cerr << "Benchmark failed with error: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}