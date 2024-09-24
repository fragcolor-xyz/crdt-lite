// crdt_benchmark.cpp
#include "crdt.hpp"
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

// Define types for the CRDT
using RecordId = std::string;
using FieldValue = std::string;
using MyCRDT = CRDT<RecordId, FieldValue>;

class CrdtBenchmark {
public:
  CrdtBenchmark(CrdtNodeId node1_id, CrdtNodeId node2_id)
      : node1(node1_id), node2(node2_id), last_db_version_node1(0), last_db_version_node2(0) {}

  void runBenchmark() {
    std::cout << "Starting CRDT Benchmark..." << std::endl;

    insertRecords(1000);
    updateRecords(1000, std::chrono::seconds(5));
    deleteRecords(500);
    synchronizeNodes();

    std::cout << "CRDT Benchmark completed." << std::endl;
  }

private:
  MyCRDT node1;
  MyCRDT node2;
  uint64_t last_db_version_node1;
  uint64_t last_db_version_node2;
  std::vector<RecordId> record_ids;

  // Generates a random string of given length
  std::string generate_random_string(size_t length) {
    const char charset[] = "0123456789"
                           "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                           "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = sizeof(charset) - 1;
    std::default_random_engine rng(std::random_device{}());
    std::uniform_int_distribution<> dist(0, max_index - 1);

    std::string str(length, 0);
    for (size_t i = 0; i < length; ++i) {
      str[i] = charset[dist(rng)];
    }
    return str;
  }

  void insertRecords(size_t count) {
    std::cout << "Inserting " << count << " records..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < count; ++i) {
      RecordId rid = "record_" + std::to_string(i) + "_" + generate_random_string(8);
      record_ids.push_back(rid);

      CrdtMap<CrdtString, FieldValue> fields;
      fields.emplace("field1", "value1_" + std::to_string(i));
      fields.emplace("field2", "value2_" + std::to_string(i));

      node1.insert_or_update(rid, fields);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Inserted " << count << " records in " << duration_ms << " ms." << std::endl;
  }

  void updateRecords(size_t count, std::chrono::seconds duration) {
    std::cout << "Updating records for " << duration.count() << " seconds..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    size_t updates = 0;
    std::default_random_engine rng(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, record_ids.size() - 1);
    std::uniform_int_distribution<int> field_dist(1, 2);

    while (std::chrono::high_resolution_clock::now() - start < duration) {
      size_t index = dist(rng);
      RecordId rid = record_ids[index];

      CrdtMap<CrdtString, FieldValue> fields;
      int field_num = field_dist(rng);
      fields.emplace("field" + std::to_string(field_num), "updated_value_" + std::to_string(updates));

      node1.insert_or_update(rid, fields);
      updates++;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Performed " << updates << " updates in " << elapsed_ms << " ms." << std::endl;
  }

  void deleteRecords(size_t count) {
    std::cout << "Deleting " << count << " records..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    size_t deletions = 0;
    for (size_t i = 0; i < count && i < record_ids.size(); ++i) {
      RecordId rid = record_ids[i];
      node1.delete_record(rid);
      deletions++;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Deleted " << deletions << " records in " << duration_ms << " ms." << std::endl;
  }

  void synchronizeNodes() {
    std::cout << "Synchronizing node1 to node2..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    sync_nodes(node1, node2, last_db_version_node1);
    sync_nodes(node2, node1, last_db_version_node2);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Synchronization completed in " << duration_ms << " ms." << std::endl;

    // Optionally, verify consistency
    verifyConsistency();
  }

  void verifyConsistency() const {
    std::cout << "Verifying consistency between nodes..." << std::endl;

    const auto &data1 = node1.get_data();
    const auto &data2 = node2.get_data();

    bool consistent = (data1.size() == data2.size());

    if (consistent) {
      for (const auto &[rid, record] : data1) {
        auto it = data2.find(rid);
        if (it == data2.end() || !(it->second == record)) {
          consistent = false;
          break;
        }
      }
    }

    if (consistent) {
      std::cout << "Consistency check passed: Both nodes have identical data." << std::endl;
    } else {
      std::cout << "Consistency check failed: Nodes have differing data." << std::endl;
    }
  }
};

// Entry point for the benchmark
int main() {
  // Initialize two CRDT nodes with unique node IDs
  CrdtNodeId node1_id = 1;
  CrdtNodeId node2_id = 2;

  CrdtBenchmark benchmark(node1_id, node2_id);
  benchmark.runBenchmark();

  return 0;
}
