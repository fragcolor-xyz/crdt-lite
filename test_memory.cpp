#include "crdt.hpp"
#include <chrono>
#include <iostream>
#include <string>
#include <atomic>
#include <mutex>
#include <iomanip>
#include <unordered_set>

// Custom allocator that uses malloc/free directly to avoid circular dependency
template <typename T> class MallocAllocator {
public:
  using value_type = T;

  MallocAllocator() = default;

  template <typename U> MallocAllocator(const MallocAllocator<U> &) {}

  T *allocate(std::size_t n) {
    if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
      std::abort(); // Cannot use exceptions, so abort on allocation failure
    }

    void *ptr = std::malloc(n * sizeof(T));
    if (!ptr) {
      std::abort(); // Cannot use exceptions, so abort on allocation failure
    }
    return static_cast<T *>(ptr);
  }

  void deallocate(T *p, std::size_t) noexcept { std::free(p); }

  template <typename U> bool operator==(const MallocAllocator<U> &) const noexcept { return true; }

  template <typename U> bool operator!=(const MallocAllocator<U> &) const noexcept { return false; }
};

// Memory tracking global variables
std::atomic<size_t> g_allocated_bytes{0};
std::atomic<size_t> g_peak_bytes{0};
std::atomic<size_t> g_allocation_count{0};
std::atomic<size_t> g_deallocation_count{0};
std::mutex g_memory_mutex;
std::unordered_map<uintptr_t, size_t, std::hash<uintptr_t>, std::equal_to<uintptr_t>,
                   MallocAllocator<std::pair<const uintptr_t, size_t>>>
    g_tracked_allocations;

// Memory snapshot structure
struct MemorySnapshot {
  size_t allocated_bytes;
  size_t peak_bytes;
  size_t allocation_count;
  size_t deallocation_count;
  size_t tracked_blocks;

  MemorySnapshot() {
    std::lock_guard<std::mutex> lock(g_memory_mutex);
    allocated_bytes = g_allocated_bytes.load();
    peak_bytes = g_peak_bytes.load();
    allocation_count = g_allocation_count.load();
    deallocation_count = g_deallocation_count.load();
    tracked_blocks = g_tracked_allocations.size();
  }
};

// Custom memory allocator
class MemoryTracker {
public:
  static void reset() {
    std::lock_guard<std::mutex> lock(g_memory_mutex);
    g_allocated_bytes = 0;
    g_peak_bytes = 0;
    g_allocation_count = 0;
    g_deallocation_count = 0;
    g_tracked_allocations.clear();
  }

  static void record_allocation(void *ptr, size_t size) {
    if (!ptr)
      return;

    std::lock_guard<std::mutex> lock(g_memory_mutex);
    g_tracked_allocations.insert({reinterpret_cast<uintptr_t>(ptr), size});

    size_t old_allocated = g_allocated_bytes.fetch_add(size);
    size_t new_allocated = old_allocated + size;

    // Update peak if necessary
    size_t current_peak = g_peak_bytes.load();
    while (new_allocated > current_peak && !g_peak_bytes.compare_exchange_weak(current_peak, new_allocated)) {
      // Keep trying until we successfully update the peak
    }

    g_allocation_count.fetch_add(1);
  }

  static void record_deallocation(void *ptr) {
    if (!ptr)
      return;

    std::lock_guard<std::mutex> lock(g_memory_mutex);
    auto it = g_tracked_allocations.find(reinterpret_cast<uintptr_t>(ptr));
    if (it != g_tracked_allocations.end()) {
      g_allocated_bytes.fetch_sub(it->second);
      g_tracked_allocations.erase(it);
      g_deallocation_count.fetch_add(1);
    }
  }

  static MemorySnapshot take_snapshot() { return MemorySnapshot(); }

  static void print_stats(const std::string &test_name) {
    std::cout << "\n=== Memory Statistics for: " << test_name << " ===" << std::endl;
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Current allocated: " << format_bytes(g_allocated_bytes.load()) << std::endl;
    std::cout << "Peak allocated: " << format_bytes(g_peak_bytes.load()) << std::endl;
    std::cout << "Total allocations: " << g_allocation_count.load() << std::endl;
    std::cout << "Total deallocations: " << g_deallocation_count.load() << std::endl;
    std::cout << "Outstanding allocations: " << (g_allocation_count.load() - g_deallocation_count.load()) << std::endl;

    std::lock_guard<std::mutex> lock(g_memory_mutex);
    std::cout << "Tracked outstanding blocks: " << g_tracked_allocations.size() << std::endl;
    std::cout << "=============================================" << std::endl;
  }

  static void print_delta_stats(const std::string &phase_name, const MemorySnapshot &before, const MemorySnapshot &after) {
    std::cout << "\n--- Delta Memory for: " << phase_name << " ---" << std::endl;
    std::cout << std::fixed << std::setprecision(2);

    long long allocated_delta = static_cast<long long>(after.allocated_bytes) - static_cast<long long>(before.allocated_bytes);
    long long peak_delta = static_cast<long long>(after.peak_bytes) - static_cast<long long>(before.peak_bytes);
    long long allocation_delta = static_cast<long long>(after.allocation_count) - static_cast<long long>(before.allocation_count);
    long long deallocation_delta =
        static_cast<long long>(after.deallocation_count) - static_cast<long long>(before.deallocation_count);
    long long blocks_delta = static_cast<long long>(after.tracked_blocks) - static_cast<long long>(before.tracked_blocks);

    std::cout << "Memory delta: " << format_bytes_delta(allocated_delta) << std::endl;
    std::cout << "Peak delta: " << format_bytes_delta(peak_delta) << std::endl;
    std::cout << "New allocations: " << allocation_delta << std::endl;
    std::cout << "New deallocations: " << deallocation_delta << std::endl;
    std::cout << "Net new blocks: " << blocks_delta << std::endl;
    std::cout << "Current allocated: " << format_bytes(after.allocated_bytes) << std::endl;
    std::cout << "----------------------------------------" << std::endl;
  }

private:
  static std::string format_bytes(size_t bytes) {
    const char *units[] = {"B", "KB", "MB", "GB"};
    double size = static_cast<double>(bytes);
    int unit = 0;

    while (size >= 1024.0 && unit < 3) {
      size /= 1024.0;
      unit++;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << size << " " << units[unit];
    return oss.str();
  }

  static std::string format_bytes_delta(long long bytes) {
    const char *units[] = {"B", "KB", "MB", "GB"};
    double size = static_cast<double>(std::abs(bytes));
    int unit = 0;

    while (size >= 1024.0 && unit < 3) {
      size /= 1024.0;
      unit++;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (bytes >= 0) {
      oss << "+" << size << " " << units[unit];
    } else {
      oss << "-" << size << " " << units[unit];
    }
    return oss.str();
  }
};

// Override global new and delete operators
void *operator new(size_t size) {
  void *ptr = std::malloc(size);
  if (!ptr) {
    std::abort(); // Cannot use exceptions, so abort on allocation failure
  }
  MemoryTracker::record_allocation(ptr, size);
  return ptr;
}

void *operator new[](size_t size) {
  void *ptr = std::malloc(size);
  if (!ptr) {
    std::abort(); // Cannot use exceptions, so abort on allocation failure
  }
  MemoryTracker::record_allocation(ptr, size);
  return ptr;
}

void operator delete(void *ptr) noexcept {
  if (ptr) {
    MemoryTracker::record_deallocation(ptr);
    std::free(ptr);
  }
}

void operator delete[](void *ptr) noexcept {
  if (ptr) {
    MemoryTracker::record_deallocation(ptr);
    std::free(ptr);
  }
}

// Helper function to generate unique IDs
std::string generate_uuid() {
  static uint64_t counter = 0;
  return "uuid-" + std::to_string(++counter);
}

// Test utility to measure time
template <typename Func> double measure_time_ms(Func &&func) {
  auto start = std::chrono::high_resolution_clock::now();
  func();
  auto end = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double, std::milli>(end - start).count();
}

// Additional test: Memory usage comparison
void test_memory_comparison() {
  std::cout << "\n>>> Memory Comparison Test <<<" << std::endl;

  // Test 1: Only active records (baseline)
  MemoryTracker::reset();
  {
    auto baseline_start = MemoryTracker::take_snapshot();
    CRDT<std::string, std::string> crdt_baseline(1);

    std::cout << "Creating 100 active records (baseline)..." << std::endl;
    for (int i = 0; i < 100; i++) {
      std::string record_id = "baseline_record_" + std::to_string(i);
      crdt_baseline.insert_or_update(record_id, std::make_pair("field_01", "value_01_" + std::to_string(i)),
                                     std::make_pair("field_02", "value_02_" + std::to_string(i)),
                                     std::make_pair("field_03", "value_03_" + std::to_string(i)),
                                     std::make_pair("field_04", "value_04_" + std::to_string(i)),
                                     std::make_pair("field_05", "value_05_" + std::to_string(i)),
                                     std::make_pair("field_06", "value_06_" + std::to_string(i)),
                                     std::make_pair("field_07", "value_07_" + std::to_string(i)),
                                     std::make_pair("field_08", "value_08_" + std::to_string(i)),
                                     std::make_pair("field_09", "value_09_" + std::to_string(i)),
                                     std::make_pair("field_10", "value_10_" + std::to_string(i)),
                                     std::make_pair("field_11", "value_11_" + std::to_string(i)),
                                     std::make_pair("field_12", "value_12_" + std::to_string(i)),
                                     std::make_pair("field_13", "value_13_" + std::to_string(i)),
                                     std::make_pair("field_14", "value_14_" + std::to_string(i)),
                                     std::make_pair("field_15", "value_15_" + std::to_string(i)),
                                     std::make_pair("field_16", "value_16_" + std::to_string(i)));
    }

    auto baseline_final = MemoryTracker::take_snapshot();
    MemoryTracker::print_delta_stats("Baseline: CRDT with 100 Active Records", baseline_start, baseline_final);
  }

  // Test 2: Various tombstone counts
  std::vector<int> tombstone_counts = {1000, 10000, 50000, 100000, 300000, 1200000};

  for (int tombstone_count : tombstone_counts) {

    auto test_start = MemoryTracker::take_snapshot();
    CRDT<std::string, std::string> crdt_test(1);

    std::cout << "\nCreating " << tombstone_count << " tombstones..." << std::endl;

    double creation_time = measure_time_ms([&]() {
      // Create tombstones
      std::string buf{"tomb_"};
      for (int i = 0; i < tombstone_count; i++) {
        buf.resize(256);
        size_t dst = sprintf(buf.data() + 5, "%d", i);
        buf.resize(dst + 5);
        crdt_test.delete_record(buf);
      }
    });

    auto test_final = MemoryTracker::take_snapshot();
    std::cout << "Creation completed in " << creation_time << " ms" << std::endl;
    MemoryTracker::print_delta_stats("CRDT with " + std::to_string(tombstone_count) + " Tombstones + 100 Active", test_start,
                                     test_final);

    // Calculate memory per tombstone
    double memory_per_tombstone_bytes = (test_final.allocated_bytes - test_start.allocated_bytes) / tombstone_count;
    std::cout << "Memory per tombstone: " << memory_per_tombstone_bytes << " bytes" << std::endl;
  }
}

int main() {
  std::cout << "=== CRDT Memory Performance Test Suite ===" << std::endl;

  // Additional comparison tests
  test_memory_comparison();

  std::cout << "\n=== All Memory Tests Completed Successfully ===" << std::endl;

  return 0;
}
