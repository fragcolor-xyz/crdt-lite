# Building and Testing CRDT-SQLite

## Quick Start

### Test with int64_t (default)
```bash
# GCC
g++ -std=c++20 -I. test_crdt_sqlite.cpp crdt_sqlite.cpp -lsqlite3 -o test_crdt_sqlite
./test_crdt_sqlite

# Clang with sanitizers
clang++ -std=c++20 -I. -fsanitize=address,undefined -g \
  test_crdt_sqlite.cpp crdt_sqlite.cpp -lsqlite3 -o test_crdt_sqlite_asan
./test_crdt_sqlite_asan
```

### Test with uint128_t

**IMPORTANT:** When compiling for uint128_t, you MUST define `CRDT_RECORD_ID_TYPE` for BOTH the implementation (.cpp) and test files:

```bash
# Step 1: Compile crdt_sqlite.cpp with uint128_t support
clang++ -std=c++20 -I. -DCRDT_RECORD_ID_TYPE=__uint128_t \
  -fsanitize=address,undefined -g -c crdt_sqlite.cpp -o crdt_sqlite_uint128.o

# Step 2: Compile and link the test
clang++ -std=c++20 -I. -DCRDT_RECORD_ID_TYPE=__uint128_t \
  -fsanitize=address,undefined -g \
  test_crdt_sqlite_uint128.cpp crdt_sqlite_uint128.o -lsqlite3 -o test_uint128_asan

# Step 3: Run
./test_uint128_asan
```

### Test the template-based CRDT (crdt.hpp)
```bash
clang++ -std=c++20 -I. -fsanitize=address,undefined -g \
  test_uint128.cpp -o test_uint128_crdt
./test_uint128_crdt
```

## Using the Makefile

We provide a Makefile for convenience:

```bash
# Build and run all tests
make test

# Build specific targets
make test_int64        # Build int64_t tests
make test_uint128      # Build uint128_t tests
make test_asan         # Build with sanitizers

# Clean build artifacts
make clean
```

## Sanitizer Options

### Address Sanitizer (ASan)
Detects memory errors: use-after-free, buffer overflows, leaks

```bash
clang++ -fsanitize=address -fno-omit-frame-pointer -g ...
```

### Undefined Behavior Sanitizer (UBSan)
Detects undefined behavior: integer overflow, null pointer dereference, etc.

```bash
clang++ -fsanitize=undefined -g ...
```

### Thread Sanitizer (TSan) - Not Recommended
CRDTSQLite is NOT thread-safe by design. Running TSan will report data races.
See documentation in `crdt_sqlite.hpp` for thread safety requirements.

```bash
# This WILL report races - expected behavior
clang++ -fsanitize=thread -g ...
```

## Common Build Issues

### 1. Undefined symbols for uint128_t

**Error:**
```
Undefined symbols for architecture arm64:
  "CRDTSQLite::merge_changes(std::vector<Change<unsigned __int128, ...>>)"
```

**Cause:** You compiled `crdt_sqlite.cpp` without `-DCRDT_RECORD_ID_TYPE=__uint128_t`

**Fix:** Compile crdt_sqlite.cpp with the correct define (see above)

### 2. Hash redefinition error

**Error:**
```
error: redefinition of 'hash<unsigned __int128>'
```

**Cause:** Your libc++ version (macOS 15+) already provides hash<__uint128_t>

**Fix:** Already handled in code via version check. Update to latest code.

### 3. SQLite not found

**Error:**
```
ld: library not found for -lsqlite3
```

**Fix (macOS):**
```bash
brew install sqlite3
# Or use system SQLite (usually works)
```

**Fix (Linux):**
```bash
sudo apt-get install libsqlite3-dev
```

## Platform Notes

### macOS (Apple Clang)
- Sanitizers work well with Apple Clang 17+
- uint128_t fully supported on arm64 and x86_64

### Linux (GCC/Clang)
- Use GCC 10+ or Clang 11+ for best C++20 support
- Sanitizers available on most recent distributions

### Windows (MSVC)
- uint128_t NOT supported (no `__uint128_t` type)
- Use int64_t only
- Sanitizers have limited support in MSVC

## Integration with CMake

```cmake
# Example CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(MyProject CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(SQLite3 REQUIRED)

# int64_t version (default)
add_executable(my_app_int64
  main.cpp
  crdt_sqlite.cpp
)
target_link_libraries(my_app_int64 SQLite::SQLite3)

# uint128_t version
add_executable(my_app_uint128
  main.cpp
  crdt_sqlite.cpp
)
target_compile_definitions(my_app_uint128 PRIVATE CRDT_RECORD_ID_TYPE=__uint128_t)
target_link_libraries(my_app_uint128 SQLite::SQLite3)

# Enable sanitizers for debug builds
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  target_compile_options(my_app_int64 PRIVATE -fsanitize=address,undefined)
  target_link_options(my_app_int64 PRIVATE -fsanitize=address,undefined)
endif()
```

## Verifying Build

All tests should pass:

```bash
# int64_t tests (16 tests)
./test_crdt_sqlite
# Expected: "All tests passed!"

# uint128_t tests (4 tests)
./test_uint128_asan
# Expected: "All uint128_t tests passed! ✅"

# Template CRDT test
./test_uint128_crdt
# Expected: "All uint128_t tests passed! ✅"
```

Total: 20 tests should pass.

## Performance Testing

For performance benchmarks, compile with optimizations:

```bash
clang++ -std=c++20 -O3 -DNDEBUG -I. \
  benchmark.cpp crdt_sqlite.cpp -lsqlite3 -o benchmark
./benchmark
```

Note: Sanitizers add significant overhead (~2-5x). Don't use for benchmarking.
