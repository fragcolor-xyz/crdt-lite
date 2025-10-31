# Makefile for CRDT-SQLite
# Supports both int64_t and uint128_t record ID types

CXX := clang++
CXXFLAGS := -std=c++20 -I.
LDFLAGS := -lsqlite3
SANITIZE_FLAGS := -fsanitize=address,undefined -fno-omit-frame-pointer -g

# Default target
.PHONY: all
all: test

# Build all test executables
.PHONY: build-all
build-all: test_int64 test_uint128 test_uint128_crdt

# int64_t tests (default)
test_int64: test_crdt_sqlite.cpp crdt_sqlite.cpp crdt_sqlite.hpp record_id_types.hpp
	$(CXX) $(CXXFLAGS) test_crdt_sqlite.cpp crdt_sqlite.cpp $(LDFLAGS) -o $@

# int64_t tests with sanitizers
test_int64_asan: test_crdt_sqlite.cpp crdt_sqlite.cpp crdt_sqlite.hpp record_id_types.hpp
	$(CXX) $(CXXFLAGS) $(SANITIZE_FLAGS) test_crdt_sqlite.cpp crdt_sqlite.cpp $(LDFLAGS) -o $@

# uint128_t tests - requires separate compilation
crdt_sqlite_uint128.o: crdt_sqlite.cpp crdt_sqlite.hpp record_id_types.hpp
	$(CXX) $(CXXFLAGS) -DCRDT_RECORD_ID_TYPE=__uint128_t -c crdt_sqlite.cpp -o $@

crdt_sqlite_uint128_asan.o: crdt_sqlite.cpp crdt_sqlite.hpp record_id_types.hpp
	$(CXX) $(CXXFLAGS) $(SANITIZE_FLAGS) -DCRDT_RECORD_ID_TYPE=__uint128_t -c crdt_sqlite.cpp -o $@

test_uint128: test_crdt_sqlite_uint128.cpp crdt_sqlite_uint128.o crdt_sqlite.hpp record_id_types.hpp
	$(CXX) $(CXXFLAGS) -DCRDT_RECORD_ID_TYPE=__uint128_t \
		test_crdt_sqlite_uint128.cpp crdt_sqlite_uint128.o $(LDFLAGS) -o $@

test_uint128_asan: test_crdt_sqlite_uint128.cpp crdt_sqlite_uint128_asan.o crdt_sqlite.hpp record_id_types.hpp
	$(CXX) $(CXXFLAGS) $(SANITIZE_FLAGS) -DCRDT_RECORD_ID_TYPE=__uint128_t \
		test_crdt_sqlite_uint128.cpp crdt_sqlite_uint128_asan.o $(LDFLAGS) -o $@

# Template-based CRDT test (header-only, works with uint128_t)
test_uint128_crdt: test_uint128.cpp crdt.hpp record_id_types.hpp
	$(CXX) $(CXXFLAGS) test_uint128.cpp -o $@

test_uint128_crdt_asan: test_uint128.cpp crdt.hpp record_id_types.hpp
	$(CXX) $(CXXFLAGS) $(SANITIZE_FLAGS) test_uint128.cpp -o $@

# Run all tests
.PHONY: test
test: test-int64 test-uint128 test-uint128-crdt
	@echo ""
	@echo "=========================================="
	@echo "✅ ALL TESTS PASSED (20/20)"
	@echo "=========================================="

.PHONY: test-int64
test-int64: test_int64_asan
	@echo ""
	@echo "=========================================="
	@echo "Running int64_t tests with sanitizers..."
	@echo "=========================================="
	./test_int64_asan

.PHONY: test-uint128
test-uint128: test_uint128_asan
	@echo ""
	@echo "=========================================="
	@echo "Running uint128_t tests with sanitizers..."
	@echo "=========================================="
	./test_uint128_asan

.PHONY: test-uint128-crdt
test-uint128-crdt: test_uint128_crdt_asan
	@echo ""
	@echo "=========================================="
	@echo "Running template CRDT uint128_t tests..."
	@echo "=========================================="
	./test_uint128_crdt_asan

# Run tests without sanitizers (faster)
.PHONY: test-fast
test-fast: test_int64 test_uint128 test_uint128_crdt
	@echo ""
	@echo "Running int64_t tests..."
	./test_int64
	@echo ""
	@echo "Running uint128_t tests..."
	./test_uint128
	@echo ""
	@echo "Running template CRDT uint128_t tests..."
	./test_uint128_crdt
	@echo ""
	@echo "✅ ALL FAST TESTS PASSED"

# Clean build artifacts
.PHONY: clean
clean:
	rm -f test_int64 test_int64_asan
	rm -f test_uint128 test_uint128_asan
	rm -f test_uint128_crdt test_uint128_crdt_asan
	rm -f crdt_sqlite_uint128.o crdt_sqlite_uint128_asan.o
	rm -f *.db *.db-shm *.db-wal
	rm -f test_crdt_sqlite test_crdt_sqlite_asan test_crdt_sqlite_clang
	@echo "Cleaned build artifacts"

# Help
.PHONY: help
help:
	@echo "CRDT-SQLite Build Targets:"
	@echo ""
	@echo "  make              - Build and run all tests with sanitizers"
	@echo "  make test         - Build and run all tests with sanitizers"
	@echo "  make test-fast    - Build and run all tests without sanitizers"
	@echo "  make build-all    - Build all test executables"
	@echo ""
	@echo "Individual test targets:"
	@echo "  make test_int64           - Build int64_t tests"
	@echo "  make test_int64_asan      - Build int64_t tests with sanitizers"
	@echo "  make test_uint128         - Build uint128_t tests"
	@echo "  make test_uint128_asan    - Build uint128_t tests with sanitizers"
	@echo "  make test_uint128_crdt    - Build template CRDT tests"
	@echo ""
	@echo "  make clean        - Remove all build artifacts"
	@echo "  make help         - Show this help message"
