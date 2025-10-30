cmake_minimum_required(VERSION 3.15)
project(crdt-lite VERSION 0.4.1 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler warnings
if(MSVC)
  add_compile_options(/W4)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Options
option(BUILD_TESTS "Build tests" ON)
option(BUILD_SQLITE_WRAPPER "Build SQLite CRDT wrapper" ON)

# Header-only CRDT library
add_library(crdt-lite INTERFACE)
target_include_directories(crdt-lite INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}>
  $<INSTALL_INTERFACE:include>
)
target_compile_features(crdt-lite INTERFACE cxx_std_20)

# SQLite CRDT wrapper library
if(BUILD_SQLITE_WRAPPER)
  # Find SQLite3
  find_package(SQLite3)

  if(SQLite3_FOUND)
    add_library(crdt_sqlite STATIC
      crdt_sqlite.cpp
      crdt_sqlite.hpp
      crdt.hpp
    )

    target_include_directories(crdt_sqlite PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
      $<INSTALL_INTERFACE:include>
    )

    target_link_libraries(crdt_sqlite PUBLIC
      SQLite::SQLite3
    )

    message(STATUS "SQLite CRDT wrapper enabled")
  else()
    message(WARNING "SQLite3 not found, disabling SQLite wrapper")
    set(BUILD_SQLITE_WRAPPER OFF)
  endif()
endif()

# Tests
if(BUILD_TESTS)
  enable_testing()

  # Original CRDT tests
  add_executable(tests tests.cpp)
  target_link_libraries(tests PRIVATE crdt-lite)
  add_test(NAME crdt_tests COMMAND tests)

  # Memory tests
  add_executable(test_memory test_memory.cpp)
  target_link_libraries(test_memory PRIVATE crdt-lite)
  add_test(NAME memory_tests COMMAND test_memory)

  # Text CRDT tests (if exists)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test_text_crdt.cpp")
    add_executable(test_text_crdt test_text_crdt.cpp)
    target_link_libraries(test_text_crdt PRIVATE crdt-lite)
    add_test(NAME text_crdt_tests COMMAND test_text_crdt)
  endif()

  # SQLite CRDT tests
  if(BUILD_SQLITE_WRAPPER AND TARGET crdt_sqlite)
    add_executable(test_crdt_sqlite test_crdt_sqlite.cpp)
    target_link_libraries(test_crdt_sqlite PRIVATE crdt_sqlite)
    add_test(NAME crdt_sqlite_tests COMMAND test_crdt_sqlite)

    # SQLite CRDT uint128 tests (if __uint128_t is supported)
    include(CheckCXXSourceCompiles)
    check_cxx_source_compiles("
      int main() {
        __uint128_t x = 0;
        return 0;
      }
    " HAVE_UINT128)

    if(HAVE_UINT128)
      add_library(crdt_sqlite_uint128 STATIC crdt_sqlite.cpp crdt_sqlite.hpp crdt.hpp record_id_types.hpp)
      target_compile_definitions(crdt_sqlite_uint128 PRIVATE CRDT_RECORD_ID_TYPE=__uint128_t)
      target_link_libraries(crdt_sqlite_uint128 PUBLIC SQLite::SQLite3)

      add_executable(test_crdt_sqlite_uint128 test_crdt_sqlite_uint128.cpp)
      target_compile_definitions(test_crdt_sqlite_uint128 PRIVATE CRDT_RECORD_ID_TYPE=__uint128_t)
      target_link_libraries(test_crdt_sqlite_uint128 PRIVATE crdt_sqlite_uint128)
      add_test(NAME crdt_sqlite_uint128_tests COMMAND test_crdt_sqlite_uint128)
    endif()
  endif()
endif()

# Print configuration summary
message(STATUS "")
message(STATUS "Configuration Summary:")
message(STATUS "  CMake version: ${CMAKE_VERSION}")
message(STATUS "  C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  Build tests: ${BUILD_TESTS}")
message(STATUS "  Build SQLite wrapper: ${BUILD_SQLITE_WRAPPER}")
if(SQLite3_FOUND)
  message(STATUS "  SQLite3 version: ${SQLite3_VERSION}")
  message(STATUS "  SQLite3 libraries: ${SQLite3_LIBRARIES}")
endif()
message(STATUS "")