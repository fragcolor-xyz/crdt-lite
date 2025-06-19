cmake_minimum_required(VERSION 3.10)
project(crdt-lite)

add_library(crdt-lite INTERFACE)
target_include_directories(crdt-lite INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_compile_features(crdt-lite INTERFACE cxx_std_20)

add_executable(tests tests.cpp)
target_link_libraries(tests PRIVATE crdt-lite)