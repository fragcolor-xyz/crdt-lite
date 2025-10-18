set fallback := true

test:
  clang++ -std=c++20 -O2 -o crdt tests.cpp && ./crdt