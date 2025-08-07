#include "crdt.hpp"
#include <iostream>

int main() {
    std::cout << "ColumnVersion size: " << sizeof(ColumnVersion) << " bytes" << std::endl;
    std::cout << "TombstoneInfo size: " << sizeof(TombstoneInfo) << " bytes" << std::endl;
    std::cout << "Memory savings per tombstone: " << (sizeof(ColumnVersion) - sizeof(TombstoneInfo)) << " bytes" << std::endl;
    std::cout << "Percentage reduction: " << (100.0 * (sizeof(ColumnVersion) - sizeof(TombstoneInfo)) / sizeof(ColumnVersion)) << "%" << std::endl;
    return 0;
}