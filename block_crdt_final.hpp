#pragma once

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <optional>
#include <array>
#include <bitset>
#include <algorithm>
#include <functional>
#include <filesystem>
#include <fstream>
#include <concepts>
#include <cstring>

namespace block_crdt {

// Final Version: Block-level CRDT for Sandbox Scale
// Templated block data type for NFTs, materials, or custom data
// Designed for 1 LAND = 96x96x128 blocks = 1.18M blocks total

using NodeId = uint64_t;
using ChunkSize = std::integral_constant<int, 16>; // 16x16x16 = 4K blocks per chunk

struct BlockCoord {
    int32_t x, y, z;
    
    constexpr BlockCoord(int32_t x = 0, int32_t y = 0, int32_t z = 0) : x(x), y(y), z(z) {}
    
    constexpr bool operator==(const BlockCoord& other) const {
        return x == other.x && y == other.y && z == other.z;
    }
    
    constexpr BlockCoord operator+(const BlockCoord& other) const {
        return {x + other.x, y + other.y, z + other.z};
    }
    
    constexpr BlockCoord operator-(const BlockCoord& other) const {
        return {x - other.x, y - other.y, z - other.z};
    }
};

struct ChunkCoord {
    int32_t x, y, z;
    
    constexpr ChunkCoord(int32_t x = 0, int32_t y = 0, int32_t z = 0) : x(x), y(y), z(z) {}
    
    constexpr bool operator==(const ChunkCoord& other) const {
        return x == other.x && y == other.y && z == other.z;
    }
    
    // Convert world block coordinate to chunk coordinate
    static constexpr ChunkCoord from_block(const BlockCoord& block) {
        return {
            block.x >> 4, // divide by 16
            block.y >> 4,
            block.z >> 4
        };
    }
    
    // Get all 26 neighboring chunk coordinates
    std::vector<ChunkCoord> neighbors() const {
        std::vector<ChunkCoord> result;
        result.reserve(26);
        for (int dx = -1; dx <= 1; ++dx) {
            for (int dy = -1; dy <= 1; ++dy) {
                for (int dz = -1; dz <= 1; ++dz) {
                    if (dx == 0 && dy == 0 && dz == 0) continue;
                    result.emplace_back(x + dx, y + dy, z + dz);
                }
            }
        }
        return result;
    }
};

struct BoundingBox {
    BlockCoord min, max;
    
    constexpr BoundingBox(BlockCoord min, BlockCoord max) : min(min), max(max) {}
    
    bool contains(const BlockCoord& coord) const {
        return coord.x >= min.x && coord.x <= max.x &&
               coord.y >= min.y && coord.y <= max.y &&
               coord.z >= min.z && coord.z <= max.z;
    }
    
    // Get all chunk coordinates that intersect this bounding box
    std::vector<ChunkCoord> intersecting_chunks() const {
        ChunkCoord min_chunk = ChunkCoord::from_block(min);
        ChunkCoord max_chunk = ChunkCoord::from_block(max);
        
        std::vector<ChunkCoord> chunks;
        for (int x = min_chunk.x; x <= max_chunk.x; ++x) {
            for (int y = min_chunk.y; y <= max_chunk.y; ++y) {
                for (int z = min_chunk.z; z <= max_chunk.z; ++z) {
                    chunks.emplace_back(x, y, z);
                }
            }
        }
        return chunks;
    }
    
    size_t volume() const {
        return static_cast<size_t>(max.x - min.x + 1) * 
               static_cast<size_t>(max.y - min.y + 1) * 
               static_cast<size_t>(max.z - min.z + 1);
    }
};

// Sandbox LAND helper
struct SandboxLAND {
    static constexpr BoundingBox bounds(BlockCoord origin = {0, 0, 0}) {
        return {origin, {origin.x + 95, origin.y + 95, origin.z + 127}};
    }
    
    static constexpr size_t block_count() { return 96 * 96 * 128; } // 1,179,648 blocks
    static constexpr size_t chunk_count() { return 6 * 6 * 8; }     // 288 chunks
};

// Hash functions
struct BlockCoordHash {
    std::size_t operator()(const BlockCoord& coord) const {
        return std::hash<uint64_t>{}(
            (static_cast<uint64_t>(coord.x) << 32) |
            (static_cast<uint64_t>(coord.y) << 16) |
            static_cast<uint64_t>(coord.z)
        );
    }
};

struct ChunkCoordHash {
    std::size_t operator()(const ChunkCoord& coord) const {
        return std::hash<uint64_t>{}(
            (static_cast<uint64_t>(coord.x) << 32) |
            (static_cast<uint64_t>(coord.y) << 16) |
            static_cast<uint64_t>(coord.z)
        );
    }
};

template<typename BlockData>
struct BlockChange {
    BlockCoord coord;
    std::optional<BlockData> value; // nullopt = deletion
    uint64_t version;
    NodeId node_id;
    
    BlockChange(BlockCoord coord, std::optional<BlockData> value, uint64_t version, NodeId node_id)
        : coord(coord), value(value), version(version), node_id(node_id) {}
};

// Compact version tracking per block within chunk
struct BlockVersion {
    uint32_t version : 24;  // 16M versions should be enough per block
    uint32_t node_id : 8;   // 256 concurrent nodes max
    
    BlockVersion(uint32_t v = 0, uint32_t n = 0) : version(v & 0xFFFFFF), node_id(n & 0xFF) {}
    
    bool should_accept(const BlockVersion& remote) const {
        return remote.version > version || 
               (remote.version == version && remote.node_id > node_id);
    }
};

// RLE compression for contiguous regions of same block type
template<typename BlockData>
struct RLESegment {
    BlockData value;
    uint16_t run_length;  // up to 65K consecutive blocks
    
    RLESegment(BlockData val, uint16_t len) : value(val), run_length(len) {}
};

// Forward declaration
template<typename BlockData>
class BlockChunk;

// Storage backend concept - can be SQLite, files, network, etc.
template<typename T, typename BlockData>
concept BlockStorage = requires(T storage, const ChunkCoord& coord, const BlockChunk<BlockData>& chunk) {
    { storage.save_chunk(coord, chunk) } -> std::same_as<bool>;
    { storage.load_chunk(coord) } -> std::same_as<std::optional<BlockChunk<BlockData>>>;
    { storage.delete_chunk(coord) } -> std::same_as<bool>;
    { storage.list_chunks() } -> std::same_as<std::vector<ChunkCoord>>;
    { storage.chunk_exists(coord) } -> std::same_as<bool>;
};

template<typename BlockData>
class BlockChunk {
    static constexpr size_t CHUNK_VOLUME = ChunkSize::value * ChunkSize::value * ChunkSize::value;
    
    std::array<BlockData, CHUNK_VOLUME> data_{};
    std::array<BlockVersion, CHUNK_VOLUME> versions_{};
    std::bitset<CHUNK_VOLUME> active_{}; // track which blocks are set
    
    uint64_t chunk_version_ = 0;
    bool compressed_ = false;
    std::vector<RLESegment<BlockData>> rle_data_; // compressed representation
    
    static constexpr size_t coord_to_index(int x, int y, int z) {
        return (z * ChunkSize::value + y) * ChunkSize::value + x;
    }
    
    static constexpr size_t local_coord_to_index(const BlockCoord& local) {
        return coord_to_index(local.x & 15, local.y & 15, local.z & 15);
    }
    
    void decompress() {
        if (!compressed_) return;
        
        size_t index = 0;
        for (const auto& segment : rle_data_) {
            for (uint16_t i = 0; i < segment.run_length && index < CHUNK_VOLUME; ++i, ++index) {
                if constexpr (std::is_arithmetic_v<BlockData>) {
                    if (segment.value != BlockData{}) {
                        data_[index] = segment.value;
                        active_[index] = true;
                    } else {
                        active_[index] = false;
                    }
                } else {
                    // For custom types, assume default construction means empty
                    data_[index] = segment.value;
                    active_[index] = true;
                }
            }
        }
        compressed_ = false;
        rle_data_.clear();
    }
    
    void compress() {
        if (compressed_) return;
        
        rle_data_.clear();
        if (active_.none()) {
            compressed_ = true;
            return; // Empty chunk compresses to nothing
        }
        
        BlockData current_value = active_[0] ? data_[0] : BlockData{};
        uint16_t run_length = 1;
        
        for (size_t i = 1; i < CHUNK_VOLUME; ++i) {
            BlockData value = active_[i] ? data_[i] : BlockData{};
            
            if (value == current_value && run_length < UINT16_MAX) {
                ++run_length;
            } else {
                rle_data_.emplace_back(current_value, run_length);
                current_value = value;
                run_length = 1;
            }
        }
        
        // Add final segment
        rle_data_.emplace_back(current_value, run_length);
        compressed_ = true;
    }

public:
    std::optional<BlockData> get_block(const BlockCoord& local_coord) const {
        const_cast<BlockChunk*>(this)->decompress(); // Lazy decompression
        size_t idx = local_coord_to_index(local_coord);
        return active_[idx] ? std::optional{data_[idx]} : std::nullopt;
    }
    
    bool set_block(const BlockCoord& local_coord, std::optional<BlockData> value, 
                   uint64_t version, NodeId node_id) {
        decompress(); // Ensure we're working with uncompressed data
        
        size_t idx = local_coord_to_index(local_coord);
        BlockVersion new_version(version, node_id);
        
        if (!active_[idx] || versions_[idx].should_accept(new_version)) {
            if (value.has_value()) {
                data_[idx] = *value;
                active_[idx] = true;
            } else {
                active_[idx] = false;
            }
            versions_[idx] = new_version;
            chunk_version_ = std::max(chunk_version_, version);
            return true;
        }
        return false;
    }
    
    // Bulk operation: fill a region with same block type
    size_t fill_region(const BlockCoord& start, const BlockCoord& end, const BlockData& value, 
                       uint64_t version, NodeId node_id) {
        decompress();
        
        size_t changes = 0;
        BlockVersion new_version(version, node_id);
        
        for (int x = std::max(0, start.x); x <= std::min(15, end.x); ++x) {
            for (int y = std::max(0, start.y); y <= std::min(15, end.y); ++y) {
                for (int z = std::max(0, start.z); z <= std::min(15, end.z); ++z) {
                    size_t idx = coord_to_index(x, y, z);
                    if (!active_[idx] || versions_[idx].should_accept(new_version)) {
                        data_[idx] = value;
                        active_[idx] = true;
                        versions_[idx] = new_version;
                        chunk_version_ = std::max(chunk_version_, version);
                        ++changes;
                    }
                }
            }
        }
        
        return changes;
    }
    
    // Get all blocks in a region (for spatial queries)
    std::vector<std::pair<BlockCoord, BlockData>> get_region(const BlockCoord& start, const BlockCoord& end) const {
        const_cast<BlockChunk*>(this)->decompress();
        
        std::vector<std::pair<BlockCoord, BlockData>> result;
        
        for (int x = std::max(0, start.x); x <= std::min(15, end.x); ++x) {
            for (int y = std::max(0, start.y); y <= std::min(15, end.y); ++y) {
                for (int z = std::max(0, start.z); z <= std::min(15, end.z); ++z) {
                    size_t idx = coord_to_index(x, y, z);
                    if (active_[idx]) {
                        result.emplace_back(BlockCoord{x, y, z}, data_[idx]);
                    }
                }
            }
        }
        
        return result;
    }
    
    uint64_t get_chunk_version() const { return chunk_version_; }
    
    std::vector<BlockChange<BlockData>> get_changes_since(const ChunkCoord& chunk_coord, uint64_t since_version) const {
        const_cast<BlockChunk*>(this)->decompress();
        
        std::vector<BlockChange<BlockData>> changes;
        
        for (int z = 0; z < ChunkSize::value; ++z) {
            for (int y = 0; y < ChunkSize::value; ++y) {
                for (int x = 0; x < ChunkSize::value; ++x) {
                    size_t idx = coord_to_index(x, y, z);
                    if (versions_[idx].version > since_version) {
                        BlockCoord world_coord{
                            chunk_coord.x * ChunkSize::value + x,
                            chunk_coord.y * ChunkSize::value + y,
                            chunk_coord.z * ChunkSize::value + z
                        };
                        
                        std::optional<BlockData> value = active_[idx] ? 
                            std::optional{data_[idx]} : std::nullopt;
                        
                        changes.emplace_back(world_coord, value, versions_[idx].version, versions_[idx].node_id);
                    }
                }
            }
        }
        
        return changes;
    }
    
    size_t active_block_count() const {
        return active_.count();
    }
    
    bool is_empty() const {
        return active_.none();
    }
    
    // Compression interface
    void force_compress() { compress(); }
    bool is_compressed() const { return compressed_; }
    size_t compressed_size() const { return rle_data_.size(); }
    
    // Serialization support
    std::vector<uint8_t> serialize() const;
    static std::optional<BlockChunk<BlockData>> deserialize(const std::vector<uint8_t>& data);
    
    template<typename StorageType>
    friend class FileStorage; // For direct access during serialization
};

// Simple in-memory storage for testing (no file dependencies)
template<typename BlockData>
class MemoryStorage {
    std::unordered_map<ChunkCoord, BlockChunk<BlockData>, ChunkCoordHash> chunks_;
    
public:
    MemoryStorage() = default;
    
    bool save_chunk(const ChunkCoord& coord, const BlockChunk<BlockData>& chunk) {
        chunks_[coord] = chunk;
        return true;
    }
    
    std::optional<BlockChunk<BlockData>> load_chunk(const ChunkCoord& coord) {
        auto it = chunks_.find(coord);
        return it != chunks_.end() ? std::optional{it->second} : std::nullopt;
    }
    
    bool delete_chunk(const ChunkCoord& coord) {
        return chunks_.erase(coord) > 0;
    }
    
    bool chunk_exists(const ChunkCoord& coord) {
        return chunks_.contains(coord);
    }
    
    std::vector<ChunkCoord> list_chunks() {
        std::vector<ChunkCoord> result;
        result.reserve(chunks_.size());
        for (const auto& [coord, _] : chunks_) {
            result.push_back(coord);
        }
        return result;
    }
};

// Simple file-based storage backend for testing
template<typename BlockData>
class FileStorage {
    std::filesystem::path base_path_;
    
    std::string chunk_filename(const ChunkCoord& coord) const {
        return base_path_ / ("chunk_" + std::to_string(coord.x) + "_" + 
                            std::to_string(coord.y) + "_" + std::to_string(coord.z) + ".blocks");
    }
    
public:
    explicit FileStorage(const std::filesystem::path& path) : base_path_(path) {
        std::filesystem::create_directories(base_path_);
    }
    
    bool save_chunk(const ChunkCoord& coord, const BlockChunk<BlockData>& chunk) {
        try {
            auto data = chunk.serialize();
            std::ofstream file(chunk_filename(coord), std::ios::binary);
            file.write(reinterpret_cast<const char*>(data.data()), data.size());
            return file.good();
        } catch (...) {
            return false;
        }
    }
    
    std::optional<BlockChunk<BlockData>> load_chunk(const ChunkCoord& coord) {
        try {
            std::ifstream file(chunk_filename(coord), std::ios::binary);
            if (!file.good()) return std::nullopt;
            
            file.seekg(0, std::ios::end);
            size_t size = file.tellg();
            file.seekg(0, std::ios::beg);
            
            std::vector<uint8_t> data(size);
            file.read(reinterpret_cast<char*>(data.data()), size);
            
            return BlockChunk<BlockData>::deserialize(data);
        } catch (...) {
            return std::nullopt;
        }
    }
    
    bool delete_chunk(const ChunkCoord& coord) {
        try {
            return std::filesystem::remove(chunk_filename(coord));
        } catch (...) {
            return false;
        }
    }
    
    bool chunk_exists(const ChunkCoord& coord) {
        return std::filesystem::exists(chunk_filename(coord));
    }
    
    std::vector<ChunkCoord> list_chunks() {
        std::vector<ChunkCoord> chunks;
        
        try {
            for (const auto& entry : std::filesystem::directory_iterator(base_path_)) {
                if (entry.is_regular_file() && entry.path().extension() == ".blocks") {
                    std::string filename = entry.path().stem().string();
                    if (filename.starts_with("chunk_")) {
                        // Parse "chunk_x_y_z"
                        auto parts = filename.substr(6); // Remove "chunk_"
                        size_t pos1 = parts.find('_');
                        size_t pos2 = parts.find('_', pos1 + 1);
                        
                        if (pos1 != std::string::npos && pos2 != std::string::npos) {
                            int x = std::stoi(parts.substr(0, pos1));
                            int y = std::stoi(parts.substr(pos1 + 1, pos2 - pos1 - 1));
                            int z = std::stoi(parts.substr(pos2 + 1));
                            chunks.emplace_back(x, y, z);
                        }
                    }
                }
            }
        } catch (...) {
            // Return partial results on error
        }
        
        return chunks;
    }
};

template<typename BlockData, BlockStorage<BlockData> StorageBackend = FileStorage<BlockData>>
class BlockCRDT {
    std::unordered_map<ChunkCoord, BlockChunk<BlockData>, ChunkCoordHash> chunks_;
    StorageBackend storage_;
    NodeId node_id_;
    uint64_t current_version_ = 0;
    
    BlockChunk<BlockData>& load_or_create_chunk(const ChunkCoord& coord) {
        auto it = chunks_.find(coord);
        if (it != chunks_.end()) {
            return it->second;
        }
        
        // Try loading from storage
        if (auto loaded = storage_.load_chunk(coord)) {
            auto [inserted_it, success] = chunks_.emplace(coord, std::move(*loaded));
            return inserted_it->second;
        }
        
        // Create new chunk
        auto [inserted_it, success] = chunks_.emplace(coord, BlockChunk<BlockData>{});
        return inserted_it->second;
    }
    
public:
    explicit BlockCRDT(NodeId node_id) requires std::is_default_constructible_v<StorageBackend> 
        : node_id_(node_id) {}
    
    explicit BlockCRDT(NodeId node_id, StorageBackend storage) 
        : storage_(std::move(storage)), node_id_(node_id) {}
    
    void set_block(const BlockCoord& coord, const BlockData& value) {
        ChunkCoord chunk_coord = ChunkCoord::from_block(coord);
        BlockChunk<BlockData>& chunk = load_or_create_chunk(chunk_coord);
        
        ++current_version_;
        if (chunk.set_block(coord, value, current_version_, node_id_)) {
            storage_.save_chunk(chunk_coord, chunk); // Auto-persist
        }
    }
    
    void remove_block(const BlockCoord& coord) {
        ChunkCoord chunk_coord = ChunkCoord::from_block(coord);
        BlockChunk<BlockData>& chunk = load_or_create_chunk(chunk_coord);
        
        ++current_version_;
        if (chunk.set_block(coord, std::nullopt, current_version_, node_id_)) {
            if (chunk.is_empty()) {
                chunks_.erase(chunk_coord);
                storage_.delete_chunk(chunk_coord);
            } else {
                storage_.save_chunk(chunk_coord, chunk);
            }
        }
    }
    
    std::optional<BlockData> get_block(const BlockCoord& coord) {
        ChunkCoord chunk_coord = ChunkCoord::from_block(coord);
        BlockChunk<BlockData>& chunk = load_or_create_chunk(chunk_coord);
        return chunk.get_block(coord);
    }
    
    // Bulk operations
    size_t fill_region(const BoundingBox& bbox, const BlockData& value) {
        size_t total_changes = 0;
        ++current_version_;
        
        for (const auto& chunk_coord : bbox.intersecting_chunks()) {
            BlockChunk<BlockData>& chunk = load_or_create_chunk(chunk_coord);
            
            // Convert world bounding box to chunk-local coordinates
            BlockCoord chunk_base{chunk_coord.x * 16, chunk_coord.y * 16, chunk_coord.z * 16};
            BlockCoord local_start = bbox.min - chunk_base;
            BlockCoord local_end = bbox.max - chunk_base;
            
            size_t changes = chunk.fill_region(local_start, local_end, value, current_version_, node_id_);
            if (changes > 0) {
                total_changes += changes;
                storage_.save_chunk(chunk_coord, chunk);
            }
        }
        
        return total_changes;
    }
    
    // Spatial queries
    std::vector<std::pair<BlockCoord, BlockData>> get_region(const BoundingBox& bbox) {
        std::vector<std::pair<BlockCoord, BlockData>> result;
        
        for (const auto& chunk_coord : bbox.intersecting_chunks()) {
            BlockChunk<BlockData>& chunk = load_or_create_chunk(chunk_coord);
            
            BlockCoord chunk_base{chunk_coord.x * 16, chunk_coord.y * 16, chunk_coord.z * 16};
            BlockCoord local_start = bbox.min - chunk_base;
            BlockCoord local_end = bbox.max - chunk_base;
            
            auto chunk_blocks = chunk.get_region(local_start, local_end);
            for (auto& [local_coord, value] : chunk_blocks) {
                result.emplace_back(local_coord + chunk_base, value);
            }
        }
        
        return result;
    }
    
    // Get 6-connected neighbors (for physics/collision)
    std::array<std::optional<BlockData>, 6> get_neighbors(const BlockCoord& coord) {
        static const std::array<BlockCoord, 6> offsets = {{
            {-1, 0, 0}, {1, 0, 0},  // left, right
            {0, -1, 0}, {0, 1, 0},  // down, up  
            {0, 0, -1}, {0, 0, 1}   // back, front
        }};
        
        std::array<std::optional<BlockData>, 6> neighbors;
        for (size_t i = 0; i < 6; ++i) {
            neighbors[i] = get_block(coord + offsets[i]);
        }
        return neighbors;
    }
    
    void merge_changes(const std::vector<BlockChange<BlockData>>& changes) {
        std::unordered_set<ChunkCoord, ChunkCoordHash> dirty_chunks;
        
        for (const auto& change : changes) {
            ChunkCoord chunk_coord = ChunkCoord::from_block(change.coord);
            BlockChunk<BlockData>& chunk = load_or_create_chunk(chunk_coord);
            
            if (chunk.set_block(change.coord, change.value, change.version, change.node_id)) {
                current_version_ = std::max(current_version_, change.version);
                dirty_chunks.insert(chunk_coord);
            }
        }
        
        // Persist all modified chunks
        for (const auto& coord : dirty_chunks) {
            auto it = chunks_.find(coord);
            if (it != chunks_.end()) {
                if (it->second.is_empty()) {
                    chunks_.erase(it);
                    storage_.delete_chunk(coord);
                } else {
                    storage_.save_chunk(coord, it->second);
                }
            }
        }
    }
    
    std::vector<BlockChange<BlockData>> get_changes_since(uint64_t since_version) {
        std::vector<BlockChange<BlockData>> all_changes;
        
        // Check in-memory chunks first
        for (const auto& [chunk_coord, chunk] : chunks_) {
            if (chunk.get_chunk_version() > since_version) {
                auto chunk_changes = chunk.get_changes_since(chunk_coord, since_version);
                all_changes.insert(all_changes.end(), chunk_changes.begin(), chunk_changes.end());
            }
        }
        
        return all_changes;
    }
    
    // Force compression on all loaded chunks
    void compress_all() {
        for (auto& [coord, chunk] : chunks_) {
            chunk.force_compress();
            storage_.save_chunk(coord, chunk);
        }
    }
    
    uint64_t get_version() const { return current_version_; }
    size_t chunk_count() const { return chunks_.size(); }
    size_t total_block_count() const {
        size_t total = 0;
        for (const auto& [_, chunk] : chunks_) {
            total += chunk.active_block_count();
        }
        return total;
    }
    
    StorageBackend& get_storage() { return storage_; }
    
    // Sandbox-specific helpers
    void fill_land(const BlockCoord& origin, const BlockData& value) {
        fill_region(SandboxLAND::bounds(origin), value);
    }
    
    std::vector<std::pair<BlockCoord, BlockData>> get_land(const BlockCoord& origin) {
        return get_region(SandboxLAND::bounds(origin));
    }
};

// Common block data types for different use cases
template<typename StorageBackend = MemoryStorage<uint32_t>>
using SimpleBlockCRDT = BlockCRDT<uint32_t, StorageBackend>;              // Simple material IDs

template<typename StorageBackend = MemoryStorage<uint64_t>>
using NFTBlockCRDT = BlockCRDT<uint64_t, StorageBackend>;                 // NFT references

template<typename StorageBackend = MemoryStorage<std::pair<uint64_t, uint32_t>>>
using RichBlockCRDT = BlockCRDT<std::pair<uint64_t, uint32_t>, StorageBackend>; // NFT ID + metadata

// Serialization implementation for BlockChunk
template<typename BlockData>
std::vector<uint8_t> BlockChunk<BlockData>::serialize() const {
    const_cast<BlockChunk*>(this)->compress(); // Compress before serializing
    
    std::vector<uint8_t> result;
    result.reserve(1024); // Reasonable initial size
    
    // Header: version + flags + compressed size
    uint64_t header = chunk_version_;
    result.insert(result.end(), reinterpret_cast<const uint8_t*>(&header), 
                  reinterpret_cast<const uint8_t*>(&header) + sizeof(header));
    
    uint32_t segment_count = static_cast<uint32_t>(rle_data_.size());
    result.insert(result.end(), reinterpret_cast<const uint8_t*>(&segment_count),
                  reinterpret_cast<const uint8_t*>(&segment_count) + sizeof(segment_count));
    
    // RLE data
    for (const auto& segment : rle_data_) {
        result.insert(result.end(), reinterpret_cast<const uint8_t*>(&segment.value),
                      reinterpret_cast<const uint8_t*>(&segment.value) + sizeof(segment.value));
        result.insert(result.end(), reinterpret_cast<const uint8_t*>(&segment.run_length),
                      reinterpret_cast<const uint8_t*>(&segment.run_length) + sizeof(segment.run_length));
    }
    
    return result;
}

template<typename BlockData>
std::optional<BlockChunk<BlockData>> BlockChunk<BlockData>::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < sizeof(uint64_t) + sizeof(uint32_t)) {
        return std::nullopt;
    }
    
    BlockChunk chunk;
    size_t offset = 0;
    
    // Read header
    std::memcpy(&chunk.chunk_version_, data.data() + offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);
    
    uint32_t segment_count;
    std::memcpy(&segment_count, data.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    // Read RLE segments
    chunk.rle_data_.reserve(segment_count);
    for (uint32_t i = 0; i < segment_count; ++i) {
        if (offset + sizeof(BlockData) + sizeof(uint16_t) > data.size()) {
            return std::nullopt;
        }
        
        BlockData value;
        uint16_t run_length;
        
        std::memcpy(&value, data.data() + offset, sizeof(BlockData));
        offset += sizeof(BlockData);
        
        std::memcpy(&run_length, data.data() + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        
        chunk.rle_data_.emplace_back(value, run_length);
    }
    
    chunk.compressed_ = true;
    return chunk;
}

} // namespace block_crdt